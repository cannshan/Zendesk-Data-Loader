import requests
import time
import logging
import csv
import threading
import os
import re
import queue
import random
from requests.auth import HTTPBasicAuth
from zendesk_fields import fetch_zendesk_fields
import orjson
import mimetypes
import urllib.parse
from threading import Lock
import json
from progress_manager import ProgressManager
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from requests.exceptions import RequestException, HTTPError, ConnectionError, Timeout, ReadTimeout
from http.client import IncompleteRead
from helpers import sanitize_csv_value, map_fields, extract_ids, process_api_response, check_job_status, sanitize_csv_value, fetch_light_agent_role_id
from globals import rate_limit_event, rate_limit_resume_time, rate_limit_lock
from zendesk_fields import fetch_zendesk_fields, fetch_custom_objects



# Setup Logging
logging.basicConfig(filename="app.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)  # Define the logger instance

# Global variable for total records (used in export)
total_records = None



# Global variable for total records (used in export)
total_records = None

# Global lists for storing results
all_success_records = []
all_error_records = []

submission_futures = {}
status_futures = {}
submit_executor = None
status_executor = None

is_exporting = False

# Thread-safe set for tracking processed job IDs (global or function-scoped)
processed_job_ids = set()
processed_job_ids_lock = threading.Lock()

# Thread-safe set for tracking queued job IDs (global or function-scoped)
queued_job_ids = set()
queued_job_ids_lock = threading.Lock()


stop_event = threading.Event()





def make_request_with_retries(method, url, auth, json=None, max_retries=5, base_delay=5, stop_event=None):
    """
    Makes an HTTP request with retries for transient errors (e.g., 429, 503).
    Raises immediately on 400 Bad Request or 413 Payload Too Large without retrying.
    """
    global rate_limit_resume_time
    retry_delay = base_delay
    headers = {"Content-Type": "application/json"}

    for attempt in range(max_retries):
        # Check stop event
        if stop_event and stop_event.is_set():
            logger.info(f"🛑 Stop event detected. Aborting request to {url}")
            raise Exception("Operation stopped by user")

        # Handle rate limiting
        with rate_limit_lock:
            if not rate_limit_event.is_set():
                current_time = time.time()
                if current_time < rate_limit_resume_time:
                    pause_duration = rate_limit_resume_time - current_time
                    logger.info(f"⏸️ Pausing for {pause_duration:.1f} seconds due to prior rate limit.")
                    time.sleep(pause_duration)
                jitter = random.uniform(0, 0.3)
                logger.info(f"⏳ Adding {jitter:.1f}s jitter before resuming.")
                time.sleep(jitter)
                rate_limit_event.set()

        try:
            logger.info(f"Sending {method} request to {url}")
            if method == "GET":
                time.sleep(random.uniform(0, 0.8))
                response = requests.get(url, auth=auth, headers=headers)
            elif method == "POST":
                time.sleep(random.uniform(0, 0.8))
                response = requests.post(url, json=json, auth=auth, headers=headers)
            elif method == "PUT":
                time.sleep(random.uniform(0, 0.8))
                response = requests.put(url, json=json, auth=auth, headers=headers)
            elif method == "DELETE":
                time.sleep(random.uniform(0, 0.8))
                response = requests.delete(url, auth=auth, headers=headers)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            # Handle specific status codes
            if response.status_code == 400:  # Bad Request - no retry
                error_message = (
                    "Bad request submitted. Please check the following:\n"
                    "- Date formats (e.g., use YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ for standard fields).\n"
                    "- Role values (e.g., 'end-user', 'agent', 'admin' for Users; or valid custom_role_id for Agents).\n"
                    "- Drop-down or multi-select formats (ensure values match Zendesk options exactly, case-sensitive).\n"
                    "- Required fields are present and correctly mapped."
                )
                logger.warning(f"⚠️ Bad Request (400) for {url}. Details: {response.text}")
                raise requests.exceptions.RequestException(error_message, response=response)
            elif response.status_code == 413:  # Payload Too Large - no retry
                logger.error(f"⚠️ Payload Too Large (413) for {url}. Aborting retries.")
                raise requests.exceptions.RequestException("Payload Too Large", response=response)
            elif response.status_code == 429:  # Rate limit
                retry_after = int(response.headers.get("Retry-After", retry_delay))
                logger.warning(f"⚠️ Rate limit (429) hit. Retry-After: {retry_after} seconds (attempt {attempt + 1}/{max_retries})")
                with rate_limit_lock:
                    current_time = time.time()
                    new_resume_time = current_time + retry_after
                    if new_resume_time > rate_limit_resume_time:
                        rate_limit_resume_time = new_resume_time
                        rate_limit_event.clear()
                pause_time = retry_after + random.uniform(2, 5)
                logger.info(f"⏳ Pausing for {pause_time:.1f} seconds (Retry-After: {retry_after}s + jitter).")
                time.sleep(pause_time)
                retry_delay *= 2  # Exponential backoff
                continue
            elif response.status_code == 422:  # Unprocessable Entity - no retry
                logger.warning(f"⚠️ Validation error (422) for {url}. Aborting retries. Details: {response.text}")
                return response  # Return immediately with the 422 response

            response.raise_for_status()  # Raise for other errors (e.g., 500)
            return response

        except requests.exceptions.RequestException as e:
            if (e.response and e.response.status_code in [400, 413]) or str(e) == "Payload Too Large":
                # Raise immediately on 400 or 413 without retrying
                logger.error(f"Request failed for {url}: {e}")
                raise
            elif attempt < max_retries - 1:
                logger.warning(f"Request failed: {e}. Retrying in {retry_delay} seconds (attempt {attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
                continue
            else:
                logger.error(f"Max retries reached for {method} {url}: {e}")
                raise  # Re-raise after max retries

    logger.error(f"All retry attempts failed for {method} {url}")
    return None  # Fallback (shouldn’t reach here due to raise)






def remove_default_group_membership(subdomain, email, token, user_id, default_group_id, mapped_group_ids, stop_event):
    """Remove the instance default group membership if not mapped."""
    base_url = f"https://{subdomain}.zendesk.com"
    auth = HTTPBasicAuth(f"{email}/token", token)
    memberships_url = f"{base_url}/api/v2/users/{user_id}/group_memberships.json"

    if not mapped_group_ids:
        logger.info(f"No groups mapped for user {user_id}. Keeping default group.")
        return

    if default_group_id in mapped_group_ids:
        logger.info(f"Default group {default_group_id} was mapped for user {user_id}. No removal needed.")
        return

    try:
        response = make_request_with_retries("GET", memberships_url, auth, stop_event=stop_event)
        memberships = response.json().get("group_memberships", [])
        for membership in memberships:
            if membership["group_id"] == default_group_id:
                membership_id = membership["id"]
                delete_url = f"{base_url}/api/v2/group_memberships/{membership_id}.json"
                logger.info(f"Removing default group membership {membership_id} for user {user_id}")
                make_request_with_retries("DELETE", delete_url, auth, stop_event=stop_event)
                logger.info(f"Successfully removed default group {default_group_id} for user {user_id}")
                return
        logger.info(f"No membership found for default group {default_group_id} for user {user_id}")
    except Exception as e:
        logger.error(f"Failed to remove default group for user {user_id}: {e}")





def fetch_total_records(subdomain, email, token, selected_object):
    """Fetch and set total_records from API response using appropriate endpoints."""
    global total_records
    base_url = f"https://{subdomain}.zendesk.com"
    auth = (f"{email}/token", token)
    headers = {"Content-Type": "application/json"}

    builtin_objects = ["User", "Users", "Agent", "Organization", "Organizations", "Ticket", "Tickets", "Attachments"]
    is_custom_object = selected_object not in builtin_objects

    if is_custom_object:
        count_url = f"{base_url}/api/v2/custom_objects/{selected_object}/records/count"
        print(f"🔍 Fetching total {selected_object} count from: {count_url}")
        logging.info(f"Fetching total {selected_object} count from: {count_url}")
        try:
            response = requests.get(count_url, auth=auth, headers=headers)
            response.raise_for_status()
            json_response = orjson.loads(response.content)
            total_records = json_response.get("count", {}).get("value", 0)
            print(f"✅ Total {selected_object}: {total_records}")
            logging.info(f"Total {selected_object}: {total_records}")
        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Error fetching total {selected_object} records: {e}")
            total_records = 0
    else:
        object_map = {
            "User": "Users",
            "Users": "Users",
            "Agent": "Agent",
            "Organization": "Organizations",
            "Organizations": "Organizations",
            "Ticket": "Tickets",
            "Tickets": "Tickets",
            "Attachments": "Tickets"  # Estimate based on tickets
        }
        selected_object = object_map.get(selected_object, selected_object)
        query_map = {
            "Tickets": "type:ticket",
            "Users": "type:user role:end-user",
            "Agent": "type:user role:agent role:admin",
            "Organizations": "type:organization"
        }
        query = query_map.get(selected_object)
        if not query:
            logging.error(f"Invalid selected object: {selected_object}")
            return

        count_url = f"{base_url}/api/v2/search/count.json?query={query}"
        print(f"🔍 Fetching total {selected_object} count from: {count_url}")
        logging.info(f"Fetching total {selected_object} count from: {count_url}")
        try:
            response = requests.get(count_url, auth=auth, headers=headers)
            response.raise_for_status()
            json_response = orjson.loads(response.content)
            total_records = json_response.get("count", 0)
            print(f"✅ Total {selected_object}: {total_records}")
            logging.info(f"Total {selected_object}: {total_records}")
        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Error fetching total {selected_object} records: {e}")
            total_records = 0


def get_total_records():
    """Returns the total number of records fetched from Zendesk API."""
    global total_records
    return total_records if total_records is not None else 0






def execute_bulk_operation(
    subdomain, email, token, selected_object, csv_data, csv_headers, field_mapping,
    action_type, save_directory=None, stop_event=None, progress_bar=None,
    comments_csv_data=None, comments_csv_headers=None, ticket_id_field=None,
    comment_ticket_id_field=None, comment_field_mapping=None, app_instance=None,
    base_file_dir=None, attachments_csv_data=None, attachments_csv_headers=None,
    attachment_comment_id_field=None, attachment_field_mapping=None, chunk_size=100,
    comment_row_numbers=None, html_body_column=None,
    success_filename=None, error_filename=None,
    attachment_success_filename=None, attachment_error_filename=None,
    comment_success_filename=None, comment_error_filename=None,
    processed_records_so_far=0
):
    valid_actions = ["export", "import", "update", "delete"]
    if action_type not in valid_actions:
        logging.error(f"❌ Invalid action type: {action_type}")
        return 0, 0, 0, 0

    logging.info(f"🚀 Starting {action_type} operation for {selected_object}")
    print(f"🚀 Starting {action_type} operation for {selected_object}")

    success_count = 0
    error_count = 0
    exported_count = 0
    total_processed = 0

    try:
        builtin_objects = ["User", "Users", "Agent", "Organization", "Organizations", "Ticket", "Tickets", "Attachments", "Trigger", "Triggers", "Macros"]
        is_custom_object = selected_object not in builtin_objects and selected_object not in ["Ticket Comments", "Tickets + Comments", "Tickets + Comments + Attachments"]

        if action_type == "export":
            if selected_object == "Comments":
                fetch_total_records(subdomain, email, token, "Tickets")
                total_records = max(get_total_records() * 10, 1)
            elif selected_object == "Agent":
                fetch_total_records(subdomain, email, token, "Agent")
                total_records = get_total_records() or 1
            elif selected_object == "Attachments":
                fetch_total_records(subdomain, email, token, "Tickets")
                total_records = max(get_total_records(), 1)
            elif selected_object in ["Trigger", "Triggers", "Macros"]:
                total_records = len(csv_data) if csv_data else 1
            elif is_custom_object:
                fetch_total_records(subdomain, email, token, selected_object)
                total_records = get_total_records() or 1
            else:
                object_map = {
                    "User": "Users", "Users": "Users",
                    "Organization": "Organizations", "Organizations": "Organizations",
                    "Ticket": "Tickets", "Tickets": "Tickets"
                }
                normalized_object = object_map.get(selected_object, selected_object)
                fetch_total_records(subdomain, email, token, normalized_object)
                total_records = get_total_records() or 1
        else:
            total_records = len(csv_data) if csv_data else 1

        logging.info(f"Total records for {action_type}: {total_records}")

        if action_type == "import":
            if selected_object == "Tickets + Comments + Attachments":
                success_count, error_count, skipped_count, total_processed = import_tickets_comments_attachments(
                    subdomain, email, token, csv_data, csv_headers,
                    comments_csv_data, comments_csv_headers,
                    attachments_csv_data, attachments_csv_headers,
                    field_mapping, save_directory, stop_event, progress_bar,
                    ticket_id_field, comment_ticket_id_field, attachment_comment_id_field,
                    app_instance, comment_field_mapping, base_file_dir, attachment_field_mapping,
                    chunk_size=chunk_size,
                    comment_row_numbers=comment_row_numbers,
                    html_body_column=html_body_column,
                    success_filename=success_filename,
                    error_filename=error_filename,
                    attachment_success_filename=attachment_success_filename,
                    attachment_error_filename=attachment_error_filename,
                    comment_success_filename=comment_success_filename,
                    comment_error_filename=comment_error_filename,
                    processed_records_so_far=processed_records_so_far
                )
            elif selected_object == "Tickets + Comments":
                success_count, error_count, skipped_count, total_processed = import_tickets_and_comments(
                    subdomain, email, token, csv_data, csv_headers,
                    comments_csv_data, comments_csv_headers, field_mapping,
                    save_directory, stop_event, progress_bar,
                    ticket_id_field, comment_ticket_id_field, app_instance,
                    comment_field_mapping, chunk_size=chunk_size,
                    success_filename=success_filename,
                    error_filename=error_filename,
                    processed_records_so_far=processed_records_so_far
                )
            elif selected_object == "Attachments":
                success_count, error_count, skipped_count, total_processed = import_attachments(
                    subdomain, email, token, csv_data, csv_headers, field_mapping,
                    save_directory, stop_event, app_instance, base_file_dir=base_file_dir,
                    chunk_size=chunk_size,
                    success_filename=success_filename,
                    error_filename=error_filename,
                    processed_records_so_far=processed_records_so_far
                )
            elif selected_object in ["Trigger", "Triggers"]:
                success_count, error_count, skipped_count, total_processed = import_triggers(
                    subdomain, email, token, csv_data, csv_headers, field_mapping,
                    save_directory, stop_event, app_instance, chunk_size=1,
                    success_filename=success_filename,
                    error_filename=error_filename,
                    processed_records_so_far=processed_records_so_far
                )
            elif selected_object == "Macros":
                success_count, error_count, skipped_count, total_processed = import_macros(
                    subdomain, email, token, csv_data, csv_headers, field_mapping,
                    save_directory, stop_event, app_instance, chunk_size=chunk_size,
                    success_filename=success_filename,
                    error_filename=error_filename,
                    processed_records_so_far=processed_records_so_far
                )
            elif is_custom_object:
                success_count, error_count, _, total_processed = import_custom_objects(
                    subdomain, email, token, selected_object, csv_data, csv_headers,
                    field_mapping, save_directory, stop_event, progress_bar, app_instance,
                    chunk_size=chunk_size,
                    success_filename=success_filename,
                    error_filename=error_filename,
                    processed_records_so_far=processed_records_so_far
                )
            else:
                success_count, error_count, skipped_count, total_processed = import_records(
                    subdomain, email, token, selected_object, csv_data, csv_headers,
                    field_mapping, save_directory, stop_event, progress_bar,
                    action_type="import", app_instance=app_instance, chunk_size=chunk_size,
                    success_filename=success_filename,
                    error_filename=error_filename,
                    processed_records_so_far=processed_records_so_far
                )
        elif action_type == "update":
            if is_custom_object:
                success_count, error_count, skipped_count, total_processed = update_custom_objects(
                    subdomain, email, token, selected_object, csv_data, csv_headers,
                    field_mapping, save_directory, stop_event, progress_bar, app_instance,
                    chunk_size=chunk_size,
                    success_filename=success_filename,
                    error_filename=error_filename,
                    processed_records_so_far=processed_records_so_far
                )
            else:
                success_count, error_count, skipped_count, total_processed = update_records(
                    subdomain, email, token, selected_object, csv_data, csv_headers,
                    field_mapping, save_directory, stop_event, progress_bar,
                    action_type="update", app_instance=app_instance, chunk_size=chunk_size,
                    success_filename=success_filename,
                    error_filename=error_filename,
                    processed_records_so_far=processed_records_so_far
                )
        elif action_type == "delete":
            if is_custom_object:
                success_count, error_count, skipped_count, total_processed = delete_custom_objects(
                    subdomain, email, token, selected_object, csv_data, csv_headers,
                    field_mapping, save_directory, stop_event, progress_bar, app_instance,
                    chunk_size=chunk_size,
                    success_filename=success_filename,
                    error_filename=error_filename,
                    processed_records_so_far=processed_records_so_far
                )
            else:
                success_count, error_count, skipped_count, total_processed = delete_records(
                    subdomain, email, token, selected_object, csv_data, csv_headers,
                    field_mapping, save_directory, stop_event, progress_bar,
                    action_type="delete", app_instance=app_instance, chunk_size=chunk_size,
                    success_filename=success_filename,
                    error_filename=error_filename,
                    processed_records_so_far=processed_records_so_far
                )
        elif action_type == "export":
            if selected_object == "Comments":
                exported_count = export_ticket_comments(
                    subdomain, email, token, save_directory, stop_event, app_instance
                )
                total_processed = exported_count
            elif selected_object == "Agent":
                exported_count = export_agents(
                    subdomain, email, token, save_directory, stop_event, app_instance
                )
                total_processed = exported_count
            elif selected_object == "Attachments":
                exported_count = export_attachments(
                    subdomain, email, token, save_directory, stop_event, app_instance,
                    base_file_dir=app_instance.attachment_directory if app_instance else save_directory
                )
                total_processed = exported_count
            elif selected_object in ["Trigger", "Triggers"]:
                success_count, error_count, skipped_count, total_processed = export_triggers(
                    subdomain, email, token, field_mapping, csv_headers,
                    save_directory, stop_event, app_instance, success_filename
                )
            elif selected_object == "Macros":
                success_count, error_count, skipped_count, total_processed = export_macros(
                    subdomain, email, token, field_mapping, csv_headers,
                    save_directory, stop_event, app_instance, success_filename
                )
            elif is_custom_object:
                exported_count = export_custom_objects(
                    subdomain, email, token, selected_object, save_directory, stop_event, app_instance
                )
                total_processed = exported_count
            else:
                exported_count = export_records(
                    subdomain, email, token, selected_object, csv_headers,
                    save_directory, stop_event, progress_bar,
                    app_instance=app_instance, total_records=total_records
                )
                total_processed = exported_count

        logging.info(f"✅ {action_type.capitalize()} operation for {selected_object} completed successfully!")
        print(f"✅ {action_type.capitalize()} operation for {selected_object} completed successfully!")

        if app_instance and hasattr(app_instance, 'progress_manager') and app_instance.progress_manager is not None and not stop_event.is_set():
            logger.info("Marking ProgressManager as completed")
            app_instance.progress_manager.mark_completed()

    except Exception as e:
        error_message = str(e)
        stop_operation = False
        if isinstance(e, requests.exceptions.RequestException) and e.response is not None:
            if e.response.status_code == 400:
                error_message = (
                    "Bad request submitted. Please check the following:\n"
                    "- Date formats (e.g., YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ for standard fields).\n"
                    "- Role values (e.g., 'end-user', 'agent', 'admin' for Users; or valid custom_role_id for Agents).\n"
                    "- Drop-down or multi-select formats (ensure values match Zendesk options exactly, case-sensitive).\n"
                    "- Required fields are present and correctly mapped."
                )
                stop_operation = True
            else:
                error_message = f"Error during {action_type} operation: {e.response.status_code} - {e.response.text}"
        
        logging.error(f"❌ Error during {action_type} operation for {selected_object}: {error_message}")
        print(f"❌ Error during {action_type} operation for {selected_object}: {error_message}")
        
        if app_instance and stop_operation:
            app_instance.stop_event.set()
        
        raise Exception(error_message)

    finally:
        if app_instance and hasattr(app_instance, 'progress_manager') and app_instance.progress_manager is not None:
            logger.info("ProgressManager present; cleanup deferred to execute_import")
        else:
            logger.debug("No ProgressManager to manage in execute_bulk_operation")

    if action_type == "export":
        return 0, 0, exported_count, total_processed
    else:
        return success_count, error_count, 0, total_processed






def export_triggers(subdomain, email, token, field_mapping, csv_headers, save_directory, stop_event, app_instance, success_filename):
    """
    Export Triggers to a CSV file using /api/v2/triggers with pagination, using count from response.
    Success is determined by the presence of the 'triggers' key in the API response.
    
    Args:
        subdomain (str): Zendesk subdomain.
        email (str): Zendesk email for authentication.
        token (str): Zendesk API token.
        field_mapping (dict): Mapping of CSV columns to Zendesk fields.
        csv_headers (list): List of CSV header names.
        save_directory (str): Directory to save the CSV file.
        stop_event (threading.Event): Event to stop the operation.
        app_instance: Application instance for progress updates.
        success_filename (str): Filename for the success CSV.
    
    Returns:
        tuple: (success_count, error_count, skipped_count, total_processed)
    """
    logger.info("Starting Trigger export")
    success_count = 0
    error_count = 0
    skipped_count = 0
    total_processed = 0
    total_records = 0  # Will be updated from API response

    base_url = f"https://{subdomain}.zendesk.com/api/v2/triggers.json"
    auth = HTTPBasicAuth(f"{email}/token", token)
    headers = {"Content-Type": "application/json"}

    # Define default fields to export if field_mapping is not provided or incomplete
    default_trigger_fields = [
        'id', 'title', 'active', 'position', 'conditions_all',
        'conditions_any', 'actions', 'created_at', 'updated_at'
    ]

    # Use field_mapping to determine fields to export, if available
    export_fields = []
    if field_mapping and csv_headers:
        # Extract Zendesk fields from field_mapping where the CSV column is mapped
        export_fields = [zendesk_field for csv_col, zendesk_field in field_mapping.items() if csv_col in csv_headers and zendesk_field]
        if not export_fields:
            logger.warning("No valid fields in field_mapping; using default fields")
            export_fields = default_trigger_fields
    else:
        logger.info("No field_mapping or csv_headers provided; using default fields")
        export_fields = default_trigger_fields

    # Use export_fields as CSV headers
    csv_headers = export_fields

    # Initialize CSV file
    if not success_filename:
        success_filename = os.path.join(save_directory, "Triggers_export_success.csv")
    os.makedirs(save_directory, exist_ok=True)
    success_filename = app_instance.get_unique_filename(success_filename)

    with open(success_filename, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=csv_headers)
        writer.writeheader()

    # Paginate through Triggers
    url = base_url + "?per_page=100"  # Use per_page=100 as per Zendesk API recommendation
    records = []

    while url and not stop_event.is_set():
        with rate_limit_lock:
            if not rate_limit_event.is_set():
                current_time = time.time()
                if current_time < rate_limit_resume_time:
                    pause_duration = rate_limit_resume_time - current_time
                    time.sleep(pause_duration)
                rate_limit_event.set()

        try:
            logger.debug(f"Fetching Triggers from {url}")
            response = requests.get(url, auth=auth, headers=headers)
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                logger.warning(f"Rate limit hit. Waiting {retry_after}s")
                with rate_limit_lock:
                    rate_limit_resume_time = time.time() + retry_after
                    rate_limit_event.clear()
                time.sleep(retry_after)
                continue
            elif response.status_code == 503:
                logger.warning("Service Unavailable (503). Retrying after 10s")
                time.sleep(10)
                continue
            response.raise_for_status()

            data = response.json()
            
            # Check for 'triggers' key in the response
            if "triggers" not in data:
                logger.error("API response missing 'triggers' key")
                error_count += 1
                raise ValueError("API response missing 'triggers' key")

            triggers = data["triggers"]
            records.extend(triggers)
            total_processed += len(triggers)

            # Update total_records from the count field in the first response
            if total_records == 0:
                total_records = data.get("count", len(triggers))  # Fallback to number of triggers if count is missing
                logger.info(f"Total Triggers to export: {total_records}")

            # Update progress
            progress = (total_processed / total_records * 100) if total_records > 0 else min(total_processed * 5, 99)
            app_instance.safe_update_progress_bar(progress, total_records)
            logger.debug(f"Processed {total_processed}/{total_records} Triggers ({progress:.2f}%)")

            # Write records to CSV
            with open(success_filename, 'a', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=csv_headers)
                for trigger in triggers:
                    row = {}
                    for field in export_fields:
                        if field == 'conditions_all':
                            value = trigger.get('conditions', {}).get('all', [])
                        elif field == 'conditions_any':
                            value = trigger.get('conditions', {}).get('any', [])
                        else:
                            value = trigger.get(field)
                        if field in ['conditions_all', 'conditions_any', 'actions'] and value:
                            value = json.dumps(value)  # Serialize complex objects
                        elif field == 'active':
                            value = str(value).lower()  # Convert boolean to string
                        row[field] = sanitize_csv_value(value, strip_html=False)
                    writer.writerow(row)
                    success_count += 1

            url = data.get("next_page")
            logger.debug(f"Next page: {url}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching Triggers: {e}")
            error_count += 1
            time.sleep(5)  # Brief pause before retry
            continue
        except ValueError as e:
            logger.error(f"Failed to process response: {e}")
            error_count += 1
            break  # Stop if 'triggers' key is missing

    # Set progress to 100% upon completion (unless stopped)
    if not stop_event.is_set():
        app_instance.safe_update_progress_bar(100, total_records)
        logger.debug("Export completed, setting progress to 100%")

    if stop_event.is_set():
        logger.info("Export stopped by user")
        message = f"Export stopped by user.\nErrors: {error_count}"
    else:
        logger.info(f"Export completed successfully.\nErrors: {error_count}")
        message = f"Export completed successfully!\nErrors: {error_count}"

    logger.info(message)
    return success_count, error_count, skipped_count, total_processed





def import_triggers(
    subdomain, email, token, csv_data, csv_headers, field_mapping, save_directory,
    stop_event=None, app_instance=None, chunk_size=1, success_filename=None, error_filename=None
):
    """
    Imports Triggers to Zendesk using POST /api/v2/triggers.
    
    Args:
        subdomain (str): Zendesk subdomain.
        email (str): Zendesk email for authentication.
        token (str): Zendesk API token.
        csv_data (list): List of CSV records.
        csv_headers (list): List of CSV header names.
        field_mapping (dict): Mapping of CSV columns to Zendesk fields.
        save_directory (str): Directory to save success/error CSVs.
        stop_event (threading.Event, optional): Event to stop the operation.
        app_instance: Application instance for progress updates.
        chunk_size (int): Size of batches for API calls (1 for Triggers due to single POST).
        success_filename (str, optional): Filename for success CSV.
        error_filename (str, optional): Filename for error CSV.
    
    Returns:
        tuple: (success_count, error_count, skipped_count, total_processed)
    """
    logger.info("Starting Trigger import")
    base_url = f"https://{subdomain}.zendesk.com/api/v2/triggers"
    auth = HTTPBasicAuth(f"{email}/token", token)

    # Define headers for success and error CSVs
    success_headers = csv_headers + ["Status", "ID"]
    error_headers = csv_headers + ["Error Message"]

    # Use provided filenames or fallback to default
    success_filename = success_filename or os.path.join(save_directory, "Triggers_import_success.csv")
    error_filename = error_filename or os.path.join(save_directory, "Triggers_import_errors.csv")

  

    success_records = []
    error_records = []
    total_records = len(csv_data)
    cumulative_processed = 0
    total_success_count = 0
    total_error_count = 0
    total_skipped_count = 0

    logger.info(f"Total Trigger records to process: {total_records}")

    # Initialize CSV files
    def write_csv_records(filename, records, headers, append=False):
        mode = 'a' if append else 'w'
        try:
            with open(filename, mode, newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                if not append:
                    writer.writeheader()
                writer.writerows(records)
                f.flush()
        except Exception as e:
            logger.error(f"Failed to write to {filename}: {e}")

    write_csv_records(success_filename, [], success_headers)
    write_csv_records(error_filename, [], error_headers)

    # Validate required fields
    required_fields = ['title', 'actions', 'conditions_all']
    missing_fields = [f for f in required_fields if f not in field_mapping.values()]
    if missing_fields:
        logger.error(f"Missing required fields in field_mapping: {missing_fields}")
        return 0, 0, 0, 0

    # Process each Trigger individually (chunk_size=1 due to single POST)
    for record in [dict(zip(csv_headers, row)) for row in csv_data]:
        if stop_event and stop_event.is_set():
            logger.info("Stop event detected, aborting operation")
            break

        try:
            # Build the Trigger payload
            trigger = {}
            conditions = {"all": [], "any": []}

            for csv_col, zendesk_field in field_mapping.items():
                if csv_col not in record or not zendesk_field:
                    continue
                value = record[csv_col]
                
                if zendesk_field == 'conditions_all' and value:
                    try:
                        conditions["all"] = json.loads(value) if value else []
                    except json.JSONDecodeError as e:
                        logger.error(f"Invalid JSON for conditions_all: {value}")
                        raise ValueError(f"Invalid JSON for conditions_all: {e}")
                elif zendesk_field == 'conditions_any' and value:
                    try:
                        conditions["any"] = json.loads(value) if value else []
                    except json.JSONDecodeError as e:
                        logger.error(f"Invalid JSON for conditions_any: {value}")
                        raise ValueError(f"Invalid JSON for conditions_any: {e}")
                elif zendesk_field == 'actions' and value:
                    try:
                        trigger["actions"] = json.loads(value) if value else []
                    except json.JSONDecodeError as e:
                        logger.error(f"Invalid JSON for actions: {value}")
                        raise ValueError(f"Invalid JSON for actions: {e}")
                elif zendesk_field == 'active':
                    trigger["active"] = value.lower() == "true" if value else False
                elif zendesk_field == 'position':
                    try:
                        trigger["position"] = int(value) if value else 0
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid position value: {value}, defaulting to 0")
                        trigger["position"] = 0
                elif zendesk_field == 'category_id':
                    try:
                        trigger["category_id"] = str(value) if value else None
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid category_id value: {value}, skipping")
                        trigger["category_id"] = None
                elif zendesk_field in ['title', 'created_at', 'updated_at']:
                    trigger[zendesk_field] = value if value else None

            # Ensure required fields are present
            if not trigger.get("title"):
                raise ValueError("Title is required")
            if not trigger.get("actions"):
                raise ValueError("Actions are required")
            trigger["conditions"] = conditions

            # Remove None values from trigger to avoid API errors
            trigger = {k: v for k, v in trigger.items() if v is not None}

            # Construct the payload
            payload = {"trigger": trigger}
            logger.debug(f"Submitting Trigger payload: {json.dumps(payload, default=str)}")

            # Send POST request
            response = make_request_with_retries(
                "POST", base_url, auth, json=payload, stop_event=stop_event
            )

            # Process response
            if response and response.status_code in [201]:
                trigger_data = response.json().get("trigger", {})
                trigger_id = trigger_data.get("id")
                if trigger_id:
                    with threading.Lock():
                        success_records.append({
                            **record,
                            "Status": "Created",
                            "ID": trigger_id
                        })
                        cumulative_processed += 1
                        total_success_count += 1
                        if len(success_records) >= 100:
                            write_csv_records(success_filename, success_records, success_headers, append=True)
                            success_records.clear()
                        if app_instance:
                            app_instance.safe_update_progress_bar(
                                (cumulative_processed / total_records) * 100, total_records
                            )
            else:
                error_msg = response.text if response else "No response from API"
                with threading.Lock():
                    error_records.append({
                        **record,
                        "Error Message": error_msg
                    })
                    cumulative_processed += 1
                    total_error_count += 1
                    if len(error_records) >= 100:
                        write_csv_records(error_filename, error_records, error_headers, append=True)
                        error_records.clear()
                    if app_instance:
                        app_instance.safe_update_progress_bar(
                            (cumulative_processed / total_records) * 100, total_records
                        )

        except Exception as e:
            logger.error(f"Error processing Trigger: {e}")
            with threading.Lock():
                error_records.append({
                    **record,
                    "Error Message": str(e)
                })
                cumulative_processed += 1
                total_error_count += 1
                if len(error_records) >= 100:
                    write_csv_records(error_filename, error_records, error_headers, append=True)
                    error_records.clear()
                if app_instance:
                    app_instance.safe_update_progress_bar(
                        (cumulative_processed / total_records) * 100, total_records
                    )

    # Finalize CSV writing
    with threading.Lock():
        if success_records:
            write_csv_records(success_filename, success_records, success_headers, append=True)
            
        if error_records:
            write_csv_records(error_filename, error_records, error_headers, append=True)
            
        if app_instance and total_records > 0 and not stop_event.is_set() and cumulative_processed >= total_records:
            app_instance.safe_update_progress_bar(100, total_records)

    logger.info(
        f"Trigger import completed: Success={total_success_count}, Errors={total_error_count}, "
        f"Skipped={total_skipped_count}, Processed={cumulative_processed}"
    )
    if app_instance:
        app_instance.success_filename = success_filename
        app_instance.error_filename = error_filename

    return total_success_count, total_error_count, total_skipped_count, cumulative_processed










def export_macros(subdomain, email, token, field_mapping, csv_headers, save_directory, stop_event=None, app_instance=None, success_filename=None):
    """
    Export all macros from Zendesk to a CSV file using /api/v2/macros with pagination.
    
    Args:
        subdomain (str): Zendesk subdomain.
        email (str): Zendesk email for authentication.
        token (str): Zendesk API token.
        field_mapping (dict): Mapping of CSV columns to Zendesk fields.
        csv_headers (list): List of CSV header names.
        save_directory (str): Directory to save the CSV file.
        stop_event (threading.Event, optional): Event to stop the operation.
        app_instance (AppAPI, optional): Instance of AppAPI for progress updates.
        success_filename (str, optional): Specific filename for the success CSV.
    
    Returns:
        tuple: (success_count, error_count, skipped_count, total_processed)
    """
    base_url = f"https://{subdomain}.zendesk.com"
    auth = HTTPBasicAuth(f"{email}/token", token)
    headers = {"Content-Type": "application/json"}
    url = f"{base_url}/api/v2/macros.json"
    success_count = 0
    error_count = 0
    skipped_count = 0
    total_processed = 0

    # Use provided success_filename or generate a default
    os.makedirs(save_directory, exist_ok=True)
    output_filename = success_filename or os.path.join(save_directory, "Macros_export_success.csv")
    if not success_filename:
        counter = 1
        while os.path.exists(output_filename):
            output_filename = os.path.join(save_directory, f"Macros_export_success ({counter}).csv")
            counter += 1
    else:
        # Ensure uniqueness if success_filename is provided
        base, ext = os.path.splitext(output_filename)
        counter = 1
        while os.path.exists(output_filename):
            output_filename = f"{base} ({counter}){ext}"
            counter += 1

    # Define CSV headers based on field_mapping or default fields
    export_headers = list(field_mapping.keys()) if field_mapping else csv_headers
    if not export_headers:
        export_headers = ["id", "title", "active", "actions", "created_at", "updated_at", "position", "restriction"]

    logger.info(f"Starting macro export to {output_filename}")

    with open(output_filename, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=export_headers)
        writer.writeheader()

        while url:
            if stop_event and stop_event.is_set():
                logger.info("Stop event detected, aborting macro export")
                break

            try:
                response = requests.get(url, auth=auth, headers=headers)
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    logger.warning(f"Rate limit hit. Waiting {retry_after}s")
                    time.sleep(retry_after)
                    continue
                elif response.status_code == 503:
                    logger.warning("Service Unavailable (503). Retrying after 10s")
                    time.sleep(10)
                    continue
                response.raise_for_status()
                data = response.json()
                macros = data.get("macros", [])
                next_page = data.get("next_page")

                for macro in macros:
                    total_processed += 1
                    record = {}
                    for csv_col in export_headers:
                        zendesk_field = field_mapping.get(csv_col, csv_col)
                        value = macro.get(zendesk_field, "")
                        if zendesk_field == "actions" and value:
                            value = json.dumps(value)  # Convert actions list to JSON string
                        elif zendesk_field == "restriction" and value:
                            value = json.dumps(value)  # Convert restriction dict to JSON string
                        record[csv_col] = sanitize_csv_value(value, strip_html=True)
                    
                    try:
                        writer.writerow(record)
                        success_count += 1
                        logger.debug(f"Exported macro ID {macro.get('id')}")
                    except Exception as e:
                        error_count += 1
                        logger.error(f"Failed to write macro ID {macro.get('id')}: {e}")

                    if app_instance:
                        app_instance.safe_update_progress_bar((total_processed / max(total_processed, 1)) * 100, total_processed)

                url = next_page
                logger.info(f"Fetched {len(macros)} macros, next page: {next_page}")

            except requests.exceptions.RequestException as e:
                error_count += 1
                logger.error(f"Failed to fetch macros: {e}")
                break

    logger.info(f"Macro export completed: {success_count} exported, {error_count} errors")
    return success_count, error_count, skipped_count, total_processed








def import_macros(subdomain, email, token, csv_data, csv_headers, field_mapping, save_directory, stop_event=None, app_instance=None, chunk_size=100, success_filename=None, error_filename=None):
    base_url = f"https://{subdomain}.zendesk.com"
    auth = HTTPBasicAuth(f"{email}/token", token)
    success_count = 0
    error_count = 0
    skipped_count = 0
    total_processed = 0

    # Ensure save_directory exists
    os.makedirs(save_directory, exist_ok=True)

    # Use provided filenames, raising an error if not provided
    if not success_filename or not error_filename:
        logger.error("Success or error filename not provided")
        raise ValueError("Success and error filenames must be provided")

    # Define headers for success and error CSVs
    success_headers = csv_headers + ["ID", "Status"]
    error_headers = csv_headers + ["Error Message"]

    logger.info(f"Starting macro import: {len(csv_data)} records to {success_filename}")

    for index, row in enumerate(csv_data):
        if stop_event and stop_event.is_set():
            logger.info("Stop event detected, aborting macro import")
            break

        total_processed += 1
        record_dict = dict(zip(csv_headers, row))

        # Prepare single macro payload
        macro = {}
        for csv_col, zendesk_field in field_mapping.items():
            if zendesk_field and zendesk_field != " " and csv_col in record_dict:
                value = record_dict[csv_col]
                if zendesk_field == "actions" and value:
                    try:
                        value = json.loads(value) if isinstance(value, str) else value
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON in actions for record: {value}")
                        value = []
                elif zendesk_field == "restriction" and value:
                    try:
                        value = json.loads(value) if isinstance(value, str) else value
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON in restriction for record: {value}")
                        value = None
                elif zendesk_field == "active" and value:
                    value = value.lower() in ['true', '1', 'yes']
                elif zendesk_field == "position" and value:
                    try:
                        value = int(value)
                    except ValueError:
                        logger.warning(f"Invalid position value: {value}, setting to None")
                        value = None
                macro[zendesk_field] = value

        # Skip invalid macro
        if not macro or not macro.get("title") or not macro.get("actions"):
            skipped_count += 1
            logger.debug(f"Skipping invalid macro at index {index}: {macro}")
            error_records = [{
                **record_dict,
                "Error Message": "Missing required fields: title or actions"
            }]
            with open(error_filename, 'a', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=error_headers)
                writer.writerows(error_records)
            continue

        # Remove None values from payload to avoid API errors
        macro = {k: v for k, v in macro.items() if v is not None}

        # Send API request for single macro
        try:
            url = f"{base_url}/api/v2/macros"
            payload = {"macro": macro}
            logger.debug(f"Payload for macro at index {index}: {json.dumps(payload, indent=2)}")
            response = make_request_with_retries("POST", url, auth, json=payload, stop_event=stop_event)
            logger.debug(f"API response: {response.status_code} {response.text}")

            if response.status_code == 201:
                macro_data = response.json().get("macro", {})
                macro_id = macro_data.get("id")
                if macro_id:
                    success_records = [{
                        **record_dict,
                        "ID": macro_id,
                        "Status": "Created"
                    }]
                    success_count += 1
                    with open(success_filename, 'a', newline='', encoding='utf-8-sig') as f:
                        writer = csv.DictWriter(f, fieldnames=success_headers)
                        writer.writerows(success_records)
                    logger.info(f"Successfully created macro ID {macro_id}")
                else:
                    error_count += 1
                    error_records = [{
                        **record_dict,
                        "Error Message": "No macro ID returned in response"
                    }]
                    with open(error_filename, 'a', newline='', encoding='utf-8-sig') as f:
                        writer = csv.DictWriter(f, fieldnames=error_headers)
                        writer.writerows(error_records)
            else:
                error_count += 1
                error_records = [{
                    **record_dict,
                    "Error Message": f"HTTP {response.status_code}: {response.text}"
                }]
                with open(error_filename, 'a', newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, fieldnames=error_headers)
                    writer.writerows(error_records)

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to import macro at index {index}: {e}")
            error_count += 1
            error_records = [{
                **record_dict,
                "Error Message": str(e)
            }]
            with open(error_filename, 'a', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=error_headers)
                writer.writerows(error_records)

        if app_instance:
            app_instance.safe_update_progress_bar((total_processed / max(len(csv_data), 1)) * 100, len(csv_data))

    logger.info(f"Macro import completed: {success_count} successes, {error_count} errors, {skipped_count} skipped")
    return success_count, error_count, skipped_count, total_processed












def flatten_record(record, field_mapping=None):
    if field_mapping is None:
        field_mapping = {}  # Prevents errors if None is passed

    """Flattens a nested JSON record, sanitizing values for CSV export."""
    flat_record = {}

    def sanitize_column_name(name):
        return re.sub(r'[^a-zA-Z0-9_]', '_', name)

    # Fields to exclude
    exclude_fields = {"via_source", "fields"}

    for key, value in record.items():
        if key in exclude_fields:
            continue  # Skip these fields

        # Fix External ID issue
        if key == "external_id":
            if not value or str(value).strip() in ["", "None", "null"]:
                continue  # Remove empty External IDs completely
            else:
                value = str(value)  # Ensure it's stored as a string without quotes

        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                column_name = sanitize_column_name(f"custom_{sub_key}")
                if isinstance(sub_value, list):
                    sanitized_list = [sanitize_csv_value(item) for item in sub_value]
                    flat_record[column_name] = ", ".join(map(str, sanitized_list)) if sanitized_list else ""
                else:
                    flat_record[column_name] = sanitize_csv_value(sub_value) or ""
        elif isinstance(value, list):
            if key == "custom_fields":
                for field in value:
                    if isinstance(field, dict) and "id" in field and "value" in field:
                        field_id = str(field["id"])
                        display_name = field_mapping.get(field_id, f"Field_{field_id}")
                        safe_display_name = sanitize_column_name(f"custom_{display_name}")
                        field_value = field["value"]
                        if isinstance(field_value, list):
                            sanitized_list = [sanitize_csv_value(item) for item in field_value]
                            flat_record[safe_display_name] = ", ".join(map(str, sanitized_list)) if sanitized_list else ""
                        else:
                            flat_record[safe_display_name] = sanitize_csv_value(field_value) or ""
            else:
                column_name = sanitize_column_name(key)
                sanitized_list = [sanitize_csv_value(item) for item in value]
                flat_record[column_name] = ", ".join(map(str, sanitized_list)) if sanitized_list else ""
        else:
            flat_record[sanitize_column_name(key)] = sanitize_csv_value(value) or ""

    return flat_record









def import_attachments(subdomain, email, token, csv_data, csv_headers, field_mapping, save_directory, stop_event=None, app_instance=None, base_file_dir=None, chunk_size=1):
    base_url = f"https://{subdomain}.zendesk.com"
    auth = HTTPBasicAuth(f"{email}/token", token)

    success_filename = get_unique_filename(os.path.join(save_directory, "Attachments_import_success.csv"))
    error_filename = get_unique_filename(os.path.join(save_directory, "Attachments_import_errors.csv"))
    success_headers = csv_headers + ["Upload Token", "Status"]
    error_headers = csv_headers + ["Error Message"]

    if "file_name" not in field_mapping.values():
        logger.error("Field mapping does not include 'file_name', required for attachments")
        return 0, 0, 0, 0

    file_name_column = [csv_field for csv_field, zendesk_field in field_mapping.items() if zendesk_field == "file_name"][0]
    if file_name_column not in csv_headers:
        logger.error(f"CSV data does not include the mapped 'file_name' column: {file_name_column}")
        return 0, 0, 0, 0

    submit_executor = ThreadPoolExecutor(max_workers=1)
    status_executor = ThreadPoolExecutor(max_workers=15)

    success_records = []
    error_records = []
    total_records = len(csv_data)
    cumulative_processed = 0
    total_success_count = 0
    total_error_count = 0
    total_skipped_count = 0

    logger.info(f"Total attachment records to process: {total_records}")
    print(f"Total attachment records to process: {total_records}")

    def write_csv_records(filename, records, headers, append=False):
        mode = 'a' if append else 'w'
        with open(filename, mode, newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            if not append:
                writer.writeheader()
            writer.writerows(records)

    write_csv_records(success_filename, [], success_headers)
    write_csv_records(error_filename, [], error_headers)

    if not base_file_dir:
        logger.warning("No base_file_dir provided; assuming file_name column contains full file paths")
    else:
        logger.info(f"Using base_file_dir: {base_file_dir} to resolve file paths")

    def upload_attachment(file_path, api_filename):
        upload_url = f"{base_url}/api/v2/uploads.json"
        content_type, _ = mimetypes.guess_type(file_path)
        if content_type:
            extension = mimetypes.guess_extension(content_type) or os.path.splitext(file_path)[1]
            if extension and not api_filename.endswith(extension):
                api_filename += extension
        else:
            extension = os.path.splitext(file_path)[1]
            if extension and not api_filename.endswith(extension):
                api_filename += extension

        params = {'filename': api_filename}
        full_url = requests.Request('POST', upload_url, params=params).prepare().url
        print(f"📤 Sending attachment to URL: {full_url}")
        headers = {'Content-Type': content_type or 'application/octet-stream'}
        
        try:
            with open(file_path, 'rb') as file:
                response = requests.post(upload_url, params=params, data=file, headers=headers, auth=auth)
                response.raise_for_status()
                return response
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
            raise Exception(f"File not found: {file_path}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Upload failed for {file_path}: {str(e)}")
            raise e

    def process_upload(record, file_path):
        nonlocal cumulative_processed, total_success_count, total_error_count, total_skipped_count
        
        api_filename = os.path.basename(file_path)
        try:
            response = upload_attachment(file_path, api_filename)
            upload_data = response.json().get("upload", {})
            token = upload_data.get("token", "")
            if token:
                with threading.Lock():
                    success_records.append({
                        **record,
                        "Upload Token": token,
                        "Status": "Uploaded"
                    })
                    cumulative_processed += 1
                    if len(success_records) >= 5000:
                        write_csv_records(success_filename, success_records, success_headers, append=True)
                        total_success_count += len(success_records)
                        success_records.clear()
                    if app_instance:
                        app_instance.safe_update_progress_bar((cumulative_processed / total_records) * 100)
            else:
                with threading.Lock():
                    error_records.append({
                        **record,
                        "Error Message": "No upload token returned"
                    })
                    cumulative_processed += 1
                    total_error_count += 1
                    if len(error_records) >= 5000:
                        write_csv_records(error_filename, error_records, error_headers, append=True)
                        error_records.clear()
                    if app_instance:
                        app_instance.safe_update_progress_bar((cumulative_processed / total_records) * 100)
        except Exception as e:
            with threading.Lock():
                error_records.append({
                    **record,
                    "Error Message": str(e)
                })
                cumulative_processed += 1
                total_error_count += 1
                if len(error_records) >= 5000:
                    write_csv_records(error_filename, error_records, error_headers, append=True)
                    error_records.clear()
                if app_instance:
                    app_instance.safe_update_progress_bar((cumulative_processed / total_records) * 100)

    try:
        submission_futures = {}
        start_time = time.time()
        requests_this_minute = 0
        minute_interval = 60
        request_delay = 2

        for record in [dict(zip(csv_headers, row)) for row in csv_data]:
            if stop_event and stop_event.is_set():
                logger.info("🛑 Stop detected. No new uploads submitted.")
                break

            file_name = record.get(file_name_column, "").strip()
            if not file_name:
                with threading.Lock():
                    error_records.append({
                        **record,
                        "Error Message": "Empty or missing file_name"
                    })
                    cumulative_processed += 1
                    total_error_count += 1
                    if len(error_records) >= 5000:
                        write_csv_records(error_filename, error_records, error_headers, append=True)
                        error_records.clear()
                    if app_instance:
                        app_instance.safe_update_progress_bar((cumulative_processed / total_records) * 100)
                continue

            file_path = os.path.join(base_file_dir, file_name) if base_file_dir else file_name
            
            if not os.path.exists(file_path):
                common_extensions = ['.jpg', '.jpeg', '.png', '.pdf', '.txt']
                for ext in common_extensions:
                    test_path = file_path + ext
                    if os.path.exists(test_path):
                        file_path = test_path
                        logger.info(f"Found file with extension: {file_path}")
                        break
                else:
                    logger.error(f"No file found for {file_path} with common extensions")
                    with threading.Lock():
                        error_records.append({
                            **record,
                            "Error Message": f"No file found for {file_name} with common extensions"
                        })
                        cumulative_processed += 1
                        total_error_count += 1
                        if len(error_records) >= 5000:
                            write_csv_records(error_filename, error_records, error_headers, append=True)
                            error_records.clear()
                        if app_instance:
                            app_instance.safe_update_progress_bar((cumulative_processed / total_records) * 100)
                    continue

            api_filename = os.path.basename(file_path)
            encoded_filename = requests.utils.quote(api_filename)

            logger.info(f"Preparing to upload attachment: {file_path} (API filename: {encoded_filename})")
            print(f"📡 Preparing to upload attachment: {file_path} (API filename: {encoded_filename})")

            requests_this_minute += 1
            elapsed_time = time.time() - start_time
            if elapsed_time >= minute_interval:
                pause_time = max(0, minute_interval - elapsed_time)
                logger.info(f"⏸️ Pausing uploads for {pause_time:.2f}s to reset rate limit")
                time.sleep(pause_time)
                start_time = time.time()
                requests_this_minute = 0

            jittered_delay = request_delay + random.uniform(0, 0.3)
            time.sleep(jittered_delay)
            logger.info(f"⏳ Applied jittered delay of {jittered_delay:.3f}s before upload")

            future = submit_executor.submit(process_upload, record, file_path)
            submission_futures[future] = record

            for future in [f for f in submission_futures if f.done()]:
                submission_futures.pop(future)

        for future in as_completed(submission_futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error in upload: {e}")

    finally:
        submit_executor.shutdown(wait=True)
        status_executor.shutdown(wait=True)

        with threading.Lock():
            if success_records:
                write_csv_records(success_filename, success_records, success_headers, append=True)
                total_success_count += len(success_records)
                success_records.clear()
            if error_records:
                write_csv_records(error_filename, error_records, error_headers, append=True)
                error_records.clear()
            if app_instance and total_records > 0 and not stop_event.is_set() and cumulative_processed >= total_records:
                app_instance.safe_update_progress_bar(100)

        logger.info(f"Final counts: Success={total_success_count}, Errors={total_error_count}, Skipped={total_skipped_count}, Processed={cumulative_processed}")
        if app_instance:
            app_instance.success_filename = success_filename
            app_instance.error_filename = error_filename

    return total_success_count, total_error_count, total_skipped_count, cumulative_processed












def import_tickets_comments_attachments(
    subdomain,
    email,
    token,
    ticket_csv_data,
    ticket_csv_headers,
    comments_csv_data,
    comments_csv_headers,
    attachments_csv_data,
    attachments_csv_headers,
    field_mapping,
    save_directory,
    stop_event=None,
    progress_bar=None,
    ticket_id_field=None,
    comment_ticket_id_field=None,
    attachment_comment_id_field=None,
    app_instance=None,
    comment_field_mapping=None,
    base_file_dir=None,
    attachment_field_mapping=None,
    comment_row_numbers=None,
    html_body_column=None,
    chunk_size=100,
    success_filename=None,
    error_filename=None,
    attachment_success_filename=None,
    attachment_error_filename=None,
    comment_success_filename=None,
    comment_error_filename=None,
    processed_records_so_far=0
):
    base_url = f"https://{subdomain}.zendesk.com"
    auth = HTTPBasicAuth(f"{email}/token", token)
    ticket_api_url = f"{base_url}/api/v2/imports/tickets/create_many"

    if not all([success_filename, error_filename, attachment_success_filename, attachment_error_filename, comment_success_filename, comment_error_filename]):
        logger.error("Missing required filenames for output CSVs")
        print("❌ Error: Missing required filenames for output CSVs")
        return 0, 0, 0, 0

    if not base_file_dir or not os.path.exists(base_file_dir):
        logger.error(f"Attachment directory does not exist: {base_file_dir}")
        print(f"❌ Error: Attachment directory does not exist: {base_file_dir}")
        return 0, 0, 0, 0

    ticket_success_headers = ticket_csv_headers + ["ID", "Status"]
    ticket_error_headers = ticket_csv_headers + ["Error Message"]
    attachment_success_headers = [comment_ticket_id_field, "comment_id", "file_name", "upload_token"]
    attachment_error_headers = [comment_ticket_id_field, "comment_id", "file_name", "error_message"]
    comment_success_headers = comments_csv_headers + ["html_body", "Status"] if "html_body" not in comments_csv_headers else comments_csv_headers + ["Status"]
    comment_error_headers = comments_csv_headers + ["html_body", "Error Message"] if "html_body" not in comments_csv_headers else comments_csv_headers + ["Error Message"]

    batch_size = 5
    buffer_size = 1000
    ticket_success_records = []
    ticket_error_records = []
    attachment_success_records = []
    attachment_error_records = []
    comment_success_records = []
    comment_error_records = []
    total_records = len(ticket_csv_data)
    cumulative_processed = 0
    total_success_count = 0
    total_error_count = 0
    total_skipped_count = 0

    processed_job_ids = set()
    processed_job_ids_lock = threading.Lock()
    queued_job_ids = set()
    queued_job_ids_lock = threading.Lock()
    records_lock = threading.Lock()
    rate_limit_lock = threading.Lock()
    rate_limit_event = threading.Event()
    rate_limit_event.set()
    rate_limit_resume_time = 0

    logger.info(f"Starting import of {total_records} tickets with comments and attachments")
    print(f"🚀 Starting import of {total_records} tickets with comments and attachments")
    logger.info(f"Using base_file_dir: {base_file_dir}")
    print(f"📁 Using base_file_dir: {base_file_dir}")

    if app_instance and not app_instance.progress_manager:
        app_instance.progress_manager = ProgressManager(app_instance, total_records, update_interval=1000)
        app_instance.progress_manager.start()
        print(f"📊 Initialized ProgressManager for {total_records} tickets")

    submit_executor = ThreadPoolExecutor(max_workers=20)
    status_executor = ThreadPoolExecutor(max_workers=20)
    upload_executor = ThreadPoolExecutor(max_workers=150)

    def write_csv_records(filename, records, headers, append=False):
        mode = 'a' if append else 'w'
        logger.debug(f"Writing {len(records)} records to {filename} (append={append})")
        try:
            with open(filename, mode, newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                if not append:
                    writer.writeheader()
                writer.writerows(records)
                f.flush()
            print(f"📝 Wrote {len(records)} records to {filename} (append={append})")
        except Exception as e:
            logger.error(f"Failed to write to {filename}: {e}")
            print(f"❌ Failed to write to {filename}: {e}")

    print("🔍 Validating input fields...")
    if not ticket_id_field or ticket_id_field not in ticket_csv_headers:
        logger.error(f"Invalid or missing ticket_id_field: {ticket_id_field}")
        print(f"❌ Error: Invalid or missing ticket_id_field: {ticket_id_field}")
        return 0, 0, 0, 0
    if not comment_ticket_id_field or comment_ticket_id_field not in comments_csv_headers:
        logger.error(f"Invalid or missing comment_ticket_id_field: {comment_ticket_id_field}")
        print(f"❌ Error: Invalid or missing comment_ticket_id_field: {comment_ticket_id_field}")
        return 0, 0, 0, 0
    if not attachment_comment_id_field or attachment_comment_id_field not in comments_csv_headers:
        logger.error(f"Invalid or missing attachment_comment_id_field: {attachment_comment_id_field}")
        print(f"❌ Error: Invalid or missing attachment_comment_id_field: {attachment_comment_id_field}")
        return 0, 0, 0, 0
    if not comment_field_mapping:
        logger.warning("No comment_field_mapping provided; using field_mapping")
        print("⚠️ Warning: No comment_field_mapping provided; using field_mapping")
        comment_field_mapping = field_mapping
    if not base_file_dir:
        logger.error("No attachment directory provided for attachments")
        print("❌ Error: No attachment directory provided for attachments")
        return 0, 0, 0, 0
    if not attachment_field_mapping:
        logger.error("No attachment_field_mapping provided")
        print("❌ Error: No attachment_field_mapping provided")
        return 0, 0, 0, 0
    file_name_mapped = any(zf == "file_name" for zf in attachment_field_mapping.values())
    if not file_name_mapped:
        logger.error("No 'file_name' field mapped in attachment_field_mapping")
        print("❌ Error: No 'file_name' field mapped in attachment_field_mapping")
        return 0, 0, 0, 0
    print("✅ Input fields validated successfully")

    print("🔍 Fetching Zendesk ticket fields...")
    try:
        ticket_fields, ticket_custom_field_info = fetch_zendesk_fields(subdomain, email, token, "Tickets", "import")
        print(f"✅ Fetched {len(ticket_fields)} ticket fields")
    except Exception as e:
        logger.error(f"Failed to fetch Zendesk fields for Tickets: {e}")
        print(f"❌ Error: Failed to fetch Zendesk fields for Tickets: {e}")
        return 0, 0, 0, 0

    html_body_values = {}
    if html_body_column and comment_row_numbers and app_instance:
        print(f"📋 Fetching html_body for {len(comment_row_numbers)} comments...")
        html_body_values = app_instance.fetch_html_body_values(comment_row_numbers, html_body_column)
        print(f"✅ Retrieved html_body for {len(html_body_values)} comments")

    def upload_attachment(file_path, api_filename, ticket_id, comment_id, max_retries=10):
        upload_url = f"{base_url}/api/v2/uploads.json"
        content_type, _ = mimetypes.guess_type(file_path)
        if content_type:
            extension = mimetypes.guess_extension(content_type) or os.path.splitext(file_path)[1]
            if extension and not api_filename.endswith(extension):
                api_filename += extension
        else:
            extension = os.path.splitext(file_path)[1]
            if extension and not api_filename.endswith(extension):
                api_filename += extension

        params = {'filename': api_filename}
        headers = {'Content-Type': content_type or 'application/octet-stream'}
        print(f"📤 Uploading attachment '{api_filename}' for ticket {ticket_id}, comment {comment_id}...")

        retries = 0
        while retries < max_retries:
            try:
                with open(file_path, 'rb') as file:
                    with rate_limit_lock:
                        if not rate_limit_event.is_set():
                            pause_duration = max(0, rate_limit_resume_time - time.time())
                            logger.info(f"Pausing for {pause_duration:.1f} seconds due to rate limit")
                            time.sleep(pause_duration)
                            rate_limit_event.set()

                    response = requests.post(
                        upload_url,
                        params=params,
                        data=file,
                        headers=headers,
                        auth=auth,
                        timeout=60
                    )

                    if response.status_code == 429:
                        retry_after = int(response.headers.get("Retry-After", 60))
                        logger.warning(f"Rate limit hit (429), retrying after {retry_after} seconds (attempt {retries + 1}/{max_retries})")
                        print(f"⚠️ Rate limit hit (429), retrying after {retry_after} seconds (attempt {retries + 1}/{max_retries})")
                        with rate_limit_lock:
                            rate_limit_resume_time = time.time() + retry_after
                            rate_limit_event.clear()
                        time.sleep(retry_after)
                        retries += 1
                        continue

                    if response.status_code == 422:
                        error_msg = f"Unprocessable entity (422): {response.text}"
                        with records_lock:
                            attachment_error_records.append({
                                comment_ticket_id_field: ticket_id,
                                "comment_id": comment_id,
                                "file_name": api_filename,
                                "error_message": error_msg
                            })
                            if len(attachment_error_records) >= buffer_size:
                                write_csv_records(attachment_error_filename, attachment_error_records, attachment_error_headers, append=True)
                                attachment_error_records.clear()
                        logger.error(f"Attachment upload failed for '{api_filename}': {error_msg}")
                        print(f"❌ Attachment upload failed for '{api_filename}': {error_msg}")
                        return None

                    if response.status_code == 413:
                        error_msg = f"Payload Too Large"
                        with records_lock:
                            attachment_error_records.append({
                                comment_ticket_id_field: ticket_id,
                                "comment_id": comment_id,
                                "file_name": api_filename,
                                "error_message": error_msg
                            })
                            if len(attachment_error_records) >= buffer_size:
                                write_csv_records(attachment_error_filename, attachment_error_records, attachment_error_headers, append=True)
                                attachment_error_records.clear()
                        logger.error(f"Attachment upload failed for '{api_filename}': {error_msg}")
                        print(f"❌ Attachment upload failed for '{api_filename}': {error_msg}")
                        return None

                    if response.status_code in [502, 503, 504]:
                        backoff = (2 ** retries)
                        logger.warning(f"Transient error ({response.status_code}), retrying after {backoff:.2f} seconds (attempt {retries + 1}/{max_retries})")
                        print(f"⚠️ Transient error ({response.status_code}), retrying after {backoff:.2f} seconds (attempt {retries + 1}/{max_retries})")
                        time.sleep(backoff)
                        retries += 1
                        continue

                    response.raise_for_status()
                    upload_data = response.json().get("upload", {})
                    token = upload_data.get("token", "")
                    if token:
                        with records_lock:
                            attachment_success_records.append({
                                comment_ticket_id_field: ticket_id,
                                "comment_id": comment_id,
                                "file_name": api_filename,
                                "upload_token": token
                            })
                            if len(attachment_success_records) >= buffer_size:
                                write_csv_records(attachment_success_filename, attachment_success_records, attachment_success_headers, append=True)
                                attachment_success_records.clear()
                        print(f"✅ Successfully uploaded '{api_filename}' (token: {token})")
                        return token
                    else:
                        error_msg = "No upload token returned"
                        with records_lock:
                            attachment_error_records.append({
                                comment_ticket_id_field: ticket_id,
                                "comment_id": comment_id,
                                "file_name": api_filename,
                                "error_message": error_msg
                            })
                            if len(attachment_error_records) >= buffer_size:
                                write_csv_records(attachment_error_filename, attachment_error_records, attachment_error_headers, append=True)
                                attachment_error_records.clear()
                        print(f"❌ Upload failed for '{api_filename}': {error_msg}")
                        return None

            except FileNotFoundError as e:
                error_msg = f"File not found: {file_path}"
                with records_lock:
                    attachment_error_records.append({
                        comment_ticket_id_field: ticket_id,
                        "comment_id": comment_id,
                        "file_name": api_filename,
                        "error_message": error_msg
                    })
                    if len(attachment_error_records) >= buffer_size:
                        write_csv_records(attachment_error_filename, attachment_error_records, attachment_error_headers, append=True)
                        attachment_error_records.clear()
                print(f"❌ Error uploading '{api_filename}': {error_msg}")
                return None
            except requests.exceptions.RequestException as e:
                if retries < max_retries - 1:
                    backoff = (2 ** retries)
                    logger.warning(f"Request error ({str(e)}), retrying after {backoff:.2f} seconds (attempt {retries + 1}/{max_retries})")
                    print(f"⚠️ Request error ({str(e)}), retrying after {backoff:.2f} seconds (attempt {retries + 1}/{max_retries})")
                    time.sleep(backoff)
                    retries += 1
                    continue
                error_msg = f"Upload failed after {max_retries} attempts: {str(e)}"
                with records_lock:
                    attachment_error_records.append({
                        comment_ticket_id_field: ticket_id,
                        "comment_id": comment_id,
                        "file_name": api_filename,
                        "error_message": error_msg
                    })
                    if len(attachment_error_records) >= buffer_size:
                        write_csv_records(attachment_error_filename, attachment_error_records, attachment_error_headers, append=True)
                        attachment_error_records.clear()
                print(f"❌ Error uploading '{api_filename}': {error_msg}")
                return None

    def process_job_status(job_id, batch, response):
        nonlocal cumulative_processed, total_success_count, total_error_count, total_skipped_count
        with processed_job_ids_lock:
            if job_id in processed_job_ids:
                print(f"ℹ️ Job {job_id} already processed, skipping")
                return
            processed_job_ids.add(job_id)

        print(f"🔍 Checking status for job {job_id}...")
        job_url = f"{base_url}/api/v2/job_statuses/{job_id}.json"
        time.sleep(2)
        job_results = check_job_status(job_url, auth, stop_event=stop_event)
        if job_results:
            batch_dicts = [dict(zip(ticket_csv_headers, record)) for record in batch]
            results = job_results.get("results", [])
            print(f"🔍 Job {job_id} returned {len(results)} results")

            ticket_id_index = ticket_csv_headers.index(ticket_id_field)
            comment_ticket_id_index = comments_csv_headers.index(comment_ticket_id_field) if comment_ticket_id_field in comments_csv_headers else None
            comment_id_index = comments_csv_headers.index(attachment_comment_id_field) if attachment_comment_id_field in comments_csv_headers else None
            ticket_comments = {}
            for idx, comment_row in enumerate(comments_csv_data):
                if comment_ticket_id_index is not None and comment_id_index is not None:
                    ticket_id = str(comment_row[comment_ticket_id_index]).strip()
                    comment_id = str(comment_row[comment_id_index]).strip()
                    comment_dict = dict(zip(comments_csv_headers, comment_row))
                    if html_body_column and comment_row_numbers and idx < len(comment_row_numbers):
                        row_number = comment_row_numbers[idx]
                        comment_dict["html_body"] = html_body_values.get(row_number, "")
                    ticket_comments.setdefault(ticket_id, []).append((comment_id, comment_dict))

            for index, result in enumerate(results):
                if index >= len(batch_dicts):
                    print(f"⚠️ Warning: Result index {index} exceeds batch size {len(batch_dicts)}")
                    break
                record = batch_dicts[index]
                record_id = result.get("id")
                status = result.get("status", "")
                ticket_id = str(record[ticket_id_field]).strip()
                comments_for_ticket = ticket_comments.get(ticket_id, [])

                if record_id:
                    with records_lock:
                        ticket_success_records.append({
                            **record,
                            "ID": record_id,
                            "Status": "Created"
                        })
                        for comment_id, comment_dict in comments_for_ticket:
                            comment_success_records.append({
                                **comment_dict,
                                "Status": "Created"
                            })
                        cumulative_processed += 1
                        total_success_count += 1
                    print(f"✅ Ticket {record_id} created successfully")
                else:
                    error_msg = result.get("message") or result.get("details") or result.get("error") or "Unknown error"
                    if isinstance(error_msg, list):
                        error_msg = "; ".join(error_msg)
                    with records_lock:
                        ticket_error_records.append({
                            **record,
                            "Error Message": error_msg
                        })
                        for comment_id, comment_dict in comments_for_ticket:
                            comment_error_records.append({
                                **comment_dict,
                                "Error Message": error_msg
                            })
                        cumulative_processed += 1
                        total_error_count += 1
                    print(f"❌ Ticket failed: {error_msg}")

                with records_lock:
                    if len(ticket_success_records) >= buffer_size:
                        write_csv_records(success_filename, ticket_success_records, ticket_success_headers, append=True)
                        ticket_success_records.clear()
                    if len(ticket_error_records) >= buffer_size:
                        write_csv_records(error_filename, ticket_error_records, ticket_error_headers, append=True)
                        ticket_error_records.clear()
                    if len(comment_success_records) >= buffer_size:
                        write_csv_records(comment_success_filename, comment_success_records, comment_success_headers, append=True)
                        comment_success_records.clear()
                    if len(comment_error_records) >= buffer_size:
                        write_csv_records(comment_error_filename, comment_error_records, comment_error_headers, append=True)
                        comment_error_records.clear()
                    if len(attachment_success_records) >= buffer_size:
                        write_csv_records(attachment_success_filename, attachment_success_records, attachment_success_headers, append=True)
                        attachment_success_records.clear()
                    if len(attachment_error_records) >= buffer_size:
                        write_csv_records(attachment_error_filename, attachment_error_records, attachment_error_headers, append=True)
                        attachment_error_records.clear()
        else:
            print(f"❌ Failed to retrieve job status for job {job_id}")
            with records_lock:
                total_skipped_count += len(batch)
                cumulative_processed += len(batch)  # Count skipped batch toward processed total
                if app_instance and total_records > 0:
                    app_instance.safe_update_progress_bar(
                        ((processed_records_so_far + cumulative_processed) / total_records) * 100,
                        processed_records_so_far + cumulative_processed
                    )
                    print(f"📊 Progress updated: {processed_records_so_far + cumulative_processed}/{total_records} tickets")

    try:
        submission_futures = {}
        status_futures = {}
        start_time = time.time()
        requests_this_minute = 0
        max_requests_per_minute = 1800

        ticket_id_index = ticket_csv_headers.index(ticket_id_field)
        ticket_ids_set = {str(row[ticket_id_index]).strip() for row in ticket_csv_data}

        print(f"📦 Processing {len(ticket_csv_data)} tickets in batches of {batch_size} with 15 workers")
        for batch_index in range(0, len(ticket_csv_data), batch_size):
            if stop_event and stop_event.is_set():
                print("🛑 Stop event detected, no new batches submitted")
                break
            batch = ticket_csv_data[batch_index:batch_index + batch_size]
            print(f"📤 Preparing batch {batch_index // batch_size + 1} with {len(batch)} tickets")

            ticket_ids = [str(row[ticket_id_index]).strip() for row in batch]

            upload_futures = []
            batch_comments = {}
            comment_id_idx = comments_csv_headers.index(attachment_comment_id_field) if attachment_comment_id_field in comments_csv_headers else None
            file_name_column = [cf for cf, zf in attachment_field_mapping.items() if zf == "file_name"][0]

            print(f"📋 Collecting attachments for {len(comments_csv_data)} comments in batch")
            for idx, comment_row in enumerate(comments_csv_data):
                comment_dict = dict(zip(comments_csv_headers, comment_row))
                ticket_id = str(comment_dict.get(comment_ticket_id_field, "")).strip()
                if ticket_id not in ticket_ids:
                    continue
                comment_id = str(comment_dict.get(attachment_comment_id_field, "")).strip() if comment_id_idx is not None else ""
                file_name = comment_dict.get(file_name_column, "").strip()
                if file_name and comment_id:
                    file_name = os.path.normpath(file_name.replace('\\', '/')).strip()
                    file_path = os.path.join(base_file_dir, f"ticket_{ticket_id}", f"comment_{comment_id}", file_name)
                    if not os.path.exists(file_path):
                        common_extensions = ['.jpg', '.jpeg', '.png', '.pdf', '.txt']
                        for ext in common_extensions:
                            test_path = file_path + ext
                            if os.path.exists(test_path):
                                file_path = test_path
                                print(f"ℹ️ Found file with extension: {file_path}")
                                break
                        else:
                            with records_lock:
                                attachment_error_records.append({
                                    comment_ticket_id_field: ticket_id,
                                    "comment_id": comment_id,
                                    "file_name": file_name,
                                    "error_message": f"File not found: {file_name}"
                                })
                                if len(attachment_error_records) >= buffer_size:
                                    write_csv_records(attachment_error_filename, attachment_error_records, attachment_error_headers, append=True)
                                    attachment_error_records.clear()
                            print(f"❌ Attachment file not found: {file_path}")
                            continue
                    future = upload_executor.submit(upload_attachment, file_path, file_name, ticket_id, comment_id)
                    upload_futures.append((future, ticket_id, comment_id, idx))

            comment_tokens = {}
            for future, ticket_id, comment_id, comment_idx in upload_futures:
                token = future.result()
                if token:
                    comment_tokens.setdefault((ticket_id, comment_id), []).append(token)
                    print(f"✅ Attachment uploaded for ticket {ticket_id}, comment {comment_id} (token: {token})")
                else:
                    print(f"❌ Attachment upload failed for ticket {ticket_id}, comment {comment_id}")

            print(f"📋 Processing comments for batch")
            for idx, comment_row in enumerate(comments_csv_data):
                comment_dict = dict(zip(comments_csv_headers, comment_row))
                ticket_id = str(comment_dict.get(comment_ticket_id_field, "")).strip()
                if ticket_id not in ticket_ids:
                    continue
                comment_id = str(comment_dict.get(attachment_comment_id_field, "")).strip() if comment_id_idx is not None else ""
                comment_data = {
                    zf: comment_dict.get(cf)
                    for cf, zf in comment_field_mapping.items()
                    if cf in comment_dict and zf != " " and zf not in ["ticket_id"]
                }
                if html_body_column and comment_row_numbers and idx < len(comment_row_numbers):
                    row_number = comment_row_numbers[idx]
                    comment_data["html_body"] = html_body_values.get(row_number, "")
                tokens = comment_tokens.get((ticket_id, comment_id), [])
                if tokens:
                    comment_data["uploads"] = tokens
                batch_comments.setdefault(ticket_id, []).append(comment_data)

            mapped_tickets, _ = map_fields(batch, field_mapping, ticket_csv_headers, "Tickets", ticket_custom_field_info, action_type="import")
            for ticket in mapped_tickets:
                ticket_id = str(batch[mapped_tickets.index(ticket)][ticket_id_index]).strip()
                ticket["comments"] = batch_comments.get(ticket_id, [])

            payload = {"tickets": mapped_tickets}
            logger.info(f"Submitting batch {batch_index // batch_size + 1}: {len(batch)} tickets")
            print(f"📡 Submitting POST request {batch_index // batch_size + 1} to {ticket_api_url}")
            with rate_limit_lock:
                requests_this_minute += 1
                elapsed_time = time.time() - start_time
                if requests_this_minute >= max_requests_per_minute:
                    pause_time = max(0, minute_interval - elapsed_time)
                    logger.info(f"⏸️ Pausing submissions for {pause_time:.2f}s to respect rate limit")
                    print(f"⏸️ Pausing submissions for {pause_time:.2f}s to respect rate limit")
                    time.sleep(pause_time)
                    start_time = time.time()
                    requests_this_minute = 0

            future = submit_executor.submit(make_request_with_retries, "POST", ticket_api_url, auth, json=payload, stop_event=stop_event)
            submission_futures[future] = batch

            for future in [f for f in submission_futures if f.done()]:
                batch = submission_futures.pop(future)
                try:
                    response = future.result()
                    if response.status_code == 400:
                        error_message = "Bad request submitted. Please check date formats, role values, dropdowns, or required fields."
                        logger.error(f"Bad Request (400) for {ticket_api_url}: {response.text}")
                        print(f"❌ Bad Request (400) for {ticket_api_url}: {response.text}")
                        raise requests.exceptions.RequestException(error_message, response=response)

                    if response and response.status_code in [200, 201]:
                        job_id = response.json().get("job_status", {}).get("id", "")
                        if job_id:
                            with queued_job_ids_lock:
                                if job_id not in queued_job_ids:
                                    queued_job_ids.add(job_id)
                                    status_future = status_executor.submit(process_job_status, job_id, batch, response)
                                    status_futures[status_future] = (job_id, batch)
                    else:
                        with records_lock:
                            error_msg = f"HTTP {response.status_code if response else 'Unknown'}: {response.text if response else 'No response'}"
                            batch_dicts = [dict(zip(ticket_csv_headers, record)) for record in batch]
                            ticket_error_records.extend([{
                                **record,
                                "Error Message": error_msg
                            } for record in batch_dicts])
                            ticket_comments = {}
                            for idx, comment_row in enumerate(comments_csv_data):
                                ticket_id = str(comment_row[comments_csv_headers.index(comment_ticket_id_field)]).strip()
                                if ticket_id in ticket_ids:
                                    comment_dict = dict(zip(comments_csv_headers, comment_row))
                                    if html_body_column and comment_row_numbers and idx < len(comment_row_numbers):
                                        comment_dict["html_body"] = html_body_values.get(comment_row_numbers[idx], "")
                                    ticket_comments.setdefault(ticket_id, []).append((None, comment_dict))
                            for record in batch_dicts:
                                ticket_id = str(record[ticket_id_field]).strip()
                                comment_error_records.extend([{
                                    **comment_dict,
                                    "Error Message": error_msg
                                } for _, comment_dict in ticket_comments.get(ticket_id, [])])
                            cumulative_processed += 1
                            total_error_count += len(batch)
                            if len(ticket_error_records) >= buffer_size:
                                write_csv_records(error_filename, ticket_error_records, ticket_error_headers, append=True)
                                ticket_error_records.clear()
                            if len(comment_error_records) >= buffer_size:
                                write_csv_records(comment_error_filename, comment_error_records, comment_error_headers, append=True)
                                comment_error_records.clear()
                except Exception as e:
                    logger.error(f"Error in batch submission: {e}")
                    print(f"❌ Error in batch submission: {e}")
                    if isinstance(e, requests.exceptions.RequestException) and e.response and e.response.status_code == 400:
                        raise
                    with records_lock:
                        total_skipped_count += len(batch)

        for future in as_completed(submission_futures):
            batch = submission_futures[future]
            try:
                response = future.result()
                if response.status_code == 400:
                    error_message = "Bad request submitted. Please check date formats, role values, dropdowns, or required fields."
                    logger.error(f"Bad Request (400) for {ticket_api_url}: {response.text}")
                    print(f"❌ Bad Request (400) for {ticket_api_url}: {response.text}")
                    raise requests.exceptions.RequestException(error_message, response=response)

                if response and response.status_code in [200, 201]:
                    job_id = response.json().get("job_status", {}).get("id", "")
                    if job_id:
                        with queued_job_ids_lock:
                            if job_id not in queued_job_ids:
                                queued_job_ids.add(job_id)
                                status_future = status_executor.submit(process_job_status, job_id, batch, response)
                                status_futures[status_future] = (job_id, batch)
            except Exception as e:
                logger.error(f"Error in batch submission: {e}")
                print(f"❌ Error in batch submission: {e}")
                if isinstance(e, requests.exceptions.RequestException) and e.response and e.response.status_code == 400:
                    raise
                with records_lock:
                    total_skipped_count += len(batch)

        for future in as_completed(status_futures):
            future.result()

    except Exception as e:
        logger.error(f"Unexpected error in import_tickets_comments_attachments: {e}")
        print(f"❌ Unexpected error in import_tickets_comments_attachments: {e}")
        raise

    finally:
        submit_executor.shutdown(wait=True)
        status_executor.shutdown(wait=True)
        upload_executor.shutdown(wait=True)

        with records_lock:
            if ticket_success_records:
                write_csv_records(success_filename, ticket_success_records, ticket_success_headers, append=True)
            if ticket_error_records:
                write_csv_records(error_filename, ticket_error_records, ticket_error_headers, append=True)
            if comment_success_records:
                write_csv_records(comment_success_filename, comment_success_records, comment_success_headers, append=True)
            if comment_error_records:
                write_csv_records(comment_error_filename, comment_error_records, comment_error_headers, append=True)
            if attachment_success_records:
                write_csv_records(attachment_success_filename, attachment_success_records, attachment_success_headers, append=True)
            if attachment_error_records:
                write_csv_records(attachment_error_filename, attachment_error_records, attachment_error_headers, append=True)
            if app_instance and total_records > 0 and not stop_event.is_set() and cumulative_processed >= total_records:
                app_instance.safe_update_progress_bar(100)
                print("📊 Progress updated: 100%")

        logger.info(f"Final counts: Success={total_success_count}, Errors={total_error_count}, Skipped={total_skipped_count}, Processed={cumulative_processed}")
        print(f"🏁 Final counts: Success={total_success_count}, Errors={total_error_count}, Skipped={total_skipped_count}, Processed={cumulative_processed}")
        if app_instance:
            app_instance.success_filename = success_filename
            app_instance.error_filename = error_filename
            app_instance.attachment_success_filename = attachment_success_filename
            app_instance.attachment_error_filename = attachment_error_filename
            app_instance.comment_success_filename = comment_success_filename
            app_instance.comment_error_filename = comment_error_filename

    return total_success_count, total_error_count, total_skipped_count, cumulative_processed








def import_custom_objects(
    subdomain,
    email,
    token,
    selected_object,
    csv_data,
    csv_headers,
    field_mapping,
    save_directory,
    stop_event=None,
    progress_bar=None,
    app_instance=None,
    chunk_size=100,
    success_filename=None,
    error_filename=None,
    processed_records_so_far=0
):
    """
    Imports custom object records to Zendesk in batches using the /jobs endpoint.
    
    Args:
        subdomain (str): Zendesk subdomain.
        email (str): Zendesk email for authentication.
        token (str): Zendesk API token.
        selected_object (str): Custom object key (e.g., 'product').
        csv_data (list): List of CSV records.
        csv_headers (list): List of CSV header names.
        field_mapping (dict): Mapping of CSV columns to Zendesk fields.
        save_directory (str): Directory to save success/error CSVs.
        stop_event (threading.Event, optional): Event to stop the operation.
        progress_bar (callable, optional): Callback for progress updates.
        app_instance: Application instance for progress updates.
        chunk_size (int): Size of batches for API calls.
        success_filename (str, optional): Pre-generated filename for success CSV.
        error_filename (str, optional): Pre-generated filename for error CSV.
    
    Returns:
        tuple: (success_count, error_count, skipped_count, total_processed)
    """
    base_url = f"https://{subdomain}.zendesk.com"
    auth = HTTPBasicAuth(f"{email}/token", token)

    # Use provided filenames or fallback to default
    success_filename = success_filename or os.path.join(save_directory, f"{selected_object}_import_success.csv")
    error_filename = error_filename or os.path.join(save_directory, f"{selected_object}_import_errors.csv")
    success_headers = csv_headers + ["ID", "Status"]
    error_headers = csv_headers + ["Error Message"]

    api_url = f"{base_url}/api/v2/custom_objects/{selected_object}/jobs"

    if not save_directory:
        logger.error("No save directory specified for import operation")
        return 0, 0, 0, 0

    try:
        fields, custom_field_info = fetch_zendesk_fields(
            subdomain, email, token, selected_object, "import"
        )
        logger.info(f"Zendesk fields for {selected_object}: {fields}")
    except Exception as e:
        logger.error(f"Failed to fetch Zendesk fields: {e}")
        return 0, 0, 0, 0

    with processed_job_ids_lock:
        processed_job_ids.clear()
    with queued_job_ids_lock:
        queued_job_ids.clear()

    batch_size = chunk_size
    success_records = []
    error_records = []
    total_records = len(csv_data)
    cumulative_processed = 0
    total_success_count = 0
    total_error_count = 0
    total_skipped_count = 0

    logger.info(f"Total records to process: {total_records}")
    print(f"Total records to process: {total_records}")

    submit_executor = ThreadPoolExecutor(max_workers=1)
    status_executor = ThreadPoolExecutor(max_workers=15)

    def write_csv_records(filename, records, headers, append=True):
        mode = 'a' if append else 'w'
        logger.debug(f"Writing {len(records)} records to {filename} in mode '{mode}'")
        try:
            with open(filename, mode, newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                if not append and not os.path.exists(filename):
                    writer.writeheader()
                filtered_records = [
                    {key: record.get(key, "") for key in headers}
                    for record in records
                ]
                writer.writerows(filtered_records)
                f.flush()
            print(f"📝 Wrote {len(records)} records to {filename} (append={append})")
        except Exception as e:
            logger.error(f"Failed to write to {filename}: {e}")
            print(f"❌ Failed to write to {filename}: {e}")

    def process_job_status(job_id, batch, response):
        nonlocal cumulative_processed, total_success_count, total_error_count
        nonlocal total_skipped_count
        with processed_job_ids_lock:
            if job_id in processed_job_ids:
                logger.info(f"🛑 Job {job_id} already processed. Skipping.")
                return
            processed_job_ids.add(job_id)

        job_url = f"{base_url}/api/v2/job_statuses/{job_id}.json"
        jittered_sleep = 4 + random.uniform(0, 0.3)
        time.sleep(jittered_sleep)
        logger.info(f"⏳ Applied jittered sleep of {jittered_sleep:.2f}s for job status check")
        job_results = check_job_status(job_url, auth, stop_event=stop_event)
        if job_results:
            results = job_results.get("results", [])
            batch_dicts = [dict(zip(csv_headers, record)) for record in batch]
            for index, result in enumerate(results):
                if index >= len(batch_dicts):
                    logger.warning(f"Result index {index} exceeds batch size {len(batch_dicts)}")
                    break
                record = batch_dicts[index]
                zendesk_id = result.get("id")
                status = result.get("status", "")

                if status == "Created" and zendesk_id:
                    with threading.Lock():
                        success_records.append({
                            **record,
                            "ID": zendesk_id,
                            "Status": "Created"
                        })
                        cumulative_processed += 1
                        if len(success_records) >= chunk_size:
                            write_csv_records(success_filename, success_records, success_headers, append=True)
                            total_success_count += len(success_records)
                            success_records.clear()
                            #if app_instance:
                                #app_instance.safe_update_progress_bar(
                                    #(cumulative_processed / total_records) * 100
                                #)
                else:
                    error_msg = (
                        result.get("details") or
                        result.get("error") or
                        result.get("errors") or
                        "Unknown error"
                    )
                    if isinstance(error_msg, list):
                        error_msg = "; ".join(error_msg)
                    with threading.Lock():
                        error_records.append({
                            **record,
                            "Error Message": error_msg
                        })
                        cumulative_processed += 1
                        if len(error_records) >= chunk_size:
                            write_csv_records(error_filename, error_records, error_headers, append=True)
                            total_error_count += len(error_records)
                            error_records.clear()
                            #if app_instance:
                                #app_instance.safe_update_progress_bar(
                                    #cumulative_processed / total_records) * 100
                                #)
        else:
            with threading.Lock():
                total_skipped_count += len(batch)
                logger.error(f"❌ Failed to retrieve job status for job {job_id}")

    try:
        submission_futures = {}
        status_futures = {}
        start_time = time.time()
        requests_this_minute = 0
        minute_interval = 60
        request_delay = 1

        for batch_index in range(0, len(csv_data), batch_size):
            if stop_event and stop_event.is_set():
                logger.info("🛑 Stop detected. No new batches submitted.")
                break

            batch = csv_data[batch_index:batch_index + batch_size]
            mapped_records, _ = map_fields(
                batch,
                field_mapping,
                csv_headers,
                selected_object,
                custom_field_info,
                action_type="import"
            )

            payload = {
                "job": {
                    "action": "create",
                    "items": mapped_records
                }
            }
            logger.info(f"Submitting batch: {len(batch)} records")
            print(f"📡 Submitting POST request {batch_index // batch_size + 1} to {api_url}")

            requests_this_minute += 1
            elapsed_time = time.time() - start_time
            if elapsed_time >= minute_interval:
                pause_time = 1
                logger.info(f"⏸️ Pausing submissions for {pause_time}s")
                time.sleep(pause_time)
                start_time = time.time()
                requests_this_minute = 0

            jittered_delay = request_delay + random.uniform(0, 0.3)
            time.sleep(jittered_delay)
            logger.info(f"⏳ Applied delay of {jittered_delay:.3f}s before submission")

            future = submit_executor.submit(
                make_request_with_retries,
                "POST",
                api_url,
                auth,
                json=payload,
                stop_event=stop_event
            )
            submission_futures[future] = batch

            for future in [f for f in submission_futures if f.done()]:
                batch = submission_futures.pop(future)
                try:
                    response = future.result()
                    if response.status_code == 400:
                        error_message = (
                            "Bad request submitted. Please check the following:\n"
                            "- Date formats (e.g., YYYY-MM-DD).\n"
                            "- Required fields are present and correctly mapped."
                        )
                        logger.error(f"❌ Bad Request (400) for {api_url}: {response.text}")
                        raise requests.exceptions.RequestException(error_message, response=response)
                    elif response.status_code == 422:
                        error_message = (
                            "Error in payload. Check drop-down values, dates, integers, etc."
                        )
                        logger.error(f"❌ Unprocessable Entity (422) for {api_url}: {response.text}")
                        raise requests.exceptions.RequestException(error_message, response=response)

                    if response and response.status_code in [200, 201]:
                        job_id = response.json().get("job_status", {}).get("id", "")
                        if job_id:
                            with queued_job_ids_lock:
                                if job_id not in queued_job_ids:
                                    queued_job_ids.add(job_id)
                                    status_future = status_executor.submit(
                                        process_job_status,
                                        job_id,
                                        batch,
                                        response
                                    )
                                    status_futures[status_future] = (job_id, batch)
                    else:
                        with threading.Lock():
                            error_msg = (
                                f"HTTP {response.status_code if response else 'Unknown'}: "
                                f"{response.text if response else 'No response'}"
                            )
                            batch_dicts = [dict(zip(csv_headers, record)) for record in batch]
                            error_records.extend([{
                                **record,
                                "Error Message": error_msg
                            } for record in batch_dicts])
                            cumulative_processed += len(batch)
                            total_error_count += len(batch)
                            if len(error_records) >= chunk_size:
                                write_csv_records(error_filename, error_records, error_headers, append=True)
                                total_error_count += len(error_records)
                                error_records.clear()
                            #if app_instance:
                                #app_instance.safe_update_progress_bar(
                                    #(cumulative_processed / total_records) * 100
                               #)
                except Exception as e:
                    logger.error(f"Unexpected error in batch submission: {e}")
                    if (
                        isinstance(e, requests.exceptions.RequestException) and
                        e.response and
                        e.response.status_code in [400, 422]
                    ):
                        raise
                    with threading.Lock():
                        total_skipped_count += len(batch)
                        #if app_instance:
                            #app_instance.safe_update_progress_bar(
                                #(cumulative_processed / total_records) * 100
                            #)

        for future in as_completed(submission_futures):
            batch = submission_futures[future]
            try:
                response = future.result()
                if response.status_code == 400:
                    error_message = (
                        "Bad request submitted. Please check the following:\n"
                        "- Date formats (e.g., YYYY-MM-DD).\n"
                        "- Required fields are present and correctly mapped."
                    )
                    logger.error(f"❌ Bad Request (400) for {api_url}: {response.text}")
                    raise requests.exceptions.RequestException(error_message, response=response)
                elif response.status_code == 422:
                    error_message = (
                        "Error in payload. Check drop-down values, dates, integers, etc."
                    )
                    logger.error(f"❌ Unprocessable Entity (422) for {api_url}: {response.text}")
                    raise requests.exceptions.RequestException(error_message, response=response)

                if response and response.status_code in [200, 201]:
                    job_id = response.json().get("job_status", {}).get("id", "")
                    if job_id:
                        with queued_job_ids_lock:
                            if job_id not in queued_job_ids:
                                queued_job_ids.add(job_id)
                                status_future = status_executor.submit(
                                    process_job_status,
                                    job_id,
                                    batch,
                                    response
                                )
                                status_futures[status_future] = (job_id, batch)
            except Exception as e:
                logger.error(f"Error in batch submission: {e}")
                if (
                    isinstance(e, requests.exceptions.RequestException) and
                    e.response and
                    e.response.status_code in [400, 422]
                ):
                    raise
                with threading.Lock():
                    total_skipped_count += len(batch)

        for future in as_completed(status_futures):
            future.result()

    except Exception as e:
        logger.error(f"Unexpected error in import_custom_objects: {e}")
        raise

    finally:
        submit_executor.shutdown(wait=True)
        status_executor.shutdown(wait=True)

        with threading.Lock():
            if success_records:
                write_csv_records(success_filename, success_records, success_headers, append=True)
                total_success_count += len(success_records)
                success_records.clear()
            if error_records:
                write_csv_records(error_filename, error_records, error_headers, append=True)
                total_error_count += len(error_records)
                error_records.clear()
            if (
                app_instance and
                total_records > 0 and
                not stop_event.is_set() and
                cumulative_processed >= total_records
            ):
                app_instance.safe_update_progress_bar(100)

        logger.info(
            f"Final counts: Success={total_success_count}, Errors={total_error_count}, "
            f"Skipped={total_skipped_count}, Cumulative Processed={cumulative_processed}"
        )
        if app_instance:
            app_instance.success_filename = success_filename
            app_instance.error_filename = error_filename

    return total_success_count, total_error_count, total_skipped_count, cumulative_processed










def update_custom_objects(
    subdomain,
    email,
    token,
    selected_object,
    csv_data,
    csv_headers,
    field_mapping,
    save_directory,
    stop_event=None,
    progress_bar=None,
    app_instance=None,
    chunk_size=100,
    success_filename=None,
    error_filename=None,
    processed_records_so_far=0
):
    """
    Updates custom object records in Zendesk in batches using the /jobs endpoint.
    
    Args:
        subdomain (str): Zendesk subdomain.
        email (str): Zendesk email for authentication.
        token (str): Zendesk API token.
        selected_object (str): Custom object key (e.g., 'product').
        csv_data (list): List of CSV records.
        csv_headers (list): List of CSV header names.
        field_mapping (dict): Mapping of CSV columns to Zendesk fields.
        save_directory (str): Directory to save success/error CSVs.
        stop_event (threading.Event, optional): Event to stop the operation.
        progress_bar (callable, optional): Callback for progress updates.
        app_instance: Application instance for progress updates.
        chunk_size (int): Size of batches for API calls.
        success_filename (str, optional): Pre-generated filename for success CSV.
        error_filename (str, optional): Pre-generated filename for error CSV.
    
    Returns:
        tuple: (success_count, error_count, skipped_count, total_processed)
    """
    base_url = f"https://{subdomain}.zendesk.com"
    auth = HTTPBasicAuth(f"{email}/token", token)

    # Use provided filenames or fallback to default
    success_filename = success_filename or os.path.join(save_directory, f"{selected_object}_update_success.csv")
    error_filename = error_filename or os.path.join(save_directory, f"{selected_object}_update_errors.csv")
    success_headers = csv_headers + ["Status", "ID - preserved"]
    error_headers = csv_headers + ["Error Message"]

    api_url = f"{base_url}/api/v2/custom_objects/{selected_object}/jobs"

    if not save_directory:
        logger.error("No save directory specified for update operation")
        return 0, 0, 0, 0

    try:
        fields, custom_field_info = fetch_zendesk_fields(
            subdomain, email, token, selected_object, "update"
        )
        logger.info(f"Zendesk fields for {selected_object}: {fields}")
    except Exception as e:
        logger.error(f"Failed to fetch Zendesk fields: {e}")
        return 0, 0, 0, 0

    if "id" not in field_mapping.values():
        logger.error("Field mapping does not include a mapping for 'id', required for updates")
        return 0, 0, 0, 0

    id_column = [
        csv_field for csv_field, zendesk_field in field_mapping.items()
        if zendesk_field == "id"
    ][0]
    if id_column not in csv_headers:
        logger.error(f"CSV data does not include the mapped 'id' column: {id_column}")
        return 0, 0, 0, 0

    with processed_job_ids_lock:
        processed_job_ids.clear()
    with queued_job_ids_lock:
        queued_job_ids.clear()

    batch_size = chunk_size
    success_records = []
    error_records = []
    total_records = len(csv_data)
    cumulative_processed = 0
    total_success_count = 0
    total_error_count = 0
    total_skipped_count = 0

    logger.info(f"Total records to process: {total_records}")
    print(f"Total records to process: {total_records}")

    submit_executor = ThreadPoolExecutor(max_workers=1)
    status_executor = ThreadPoolExecutor(max_workers=15)

    def write_csv_records(filename, records, headers, append=True):
        mode = 'a' if append else 'w'
        logger.debug(f"Writing {len(records)} records to {filename} in mode '{mode}'")
        try:
            with open(filename, mode, newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                if not append and not os.path.exists(filename):
                    writer.writeheader()
                filtered_records = [
                    {key: record.get(key, "") for key in headers}
                    for record in records
                ]
                writer.writerows(filtered_records)
                f.flush()
            print(f"📝 Wrote {len(records)} records to {filename} (append={append})")
        except Exception as e:
            logger.error(f"Failed to write to {filename}: {e}")
            print(f"❌ Failed to write to {filename}: {e}")

    def process_job_status(job_id, batch, response):
        nonlocal cumulative_processed, total_success_count, total_error_count
        nonlocal total_skipped_count
        with processed_job_ids_lock:
            if job_id in processed_job_ids:
                logger.info(f"🛑 Job {job_id} already processed. Skipping.")
                return
            processed_job_ids.add(job_id)

        job_url = f"{base_url}/api/v2/job_statuses/{job_id}.json"
        temp_success = []
        temp_error = []
        jittered_sleep = 4 + random.uniform(0, 0.3)
        time.sleep(jittered_sleep)
        logger.info(f"⏳ Applied jittered sleep of {jittered_sleep:.2f}s for job status check")
        job_results = check_job_status(job_url, auth)
        if job_results:
            results = job_results.get("results", [])
            batch_dicts = [dict(zip(csv_headers, record)) for record in batch]
            for index, result in enumerate(results):
                if index >= len(batch_dicts):
                    logger.warning(f"Result index {index} exceeds batch size {len(batch_dicts)}")
                    break
                record = batch_dicts[index]
                zendesk_id = result.get("id")
                status = result.get("status", "")

                if status == "Update" and zendesk_id:
                    temp_success.append({
                        **record,
                        "Status": "Updated",
                        "ID - preserved": record.get(id_column, "")  # Use original ID
                    })
                else:
                    error_msg = (
                        result.get("details") or
                        result.get("error") or
                        result.get("errors") or
                        "Unknown error"
                    )
                    if isinstance(error_msg, list):
                        error_msg = "; ".join(error_msg)
                    temp_error.append({
                        **record,
                        "Error Message": error_msg
                    })

            with threading.Lock():
                if temp_success:
                    success_records.extend(temp_success)
                    if len(success_records) >= chunk_size:
                        write_csv_records(success_filename, success_records, success_headers, append=True)
                        total_success_count += len(success_records)
                        success_records.clear()
                    #if app_instance:
                        #app_instance.safe_update_progress_bar(
                            #(cumulative_processed / total_records) * 100
                        #)
                if temp_error:
                    error_records.extend(temp_error)
                    if len(error_records) >= chunk_size:
                        write_csv_records(error_filename, error_records, error_headers, append=True)
                        total_error_count += len(error_records)
                        error_records.clear()
                    #if app_instance:
                        #app_instance.safe_update_progress_bar(
                            #(cumulative_processed / total_records) * 100
                        #)
                cumulative_processed += len(temp_success) + len(temp_error)
                logger.info(
                    f"Cumulative processed: {cumulative_processed}, "
                    f"Success: {len(temp_success)}, Errors: {len(temp_error)}"
                )
        else:
            with threading.Lock():
                total_skipped_count += len(batch)
                logger.error(f"❌ Failed to retrieve job status for job {job_id}")

    try:
        submission_futures = {}
        status_futures = {}
        start_time = time.time()
        requests_this_minute = 0
        minute_interval = 60
        request_delay = 1

        for batch_index in range(0, len(csv_data), batch_size):
            if stop_event and stop_event.is_set():
                logger.info("🛑 Stop detected. No new batches submitted.")
                break

            batch = csv_data[batch_index:batch_index + batch_size]
            mapped_records, _ = map_fields(
                batch,
                field_mapping,
                csv_headers,
                selected_object,
                custom_field_info,
                action_type="update"
            )

            for record in mapped_records:
                if "id" not in record:
                    logger.warning(
                        f"Skipping record in batch {batch_index // batch_size + 1}: No 'id' present"
                    )
                    with threading.Lock():
                        error_records.append({
                            **record,
                            "Error Message": "No 'id' present"
                        })
                        cumulative_processed += 1
                        if len(error_records) >= chunk_size:
                            write_csv_records(error_filename, error_records, error_headers, append=True)
                            total_error_count += len(error_records)
                            error_records.clear()
                        #if app_instance:
                            #app_instance.safe_update_progress_bar(
                                #(cumulative_processed / total_records) * 100
                            #)
                    continue

            payload_records = [record for record in mapped_records if "id" in record]
            if not payload_records:
                logger.warning(
                    f"Batch {batch_index // batch_size + 1} skipped: No valid records with 'id'"
                )
                cumulative_processed += len(batch)
                total_skipped_count += len(batch)
                #if app_instance:
                    #app_instance.safe_update_progress_bar(
                        #(cumulative_processed / total_records) * 100
                    #)
                continue

            payload = {
                "job": {
                    "action": "update",
                    "items": payload_records
                }
            }
            logger.info(f"Submitting batch: {len(batch)} records")
            print(f"📡 Submitting POST request {batch_index // batch_size + 1} to {api_url}")

            requests_this_minute += 1
            elapsed_time = time.time() - start_time
            if elapsed_time >= minute_interval:
                pause_time = 1
                logger.info(f"⏸️ Pausing submissions for {pause_time}s")
                time.sleep(pause_time)
                start_time = time.time()
                requests_this_minute = 0

            jittered_delay = request_delay + random.uniform(0, 0.3)
            time.sleep(jittered_delay)
            logger.info(f"⏳ Applied delay of {jittered_delay:.3f}s before submission")

            future = submit_executor.submit(
                make_request_with_retries,
                "POST",  # Jobs endpoint uses POST for all actions
                api_url,
                auth,
                json=payload,
                stop_event=stop_event
            )
            submission_futures[future] = batch

            for future in [f for f in submission_futures if f.done()]:
                batch = submission_futures.pop(future)
                try:
                    response = future.result()
                    if response.status_code == 400:
                        error_message = (
                            "Bad request submitted. Please check the following:\n"
                            "- Date formats (e.g., YYYY-MM-DD).\n"
                            "- Required fields are present and correctly mapped."
                        )
                        logger.error(f"❌ Bad Request (400) for {api_url}: {response.text}")
                        raise requests.exceptions.RequestException(error_message, response=response)

                    if response and response.status_code in [200, 201]:
                        job_id = response.json().get("job_status", {}).get("id", "")
                        if job_id:
                            with queued_job_ids_lock:
                                if job_id not in queued_job_ids:
                                    queued_job_ids.add(job_id)
                                    status_future = status_executor.submit(
                                        process_job_status,
                                        job_id,
                                        batch,
                                        response
                                    )
                                    status_futures[status_future] = (job_id, batch)
                    else:
                        with threading.Lock():
                            error_msg = (
                                f"HTTP {response.status_code if response else 'Unknown'}: "
                                f"{response.text if response else 'No response'}"
                            )
                            batch_dicts = [dict(zip(csv_headers, record)) for record in batch]
                            error_records.extend([{
                                **record,
                                "Error Message": error_msg
                            } for record in batch_dicts])
                            cumulative_processed += len(batch)
                            total_error_count += len(batch)
                            if len(error_records) >= chunk_size:
                                write_csv_records(error_filename, error_records, error_headers, append=True)
                                total_error_count += len(error_records)
                                error_records.clear()
                            #if app_instance:
                                #app_instance.safe_update_progress_bar(
                                    #(cumulative_processed / total_records) * 100
                                #)
                except Exception as e:
                    logger.error(f"Unexpected error in batch submission: {e}")
                    if (
                        isinstance(e, requests.exceptions.RequestException) and
                        e.response and
                        e.response.status_code == 400
                    ):
                        raise
                    with threading.Lock():
                        total_skipped_count += len(batch)
                        #if app_instance:
                            #app_instance.safe_update_progress_bar(
                                #(cumulative_processed / total_records) * 100
                            #)

        for future in as_completed(submission_futures):
            batch = submission_futures[future]
            try:
                response = future.result()
                if response.status_code == 400:
                    error_message = (
                        "Bad request submitted. Please check the following:\n"
                        "- Date formats (e.g., YYYY-MM-DD).\n"
                        "- Required fields are present and correctly mapped."
                    )
                    logger.error(f"❌ Bad Request (400) for {api_url}: {response.text}")
                    raise requests.exceptions.RequestException(error_message, response=response)

                if response and response.status_code in [200, 201]:
                    job_id = response.json().get("job_status", {}).get("id", "")
                    if job_id:
                        with queued_job_ids_lock:
                            if job_id not in queued_job_ids:
                                queued_job_ids.add(job_id)
                                status_future = status_executor.submit(
                                    process_job_status,
                                    job_id,
                                    batch,
                                    response
                                )
                                status_futures[status_future] = (job_id, batch)
            except Exception as e:
                logger.error(f"Error in batch submission: {e}")
                if (
                    isinstance(e, requests.exceptions.RequestException) and
                    e.response and
                    e.response.status_code == 400
                ):
                    raise
                with threading.Lock():
                    total_skipped_count += len(batch)

        for future in as_completed(status_futures):
            future.result()

    except Exception as e:
        logger.error(f"Unexpected error in update_custom_objects: {e}")
        raise

    finally:
        submit_executor.shutdown(wait=True)
        status_executor.shutdown(wait=True)

        with threading.Lock():
            if success_records:
                write_csv_records(success_filename, success_records, success_headers, append=True)
                total_success_count += len(success_records)
                success_records.clear()
            if error_records:
                write_csv_records(error_filename, error_records, error_headers, append=True)
                total_error_count += len(error_records)
                error_records.clear()
            if (
                app_instance and
                total_records > 0 and
                not stop_event.is_set() and
                cumulative_processed >= total_records
            ):
                app_instance.safe_update_progress_bar(100)

        logger.info(
            f"Final counts: Success={total_success_count}, Errors={total_error_count}, "
            f"Skipped={total_skipped_count}, Cumulative Processed={cumulative_processed}"
        )
        if app_instance:
            app_instance.success_filename = success_filename
            app_instance.error_filename = error_filename

    return total_success_count, total_error_count, total_skipped_count, cumulative_processed








def delete_custom_objects(
    subdomain,
    email,
    token,
    selected_object,
    csv_data,
    csv_headers,
    field_mapping,
    save_directory,
    stop_event=None,
    progress_bar=None,
    app_instance=None,
    chunk_size=100,
    success_filename=None,
    error_filename=None,
    processed_records_so_far=0
):
    """
    Deletes custom object records from Zendesk in batches using the /jobs endpoint.
    
    Args:
        subdomain (str): Zendesk subdomain.
        email (str): Zendesk email for authentication.
        token (str): Zendesk API token.
        selected_object (str): Custom object key (e.g., 'product').
        csv_data (list): List of CSV records.
        csv_headers (list): List of CSV header names.
        field_mapping (dict): Mapping of CSV columns to Zendesk fields.
        save_directory (str): Directory to save success/error CSVs.
        stop_event (threading.Event, optional): Event to stop the operation.
        progress_bar (callable, optional): Callback for progress updates.
        app_instance: Application instance for progress updates.
        chunk_size (int): Size of batches for API calls.
        success_filename (str, optional): Pre-generated filename for success CSV.
        error_filename (str, optional): Pre-generated filename for error CSV.
    
    Returns:
        tuple: (success_count, error_count, skipped_count, total_processed)
    """
    base_url = f"https://{subdomain}.zendesk.com"
    auth = HTTPBasicAuth(f"{email}/token", token)

    # Use provided filenames or fallback to default
    success_filename = success_filename or os.path.join(save_directory, f"{selected_object}_delete_success.csv")
    error_filename = error_filename or os.path.join(save_directory, f"{selected_object}_delete_errors.csv")
    success_headers = csv_headers + ["Status", "ID - preserved"]
    error_headers = csv_headers + ["Error Message"]

    api_url = f"{base_url}/api/v2/custom_objects/{selected_object}/jobs"

    if not save_directory:
        logger.error("No save directory specified for delete operation")
        return 0, 0, 0, 0

    id_column = [
        csv_field for csv_field, zendesk_field in field_mapping.items()
        if zendesk_field == "id"
    ]
    if not id_column:
        logger.error("No 'id' field mapped in field_mapping")
        return 0, 0, 0, 0
    id_column = id_column[0]

    if id_column not in csv_headers:
        logger.error(f"CSV data does not include the mapped 'id' column: {id_column}")
        return 0, 0, 0, 0

    with processed_job_ids_lock:
        processed_job_ids.clear()
    with queued_job_ids_lock:
        queued_job_ids.clear()

    batch_size = chunk_size
    success_records = []
    error_records = []
    total_records = len(csv_data)
    cumulative_processed = 0
    total_success_count = 0
    total_error_count = 0
    total_skipped_count = 0

    logger.info(f"Total records to process: {total_records}")
    print(f"Total records to process: {total_records}")

    submit_executor = ThreadPoolExecutor(max_workers=1)
    status_executor = ThreadPoolExecutor(max_workers=15)

    def write_csv_records(filename, records, headers, append=True):
        mode = 'a' if append else 'w'
        logger.debug(f"Writing {len(records)} records to {filename} in mode '{mode}'")
        try:
            with open(filename, mode, newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                if not append and not os.path.exists(filename):
                    writer.writeheader()
                filtered_records = [
                    {key: record.get(key, "") for key in headers}
                    for record in records
                ]
                writer.writerows(filtered_records)
                f.flush()
            print(f"📝 Wrote {len(records)} records to {filename} (append={append})")
        except Exception as e:
            logger.error(f"Failed to write to {filename}: {e}")
            print(f"❌ Failed to write to {filename}: {e}")

    def process_job_status(job_id, batch, response):
        nonlocal cumulative_processed, total_success_count, total_error_count
        nonlocal total_skipped_count
        with processed_job_ids_lock:
            if job_id in processed_job_ids:
                logger.info(f"🛑 Job {job_id} already processed. Skipping.")
                return
            processed_job_ids.add(job_id)

        job_url = f"{base_url}/api/v2/job_statuses/{job_id}.json"
        temp_success = []
        temp_error = []
        jittered_sleep = 4 + random.uniform(0, 0.3)
        time.sleep(jittered_sleep)
        logger.info(f"⏳ Applied jittered sleep of {jittered_sleep:.2f}s for job status check")
        job_results = check_job_status(job_url, auth)
        if job_results:
            results = job_results.get("results", [])
            batch_dicts = [dict(zip(csv_headers, record)) for record in batch]

            for index, result in enumerate(results):
                if index >= len(batch_dicts):
                    logger.warning(f"Result index {index} exceeds batch size {len(batch_dicts)}")
                    break
                record = batch_dicts[index]
                zendesk_id = result.get("id")
                status = result.get("status", "")

                if status in ["deleted", "Deleted"] and zendesk_id:
                    temp_success.append({
                        **record,
                        "Status": "Deleted",
                        "ID - preserved": record.get(id_column, "")  # Use original ID
                    })
                else:
                    error_msg = (
                        result.get("details") or
                        result.get("error") or
                        result.get("errors") or
                        "Unknown error"
                    )
                    if isinstance(error_msg, list):
                        error_msg = "; ".join(error_msg)
                    temp_error.append({
                        **record,
                        "Error Message": error_msg
                    })

            with threading.Lock():
                if temp_success:
                    success_records.extend(temp_success)
                    if len(success_records) >= chunk_size:
                        write_csv_records(success_filename, success_records, success_headers, append=True)
                        total_success_count += len(success_records)
                        success_records.clear()
                    #if app_instance:
                        #app_instance.safe_update_progress_bar(
                            #(cumulative_processed / total_records) * 100
                        #)
                if temp_error:
                    error_records.extend(temp_error)
                    if len(error_records) >= chunk_size:
                        write_csv_records(error_filename, error_records, error_headers, append=True)
                        total_error_count += len(error_records)
                        error_records.clear()
                    #if app_instance:
                        #app_instance.safe_update_progress_bar(
                            #(cumulative_processed / total_records) * 100
                        #)
                cumulative_processed += len(temp_success) + len(temp_error)
                logger.info(
                    f"Cumulative processed: {cumulative_processed}, "
                    f"Success: {len(temp_success)}, Errors: {len(temp_error)}"
                )
        else:
            with threading.Lock():
                total_skipped_count += len(batch)
                logger.error(f"❌ Failed to retrieve job status for job {job_id}")

    try:
        submission_futures = {}
        status_futures = {}
        start_time = time.time()
        requests_this_minute = 0
        minute_interval = 60
        request_delay = 1.2

        for batch_index in range(0, len(csv_data), batch_size):
            if stop_event and stop_event.is_set():
                logger.info("🛑 Stop detected. No new batches submitted.")
                break

            batch = csv_data[batch_index:batch_index + batch_size]
            ids = []
            batch_dicts = [dict(zip(csv_headers, record)) for record in batch]
            for record in batch_dicts:
                if id_column in record and record[id_column]:
                    id_value = str(record[id_column])  # Ensure ID is a string
                    ids.append(id_value)
                else:
                    logger.error(f"❌ Mapped ID column '{id_column}' not found or empty in record")
                    with threading.Lock():
                        error_records.append({
                            **record,
                            "Error Message": f"Mapped ID column '{id_column}' not found or empty"
                        })
                        cumulative_processed += 1
                        if len(error_records) >= chunk_size:
                            write_csv_records(error_filename, error_records, error_headers, append=True)
                            total_error_count += len(error_records)
                            error_records.clear()
                        #if app_instance:
                            #app_instance.safe_update_progress_bar(
                                #(cumulative_processed / total_records) * 100
                            #)

            if not ids:
                logger.warning(f"Batch {batch_index // batch_size + 1} skipped: No valid IDs")
                cumulative_processed += len(batch)
                total_skipped_count += len(batch)
                #if app_instance:
                    #app_instance.safe_update_progress_bar(
                        #(cumulative_processed / total_records) * 100
                    #)
                continue

            payload = {
                "job": {
                    "action": "delete",
                    "items": ids  # Array of strings
                }
            }
            logger.info(f"Submitting batch: {len(batch)} records")
            print(f"📡 Submitting POST request {batch_index // batch_size + 1} to {api_url}")

            requests_this_minute += 1
            elapsed_time = time.time() - start_time
            if elapsed_time >= minute_interval:
                pause_time = 1
                logger.info(f"⏸️ Pausing submissions for {pause_time}s")
                time.sleep(pause_time)
                start_time = time.time()
                requests_this_minute = 0

            jittered_delay = request_delay + random.uniform(0, 0.3)
            time.sleep(jittered_delay)
            logger.info(f"⏳ Applied delay of {jittered_delay:.3f}s before submission")

            future = submit_executor.submit(
                make_request_with_retries,
                "POST",
                api_url,
                auth,
                json=payload,
                stop_event=stop_event
            )
            submission_futures[future] = batch

            for future in [f for f in submission_futures if f.done()]:
                batch = submission_futures.pop(future)
                try:
                    response = future.result()
                    if response.status_code == 400:
                        error_message = (
                            "Bad request submitted. Please check the following:\n"
                            "- ID values must be valid (numeric or string external IDs).\n"
                            "- Required fields are present."
                        )
                        logger.error(f"❌ Bad Request (400) for {api_url}: {response.text}")
                        raise requests.exceptions.RequestException(error_message, response=response)

                    if response and response.status_code in [200, 201]:
                        job_id = response.json().get("job_status", {}).get("id", "")
                        if job_id:
                            with queued_job_ids_lock:
                                if job_id not in queued_job_ids:
                                    queued_job_ids.add(job_id)
                                    status_future = status_executor.submit(
                                        process_job_status,
                                        job_id,
                                        batch,
                                        response
                                    )
                                    status_futures[status_future] = (job_id, batch)
                    else:
                        with threading.Lock():
                            error_msg = (
                                f"HTTP {response.status_code if response else 'Unknown'}: "
                                f"{response.text if response else 'No response'}"
                            )
                            batch_dicts = [dict(zip(csv_headers, record)) for record in batch]
                            error_records.extend([{
                                **record,
                                "Error Message": error_msg
                            } for record in batch_dicts])
                            cumulative_processed += len(batch)
                            total_error_count += len(batch)
                            if len(error_records) >= chunk_size:
                                write_csv_records(error_filename, error_records, error_headers, append=True)
                                total_error_count += len(error_records)
                                error_records.clear()
                            #if app_instance:
                                #app_instance.safe_update_progress_bar(
                                    #(cumulative_processed / total_records) * 100
                                #)
                except Exception as e:
                    logger.error(f"Unexpected error in batch submission: {e}")
                    with threading.Lock():
                        total_skipped_count += len(batch)
                        #if app_instance:
                            #app_instance.safe_update_progress_bar(
                                #(cumulative_processed / total_records) * 100
                            #)

        for future in as_completed(submission_futures):
            batch = submission_futures[future]
            try:
                response = future.result()
                if response.status_code == 400:
                    error_message = (
                        "Bad request submitted. Please check the following:\n"
                        "- ID values must be valid (numeric or string external IDs).\n"
                        "- Required fields are present."
                    )
                    logger.error(f"❌ Bad Request (400) for {api_url}: {response.text}")
                    raise requests.exceptions.RequestException(error_message, response=response)

                if response and response.status_code in [200, 201]:
                    job_id = response.json().get("job_status", {}).get("id", "")
                    if job_id:
                        with queued_job_ids_lock:
                            if job_id not in queued_job_ids:
                                queued_job_ids.add(job_id)
                                status_future = status_executor.submit(
                                    process_job_status,
                                    job_id,
                                    batch,
                                    response
                                )
                                status_futures[status_future] = (job_id, batch)
            except Exception as e:
                logger.error(f"Error in batch submission: {e}")
                with threading.Lock():
                    total_skipped_count += len(batch)

        for future in as_completed(status_futures):
            future.result()

    except Exception as e:
        logger.error(f"Unexpected error in delete_custom_objects: {e}")
        raise

    finally:
        submit_executor.shutdown(wait=True)
        status_executor.shutdown(wait=True)

        with threading.Lock():
            if success_records:
                write_csv_records(success_filename, success_records, success_headers, append=True)
                total_success_count += len(success_records)
                success_records.clear()
            if error_records:
                write_csv_records(error_filename, error_records, error_headers, append=True)
                total_error_count += len(error_records)
                error_records.clear()
            if (
                app_instance and
                total_records > 0 and
                not stop_event.is_set() and
                cumulative_processed >= total_records
            ):
                app_instance.safe_update_progress_bar(100)

        logger.info(
            f"Final counts: Success={total_success_count}, Errors={total_error_count}, "
            f"Skipped={total_skipped_count}, Cumulative Processed={cumulative_processed}"
        )
        if app_instance:
            app_instance.success_filename = success_filename
            app_instance.error_filename = error_filename

    return total_success_count, total_error_count, total_skipped_count, cumulative_processed











def export_attachments(subdomain, email, token, save_directory, stop_event=None, app_instance=None, base_file_dir=None, start_time=0):
    """
    Exports all ticket comments and their attachments from Zendesk, using cursor-based incremental ticket export
    to fetch ticket IDs (including archived tickets, excluding deleted with exclude_deleted=true).
    Saves ticket IDs to a CSV file. Processes ticket IDs immediately after each batch.
    Saves attachments and inline images in ticket_{ticket_id}/comment_{comment_id}/{file_name}.
    Writes comment records to CSV with file_name and is_inline populated, assuming attachments are downloaded.
    Processes attachment downloads independently, writing success/failure to separate CSVs.
    Each attachment generates a separate CSV row in comments CSV with comment metadata and attachment file_name/is_inline.
    Preserves HTML tags in html_body, strips HTML from file_name.
    Tracks tickets processed, comments processed, and attachments processed.
    Stops immediately when stop_event is set, saving partial results including all completed attachment downloads.
    Optimized for speed: comments and attachments processed independently with minimal locking.
    Waits for all attachment downloads to complete before fetching the next page of tickets.
    Removes duplicate rows from CSV files after every batch of tickets (each API page).
    Downloads inline images from <img> tags in html_body with URLs like https://*.zendesk.com/attachments/token/*.

    Args:
        subdomain (str): Zendesk subdomain
        email (str): Zendesk user email
        token (str): Zendesk API token
        save_directory (str): Directory to save CSV files
        stop_event (threading.Event, optional): Event to stop execution
        app_instance (object, optional): UI instance for progress updates
        base_file_dir (str, optional): Directory for attachments (defaults to save_directory)
        start_time (int, optional): Unix timestamp for incremental export start (default 0 for all tickets)
    """
    from bs4 import BeautifulSoup
    import re

    base_url = f"https://{subdomain}.zendesk.com"
    auth = HTTPBasicAuth(f"{email}/token", token)
    tickets_url = f"{base_url}/api/v2/incremental/tickets/cursor.json?start_time={start_time}&exclude_deleted=true"
    headers = {"Content-Type": "application/json"}

    # File setup
    export_filename = get_unique_filename(os.path.join(save_directory, "Comment_Success.csv"))
    error_filename = get_unique_filename(os.path.join(save_directory, "Comment_Errors.csv"))
    ticket_ids_filename = get_unique_filename(os.path.join(save_directory, "Ticket_IDs_export.csv"))
    success_filename = get_unique_filename(os.path.join(save_directory, "Attachment_Success.csv"))
    download_error_filename = get_unique_filename(os.path.join(save_directory, "Attachment_Errors.csv"))
    attachment_dir = base_file_dir if base_file_dir else save_directory
    logger.info(f"Saving attachments to: {attachment_dir}, ticket IDs to: {ticket_ids_filename}, comments to: {export_filename}, "
                f"success to: {success_filename}, download errors to: {download_error_filename}")

    csv_headers = ["ticket_id", "comment_id", "html_body", "author_id", "created_at", "public", "file_name", "is_inline"]
    error_csv_headers = ["ticket_id", "comment_id", "html_body", "author_id", "created_at", "public", "file_name", "is_inline", "error_message"]
    ticket_ids_headers = ["ticket_id", "status", "created_at", "updated_at"]
    success_headers = ["ticket_id", "comment_id", "file_name", "is_inline", "file_path"]
    download_error_headers = ["ticket_id", "comment_id", "file_name", "is_inline", "error_message"]
    tickets_processed = 0
    comments_processed = 0
    attachments_processed = 0
    total_tickets = 0
    comment_records = []
    error_records = []
    ticket_id_records = []
    success_records = []
    download_error_records = []
    ticket_status = {}
    processed_ticket_ids = set()  # Track processed ticket IDs to prevent duplicates
    file_lock = threading.Lock()
    rate_limit_lock = threading.Lock()
    rate_limit_event = threading.Event()
    rate_limit_event.set()
    rate_limit_resume_time = time.time()
    download_futures = set()
    batch_ticket_ids = []  # Track ticket IDs in the current batch (page)

    print("🚀 Starting comments and attachments export with incremental ticket export (excluding deleted tickets)")
    logger.info("Starting comments and attachments export with incremental ticket export (excluding deleted tickets)")

    def write_csv_records(filename, records, headers, append=False):
        nonlocal comments_processed, attachments_processed
        mode = 'a' if append else 'w'
        try:
            with file_lock:
                with open(filename, mode, newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, fieldnames=headers, quoting=csv.QUOTE_MINIMAL, escapechar='\\')
                    if not append:
                        writer.writeheader()
                        logger.info(f"Initialized CSV with header: {filename}")
                    if records:
                        sanitized_records = []
                        for record in records:
                            sanitized_record = {}
                            for key, value in record.items():
                                strip_html = key == "file_name"
                                sanitized_value = sanitize_csv_value(value, strip_html=strip_html)
                                sanitized_record[key] = sanitized_value
                                if str(value) != str(sanitized_value):
                                    logger.debug(f"Sanitized value for '{key}': '{str(value)[:50]}...' -> '{str(sanitized_value)[:50]}...'")
                            sanitized_records.append(sanitized_record)
                        writer.writerows(sanitized_records)
                        logger.info(f"Wrote {len(records)} records to {filename} (append={append})")
                        if filename == export_filename:
                            comments_processed += len(records)
                            logger.debug(f"Incremented comments_processed to {comments_processed}")
                        elif filename == success_filename:
                            attachments_processed += len(records)
                            logger.debug(f"Incremented attachments_processed to {attachments_processed}")
        except Exception as e:
            logger.error(f"Error writing CSV to {filename}: {str(e)}")
            print(f"❌ Failed to write CSV to {filename}: {str(e)}")
            raise

    def remove_duplicates_from_batch(batch_ticket_ids):
        """Remove duplicate rows from CSV files for the current batch of ticket IDs."""
        batch_ticket_ids_set = set(str(tid) for tid in batch_ticket_ids)
        csv_files = [
            (export_filename, csv_headers),
            (error_filename, error_csv_headers),
            (ticket_ids_filename, ticket_ids_headers),
            (success_filename, success_headers),
            (download_error_filename, download_error_headers)
        ]
        for filename, headers in csv_files:
            try:
                with file_lock:
                    # Read all rows
                    rows = []
                    with open(filename, 'r', newline='', encoding='utf-8-sig') as f:
                        reader = csv.DictReader(f)
                        rows = list(reader)
                    if not rows:
                        logger.debug(f"No rows to deduplicate in {filename}")
                        continue

                    # Separate rows: keep non-batch rows, deduplicate batch rows
                    non_batch_rows = [row for row in rows if row.get('ticket_id', '') not in batch_ticket_ids_set]
                    batch_rows = [row for row in rows if row.get('ticket_id', '') in batch_ticket_ids_set]
                    logger.debug(f"{filename}: {len(non_batch_rows)} non-batch rows, {len(batch_rows)} batch rows")

                    # Deduplicate batch rows
                    seen_rows = set()
                    deduplicated_batch_rows = []
                    for row in batch_rows:
                        row_tuple = tuple((k, row.get(k, '')) for k in headers)
                        if row_tuple not in seen_rows:
                            seen_rows.add(row_tuple)
                            deduplicated_batch_rows.append(row)
                    duplicates_removed = len(batch_rows) - len(deduplicated_batch_rows)
                    logger.info(f"{filename}: Removed {duplicates_removed} duplicate rows from batch")

                    # Write back all rows (non-batch + deduplicated batch)
                    with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
                        writer = csv.DictWriter(f, fieldnames=headers, quoting=csv.QUOTE_MINIMAL, escapechar='\\')
                        writer.writeheader()
                        writer.writerows(non_batch_rows + deduplicated_batch_rows)
                    logger.info(f"Rewrote {filename} with {len(non_batch_rows) + len(deduplicated_batch_rows)} rows after deduplication")
            except Exception as e:
                logger.error(f"Error deduplicating {filename}: {str(e)}")
                print(f"❌ Failed to deduplicate {filename}: {str(e)}")

    # Initialize CSV files
    try:
        write_csv_records(export_filename, [], csv_headers, append=False)
        write_csv_records(error_filename, [], error_csv_headers, append=False)
        write_csv_records(ticket_ids_filename, [], ticket_ids_headers, append=False)
        write_csv_records(success_filename, [], success_headers, append=False)
        write_csv_records(download_error_filename, [], download_error_headers, append=False)
    except Exception as e:
        print(f"❌ Failed to initialize CSV files: {str(e)}")
        logger.error(f"Failed to initialize CSV files: {str(e)}")
        raise

    def clean_filename(file_name):
        if not file_name:
            logger.warning("Empty filename provided")
            return file_name
        file_name = urllib.parse.unquote(file_name)
        logger.debug(f"Raw filename: {file_name}")
        clean_name = re.sub(r'<[^>]+>|</?\w+\s*>', '', file_name)
        clean_name = re.sub(r'[\'"[\]]+', '', clean_name)
        clean_name = re.sub(r'(_[a-zA-Z0-9]+)+(?=\.[a-zA-Z0-9]+$)', '', clean_name)
        clean_name = re.sub(r'_+$|[^\w\.\-]', '', clean_name)
        if not re.search(r'\.[a-zA-Z0-9]+$', clean_name):
            logger.warning(f"Filename {clean_name} lacks a valid extension, returning original: {file_name}")
            return file_name
        logger.debug(f"Cleaned filename: {file_name} -> {clean_name}")
        return clean_name

    def extract_inline_attachments(comment_body, ticket_id, comment_id):
        if not comment_body:
            logger.debug(f"No comment body for ticket {ticket_id}, comment {comment_id}")
            return []
        inline_attachments = []
        seen_urls = set()
        
        # Process HTML <img> tags for inline images
        soup = BeautifulSoup(comment_body, 'html.parser')
        img_tags = soup.find_all('img', src=re.compile(r'https://.*\.zendesk\.com/attachments/token/.*'))
        logger.debug(f"Found {len(img_tags)} img tags in comment {comment_id}")
        
        for index, img_tag in enumerate(img_tags):
            img_url = img_tag['src']
            logger.debug(f"Processing img tag {index+1}/{len(img_tags)}: {img_url}")
            if img_url in seen_urls:
                logger.debug(f"Skipping duplicate inline image URL: {img_url}")
                continue
            seen_urls.add(img_url)
            name_match = re.search(r'name=([^&]+)', img_url)
            base_file_name = urllib.parse.unquote(name_match.group(1)) if name_match else f"inline_image_{comment_id}_{index}.png"
            cleaned_file_name = clean_filename(base_file_name)
            # Ensure unique file name within the comment
            unique_file_name = cleaned_file_name
            suffix = 0
            while any(att['file_name'] == unique_file_name for att in inline_attachments):
                suffix += 1
                name, ext = os.path.splitext(cleaned_file_name)
                unique_file_name = f"{name}_{suffix}{ext}"
            inline_attachments.append({
                "content_url": img_url,
                "file_name": unique_file_name,
                "ticket_id": ticket_id,
                "comment_id": comment_id,
                "is_inline": True
            })
            logger.debug(f"Added inline image: {unique_file_name} for comment {comment_id}")
        
        # Process text-based inline attachments (if any)
        attachment_pattern = r'https?://[^/]+/attachments/token/[^/?]+/\?name=([^"&>\s][^"&>]*?\.[a-zA-Z0-9]+)(?=&|>|\s|$)'
        matches = re.finditer(attachment_pattern, comment_body)
        for match in matches:
            content_url = match.group(0)
            file_name = urllib.parse.unquote(match.group(1))
            if content_url in seen_urls:
                logger.debug(f"Skipping duplicate attachment URL: {content_url}")
                continue
            seen_urls.add(content_url)
            cleaned_file_name = clean_filename(file_name)
            # Ensure unique file name
            unique_file_name = cleaned_file_name
            suffix = 0
            while any(att['file_name'] == unique_file_name for att in inline_attachments):
                suffix += 1
                name, ext = os.path.splitext(cleaned_file_name)
                unique_file_name = f"{name}_{suffix}{ext}"
            inline_attachments.append({
                "content_url": content_url,
                "file_name": unique_file_name,
                "ticket_id": ticket_id,
                "comment_id": comment_id,
                "is_inline": True
            })
            logger.debug(f"Found text-based inline attachment: {unique_file_name} in comment {comment_id}")
        
        logger.debug(f"Total inline attachments extracted for comment {comment_id}: {len(inline_attachments)}")
        return inline_attachments

    def download_attachment(attachment, save_dir, is_inline=False):
        global rate_limit_resume_time
        if stop_event and stop_event.is_set():
            logger.info(f"🛑 Stop event detected. Skipping download of {attachment.get('file_name')}")
            return {"error": "Download aborted due to stop event", "ticket_id": attachment.get("ticket_id", ""), "comment_id": attachment.get("comment_id", ""), "file_name": attachment.get("file_name", ""), "is_inline": is_inline}
        content_url = attachment.get("content_url")
        file_name = attachment.get("file_name")
        ticket_id = attachment.get("ticket_id")
        comment_id = attachment.get("comment_id")
        if not all([content_url, file_name, ticket_id, comment_id]):
            logger.error(f"Invalid attachment data: {attachment}")
            return {"error": "Invalid attachment data", "ticket_id": ticket_id, "comment_id": comment_id, "file_name": file_name, "is_inline": is_inline}
        safe_file_name = re.sub(r'[<>:"/\\|?*]', '_', file_name)
        ticket_folder = os.path.join(save_dir, f"ticket_{ticket_id}")
        comment_folder = os.path.join(ticket_folder, f"comment_{comment_id}")
        file_path = os.path.join(comment_folder, safe_file_name)
        with file_lock:
            file_path = get_unique_filename(file_path)
        attachment_type = "inline" if is_inline else "full"
        logger.debug(f"Starting download of {attachment_type} attachment: {safe_file_name}")

        max_retries = 5
        retry_count = 0
        backoff_delay = 2  # Initial backoff delay for retries (seconds)
        while retry_count < max_retries:
            try:
                with rate_limit_lock:
                    if not rate_limit_event.is_set():
                        current_time = time.time()
                        if current_time < rate_limit_resume_time:
                            pause_duration = rate_limit_resume_time - current_time
                            logger.info(f"Pausing for {pause_duration:.1f}s due to rate limit")
                            time.sleep(pause_duration)
                        rate_limit_event.set()
                response = requests.get(content_url, auth=auth, stream=True, timeout=30)
                if response.status_code == 429:
                    retry_count += 1
                    retry_after = int(response.headers.get("Retry-After", 2))
                    logger.warning(f"Rate limit (429) hit for attachment {safe_file_name}, attempt {retry_count}/{max_retries}. Pausing for {retry_after}s")
                    print(f"⚠️ 429 Rate Limit Hit: Too many requests for attachment {safe_file_name}, attempt {retry_count}/{max_retries}. Pausing for {retry_after}s")
                    with rate_limit_lock:
                        rate_limit_resume_time = max(rate_limit_resume_time, time.time() + retry_after)
                        rate_limit_event.clear()
                    time.sleep(retry_after)
                    continue
                response.raise_for_status()
                with file_lock:
                    os.makedirs(comment_folder, exist_ok=True)
                    with open(file_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if stop_event and stop_event.is_set():
                                logger.info(f"🛑 Stop event detected. Aborting download of {safe_file_name}")
                                return {"error": "Download aborted due to stop event", "ticket_id": ticket_id, "comment_id": comment_id, "file_name": safe_file_name, "is_inline": is_inline}
                            if chunk:
                                f.write(chunk)
                logger.info(f"Successfully downloaded attachment: {file_path}")
                return {"file_name": safe_file_name, "is_inline": is_inline, "file_path": file_path, "ticket_id": ticket_id, "comment_id": comment_id}
            except (ReadTimeout, IncompleteRead, ConnectionError) as e:
                retry_count += 1
                logger.warning(f"Error downloading {attachment_type} attachment {safe_file_name}: {str(e)}, attempt {retry_count}/{max_retries}. Retrying after {backoff_delay}s")
                print(f"⚠️ Error: Downloading {attachment_type} attachment {safe_file_name}: {str(e)}, attempt {retry_count}/{max_retries}. Retrying after {backoff_delay}s")
                time.sleep(backoff_delay)
                backoff_delay *= 2  # Exponential backoff
                continue
            except RequestException as e:
                if isinstance(e, HTTPError) and e.response.status_code == 429:
                    continue
                logger.error(f"Network error downloading {attachment_type} attachment {safe_file_name}: {str(e)}")
                return {"error": f"Network error: {str(e)}", "ticket_id": ticket_id, "comment_id": comment_id, "file_name": safe_file_name, "is_inline": is_inline}
            except Exception as e:
                logger.error(f"Failed to download {attachment_type} attachment {safe_file_name}: {str(e)}")
                return {"error": f"Download failed: {str(e)}", "ticket_id": ticket_id, "comment_id": comment_id, "file_name": safe_file_name, "is_inline": is_inline}
        logger.error(f"Max retries ({max_retries}) exceeded for {attachment_type} attachment {safe_file_name} due to errors")
        return {"error": f"Max retries ({max_retries}) exceeded due to errors", "ticket_id": ticket_id, "comment_id": comment_id, "file_name": safe_file_name, "is_inline": is_inline}

    def fetch_comments(ticket_id):
        global rate_limit_resume_time
        if stop_event and stop_event.is_set():
            logger.info(f"Stop event detected. Skipping comment fetch for ticket {ticket_id}")
            return ticket_id, None
        comments_url = f"{base_url}/api/v2/tickets/{ticket_id}/comments.json"
        max_retries = 5
        retry_count = 0
        backoff_delay = 2  # Initial backoff delay for read timeouts (seconds)
        while retry_count < max_retries:
            try:
                with rate_limit_lock:
                    if not rate_limit_event.is_set():
                        current_time = time.time()
                        if current_time < rate_limit_resume_time:
                            pause_duration = rate_limit_resume_time - current_time
                            logger.info(f"Pausing for {pause_duration:.1f}s due to rate limit")
                            time.sleep(pause_duration)
                        rate_limit_event.set()
                response = requests.get(comments_url, auth=auth, headers=headers, timeout=30)
                if response.status_code == 429:
                    retry_count += 1
                    retry_after = int(response.headers.get("Retry-After", 10))
                    logger.warning(f"Rate limit (429) hit for comments of ticket {ticket_id}, attempt {retry_count}/{max_retries}. Pausing for {retry_after}s")
                    print(f"⚠️ 429 Rate Limit Hit: Too many requests for comments of ticket {ticket_id}, attempt {retry_count}/{max_retries}. Pausing for {retry_after}s")
                    with rate_limit_lock:
                        rate_limit_resume_time = max(rate_limit_resume_time, time.time() + retry_after)
                        rate_limit_event.clear()
                    time.sleep(retry_after)
                    continue
                response.raise_for_status()
                comments = response.json().get("comments", [])
                logger.debug(f"Found {len(comments)} comments for ticket {ticket_id}")
                return ticket_id, comments
            except ReadTimeout as e:
                retry_count += 1
                logger.warning(f"Read timeout fetching comments for ticket {ticket_id}, attempt {retry_count}/{max_retries}. Retrying after {backoff_delay}s")
                print(f"⚠️ Read Timeout: Fetching comments for ticket {ticket_id}, attempt {retry_count}/{max_retries}. Retrying after {backoff_delay}s")
                time.sleep(backoff_delay)
                backoff_delay *= 2  # Exponential backoff
                continue
            except RequestException as e:
                if isinstance(e, HTTPError) and e.response.status_code == 429:
                    continue
                logger.error(f"Network error fetching comments for ticket {ticket_id}: {str(e)}")
                return ticket_id, None
            except Exception as e:
                logger.error(f"Failed to fetch comments for ticket {ticket_id}: {str(e)}")
                return ticket_id, None
        logger.error(f"Max retries ({max_retries}) exceeded for fetching comments of ticket {ticket_id} due to read timeouts or rate limits")
        return ticket_id, None

    # Fetch total tickets (approximate)
    try:
        count_url = f"{base_url}/api/v2/search/count.json?query=type:ticket"
        response = requests.get(count_url, auth=auth, headers=headers, timeout=30)
        response.raise_for_status()
        total_tickets = response.json().get("count", 0)
        logger.info(f"Total tickets to process (approximate): {total_tickets}")
    except Exception as e:
        print(f"❌ Failed to fetch ticket count: {str(e)}")
        logger.error(f"Failed to fetch ticket count: {str(e)}")
        total_tickets = 1

    try:
        with ThreadPoolExecutor(max_workers=150) as download_executor:
            next_page_url = tickets_url
            page_count = 0

            while next_page_url:
                page_count += 1
                print(f"🔍 Starting pagination loop iteration {page_count}, URL: {next_page_url}")
                logger.debug(f"Pagination loop iteration {page_count}, URL: {next_page_url}")

                if stop_event and stop_event.is_set():
                    print("🛑 Stop event detected. Stopping new ticket fetches.")
                    logger.info("Stop detected. Stopping new ticket fetches.")
                    write_csv_records(export_filename, comment_records, csv_headers, append=True)
                    write_csv_records(success_filename, success_records, success_headers, append=True)
                    write_csv_records(download_error_filename, download_error_records, download_error_headers, append=True)
                    comment_records.clear()
                    success_records.clear()
                    download_error_records.clear()
                    print(f"🔚 Exiting pagination loop due to stop event after {page_count} iterations")
                    break

                try:
                    with rate_limit_lock:
                        if not rate_limit_event.is_set():
                            current_time = time.time()
                            if current_time < rate_limit_resume_time:
                                pause_duration = rate_limit_resume_time - current_time
                                minutes, seconds = divmod(pause_duration, 60)
                                formatted_duration = f"{int(minutes):02d}:{int(seconds):02d}"
                                print(f"⏸️ Pausing for {formatted_duration} due to rate limit")
                                logger.info(f"Pausing for {formatted_duration} due to rate limit")
                                time.sleep(pause_duration)
                            rate_limit_event.set()
                    print(f"📡 Sending request to {next_page_url}")
                    response = requests.get(next_page_url, auth=auth, headers=headers, timeout=30)
                    if response.status_code == 429:
                        retry_after = int(response.headers.get("Retry-After", 60))
                        minutes, seconds = divmod(retry_after, 60)
                        formatted_duration = f"{int(minutes):02d}:{int(seconds):02d}"
                        print(f"⚠️ Rate limit (429) hit for incremental export. Pausing for {formatted_duration}")
                        logger.warning(f"Rate limit (429) hit for incremental export. Pausing for {formatted_duration}")
                        with rate_limit_lock:
                            rate_limit_resume_time = time.time() + retry_after
                            rate_limit_event.clear()
                        time.sleep(retry_after)
                        print(f"🔄 Retrying request to {next_page_url} after rate limit pause")
                        response = requests.get(next_page_url, auth=auth, headers=headers, timeout=30)
                    response.raise_for_status()
                    data = response.json()
                    tickets = data.get("tickets", [])
                    next_cursor = data.get("after_cursor")
                    end_of_stream = data.get("end_of_stream", False)
                    print(f"✅ Received response: {len(tickets)} tickets, after_cursor={next_cursor}, end_of_stream={end_of_stream}")
                    logger.debug(f"Response: {len(tickets)} tickets, after_cursor={next_cursor}, end_of_stream={end_of_stream}")

                    if not tickets:
                        print("ℹ️ No more tickets to process. Exiting pagination loop.")
                        logger.info("No more tickets to process.")
                        break

                    # Reset batch_ticket_ids for the new batch
                    batch_ticket_ids = []

                    # Filter out duplicate and deleted tickets before processing
                    unique_tickets = []
                    for ticket in tickets:
                        ticket_id = str(ticket["id"])
                        if ticket_id in processed_ticket_ids:
                            continue
                        if ticket.get("status") == "deleted":
                            logger.debug(f"Skipping deleted ticket {ticket_id}")
                            continue
                        unique_tickets.append(ticket)
                        processed_ticket_ids.add(ticket_id)
                        batch_ticket_ids.append(ticket_id)  # Add to current batch
                    logger.debug(f"Filtered {len(tickets)} tickets to {len(unique_tickets)} unique non-deleted tickets")

                    for ticket in unique_tickets:
                        ticket_id = str(ticket["id"])
                        ticket_id_records.append({
                            "ticket_id": ticket_id,
                            "status": ticket.get("status", ""),
                            "created_at": ticket.get("created_at", ""),
                            "updated_at": ticket.get("updated_at", "")
                        })

                    write_csv_records(ticket_ids_filename, ticket_id_records, ticket_ids_headers, append=True)
                    ticket_id_records.clear()

                    with ThreadPoolExecutor(max_workers=10) as comment_executor:
                        future_to_ticket = {
                            comment_executor.submit(fetch_comments, ticket["id"]): ticket
                            for ticket in unique_tickets
                        }
                        print(f"📋 Submitted {len(future_to_ticket)} comment fetch tasks")
                        completed_futures = 0
                        for future in as_completed(future_to_ticket):
                            completed_futures += 1
                            print(f"📥 Completed {completed_futures}/{len(future_to_ticket)} comment fetch tasks")
                            if stop_event and stop_event.is_set():
                                print("🛑 Stop event detected. Waiting for comment tasks to complete.")
                                logger.info("Stop event detected. Waiting for comment tasks to complete.")
                                for remaining_future in future_to_ticket:
                                    try:
                                        remaining_future.result(timeout=60)
                                    except Exception as e:
                                        logger.warning(f"Error in remaining comment task: {str(e)}")
                                write_csv_records(export_filename, comment_records, csv_headers, append=True)
                                write_csv_records(success_filename, success_records, success_headers, append=True)
                                write_csv_records(download_error_filename, download_error_records, download_error_headers, append=True)
                                comment_records.clear()
                                success_records.clear()
                                download_error_records.clear()
                                print(f"🔚 Exiting comment processing due to stop event")
                                break
                            try:
                                ticket_id, comments = future.result(timeout=60)
                                ticket = future_to_ticket[future]
                            except TimeoutError:
                                logger.error(f"Timeout fetching comments for ticket {ticket['id']}")
                                error_records.append({
                                    "ticket_id": str(ticket['id']),
                                    "comment_id": "",
                                    "html_body": "",
                                    "author_id": "",
                                    "created_at": "",
                                    "public": "",
                                    "file_name": "",
                                    "is_inline": "",
                                    "error_message": f"Timeout fetching comments for ticket {ticket['id']}"
                                })
                                write_csv_records(error_filename, error_records, error_csv_headers, append=True)
                                error_records.clear()
                                continue
                            except Exception as e:
                                logger.error(f"Error fetching comments for ticket {ticket['id']}: {str(e)}")
                                error_records.append({
                                    "ticket_id": str(ticket['id']),
                                    "comment_id": "",
                                    "html_body": "",
                                    "author_id": "",
                                    "created_at": "",
                                    "public": "",
                                    "file_name": "",
                                    "is_inline": "",
                                    "error_message": f"Failed to fetch comments: {str(e)}"
                                })
                                write_csv_records(error_filename, error_records, error_csv_headers, append=True)
                                error_records.clear()
                                continue

                            if comments is None:
                                error_records.append({
                                    "ticket_id": str(ticket_id),
                                    "comment_id": "",
                                    "html_body": "",
                                    "author_id": "",
                                    "created_at": "",
                                    "public": "",
                                    "file_name": "",
                                    "is_inline": "",
                                    "error_message": f"Failed to fetch comments for ticket {ticket_id}"
                                })
                                write_csv_records(error_filename, error_records, error_csv_headers, append=True)
                                error_records.clear()
                                continue

                            ticket_status[ticket_id] = {
                                "comment_count": len(comments),
                                "attachment_count": 0,
                                "comments_processed": 0,
                                "attachments_queued": 0
                            }

                            for comment in comments:
                                if stop_event and stop_event.is_set():
                                    logger.info(f"Stop event detected. Skipping further comment processing for ticket {ticket_id}")
                                    write_csv_records(export_filename, comment_records, csv_headers, append=True)
                                    write_csv_records(success_filename, success_records, success_headers, append=True)
                                    write_csv_records(download_error_filename, download_error_records, download_error_headers, append=True)
                                    comment_records.clear()
                                    success_records.clear()
                                    download_error_records.clear()
                                    break
                                comment_id = comment.get("id")
                                html_body = comment.get("html_body", "")
                                author_id = comment.get("author_id", "")
                                created_at = comment.get("created_at", "")
                                public = str(comment.get("public", "")).lower()

                                base_comment_record = {
                                    "ticket_id": str(ticket_id),
                                    "comment_id": str(comment_id),
                                    "html_body": html_body,
                                    "author_id": str(author_id),
                                    "created_at": created_at,
                                    "public": public,
                                    "file_name": "",
                                    "is_inline": ""
                                }

                                ticket_status[ticket_id]["comments_processed"] += 1
                                logger.debug(f"Processed comment {comment_id} for ticket {ticket_id}")

                                full_attachments = comment.get("attachments", [])
                                body = comment.get("body", "")
                                inline_attachments = extract_inline_attachments(html_body or body, ticket_id, comment_id)
                                all_attachments = full_attachments + inline_attachments
                                ticket_status[ticket_id]["attachment_count"] += len(all_attachments)

                                has_attachments = False
                                for attachment in all_attachments:
                                    if stop_event and stop_event.is_set():
                                        logger.info(f"Stop event detected. Skipping further attachment queuing for comment {comment_id}")
                                        break
                                    if "is_inline" not in attachment:
                                        attachment["ticket_id"] = ticket_id
                                        attachment["comment_id"] = comment_id
                                        attachment["is_inline"] = False
                                        attachment["file_name"] = clean_filename(attachment.get("file_name", ""))
                                    is_inline = attachment["is_inline"]
                                    file_name = attachment["file_name"]
                                    attachment_record = base_comment_record.copy()
                                    attachment_record["file_name"] = file_name
                                    attachment_record["is_inline"] = str(is_inline).lower()
                                    comment_records.append(attachment_record)
                                    has_attachments = True
                                    future = download_executor.submit(download_attachment, attachment, attachment_dir, is_inline=is_inline)
                                    download_futures.add(future)
                                    ticket_status[ticket_id]["attachments_queued"] += 1
                                    logger.debug(f"Queued {'inline' if is_inline else 'full'} attachment: {file_name}")

                                if not has_attachments:
                                    comment_records.append(base_comment_record)
                                    logger.debug(f"No attachments for comment {comment_id}. Added single row")

                                if len(comment_records) >= 500:
                                    write_csv_records(export_filename, comment_records, csv_headers, append=True)
                                    comment_records.clear()
                                if len(error_records) >= 500:
                                    write_csv_records(error_filename, error_records, error_csv_headers, append=True)
                                    error_records.clear()

                            if (ticket_status[ticket_id]["comments_processed"] == ticket_status[ticket_id]["comment_count"] and
                                ticket_status[ticket_id]["attachments_queued"] == ticket_status[ticket_id]["attachment_count"]):
                                tickets_processed += 1
                                logger.debug(f"Incremented tickets_processed to {tickets_processed} for ticket {ticket_id}")

                    # Wait for all attachment downloads to complete before deduplicating and moving to the next page
                    print(f"⏳ Waiting for {len(download_futures)} attachment downloads to complete")
                    logger.info(f"Waiting for {len(download_futures)} attachment downloads to complete")
                    for future in as_completed(download_futures):
                        if stop_event and stop_event.is_set():
                            logger.info("Stop event detected. Stopping attachment processing.")
                            break
                        try:
                            result = future.result()
                            if result and "error" not in result:
                                success_records.append({
                                    "ticket_id": result["ticket_id"],
                                    "comment_id": result["comment_id"],
                                    "file_name": result["file_name"],
                                    "is_inline": str(result["is_inline"]).lower(),
                                    "file_path": result["file_path"]
                                })
                            else:
                                error_message = result.get("error", "Unknown error")
                                download_error_records.append({
                                    "ticket_id": result["ticket_id"],
                                    "comment_id": result["comment_id"],
                                    "file_name": result["file_name"],
                                    "is_inline": str(result["is_inline"]).lower(),
                                    "error_message": error_message
                                })
                            download_futures.discard(future)
                            if len(success_records) >= 500:
                                write_csv_records(success_filename, success_records, success_headers, append=True)
                                success_records.clear()
                            if len(download_error_records) >= 500:
                                write_csv_records(download_error_filename, download_error_records, download_error_headers, append=True)
                                download_error_records.clear()
                        except Exception as e:
                            logger.error(f"Error processing download future: {str(e)}")
                            download_futures.discard(future)
                    print(f"✅ All attachment downloads completed for batch")
                    logger.info(f"All attachment downloads completed. download_futures cleared.")

                    # Deduplicate the current batch if there are tickets
                    if batch_ticket_ids:
                        print(f"🧹 Deduplicating batch of {len(batch_ticket_ids)} tickets")
                        remove_duplicates_from_batch(batch_ticket_ids)
                        batch_ticket_ids.clear()

                    if app_instance and total_tickets > 0 and not (stop_event and stop_event.is_set()):
                        progress = (tickets_processed / total_tickets) * 100
                        app_instance.safe_update_progress_bar(progress)

                    if end_of_stream:
                        print(f"🏁 Reached end of incremental ticket stream after {page_count} iterations")
                        logger.info("Reached end of incremental ticket stream.")
                        next_page_url = None
                        break
                    else:
                        if next_cursor:
                            next_page_url = f"{base_url}/api/v2/incremental/tickets/cursor.json?cursor={next_cursor}&exclude_deleted=true"
                            print(f"➡️ Continuing pagination with next_cursor: {next_cursor}")
                        else:
                            print(f"⚠️ No after_cursor found, but end_of_stream is not true")
                            logger.warning(f"No after_cursor found, but end_of_stream is not true")
                            next_page_url = None
                            break

                except RequestException as e:
                    print(f"❌ Network error fetching tickets from {next_page_url}: {str(e)}")
                    logger.error(f"Network error fetching tickets from {next_page_url}: {str(e)}")
                    write_csv_records(export_filename, comment_records, csv_headers, append=True)
                    write_csv_records(success_filename, success_records, success_headers, append=True)
                    write_csv_records(download_error_filename, download_error_records, download_error_headers, append=True)
                    comment_records.clear()
                    success_records.clear()
                    download_error_records.clear()
                    print(f"🔚 Exiting pagination loop due to network error after {page_count} iterations")
                    break
                except Exception as e:
                    print(f"❌ Unexpected error in pagination loop: {str(e)}")
                    logger.error(f"Unexpected error in pagination loop: {str(e)}")
                    write_csv_records(export_filename, comment_records, csv_headers, append=True)
                    write_csv_records(success_filename, success_records, success_headers, append=True)
                    write_csv_records(download_error_filename, download_error_records, download_error_headers, append=True)
                    comment_records.clear()
                    success_records.clear()
                    download_error_records.clear()
                    print(f"⚠️ Continuing pagination despite error after {page_count} iterations")

    finally:
        # Process any remaining completed attachment downloads
        logger.info("Processing remaining completed attachment downloads before exit")
        completed_futures = []
        for future in as_completed(download_futures, timeout=0):
            try:
                result = future.result(timeout=0)
                if result and "error" not in result:
                    success_records.append({
                        "ticket_id": result["ticket_id"],
                        "comment_id": result["comment_id"],
                        "file_name": result["file_name"],
                        "is_inline": str(result["is_inline"]).lower(),
                        "file_path": result["file_path"]
                    })
                else:
                    error_message = result.get("error", "Unknown error")
                    download_error_records.append({
                        "ticket_id": result["ticket_id"],
                        "comment_id": result["comment_id"],
                        "file_name": result["file_name"],
                        "is_inline": str(result["is_inline"]).lower(),
                        "error_message": error_message
                    })
                completed_futures.append(future)
            except TimeoutError:
                continue
        for future in completed_futures:
            download_futures.discard(future)

        write_csv_records(export_filename, comment_records, csv_headers, append=True)
        write_csv_records(error_filename, error_records, error_csv_headers, append=True)
        write_csv_records(ticket_ids_filename, ticket_id_records, ticket_ids_headers, append=True)
        write_csv_records(success_filename, success_records, success_headers, append=True)
        write_csv_records(download_error_filename, download_error_records, download_error_headers, append=True)
        if app_instance and total_tickets > 0 and not (stop_event and stop_event.is_set()):
            app_instance.safe_update_progress_bar(100)
        print(f"🎉 Export completed: {tickets_processed} tickets processed, {comments_processed} comments processed, {attachments_processed} attachments processed")
        logger.info(f"Export completed: {tickets_processed} tickets processed, {comments_processed} comments processed, {attachments_processed} attachments processed")
        if app_instance:
            app_instance.success_filename = export_filename
            app_instance.error_filename = error_filename
            app_instance.ticket_ids_filename = ticket_ids_filename
            app_instance.attachment_success_filename = success_filename
            app_instance.attachment_error_filename = download_error_filename

    return tickets_processed, comments_processed, attachments_processed








def get_unique_filename(base_filename):
    """Ensures a new unique filename is created if one already exists."""
    file_dir, file_name = os.path.split(base_filename)
    name, ext = os.path.splitext(file_name)
    counter = 1
    new_filename = base_filename
    while os.path.exists(new_filename):
        new_filename = os.path.join(file_dir, f"{name} ({counter}){ext}")
        counter += 1
    return new_filename









def export_records(
    subdomain, email, token, selected_object, csv_headers, save_directory,
    stop_event=None, progress_bar=None, app_instance=None, total_records=None
):
    """Exports records from Zendesk using the Incremental Export API (time-based) or Cursor API (for Tickets).
    Filters for role = 'end-user' and active = true when exporting Users.
    Filters out organizations with deleted_at value.
    For Tickets, includes solved_at from metric_set and filters out deleted tickets.
    Ensures CSV is correctly written and sanitized when stop event is triggered."""
    global is_exporting
    is_exporting = True

    base_url = f"https://{subdomain}.zendesk.com"
    auth = HTTPBasicAuth(f"{email}/token", token)

    # Define object-specific API endpoints and fields (plural forms)
    object_map = {
        "Users": ("users", "user"),
        "User": ("users", "user"),
        "Organizations": ("organizations", "organization"),
        "Organization": ("organizations", "organization"),
        "Tickets": ("tickets", "ticket"),
        "Ticket": ("tickets", "ticket")
    }
    api_object, response_key = object_map.get(selected_object, (selected_object.lower(), selected_object.lower()))

    # Validate selected_object
    if selected_object not in object_map:
        logger.error(f"Invalid selected_object: {selected_object}")
        raise ValueError(f"Invalid selected_object: {selected_object}")

    # Fetch total records if not provided
    if total_records is None:
        fetch_total_records(subdomain, email, token, selected_object)
        total_records = get_total_records()
        if total_records == 0:
            total_records = 1  # Avoid division by zero
    logger.info(f"Total {selected_object} records to export: {total_records}")

    # Ensure save_directory exists
    os.makedirs(save_directory, exist_ok=True)
    if not os.path.isdir(save_directory):
        logger.error(f"Save directory does not exist: {save_directory}")
        raise ValueError(f"Save directory does not exist: {save_directory}")

    # Set up CSV file
    csv_filename = get_unique_filename(os.path.join(save_directory, f"{selected_object}_export.csv"))
    fields, custom_field_info = fetch_zendesk_fields(subdomain, email, token, selected_object, "export")
    
    # Use csv_headers if provided, else fallback to fetched fields
    export_fields = csv_headers if csv_headers else [field for field in fields if field != "id"] + ["id"]
    
    # Add solved_at to export_fields for Tickets if not already present
    if selected_object in ["Tickets", "Ticket"] and "solved_at" not in export_fields:
        export_fields.insert(-1, "solved_at")  # Add before 'id'

    def write_csv_records(filename, records, headers, append=False):
        mode = 'a' if append else 'w'
        try:
            with open(filename, mode, newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=headers, quoting=csv.QUOTE_MINIMAL, escapechar='\\')
                if not append:
                    writer.writeheader()
                sanitized_records = []
                for record in records:
                    sanitized_record = {}
                    for key, value in record.items():
                        original_value = value
                        is_tags = key == "tags"
                        sanitized_value = sanitize_csv_value(value, is_tags_field=is_tags)
                        sanitized_record[key] = sanitized_value
                        if str(original_value) != str(sanitized_value):
                            logger.debug(f"Sanitized value for '{key}': '{str(original_value)[:50]}...' -> '{str(sanitized_value)[:50]}...'")
                    sanitized_records.append(sanitized_record)
                writer.writerows(sanitized_records)
                logger.info(f"Wrote {len(records)} records to {filename} (append={append})")
        except Exception as e:
            logger.error(f"Error writing CSV to {filename}: {str(e)}")
            raise

    write_csv_records(csv_filename, [], export_fields)

    # Set up API URL based on object type
    if selected_object in ["Tickets", "Ticket"]:
        api_url = f"{base_url}/api/v2/incremental/{api_object}/cursor.json?start_time=0&exclude_deleted=true&include=metric_sets"
    else:
        api_url = f"{base_url}/api/v2/incremental/{api_object}.json?start_time=0&exclude_deleted=true"

    processed_count = 0  # Total records fetched from API
    rows_written = 0    # Total records written to CSV
    batch_size = 1000   # Records per write batch
    record_buffer = []
    seen_ids = set()

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            # Initial request
            future_to_url = {executor.submit(lambda u: (print(f"Sending request for {selected_object} to URL: {u}"), make_request_with_retries("GET", u, auth, stop_event=stop_event))[1], api_url): api_url}
            seen_cursors = set()  # For cursor-based API (Tickets)

            while future_to_url:
                for future in as_completed(future_to_url):
                    url = future_to_url.pop(future)
                    try:
                        response = future.result()
                        if not response:
                            logger.warning(f"No response from {url}")
                            continue
                        json_response = response.json()
                    except Exception as e:
                        logger.error(f"Error fetching {url}: {e}")
                        continue

                    records = json_response.get(response_key + "s", [])

                    for record in records:
                        if stop_event and stop_event.is_set():
                            logger.info("Stop event detected during record processing. Finishing current batch.")
                            break

                        # Filter for active end-users when exporting Users
                        if selected_object in ["Users", "User"]:
                            if record.get("role") != "end-user" or record.get("active") is False:
                                processed_count += 1
                                continue

                        # Filter out deleted organizations
                        if selected_object in ["Organizations", "Organization"] and record.get("deleted_at") is not None:
                            processed_count += 1
                            continue

                        # Filter out deleted tickets as a fallback
                        if selected_object in ["Tickets", "Ticket"] and record.get("status") == "deleted":
                            processed_count += 1
                            continue

                        record_id = str(record.get("id"))
                        if record_id in seen_ids:
                            continue
                        seen_ids.add(record_id)
                        processed_count += 1

                        flat_record = flatten_record(record, {str(cf["id"]): cf["display_name"] for cf in custom_field_info})
                        filtered_record = {k: flat_record.get(k, "") for k in export_fields}

                        # Add solved_at from metric_set for Tickets
                        if selected_object in ["Tickets", "Ticket"]:
                            metric_sets = json_response.get("metric_sets", [])
                            for metric in metric_sets:
                                if metric.get("ticket_id") == record["id"]:
                                    filtered_record["solved_at"] = metric.get("solved_at", "")
                                    break
                            else:
                                filtered_record["solved_at"] = ""

                        record_buffer.append(filtered_record)

                    if len(record_buffer) >= batch_size:
                        write_csv_records(csv_filename, record_buffer, export_fields, append=True)
                        rows_written += len(record_buffer)
                        record_buffer.clear()
                        if app_instance and total_records > 0:
                            app_instance.safe_update_progress_bar((rows_written / total_records) * 100, rows_written)

                    # Check stop event before queuing new pages
                    if stop_event and stop_event.is_set():
                        logger.info("Stop event detected. Skipping pagination to finalize export.")
                        future_to_url.clear()
                        break

                    # Handle pagination based on object type
                    if selected_object in ["Tickets", "Ticket"]:
                        # Cursor-based pagination for Tickets
                        if json_response.get("end_of_stream", False):
                            logger.info(f"Reached end_of_stream for {selected_object}")
                            future_to_url.clear()
                        else:
                            next_cursor = json_response.get("after_cursor")
                            if next_cursor and next_cursor not in seen_cursors:
                                seen_cursors.add(next_cursor)
                                next_url = f"{base_url}/api/v2/incremental/{api_object}/cursor.json?cursor={next_cursor}&exclude_deleted=true&include=metric_sets"
                                future_to_url[executor.submit(lambda u: (print(f"Sending request for {selected_object} to URL: {u}"), make_request_with_retries("GET", u, auth, stop_event=stop_event))[1], next_url)] = next_url
                            elif not next_cursor:
                                logger.warning(f"No after_cursor found, but end_of_stream is not true for {url}")
                    else:
                        # Time-based pagination for Users and Organizations
                        if json_response.get("end_of_stream", False):
                            logger.info(f"Reached end_of_stream for {selected_object}")
                            future_to_url.clear()
                        else:
                            next_page = json_response.get("next_page")
                            if next_page and next_page not in seen_cursors:
                                seen_cursors.add(next_page)
                                future_to_url[executor.submit(lambda u: (print(f"Sending request for {selected_object} to URL: {u}"), make_request_with_retries("GET", u, auth, stop_event=stop_event))[1], next_page)] = next_page
                            elif not next_page:
                                logger.warning(f"No next_page found, but end_of_stream is not true for {url}")

    except Exception as e:
        logger.error(f"Unexpected error during export: {e}")
        raise
    finally:
        # Write any remaining records in buffer
        if record_buffer:
            logger.info(f"Writing {len(record_buffer)} remaining records to CSV before exit")
            write_csv_records(csv_filename, record_buffer, export_fields, append=True)
            rows_written += len(record_buffer)
            record_buffer.clear()

        # Final progress update
        if app_instance and total_records > 0:
            final_progress = min((rows_written / total_records) * 100, 100)
            app_instance.safe_update_progress_bar(final_progress, rows_written)

        logger.info(f"Export completed. File: {csv_filename}, Records Written: {rows_written}, Total Fetched: {processed_count}")
        if app_instance:
            app_instance.success_filename = csv_filename
            app_instance.error_filename = None

        is_exporting = False
    return rows_written







def export_agents(subdomain, email, token, save_directory, stop_event=None, app_instance=None):
    base_url = f"https://{subdomain}.zendesk.com"
    auth = (f"{email}/token", token)
    csv_filename = get_unique_filename(os.path.join(save_directory, "agents_export.csv"))
    fields, custom_field_info = fetch_zendesk_fields(subdomain, email, token, "Agent", "export")
    export_fields = [field for field in fields if field != "id"] + ["id"]

    fetch_total_records(subdomain, email, token, "Agent")
    total_records = get_total_records() or 1
    logger.info(f"Total Agents to export: {total_records}")

    def write_csv_records(filename, records, headers, append=False):
        mode = 'a' if append else 'w'
        try:
            with open(filename, mode, newline='', encoding='utf-8-sig') as f:  # Use 'utf-8-sig' for BOM
                writer = csv.DictWriter(f, fieldnames=headers, quoting=csv.QUOTE_MINIMAL, escapechar='\\')
                if not append:
                    writer.writeheader()
                # Sanitize and validate records before writing
                sanitized_records = []
                for record in records:
                    sanitized_record = {}
                    for key, value in record.items():
                        original_value = value
                        sanitized_value = sanitize_csv_value(value)
                        sanitized_record[key] = sanitized_value
                        if str(original_value) != str(sanitized_value):
                            logger.debug(f"Sanitized value for '{key}': '{str(original_value)[:50]}...' -> '{str(sanitized_value)[:50]}...'")
                    sanitized_records.append(sanitized_record)
                writer.writerows(sanitized_records)
            logger.info(f"Wrote {len(records)} records to {filename} (append={append})")
        except Exception as e:
            logger.error(f"Error writing CSV to {filename}: {str(e)}")
            raise

    write_csv_records(csv_filename, [], export_fields)

    # Using Search API with query for agents and admins, explicitly setting per_page=100
    search_query = "type:user role:agent role:admin"
    api_url = f"{base_url}/api/v2/search.json?query={urllib.parse.quote(search_query)}&per_page=100"
    headers = {"Content-Type": "application/json"}

    processed_count = 0
    rows_written = 0
    batch_size = 1000  # This is the CSV write batch size, not API page size
    record_buffer = []
    seen_ids = set()

    def fetch_data(url):
        max_retries = 5
        retry_delay = 5
        for attempt in range(max_retries):
            try:
                print(f"🛰️ Requesting URL: {url}")
                response = requests.get(url, auth=auth, headers=headers)
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", retry_delay))
                    time.sleep(retry_after)
                    continue
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    continue
                return {}

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future_to_url = {executor.submit(fetch_data, api_url): api_url}
            seen_pages = set()

            while future_to_url:
                if stop_event and stop_event.is_set():
                    break

                for future in as_completed(future_to_url):
                    json_response = future.result() or {}
                    users = json_response.get("results", [])

                    for user in users:
                        if stop_event and stop_event.is_set():
                            break
                        user_id = str(user.get("id"))
                        if user_id in seen_ids:
                            continue
                        seen_ids.add(user_id)
                        processed_count += 1

                        flat_record = flatten_record(user, {str(cf["id"]): cf["display_name"] for cf in custom_field_info})
                        filtered_record = {k: flat_record.get(k, "") for k in export_fields}
                        record_buffer.append(filtered_record)

                        if len(record_buffer) >= batch_size:
                            write_csv_records(csv_filename, record_buffer, export_fields, append=True)
                            rows_written += len(record_buffer)
                            record_buffer.clear()
                            if app_instance:
                                app_instance.safe_update_progress_bar((rows_written / total_records) * 100, rows_written)

                    next_page = json_response.get("next_page")
                    if next_page and next_page not in seen_pages:
                        seen_pages.add(next_page)
                        future_to_url[executor.submit(fetch_data, next_page)] = next_page
                    else:
                        future_to_url.clear()

    finally:
        if record_buffer:
            write_csv_records(csv_filename, record_buffer, export_fields, append=True)
            rows_written += len(record_buffer)

        if app_instance:
            final_progress = min((rows_written / total_records) * 100, 100)
            app_instance.safe_update_progress_bar(final_progress, rows_written)

        logger.info(f"Export completed. File: {csv_filename}, Records Written: {rows_written}")
        if app_instance:
            app_instance.success_filename = csv_filename
            app_instance.error_filename = None

    return rows_written








def export_ticket_comments(subdomain, email, token, save_directory, stop_event=None, app_instance=None):
    base_url = f"https://{subdomain}.zendesk.com"
    auth = (f"{email}/token", token)
    csv_filename = get_unique_filename(os.path.join(save_directory, "ticket_comments_export.csv"))
    comment_fields = [
        "ticket_id", "event_id", "author_id", "body", "html_body", "public", "created_at",
        "via_channel", "via_source_from", "via_source_to"
    ]

    # Fetch total tickets and estimate comments
    fetch_total_records(subdomain, email, token, "Tickets")
    total_tickets = get_total_records()
    estimated_total_comments = max(total_tickets * 10, 1)  # Estimate 10 comments per ticket
    logger.info(f"Estimated total comments: {estimated_total_comments}")

    def write_csv_records(filename, records, headers, append=False):
        mode = 'a' if append else 'w'
        try:
            with open(filename, mode, newline='', encoding='utf-8-sig') as f:  # Use 'utf-8-sig' for BOM
                writer = csv.DictWriter(f, fieldnames=headers, quoting=csv.QUOTE_MINIMAL, escapechar='\\')
                if not append:
                    writer.writeheader()
                # Sanitize and validate records before writing
                sanitized_records = []
                for record in records:
                    sanitized_record = {}
                    for key, value in record.items():
                        original_value = value
                        sanitized_value = sanitize_csv_value(value)
                        sanitized_record[key] = sanitized_value
                        if str(original_value) != str(sanitized_value):
                            logger.debug(f"Sanitized value for '{key}': '{str(original_value)[:50]}...' -> '{str(sanitized_value)[:50]}...'")
                    sanitized_records.append(sanitized_record)
                writer.writerows(sanitized_records)
            logger.info(f"Wrote {len(records)} records to {filename} (append={append})")
        except Exception as e:
            logger.error(f"Error writing CSV to {filename}: {str(e)}")
            raise

    write_csv_records(csv_filename, [], comment_fields)

    start_time = int(time.time()) - (7 * 24 * 60 * 60)
    api_url = f"{base_url}/api/v2/incremental/ticket_events.json?start_time={start_time}"
    headers = {"Content-Type": "application/json"}

    processed_count = 0
    rows_written = 0
    batch_size = 1000
    record_buffer = []
    seen_ids = set()

    def fetch_data(url):
        max_retries = 5
        retry_delay = 5
        for attempt in range(max_retries):
            try:
                response = requests.get(url, auth=auth, headers=headers)
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", retry_delay))
                    time.sleep(retry_after)
                    continue
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    continue
                return {}

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future_to_url = {executor.submit(fetch_data, api_url): api_url}
            seen_pages = set()

            while future_to_url:
                if stop_event and stop_event.is_set():
                    break

                for future in as_completed(future_to_url):
                    json_response = future.result() or {}
                    events = json_response.get("ticket_events", [])

                    for event in events:
                        if stop_event and stop_event.is_set():
                            break
                        ticket_id = event.get("ticket_id")
                        child_events = event.get("child_events", [])

                        for child_event in child_events:
                            if child_event.get("type") != "Comment":
                                continue
                            comment_id = str(child_event.get("id"))
                            if comment_id in seen_ids:
                                continue
                            seen_ids.add(comment_id)
                            processed_count += 1

                            via = child_event.get("via", {})
                            flat_record = {
                                "ticket_id": ticket_id,
                                "event_id": comment_id,
                                "author_id": child_event.get("author_id", ""),
                                "body": child_event.get("body", ""),
                                "html_body": child_event.get("html_body", ""),
                                "public": str(child_event.get("public", True)),
                                "created_at": child_event.get("created_at", ""),
                                "via_channel": via.get("channel", ""),
                                "via_source_from": via.get("source", {}).get("from", ""),
                                "via_source_to": via.get("source", {}).get("to", "")
                            }
                            record_buffer.append(flat_record)

                            if len(record_buffer) >= batch_size:
                                write_csv_records(csv_filename, record_buffer, comment_fields, append=True)
                                rows_written += len(record_buffer)
                                record_buffer.clear()
                                if app_instance:
                                    app_instance.safe_update_progress_bar((rows_written / estimated_total_comments) * 100, rows_written)

                    if json_response.get("end_of_stream", False):
                        future_to_url.clear()
                        break

                    next_page = json_response.get("next_page")
                    if next_page and next_page not in seen_pages:
                        seen_pages.add(next_page)
                        future_to_url[executor.submit(fetch_data, next_page)] = next_page

    finally:
        if record_buffer:
            write_csv_records(csv_filename, record_buffer, comment_fields, append=True)
            rows_written += len(record_buffer)

        if app_instance:
            final_progress = min((rows_written / estimated_total_comments) * 100, 100)
            app_instance.safe_update_progress_bar(final_progress, rows_written)

        logger.info(f"Export completed. File: {csv_filename}, Records Written: {rows_written}")
        if app_instance:
            app_instance.success_filename = csv_filename
            app_instance.error_filename = None

    return rows_written











def export_custom_objects(subdomain, email, token, object_key, save_directory, stop_event=None, app_instance=None):
    """Exports custom object records from Zendesk using cursor pagination."""
    base_url = f"https://{subdomain}.zendesk.com"
    auth = HTTPBasicAuth(f"{email}/token", token)
    api_url = f"{base_url}/api/v2/custom_objects/{object_key}/records"
    
    # Generate unique filename
    export_filename = get_unique_filename(os.path.join(save_directory, f"{object_key}_export.csv"))
    headers_written = False
    exported_count = 0
    
    # Fetch total records for progress tracking
    fetch_total_records(subdomain, email, token, object_key)
    total_records = get_total_records() or 1  # Fallback to 1 to avoid division by zero
    
    logging.info(f"Exporting {object_key} records to {export_filename}")
    print(f"Exporting {object_key} records to {export_filename}")

    try:
        url = api_url  # Start with base URL
        while url and not (stop_event and stop_event.is_set()):
            logging.info(f"📡 Sending GET request to {url}")
            print(f"📡 Sending GET request to {url}")
            response = make_request_with_retries("GET", url, auth, stop_event=stop_event)
            if not response or response.status_code not in [200]:
                logging.error(f"Failed to fetch custom object records: {response.text if response else 'No response'}")
                break
            
            data = response.json()
            records = data.get("custom_object_records", [])
            if not records:
                logging.info("No records found to export in this page.")
                break

            # Flatten records and determine headers dynamically
            flattened_records = [flatten_record(record) for record in records]
            if flattened_records:
                if not headers_written:
                    headers = list(flattened_records[0].keys())
                    with open(export_filename, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=headers)
                        writer.writeheader()
                    headers_written = True
                
                with open(export_filename, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=headers)
                    writer.writerows(flattened_records)
                
                exported_count += len(flattened_records)
                logging.info(f"Exported {exported_count} records so far")
                if app_instance:
                    app_instance.safe_update_progress_bar((exported_count / total_records) * 100, exported_count)

            # Check pagination metadata
            meta = data.get("meta", {})
            has_more = meta.get("has_more", False)
            links = data.get("links", {})
            url = links.get("next") if has_more else None
            logging.info(f"Has more: {has_more}, Next URL: {url}")

        if not stop_event or not stop_event.is_set():
            logging.info(f"✅ Exported {exported_count} {object_key} records to {export_filename}")
            print(f"✅ Exported {exported_count} {object_key} records to {export_filename}")
            if app_instance:
                app_instance.safe_update_progress_bar(100, exported_count)

    except Exception as e:
        logging.error(f"Error exporting {object_key} records: {e}")
        raise
    
    return exported_count









def make_request_with_retries(method, url, auth, json=None, max_retries=5, base_delay=5, stop_event=None):
    global rate_limit_resume_time
    retry_delay = base_delay
    headers = {"Content-Type": "application/json"}
    retryable_status_codes = {408, 429, 500, 502, 503, 504}  # Retryable HTTP status codes

    for attempt in range(max_retries):
        if stop_event and stop_event.is_set():
            logger.info(f"Stop event triggered, aborting request to {url}")
            print(f"🛑 Stop event triggered, aborting request to {url}")
            raise RequestException("Request aborted due to stop event")

        with rate_limit_lock:
            if not rate_limit_event.is_set():
                current_time = time.time()
                if current_time < rate_limit_resume_time:
                    pause_duration = rate_limit_resume_time - current_time
                    logger.info(f"⏸️ Pausing for {pause_duration:.1f} seconds due to prior rate limit.")
                    print(f"⏸️ Pausing for {pause_duration:.1f} seconds due to prior rate limit.")
                    time.sleep(pause_duration)
                jitter = random.uniform(0, 0.3)
                logger.info(f"⏳ Adding {jitter:.1f}s jitter before resuming.")
                print(f"⏳ Adding {jitter:.1f}s jitter before resuming.")
                time.sleep(jitter)
                rate_limit_event.set()

        try:
            logger.info(f"Sending {method} request to {url} (attempt {attempt + 1}/{max_retries})")
            print(f"📡 Sending {method} request to {url} (attempt {attempt + 1}/{max_retries})")
            request_kwargs = {
                'url': url,
                'auth': auth,
                'headers': headers,
                'timeout': 30  # Explicit timeout to handle slow responses
            }
            if json is not None:
                request_kwargs['json'] = json

            time.sleep(random.uniform(0, 0.8))  # Consistent jitter as in original
            if method == "GET":
                response = requests.get(**request_kwargs)
            elif method == "POST":
                response = requests.post(**request_kwargs)
            elif method == "PUT":
                response = requests.put(**request_kwargs)
            elif method == "DELETE":
                response = requests.delete(**request_kwargs)
            else:
                logger.error(f"Unsupported HTTP method: {method}")
                print(f"❌ Unsupported HTTP method: {method}")
                raise ValueError(f"Unsupported HTTP method: {method}")

            # Handle rate limiting (429)
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", retry_delay))
                logger.warning(f"⚠️ Rate limit (429) hit. Retry-After: {retry_after} seconds (attempt {attempt + 1}/{max_retries})")
                print(f"⚠️ Rate limit (429) hit. Retry-After: {retry_after} seconds (attempt {attempt + 1}/{max_retries})")
                with rate_limit_lock:
                    current_time = time.time()
                    new_resume_time = current_time + retry_after
                    if new_resume_time > rate_limit_resume_time:
                        rate_limit_resume_time = new_resume_time
                        rate_limit_event.clear()
                pause_time = retry_after + random.uniform(2, 5)
                logger.info(f"⏳ Pausing for {pause_time:.1f} seconds (Retry-After: {retry_after}s + jitter).")
                print(f"⏳ Pausing for {pause_time:.1f} seconds (Retry-After: {retry_after}s + jitter).")
                time.sleep(pause_time)
                retry_delay = base_delay * (2 ** (attempt + 1)) + random.uniform(0, 0.1 * base_delay)  # Exponential backoff with jitter
                continue

            # Handle other retryable status codes
            if response.status_code in retryable_status_codes:
                backoff_time = base_delay * (2 ** attempt) + random.uniform(0, 0.1 * base_delay)
                logger.warning(f"⚠️ Retryable status {response.status_code} on attempt {attempt + 1}/{max_retries}, waiting {backoff_time:.2f}s")
                print(f"⚠️ Retryable status {response.status_code} on attempt {attempt + 1}/{max_retries}, waiting {backoff_time:.2f}s")
                time.sleep(backoff_time)
                retry_delay = base_delay * (2 ** (attempt + 1)) + random.uniform(0, 0.1 * base_delay)
                continue

            # Handle non-retryable errors
            if response.status_code in {400, 401, 403, 404, 422}:
                logger.warning(f"⚠️ Non-retryable status {response.status_code} for {url}: {response.text}. Aborting retries.")
                print(f"⚠️ Non-retryable status {response.status_code} for {url}: {response.text}. Aborting retries.")
                return response  # Return immediately for 400, 401, 403, 404, 422

            # Raise for other HTTP errors
            response.raise_for_status()

            # Verify JSON response if expected
            if response.headers.get('Content-Type', '').startswith('application/json'):
                response.json()  # Ensure JSON is parseable
            else:
                logger.warning(f"Non-JSON response received from {url}")
                print(f"⚠️ Non-JSON response received from {url}")

        
            logger.info(f"Successful request to {url} on attempt {attempt + 1}")
            print(f"✅ Successful request to {url} on attempt {attempt + 1}")
            return response

        except (ConnectionError, Timeout) as e:
            if attempt < max_retries - 1:
                backoff_time = base_delay * (2 ** attempt) + random.uniform(0, 0.1 * base_delay)
                logger.warning(f"Network error ({type(e).__name__}) on attempt {attempt + 1}/{max_retries}, waiting {backoff_time:.2f}s: {str(e)}")
                print(f"⚠️ Network error ({type(e).__name__}) on attempt {attempt + 1}/{max_retries}, waiting {backoff_time:.2f}s: {str(e)}")
                time.sleep(backoff_time)
                retry_delay = base_delay * (2 ** (attempt + 1)) + random.uniform(0, 0.1 * base_delay)
                continue
            logger.error(f"Max retries reached for {method} {url}: {str(e)}")
            print(f"❌ Max retries reached for {method} {url}: {str(e)}")
            raise
        except HTTPError as e:
            if e.response and e.response.status_code not in retryable_status_codes:
                logger.error(f"Non-retryable HTTP error on attempt {attempt + 1}: {str(e)}")
                print(f"❌ Non-retryable HTTP error on attempt {attempt + 1}: {str(e)}")
                return e.response  # Return response for non-retryable errors
            if attempt < max_retries - 1:
                backoff_time = base_delay * (2 ** attempt) + random.uniform(0, 0.1 * base_delay)
                logger.warning(f"HTTP error on attempt {attempt + 1}/{max_retries}, waiting {backoff_time:.2f}s: {str(e)}")
                print(f"⚠️ HTTP error on attempt {attempt + 1}/{max_retries}, waiting {backoff_time:.2f}s: {str(e)}")
                time.sleep(backoff_time)
                retry_delay = base_delay * (2 ** (attempt + 1)) + random.uniform(0, 0.1 * base_delay)
                continue
            logger.error(f"Max retries reached for {method} {url}: {str(e)}")
            print(f"❌ Max retries reached for {method} {url}: {str(e)}")
            raise
        except ValueError as e:
            logger.error(f"JSON decode error on attempt {attempt + 1}: {str(e)}")
            print(f"❌ JSON decode error on attempt {attempt + 1}: {str(e)}")
            raise RequestException(f"Invalid JSON response: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error on attempt {attempt + 1}: {str(e)}")
            print(f"❌ Unexpected error on attempt {attempt + 1}: {str(e)}")
            raise

    logger.error(f"Max retries reached for {method} {url}: No response")
    print(f"❌ Max retries reached for {method} {url}: No response")
    return None








def import_records(
    subdomain,
    email,
    token,
    selected_object,
    csv_data,
    csv_headers,
    field_mapping,
    save_directory,
    stop_event=None,
    progress_bar=None,
    action_type="import",
    app_instance=None,
    chunk_size=100,
    success_filename=None,
    error_filename=None,
    processed_records_so_far=0
):
    """
    Imports records to Zendesk in batches using the appropriate create_many endpoint.
    For Agents, fetches the Light Agent custom_role_id if not mapped and adds it to the payload.
    Stops new submissions on stop_event but completes in-flight jobs.
    
    Args:
        subdomain (str): Zendesk subdomain.
        email (str): Zendesk email for authentication.
        token (str): Zendesk API token.
        selected_object (str): Type of object to import (e.g., User, Agent, Ticket).
        csv_data (list): List of CSV records.
        csv_headers (list): List of CSV header names.
        field_mapping (dict): Mapping of CSV columns to Zendesk fields.
        save_directory (str): Directory to save success/error CSVs.
        stop_event (threading.Event, optional): Event to stop the operation.
        progress_bar (callable, optional): Callback for progress updates.
        action_type (str): Operation type ('import' by default).
        app_instance: Application instance for progress and group data.
        chunk_size (int): Size of batches for API calls.
        success_filename (str, optional): Pre-generated filename for success CSV.
        error_filename (str, optional): Pre-generated filename for error CSV.
    
    Returns:
        tuple: (success_count, error_count, skipped_count, total_processed)
    """
    base_url = f"https://{subdomain}.zendesk.com"
    auth = HTTPBasicAuth(f"{email}/token", token)

    # Define headers for success and error CSVs
    success_headers = csv_headers + ["ID", "Status"]
    error_headers = csv_headers + ["Error Message"]

    # Use provided filenames or fallback to default
    success_filename = success_filename or os.path.join(save_directory, f"{selected_object}_{action_type}_success.csv")
    error_filename = error_filename or os.path.join(save_directory, f"{selected_object}_{action_type}_errors.csv")

    object_map = {
        "User": "Users",
        "Users": "Users",
        "Agent": "Users",
        "Organization": "Organizations",
        "Organizations": "Organizations",
        "Ticket": "Tickets",
        "Tickets": "Tickets"
    }
    payload_key_map = {
        "Users": "users",
        "Organizations": "organizations",
        "Tickets": "tickets"
    }
    selected_object_normalized = object_map.get(selected_object, selected_object)
    api_url = f"{base_url}/api/v2/{selected_object_normalized.lower()}/create_many.json"
    payload_key = payload_key_map.get(selected_object_normalized)

    if not api_url or not payload_key:
        logger.error(f"Invalid object for import: {selected_object}")
        return 0, 0, 0, 0

    if not save_directory:
        logger.error("No save directory specified for import operation")
        return 0, 0, 0, 0

    try:
        fields, custom_field_info = fetch_zendesk_fields(
            subdomain, email, token, selected_object, action_type
        )
        logger.info(f"Zendesk fields: {fields}")
    except Exception as e:
        logger.error(f"Failed to fetch Zendesk fields: {e}")
        return 0, 0, 0, 0

    # Fetch Light Agent role ID for Agent imports if custom_role_id is not mapped
    light_agent_role_id = None
    custom_role_id_mapped = "custom_role_id" in field_mapping.values()
    if selected_object == "Agent" and not custom_role_id_mapped:
        from helpers import fetch_light_agent_role_id
        light_agent_role_id = fetch_light_agent_role_id(subdomain, email, token)
        if light_agent_role_id is None:
            logger.warning("Light Agent role ID not found; proceeding without custom_role_id")
        else:
            logger.info(f"Using Light Agent role ID: {light_agent_role_id} for Agent imports")

    with processed_job_ids_lock:
        processed_job_ids.clear()
    with queued_job_ids_lock:
        queued_job_ids.clear()

    batch_size = chunk_size
    success_records = []
    error_records = []
    total_records = len(csv_data)
    cumulative_processed = 0
    total_success_count = 0
    total_error_count = 0
    total_skipped_count = 0

    logger.info(f"Total records to process: {total_records}")
    print(f"Total records to process: {total_records}")

    submit_executor = ThreadPoolExecutor(max_workers=1)
    status_executor = ThreadPoolExecutor(max_workers=15)

    def write_csv_records(filename, records, headers, append=True):
        mode = 'a' if append else 'w'
        logger.debug(f"Writing {len(records)} records to {filename} in mode '{mode}'")
        try:
            with open(filename, mode, newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                if not append and not os.path.exists(filename):
                    writer.writeheader()
                # Filter records to include only keys in headers
                filtered_records = [
                    {key: record.get(key, "") for key in headers}
                    for record in records
                ]
                writer.writerows(filtered_records)
                f.flush()  # Ensure data is written to disk
            print(f"📝 Wrote {len(records)} records to {filename} (append={append})")
        except Exception as e:
            logger.error(f"Failed to write to {filename}: {e}")
            print(f"❌ Failed to write to {filename}: {e}")

    def process_job_status(job_id, batch, response, group_assignments):
        nonlocal cumulative_processed, total_success_count, total_error_count, total_skipped_count
        with processed_job_ids_lock:
            if job_id in processed_job_ids:
                logger.info(f"🛑 Job {job_id} already processed. Skipping.")
                return
            processed_job_ids.add(job_id)

        job_url = f"{base_url}/api/v2/job_statuses/{job_id}.json"
        temp_success = []
        temp_error = []
        jittered_sleep = 4 + random.uniform(0, 0.3)
        time.sleep(jittered_sleep)
        logger.info(f"⏳ Applied jittered sleep of {jittered_sleep:.2f}s for job status check")
        job_results = check_job_status(job_url, auth)
        if job_results:
            results = job_results.get("results", [])
            batch_dicts = [dict(zip(csv_headers, record)) for record in batch]
            user_ids = []
            batch_processed = 0
            seen_record_keys = set()  # Track unique records to prevent duplicates
            for index, result in enumerate(results):
                if index >= len(batch_dicts):
                    logger.warning(f"Result index {index} exceeds batch size {len(batch_dicts)}")
                    break
                record = batch_dicts[index]
                zendesk_id = result.get("id")
                status = result.get("status", "")

                # Create a unique key for deduplication (e.g., zendesk_id or record hash)
                record_key = str(zendesk_id) if zendesk_id else f"{index}_{str(record)}"

                if record_key not in seen_record_keys:
                    if status == "Created" and zendesk_id:
                        success_record = {
                            **record,
                            "ID": zendesk_id,
                            "Status": "Created"
                        }
                        temp_success.append(success_record)
                        user_ids.append(zendesk_id)
                        seen_record_keys.add(record_key)
                    else:
                        error_msg = (
                            result.get("details") or
                            result.get("error") or
                            result.get("errors") or
                            "Unknown error"
                        )
                        if isinstance(error_msg, list):
                            error_msg = "; ".join(error_msg)
                        temp_error.append({
                            **record,
                            "Error Message": error_msg
                        })
                    batch_processed += 1
                    seen_record_keys.add(record_key)  # Mark record as processed
                else:
                    logger.debug(f"Skipping duplicate record for key: {record_key}")

                with threading.Lock():
                    if batch_processed > cumulative_processed:  # Only increment if record was processed
                        cumulative_processed += 1
                        total_processed_so_far = processed_records_so_far + cumulative_processed
                        if total_processed_so_far % 200 == 0 and app_instance and hasattr(app_instance, 'progress_manager'):
                            app_instance.progress_manager.update_progress(
                                total_processed_so_far, total_records
                            )
                            logger.debug(f"Progress update: {total_processed_so_far}/{total_records} records")
                    else:
                        logger.debug(f"Skipped incrementing cumulative_processed for duplicate key: {record_key}")

            with threading.Lock():
                if temp_success:
                    success_records.extend(temp_success)
                    total_success_count += len(temp_success)
                    if len(success_records) >= chunk_size:
                        write_csv_records(success_filename, success_records, success_headers, append=True)
                        success_records.clear()
                if temp_error:
                    error_records.extend(temp_error)
                    total_error_count += len(temp_error)
                    if len(error_records) >= chunk_size:
                        write_csv_records(error_filename, error_records, error_headers, append=True)
                        error_records.clear()
                logger.info(
                    f"Batch processed: {batch_processed}, Cumulative processed: {cumulative_processed}, "
                    f"Success: {len(temp_success)}, Errors: {len(temp_error)}"
                )

            # Assign group memberships for successful users
            if selected_object == "Agent" and user_ids and group_assignments:
                assign_group_memberships(user_ids, group_assignments)

        else:
            with threading.Lock():
                total_skipped_count += len(batch)
                logger.error(f"❌ Failed to retrieve job status for job {job_id}")

    def assign_group_memberships(user_ids, group_assignments):
        if not group_assignments or not app_instance.group_data:
            logger.info("No group assignments or group data available. Skipping.")
            return

        memberships = []
        for idx, group_names in group_assignments:
            if idx >= len(user_ids):
                logger.warning(f"Index {idx} exceeds user_ids length {len(user_ids)}")
                continue
            user_id = user_ids[idx]
            for group_name in group_names:
                group_id = app_instance.group_data.get(group_name)
                if group_id:
                    memberships.append({"user_id": user_id, "group_id": group_id})
                    logger.debug(
                        f"Adding membership: user_id={user_id}, group_id={group_id} "
                        f"for group '{group_name}'"
                    )
                else:
                    logger.warning(
                        f"Group '{group_name}' not found in group_data for user_id={user_id}"
                    )

        if not memberships:
            logger.info("No valid group memberships to assign.")
            return

        for i in range(0, len(memberships), 100):
            batch = memberships[i:i + 100]
            payload = {"group_memberships": batch}
            url = f"{base_url}/api/v2/group_memberships/create_many.json"
            logger.info(f"Submitting {len(batch)} group memberships")
            if stop_event and stop_event.is_set():
                logger.info("🛑 Stop detected. Skipping group membership submission.")
                return
            response = make_request_with_retries(
                "POST", url, auth, json=payload, stop_event=stop_event
            )
            if response and response.status_code in [200, 201]:
                logger.info(f"Successfully assigned {len(batch)} group memberships")
            else:
                logger.error(
                    f"Failed to assign group memberships: "
                    f"{response.text if response else 'No response'}"
                )

    try:
        submission_futures = {}
        status_futures = {}
        start_time = time.time()
        requests_this_minute = 0
        minute_interval = 60
        request_delay = 1.5 if selected_object_normalized in ["Tickets", "Ticket"] else 1

        for batch_index in range(0, len(csv_data), batch_size):
            if stop_event and stop_event.is_set():
                logger.info("🛑 Stop detected. No new batches submitted.")
                break

            batch = csv_data[batch_index:batch_index + batch_size]
            mapped_records, group_assignments = map_fields(
                batch,
                field_mapping,
                csv_headers,
                selected_object,
                custom_field_info,
                group_data=app_instance.group_data if app_instance else None,
                action_type=action_type
            )

            # Add Light Agent role ID to Agent records if not mapped
            if selected_object == "Agent" and light_agent_role_id is not None:
                for record in mapped_records:
                    if "custom_role_id" not in record:
                        record["custom_role_id"] = light_agent_role_id
                        logger.debug(f"Added Light Agent role ID {light_agent_role_id} to record")

            payload = {payload_key: mapped_records}
            logger.info(f"Submitting batch: {len(batch)} records")
            logger.debug(f"Payload: {json.dumps(payload, default=str)}")
            print(f"📡 Submitting POST request {batch_index // batch_size + 1} to {api_url}")

            requests_this_minute += 1
            elapsed_time = time.time() - start_time
            if elapsed_time >= minute_interval:
                pause_time = 1
                logger.info(f"⏸️ Pausing submissions for {pause_time}s")
                time.sleep(pause_time)
                start_time = time.time()
                requests_this_minute = 0

            jittered_delay = request_delay + random.uniform(0, 0.3)
            time.sleep(jittered_delay)
            logger.info(f"⏳ Applied delay of {jittered_delay:.3f}s before submission")

            future = submit_executor.submit(
                make_request_with_retries,
                "POST",
                api_url,
                auth,
                json=payload,
                stop_event=stop_event
            )
            submission_futures[future] = (batch, group_assignments)

            for future in [f for f in submission_futures if f.done()]:
                batch, group_assignments = submission_futures.pop(future)
                try:
                    response = future.result()
                    if response.status_code == 400:
                        if stop_event:
                            stop_event.set()
                        error_message = (
                            "Bad request submitted. Please check the following:\n"
                            "- Date formats (e.g., YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ).\n"
                            "- Role values (e.g., 'end-user', 'agent', 'admin').\n"
                            "- Dropdown or multi-select formats.\n"
                            "- Required fields are present and correctly mapped."
                        )
                        logger.error(f"❌ Bad Request (400) for {api_url}: {response.text}")
                        raise requests.exceptions.RequestException(error_message, response=response)

                    if response and response.status_code in [200, 201]:
                        job_id = response.json().get("job_status", {}).get("id", "")
                        if job_id:
                            with queued_job_ids_lock:
                                if job_id not in queued_job_ids:
                                    queued_job_ids.add(job_id)
                                    status_future = status_executor.submit(
                                        process_job_status,
                                        job_id,
                                        batch,
                                        response,
                                        group_assignments
                                    )
                                    status_futures[status_future] = (
                                        job_id,
                                        batch,
                                        group_assignments
                                    )
                    else:
                        with threading.Lock():
                            error_msg = (
                                f"HTTP {response.status_code if response else 'Unknown'}: "
                                f"{response.text if response else 'No response'}"
                            )
                            for record in batch:
                                error_records.append({
                                    **dict(zip(csv_headers, record)),
                                    "Error Message": error_msg
                                })
                            cumulative_processed += len(batch)
                            total_error_count += len(batch)
                            if len(error_records) >= chunk_size:
                                write_csv_records(
                                    error_filename,
                                    error_records,
                                    error_headers,
                                    append=True
                                )
                                total_error_count += len(error_records)
                                error_records.clear()
                            if app_instance:
                                app_instance.safe_update_progress_bar(
                                    (cumulative_processed / total_records) * 100
                                )
                except Exception as e:
                    logger.error(f"Unexpected error in batch submission: {e}")
                    if (
                        isinstance(e, requests.exceptions.RequestException) and
                        e.response and
                        e.response.status_code == 400
                    ):
                        if stop_event:
                            stop_event.set()
                        raise
                    with threading.Lock():
                        total_skipped_count += len(batch)
                        if app_instance:
                            app_instance.safe_update_progress_bar(
                                (cumulative_processed / total_records) * 100
                            )

        for future in as_completed(submission_futures):
            batch, group_assignments = submission_futures[future]
            try:
                response = future.result()
                if response.status_code == 400:
                    if stop_event:
                        stop_event.set()
                    error_message = (
                        "Bad request submitted. Please check the following:\n"
                        "- Date formats (e.g., YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ).\n"
                        "- Role values (e.g., 'end-user', 'agent', 'admin').\n"
                        "- Dropdown or multi-select formats.\n"
                        "- Required fields are present and correctly mapped."
                    )
                    logger.error(f"❌ Bad Request (400) for {api_url}: {response.text}")
                    raise requests.exceptions.RequestException(error_message, response=response)

                if response and response.status_code in [200, 201]:
                    job_id = response.json().get("job_status", {}).get("id", "")
                    if job_id:
                        with queued_job_ids_lock:
                            if job_id not in queued_job_ids:
                                queued_job_ids.add(job_id)
                                status_future = status_executor.submit(
                                    process_job_status,
                                    job_id,
                                    batch,
                                    response,
                                    group_assignments
                                )
                                status_futures[status_future] = (
                                    job_id,
                                    batch,
                                    group_assignments
                                )
            except Exception as e:
                logger.error(f"Error in batch submission: {e}")
                if (
                    isinstance(e, requests.exceptions.RequestException) and
                    e.response and
                    e.response.status_code == 400
                ):
                    if stop_event:
                        stop_event.set()
                    raise
                with threading.Lock():
                    total_skipped_count += len(batch)

        for future in as_completed(status_futures):
            future.result()

    except Exception as e:
        logger.error(f"Unexpected error in import_records: {e}")
        raise

    finally:
        submit_executor.shutdown(wait=True)
        status_executor.shutdown(wait=True)

        with threading.Lock():
            if success_records:
                write_csv_records(success_filename, success_records, success_headers, append=True)
                
                success_records.clear()
            if error_records:
                write_csv_records(error_filename, error_records, error_headers, append=True)
                
                error_records.clear()
            if (
                app_instance and
                total_records > 0 and
                not stop_event.is_set() and
                cumulative_processed >= total_records
            ):
                app_instance.safe_update_progress_bar(100)

        logger.info(
            f"Final counts: Success={total_success_count}, Errors={total_error_count}, "
            f"Skipped={total_skipped_count}, Cumulative Processed={cumulative_processed}"
        )
        if app_instance:
            app_instance.success_filename = success_filename
            app_instance.error_filename = error_filename

    return total_success_count, total_error_count, total_skipped_count, cumulative_processed







def import_tickets_and_comments(
    subdomain,
    email,
    token,
    ticket_csv_data,
    ticket_csv_headers,
    comments_csv_data,
    comments_csv_headers,
    field_mapping,
    save_directory,
    stop_event=None,
    progress_bar=None,
    ticket_id_field=None,
    comment_ticket_id_field=None,
    app_instance=None,
    comment_field_mapping=None,
    processed_records_so_far=0
):
    """
    Imports tickets with associated comments to Zendesk.
    
    Args:
        subdomain (str): Zendesk subdomain.
        email (str): Zendesk email for authentication.
        token (str): Zendesk API token.
        ticket_csv_data (list): List of ticket CSV records.
        ticket_csv_headers (list): List of ticket CSV header names.
        comments_csv_data (list): List of comment CSV records.
        comments_csv_headers (list): List of comment CSV header names.
        field_mapping (dict): Mapping of ticket CSV columns to Zendesk fields.
        save_directory (str): Directory to save success/error CSVs.
        stop_event (threading.Event, optional): Event to stop the operation.
        progress_bar (callable, optional): Callback for progress updates.
        ticket_id_field (str, optional): CSV column for ticket ID.
        comment_ticket_id_field (str, optional): CSV column for ticket ID in comments.
        app_instance: Application instance for progress updates.
        comment_field_mapping (dict, optional): Mapping for comment fields.
    
    Returns:
        tuple: (success_count, error_count, skipped_count, total_processed)
    """
    base_url = f"https://{subdomain}.zendesk.com"
    auth = HTTPBasicAuth(f"{email}/token", token)
    ticket_api_url = f"{base_url}/api/v2/imports/tickets/create_many"

    success_filename = get_unique_filename(
        os.path.join(save_directory, "Tickets_Comments_import_success.csv")
    )
    error_filename = get_unique_filename(
        os.path.join(save_directory, "Tickets_Comments_import_errors.csv")
    )
    success_headers = ticket_csv_headers + ["ID", "Status"]
    error_headers = ticket_csv_headers + ["Error Message"]

    batch_size = 100
    success_records = []
    error_records = []
    total_records = len(ticket_csv_data)
    cumulative_processed = 0
    total_success_count = 0
    total_error_count = 0
    total_skipped_count = 0

    if app_instance and not app_instance.progress_manager:
        app_instance.progress_manager = ProgressManager(
            app_instance, total_records, update_interval=500
        )
        app_instance.progress_manager.start()

    submit_executor = ThreadPoolExecutor(max_workers=1)
    status_executor = ThreadPoolExecutor(max_workers=15)
    records_lock = threading.Lock()

    def write_csv_records(filename, records, headers, append=False):
        mode = 'a' if append else 'w'
        with open(filename, mode, newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            if not append:
                writer.writeheader()
            writer.writerows(records)

    write_csv_records(success_filename, [], success_headers)
    write_csv_records(error_filename, [], error_headers)

    if not ticket_id_field or ticket_id_field not in ticket_csv_headers:
        logging.error(f"Invalid or missing ticket_id_field: {ticket_id_field}")
        return 0, 0, 0, 0
    if not comment_ticket_id_field or comment_ticket_id_field not in comments_csv_headers:
        logging.error(f"Invalid or missing comment_ticket_id_field: {comment_ticket_id_field}")
        return 0, 0, 0, 0
    if not comment_field_mapping:
        logging.warning("No comment_field_mapping provided; using field_mapping")
        comment_field_mapping = field_mapping

    try:
        ticket_fields, ticket_custom_field_info = fetch_zendesk_fields(
            subdomain, email, token, "Tickets", "import"
        )
        custom_field_types = {str(cf["id"]): cf["type"] for cf in ticket_custom_field_info}
        logging.info(f"Ticket custom fields: {custom_field_types}")
    except Exception as e:
        logging.error(f"Failed to fetch Zendesk fields for Tickets: {e}")
        return 0, 0, 0, 0

    comments_by_ticket = {}
    for comment_row in comments_csv_data:
        comment_dict = dict(zip(comments_csv_headers, comment_row))
        ticket_id = comment_dict.get(comment_ticket_id_field)
        if ticket_id:
            comments_by_ticket.setdefault(ticket_id, []).append(
                {
                    zf: comment_dict[cf]
                    for cf, zf in comment_field_mapping.items()
                    if cf in comment_dict and zf != " "
                }
            )

    def process_job_status(job_id, batch, response):
        nonlocal cumulative_processed, total_success_count, total_error_count, total_skipped_count
        with processed_job_ids_lock:
            if job_id in processed_job_ids:
                logger.info(f"🛑 Job {job_id} already processed. Skipping.")
                return
            processed_job_ids.add(job_id)

        job_url = f"{base_url}/api/v2/job_statuses/{job_id}.json"
        temp_success = []
        temp_error = []
        jittered_sleep = 4 + random.uniform(0, 0.3)
        time.sleep(jittered_sleep)
        logger.info(f"⏳ Applied jittered sleep of {jittered_sleep:.2f}s for job status check")
        job_results = check_job_status(job_url, auth)
        if job_results:
            results = job_results.get("results", [])
            batch_dicts = [dict(zip(ticket_csv_headers, record)) for record in batch]
            logger.debug(f"Job {job_id}: Processing {len(results)} results for {len(batch_dicts)} batch records")
            seen_record_keys = set()  # Track unique records to prevent duplicates
            batch_processed = 0
            for index, result in enumerate(results):
                if index >= len(batch_dicts):
                    logger.warning(f"Result index {index} exceeds batch size {len(batch_dicts)}. Stopping processing.")
                    break
                record = batch_dicts[index]
                zendesk_id = result.get("id")
                status = result.get("status", "")

                # Create a unique key for deduplication (e.g., zendesk_id or record hash)
                record_key = str(zendesk_id) if zendesk_id else f"{index}_{str(record)}"

                if record_key not in seen_record_keys:
                    if zendesk_id:
                        success_record = {
                            **record,
                            "ID": zendesk_id,
                            "Status": "Created"
                        }
                        temp_success.append(success_record)
                        seen_record_keys.add(record_key)
                    else:
                        error_msg = (
                            result.get("message") or
                            result.get("details") or
                            result.get("error") or
                            "Unknown error"
                        )
                        if isinstance(error_msg, list):
                            error_msg = "; ".join(error_msg)
                        temp_error.append({
                            **record,
                            "Error Message": error_msg
                        })
                    batch_processed += 1
                    seen_record_keys.add(record_key)  # Mark record as processed
                else:
                    logger.debug(f"Skipping duplicate record for key: {record_key}")

                with threading.Lock():
                    if batch_processed > cumulative_processed:  # Only increment if record was processed
                        cumulative_processed += 1
                        total_processed_so_far = processed_records_so_far + cumulative_processed
                        if total_processed_so_far % 200 == 0 and app_instance and hasattr(app_instance, 'progress_manager'):
                            app_instance.progress_manager.update_progress(
                                total_processed_so_far, total_records
                            )
                            logger.debug(f"Progress update: {total_processed_so_far}/{total_records} records")
                    else:
                        logger.debug(f"Skipped incrementing cumulative_processed for duplicate key: {record_key}")

            with threading.Lock():
                if temp_success:
                    success_records.extend(temp_success)
                    total_success_count += len(temp_success)
                    if len(success_records) >= chunk_size:
                        write_csv_records(success_filename, success_records, success_headers, append=True)
                        success_records.clear()
                if temp_error:
                    error_records.extend(temp_error)
                    total_error_count += len(temp_error)
                    if len(error_records) >= chunk_size:
                        write_csv_records(error_filename, error_records, error_headers, append=True)
                        error_records.clear()
                logger.info(
                    f"Batch processed: {batch_processed}, Cumulative processed: {cumulative_processed}, "
                    f"Success: {len(temp_success)}, Errors: {len(temp_error)}"
                )
        else:
            with threading.Lock():
                total_skipped_count += len(batch)
                logger.error(f"❌ Failed to retrieve job status for job {job_id}")

    try:
        submission_futures = {}
        status_futures = {}
        start_time = time.time()
        requests_this_minute = 0
        minute_interval = 60
        request_delay = 2

        with processed_job_ids_lock:
            processed_job_ids.clear()
        with queued_job_ids_lock:
            queued_job_ids.clear()

        for batch_index in range(0, len(ticket_csv_data), batch_size):
            if stop_event and stop_event.is_set():
                logger.info("Stop detected. No new batches submitted.")
                break
            batch = ticket_csv_data[batch_index:batch_index + batch_size]
            mapped_tickets, _ = map_fields(
                batch,
                field_mapping,
                ticket_csv_headers,
                "Tickets",
                ticket_custom_field_info,
                action_type="import"
            )

            ticket_id_index = ticket_csv_headers.index(ticket_id_field)
            for ticket in mapped_tickets:
                ticket_id = batch[mapped_tickets.index(ticket)][ticket_id_index]
                ticket["comments"] = comments_by_ticket.get(ticket_id, [])

            payload = {"tickets": mapped_tickets}
            logger.info(f"Submitting batch {batch_index // batch_size + 1}: {len(batch)} records")
            print(f"📡 Submitting request {batch_index // batch_size + 1} to {ticket_api_url}")

            requests_this_minute += 1
            elapsed_time = time.time() - start_time
            if elapsed_time >= minute_interval:
                time.sleep(max(0, minute_interval - elapsed_time))
                start_time = time.time()
                requests_this_minute = 0

            jittered_delay = request_delay + random.uniform(0, 0.3)
            time.sleep(jittered_delay)
            logger.info(f"⏳ Applied delay of {jittered_delay:.3f}s before submission")

            future = submit_executor.submit(
                make_request_with_retries,
                "POST",
                ticket_api_url,
                auth,
                json=payload,
                stop_event=stop_event
            )
            submission_futures[future] = batch

            for future in [f for f in submission_futures if f.done()]:
                batch = submission_futures.pop(future)
                response = future.result()
                if response and response.status_code in [200, 201]:
                    job_id = response.json().get("job_status", {}).get("id", "")
                    if job_id:
                        with queued_job_ids_lock:
                            if job_id not in queued_job_ids:
                                queued_job_ids.add(job_id)
                                status_futures[status_executor.submit(
                                    process_job_status,
                                    job_id,
                                    batch,
                                    response
                                )] = (job_id, batch)

        for future in as_completed(submission_futures):
            batch = submission_futures[future]
            response = future.result()
            if response and response.status_code in [200, 201]:
                job_id = response.json().get("job_status", {}).get("id", "")
                if job_id:
                    with queued_job_ids_lock:
                        if job_id not in queued_job_ids:
                            queued_job_ids.add(job_id)
                            status_futures[status_executor.submit(
                                process_job_status,
                                job_id,
                                batch,
                                response
                            )] = (job_id, batch)

        for future in as_completed(status_futures):
            future.result()

    finally:
        submit_executor.shutdown(wait=True)
        status_executor.shutdown(wait=True)

        with records_lock:
            if success_records:
                write_csv_records(success_filename, success_records, success_headers, append=True)
                total_success_count += len(success_records)
            if error_records:
                write_csv_records(error_filename, error_records, error_headers, append=True)
                total_error_count += len(error_records)
            #if app_instance and total_records > 0:
                #app_instance.safe_update_progress_bar(
                    #100 if not stop_event.is_set() and cumulative_processed >= total_records
                    #else (cumulative_processed / total_records) * 100
                #)

        logger.info(
            f"Final counts: Success={total_success_count}, Errors={total_error_count}, "
            f"Skipped={total_skipped_count}, Processed={cumulative_processed}"
        )
        if app_instance:
            app_instance.success_filename = success_filename
            app_instance.error_filename = error_filename
            if app_instance.progress_manager:
                app_instance.progress_manager.stop(stopped_by_user=stop_event.is_set())

    return total_success_count, total_error_count, total_skipped_count, cumulative_processed






def update_records(
    subdomain,
    email,
    token,
    selected_object,
    csv_data,
    csv_headers,
    field_mapping,
    save_directory,
    stop_event=None,
    progress_bar=None,
    action_type="update",
    app_instance=None,
    chunk_size=100,
    success_filename=None,
    error_filename=None,
    processed_records_so_far=0
):
    """
    Updates records in Zendesk in batches using the appropriate update_many endpoint.
    Stops new submissions on stop_event but completes in-flight jobs.
    
    Args:
        subdomain (str): Zendesk subdomain.
        email (str): Zendesk email for authentication.
        token (str): Zendesk API token.
        selected_object (str): Type of object to update (e.g., User, Ticket).
        csv_data (list): List of CSV records.
        csv_headers (list): List of CSV header names.
        field_mapping (dict): Mapping of CSV columns to Zendesk fields.
        save_directory (str): Directory to save success/error CSVs.
        stop_event (threading.Event, optional): Event to stop the operation.
        progress_bar (callable, optional): Callback for progress updates.
        action_type (str): Operation type ('update' by default).
        app_instance: Application instance for progress and group data.
        chunk_size (int): Size of batches for API calls.
        success_filename (str, optional): Pre-generated filename for success CSV.
        error_filename (str, optional): Pre-generated filename for error CSV.
    
    Returns:
        tuple: (success_count, error_count, skipped_count, total_processed)
    """
    base_url = f"https://{subdomain}.zendesk.com"
    auth = HTTPBasicAuth(f"{email}/token", token)

    # Define headers for success and error CSVs
    success_headers = csv_headers + ["Status", "ID - preserved"]
    error_headers = csv_headers + ["Error Message"]

    # Use provided filenames or fallback to default
    success_filename = success_filename or os.path.join(save_directory, f"{selected_object}_{action_type}_success.csv")
    error_filename = error_filename or os.path.join(save_directory, f"{selected_object}_{action_type}_errors.csv")

    object_map = {
        "User": "Users",
        "Users": "Users",
        "Agent": "Users",
        "Organization": "Organizations",
        "Organizations": "Organizations",
        "Ticket": "Tickets",
        "Tickets": "Tickets"
    }
    payload_key_map = {
        "Users": "users",
        "Organizations": "organizations",
        "Tickets": "tickets"
    }
    selected_object_normalized = object_map.get(selected_object, selected_object)
    api_url = f"{base_url}/api/v2/{selected_object_normalized.lower()}/update_many.json"
    payload_key = payload_key_map.get(selected_object_normalized)

    if not api_url or not payload_key:
        logger.error(f"Invalid object for update: {selected_object}")
        return 0, 0, 0, 0

    if not save_directory:
        logger.error("No save directory specified for update operation")
        return 0, 0, 0, 0

    try:
        fields, custom_field_info = fetch_zendesk_fields(
            subdomain, email, token, selected_object, action_type
        )
        logger.info(f"Zendesk fields: {fields}")
    except Exception as e:
        logger.error(f"Failed to fetch Zendesk fields: {e}")
        return 0, 0, 0, 0

    if "id" not in field_mapping.values():
        logger.error("Field mapping does not include a mapping for 'id', required for updates")
        return 0, 0, 0, 0

    id_column = [
        csv_field for csv_field, zendesk_field in field_mapping.items()
        if zendesk_field == "id"
    ][0]
    if id_column not in csv_headers:
        logger.error(f"CSV data does not include the mapped 'id' column: {id_column}")
        return 0, 0, 0, 0

    with processed_job_ids_lock:
        processed_job_ids.clear()
    with queued_job_ids_lock:
        queued_job_ids.clear()

    batch_size = chunk_size
    success_records = []
    error_records = []
    total_records = len(csv_data)
    cumulative_processed = 0
    total_success_count = 0
    total_error_count = 0
    total_skipped_count = 0

    logger.info(f"Total records to process: {total_records}")
    print(f"Total records to process: {total_records}")

    submit_executor = ThreadPoolExecutor(max_workers=1)
    status_executor = ThreadPoolExecutor(max_workers=15)

    def write_csv_records(filename, records, headers, append=True):
        mode = 'a' if append else 'w'
        logger.debug(f"Writing {len(records)} records to {filename} in mode '{mode}'")
        try:
            with open(filename, mode, newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                if not append and not os.path.exists(filename):
                    writer.writeheader()
                # Filter records to include only keys in headers
                filtered_records = [
                    {key: record.get(key, "") for key in headers}
                    for record in records
                ]
                writer.writerows(filtered_records)
                f.flush()  # Ensure data is written to disk
            print(f"📝 Wrote {len(records)} records to {filename} (append={append})")
        except Exception as e:
            logger.error(f"Failed to write to {filename}: {e}")
            print(f"❌ Failed to write to {filename}: {e}")

    def process_job_status(job_id, batch, response, group_assignments):
        nonlocal cumulative_processed, total_success_count, total_error_count, total_skipped_count
        with processed_job_ids_lock:
            if job_id in processed_job_ids:
                logger.info(f"🛑 Job {job_id} already processed. Skipping.")
                return
            processed_job_ids.add(job_id)

        job_url = f"{base_url}/api/v2/job_statuses/{job_id}.json"
        temp_success = []
        temp_error = []
        jittered_sleep = 4 + random.uniform(0, 0.3)
        time.sleep(jittered_sleep)
        logger.info(f"⏳ Applied jittered sleep of {jittered_sleep:.2f}s for job status check")
        job_results = check_job_status(job_url, auth)
        if job_results:
            results = job_results.get("results", [])
            batch_dicts = [dict(zip(csv_headers, record)) for record in batch]
            user_ids = []
            batch_processed = 0
            seen_record_keys = set()  # Track unique records to prevent duplicates
            for index, result in enumerate(results):
                if index >= len(batch_dicts):
                    logger.warning(f"Result index {index} exceeds batch size {len(batch_dicts)}")
                    break
                record = batch_dicts[index]
                zendesk_id = result.get("id")
                status = result.get("status", "")

                # Create a unique key for deduplication (e.g., zendesk_id or record hash)
                record_key = str(zendesk_id) if zendesk_id else f"{index}_{str(record)}"

                if status == "Updated" and zendesk_id:
                    if record_key not in seen_record_keys:
                        success_record = {
                            **record,
                            "Status": "Updated",
                            "ID - preserved": record.get(id_column, "")
                        }
                        temp_success.append(success_record)
                        user_ids.append(zendesk_id)
                        seen_record_keys.add(record_key)
                    else:
                        logger.debug(f"Skipping duplicate success record for key: {record_key}")
                else:
                    error_msg = (
                        result.get("details") or
                        result.get("error") or
                        result.get("errors") or
                        "Unknown error"
                    )
                    if isinstance(error_msg, list):
                        error_msg = "; ".join(error_msg)
                    temp_error.append({
                        **record,
                        "Error Message": error_msg
                    })
                batch_processed += 1

                with threading.Lock():
                    cumulative_processed += 1
                    total_processed_so_far = processed_records_so_far + cumulative_processed
                    if total_processed_so_far % 200 == 0 and app_instance and hasattr(app_instance, 'progress_manager'):
                        app_instance.progress_manager.update_progress(
                            total_processed_so_far, total_records
                        )
                        logger.debug(f"Progress update: {total_processed_so_far}/{total_records} records")

            with threading.Lock():
                if temp_success:
                    success_records.extend(temp_success)
                    total_success_count += len(temp_success)
                    if len(success_records) >= chunk_size:
                        write_csv_records(success_filename, success_records, success_headers, append=True)
                        success_records.clear()
                if temp_error:
                    error_records.extend(temp_error)
                    total_error_count += len(temp_error)
                    if len(error_records) >= chunk_size:
                        write_csv_records(error_filename, error_records, error_headers, append=True)
                        error_records.clear()
                logger.info(
                    f"Batch processed: {batch_processed}, Cumulative processed: {cumulative_processed}, "
                    f"Success: {len(temp_success)}, Errors: {len(temp_error)}"
                )
        else:
            with threading.Lock():
                total_skipped_count += len(batch)
                logger.error(f"❌ Failed to retrieve job status for job {job_id}")

    def assign_group_memberships(user_ids, group_assignments):
        if not group_assignments or not app_instance.group_data:
            logger.info("No group assignments or group data available. Skipping.")
            return

        memberships = []
        for idx, group_names in group_assignments:
            if idx >= len(user_ids):
                logger.warning(f"Index {idx} exceeds user_ids length {len(user_ids)}")
                continue
            user_id = user_ids[idx]
            for group_name in group_names:
                group_id = app_instance.group_data.get(group_name)
                if group_id:
                    memberships.append({"user_id": user_id, "group_id": group_id})
                    logger.debug(
                        f"Adding membership: user_id={user_id}, group_id={group_id} "
                        f"for group '{group_name}'"
                    )
                else:
                    logger.warning(
                        f"Group '{group_name}' not found in group_data for user_id={user_id}"
                    )

        if not memberships:
            logger.info("No valid group memberships to assign.")
            return

        for i in range(0, len(memberships), 100):
            batch = memberships[i:i + 100]
            payload = {"group_memberships": batch}
            url = f"{base_url}/api/v2/group_memberships/create_many.json"
            logger.info(f"Submitting {len(batch)} group memberships")
            if stop_event and stop_event.is_set():
                logger.info("🛑 Stop detected. Skipping group membership submission.")
                return
            response = make_request_with_retries(
                "POST", url, auth, json=payload, stop_event=stop_event
            )
            if response and response.status_code in [200, 201]:
                logger.info(f"Successfully assigned {len(batch)} group memberships")
            else:
                logger.error(
                    f"Failed to assign group memberships: "
                    f"{response.text if response else 'No response'}"
                )

    try:
        submission_futures = {}
        status_futures = {}
        start_time = time.time()
        requests_this_minute = 0
        minute_interval = 60
        request_delay = 1.5 if selected_object_normalized in ["Tickets", "Ticket"] else 1

        role_mapped = "role" in field_mapping.values() if selected_object == "Agent" else True
        custom_role_id_mapped = (
            "custom_role_id" in field_mapping.values() if selected_object == "Agent" else False
        )

        for batch_index in range(0, len(csv_data), batch_size):
            if stop_event and stop_event.is_set():
                logger.info("🛑 Stop detected. No new batches submitted.")
                break

            batch = csv_data[batch_index:batch_index + batch_size]
            mapped_records, group_assignments = map_fields(
                batch,
                field_mapping,
                csv_headers,
                selected_object,
                custom_field_info,
                group_data=app_instance.group_data if app_instance else None,
                action_type=action_type
            )

            for record in mapped_records:
                if "id" not in record:
                    logger.warning(
                        f"Skipping record in batch {batch_index // batch_size + 1}: No 'id' present"
                    )
                    with threading.Lock():
                        error_records.append({
                            **record,
                            "Error Message": "No 'id' present"
                        })
                        cumulative_processed += 1
                        if len(error_records) >= chunk_size:
                            write_csv_records(
                                error_filename,
                                error_records,
                                error_headers,
                                append=True
                            )
                            total_error_count += len(error_records)
                            error_records.clear()
                        if app_instance:
                            app_instance.safe_update_progress_bar(
                                (cumulative_processed / total_records) * 100
                            )
                    continue

                if selected_object == "Agent":
                    if custom_role_id_mapped and "custom_role_id" in record:
                        try:
                            record["custom_role_id"] = int(record["custom_role_id"])
                        except (ValueError, TypeError):
                            logger.error(
                                f"❌ Invalid custom_role_id format: {record['custom_role_id']}"
                            )
                            with threading.Lock():
                                error_records.append({
                                    **record,
                                    "Error Message": "Invalid custom_role_id format"
                                })
                                cumulative_processed += 1
                                if len(error_records) >= chunk_size:
                                    write_csv_records(
                                        error_filename,
                                        error_records,
                                        error_headers,
                                        append=True
                                    )
                                    total_error_count += len(error_records)
                                    error_records.clear()
                                if app_instance:
                                    app_instance.safe_update_progress_bar(
                                        (cumulative_processed / total_records) * 100
                                    )

            payload_records = [record for record in mapped_records if "id" in record]
            if not payload_records:
                logger.warning(
                    f"Batch {batch_index // batch_size + 1} skipped: No valid records with 'id'"
                )
                cumulative_processed += len(batch)
                total_skipped_count += len(batch)
                if app_instance:
                    app_instance.safe_update_progress_bar(
                        (cumulative_processed / total_records) * 100
                    )
                continue

            payload = {payload_key: payload_records}
            logger.info(f"Submitting batch: {len(batch)} records")
            logger.debug(f"Payload: {json.dumps(payload, default=str)}")
            print(f"📡 Submitting PUT request {batch_index // batch_size + 1} to {api_url}")

            requests_this_minute += 1
            elapsed_time = time.time() - start_time
            if elapsed_time >= minute_interval:
                pause_time = 1
                logger.info(f"⏸️ Pausing submissions for {pause_time}s")
                time.sleep(pause_time)
                start_time = time.time()
                requests_this_minute = 0

            jittered_delay = request_delay + random.uniform(0, 0.3)
            time.sleep(jittered_delay)
            logger.info(f"⏳ Applied delay of {jittered_delay:.3f}s before submission")

            future = submit_executor.submit(
                make_request_with_retries,
                "PUT",
                api_url,
                auth,
                json=payload,
                stop_event=stop_event
            )
            submission_futures[future] = (batch, group_assignments)

            for future in [f for f in submission_futures if f.done()]:
                batch, group_assignments = submission_futures.pop(future)
                try:
                    response = future.result()
                    if response.status_code == 400:
                        if stop_event:
                            stop_event.set()
                        error_message = (
                            "Bad request submitted. Please check the following:\n"
                            "- Date formats (e.g., YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ).\n"
                            "- Role values (e.g., 'end-user', 'agent', 'admin').\n"
                            "- Dropdown or multi-select formats.\n"
                            "- Required fields are present and correctly mapped."
                        )
                        logger.error(f"❌ Bad Request (400) for {api_url}: {response.text}")
                        raise requests.exceptions.RequestException(error_message, response=response)

                    if response and response.status_code in [200, 201]:
                        job_id = response.json().get("job_status", {}).get("id", "")
                        if job_id:
                            with queued_job_ids_lock:
                                if job_id not in queued_job_ids:
                                    queued_job_ids.add(job_id)
                                    status_future = status_executor.submit(
                                        process_job_status,
                                        job_id,
                                        batch,
                                        response,
                                        group_assignments
                                    )
                                    status_futures[status_future] = (
                                        job_id,
                                        batch,
                                        group_assignments
                                    )
                    else:
                        with threading.Lock():
                            error_msg = (
                                f"HTTP {response.status_code if response else 'Unknown'}: "
                                f"{response.text if response else 'No response'}"
                            )
                            for record in batch:
                                error_records.append({
                                    **dict(zip(csv_headers, record)),
                                    "Error Message": error_msg
                                })
                            cumulative_processed += len(batch)
                            total_error_count += len(batch)
                            if len(error_records) >= chunk_size:
                                write_csv_records(
                                    error_filename,
                                    error_records,
                                    error_headers,
                                    append=True
                                )
                                total_error_count += len(error_records)
                                error_records.clear()
                            if app_instance:
                                app_instance.safe_update_progress_bar(
                                    (cumulative_processed / total_records) * 100
                                )
                except Exception as e:
                    logger.error(f"Unexpected error in batch submission: {e}")
                    if (
                        isinstance(e, requests.exceptions.RequestException) and
                        e.response and
                        e.response.status_code == 400
                    ):
                        if stop_event:
                            stop_event.set()
                        raise
                    with threading.Lock():
                        total_skipped_count += len(batch)
                        if app_instance:
                            app_instance.safe_update_progress_bar(
                                (cumulative_processed / total_records) * 100
                            )

        for future in as_completed(submission_futures):
            batch, group_assignments = submission_futures[future]
            try:
                response = future.result()
                if response.status_code == 400:
                    if stop_event:
                        stop_event.set()
                    error_message = (
                        "Bad request submitted. Please check the following:\n"
                        "- Date formats (e.g., YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ).\n"
                        "- Role values (e.g., 'end-user', 'agent', 'admin').\n"
                        "- Dropdown or multi-select formats.\n"
                        "- Required fields are present and correctly mapped."
                    )
                    logger.error(f"❌ Bad Request (400) for {api_url}: {response.text}")
                    raise requests.exceptions.RequestException(error_message, response=response)

                if response and response.status_code in [200, 201]:
                    job_id = response.json().get("job_status", {}).get("id", "")
                    if job_id:
                        with queued_job_ids_lock:
                            if job_id not in queued_job_ids:
                                queued_job_ids.add(job_id)
                                status_future = status_executor.submit(
                                    process_job_status,
                                    job_id,
                                    batch,
                                    response,
                                    group_assignments
                                )
                                status_futures[status_future] = (
                                    job_id,
                                    batch,
                                    group_assignments
                                )
            except Exception as e:
                logger.error(f"Error in batch submission: {e}")
                if (
                    isinstance(e, requests.exceptions.RequestException) and
                    e.response and
                    e.response.status_code == 400
                ):
                    if stop_event:
                        stop_event.set()
                    raise
                with threading.Lock():
                    total_skipped_count += len(batch)

        for future in as_completed(status_futures):
            future.result()

    except Exception as e:
        logger.error(f"Unexpected error in update_records: {e}", exc_info=True)
        raise

    finally:
        submit_executor.shutdown(wait=True)
        status_executor.shutdown(wait=True)

        with threading.Lock():
            if success_records:
                write_csv_records(success_filename, success_records, success_headers, append=True)
                
                success_records.clear()
            if error_records:
                write_csv_records(error_filename, error_records, error_headers, append=True)
                
                error_records.clear()
            if (
                app_instance and
                total_records > 0 and
                not stop_event.is_set() and
                cumulative_processed >= total_records
            ):
                app_instance.safe_update_progress_bar(100)

        logger.info(
            f"Final counts: Success={total_success_count}, Errors={total_error_count}, "
            f"Skipped={total_skipped_count}, Cumulative Processed={cumulative_processed}"
        )
        if app_instance:
            app_instance.success_filename = success_filename
            app_instance.error_filename = error_filename

    return total_success_count, total_error_count, total_skipped_count, cumulative_processed








def delete_records(
    subdomain,
    email,
    token,
    selected_object,
    csv_data,
    csv_headers,
    field_mapping,
    save_directory,
    stop_event=None,
    progress_bar=None,
    action_type="delete",
    app_instance=None,
    chunk_size=100,
    success_filename=None,
    error_filename=None,
    processed_records_so_far=0
):
    """
    Deletes records from Zendesk in batches, adding an 'ID - preserved' column to success records.
    Stops new submissions on stop_event but completes in-flight jobs.
    
    Args:
        subdomain (str): Zendesk subdomain.
        email (str): Zendesk email for authentication.
        token (str): Zendesk API token.
        selected_object (str): Type of object to delete (e.g., User, Ticket).
        csv_data (list): List of CSV records.
        csv_headers (list): List of CSV header names.
        field_mapping (dict): Mapping of CSV columns to Zendesk fields.
        save_directory (str): Directory to save success/error CSVs.
        stop_event (threading.Event, optional): Event to stop the operation.
        progress_bar (callable, optional): Callback for progress updates.
        action_type (str): Operation type ('delete' by default).
        app_instance: Application instance for progress updates.
        chunk_size (int): Size of batches for API calls.
        success_filename (str, optional): Pre-generated filename for success CSV.
        error_filename (str, optional): Pre-generated filename for error CSV.
    
    Returns:
        tuple: (success_count, error_count, skipped_count, total_processed)
    """
    base_url = f"https://{subdomain}.zendesk.com"
    auth = HTTPBasicAuth(f"{email}/token", token)

    # Use provided filenames or fallback to default
    success_filename = success_filename or os.path.join(save_directory, f"{selected_object}_{action_type}_success.csv")
    error_filename = error_filename or os.path.join(save_directory, f"{selected_object}_{action_type}_errors.csv")

    # Define headers for success and error CSVs
    success_headers = csv_headers + ["Status", "ID - preserved"]
    error_headers = csv_headers + ["Error Message"]

    object_map = {
        "User": "users",
        "Users": "users",
        "Agent": "users",
        "Organization": "organizations",
        "Organizations": "organizations",
        "Ticket": "tickets",
        "Tickets": "tickets"
    }
    selected_object_normalized = object_map.get(selected_object, selected_object)
    api_url_base = f"{base_url}/api/v2/{selected_object_normalized}/destroy_many.json"

    if not api_url_base:
        logger.error(f"Invalid object for delete: {selected_object}")
        return 0, 0, 0, 0

    if not save_directory:
        logger.error("No save directory specified for delete operation")
        return 0, 0, 0, 0

    id_column = [
        csv_field for csv_field, zendesk_field in field_mapping.items()
        if zendesk_field == "id"
    ]
    if not id_column:
        logger.error("No 'id' field mapped in field_mapping")
        return 0, 0, 0, 0
    id_column = id_column[0]

    if id_column not in csv_headers:
        logger.error(f"CSV data does not include the mapped 'id' column: {id_column}")
        return 0, 0, 0, 0

    with processed_job_ids_lock:
        processed_job_ids.clear()
    with queued_job_ids_lock:
        queued_job_ids.clear()

    batch_size = chunk_size
    success_records = []
    error_records = []
    total_records = len(csv_data)
    cumulative_processed = 0
    total_success_count = 0
    total_error_count = 0
    total_skipped_count = 0

    logger.info(f"Total records to process: {total_records}")
    print(f"Total records to process: {total_records}")

    submit_executor = ThreadPoolExecutor(max_workers=1)
    status_executor = ThreadPoolExecutor(max_workers=15)

    def write_csv_records(filename, records, headers, append=True):
        mode = 'a' if append else 'w'
        logger.debug(f"Writing {len(records)} records to {filename} in mode '{mode}'")
        try:
            with open(filename, mode, newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                if not append and not os.path.exists(filename):
                    writer.writeheader()
                # Filter records to include only keys in headers
                filtered_records = [
                    {key: record.get(key, "") for key in headers}
                    for record in records
                ]
                writer.writerows(filtered_records)
                f.flush()  # Ensure data is written to disk
            print(f"📝 Wrote {len(records)} records to {filename} (append={append})")
        except Exception as e:
            logger.error(f"Failed to write to {filename}: {e}")
            print(f"❌ Failed to write to {filename}: {e}")

    def process_job_status(job_id, batch, response):
        nonlocal cumulative_processed, total_success_count, total_error_count, total_skipped_count
        with processed_job_ids_lock:
            if job_id in processed_job_ids:
                logger.info(f"🛑 Job {job_id} already processed. Skipping.")
                return
            processed_job_ids.add(job_id)

        job_url = f"{base_url}/api/v2/job_statuses/{job_id}.json"
        temp_success = []
        temp_error = []
        jittered_sleep = 4 + random.uniform(0, 0.3)
        time.sleep(jittered_sleep)
        logger.info(f"⏳ Applied jittered sleep of {jittered_sleep:.2f}s for job status check")
        job_results = check_job_status(job_url, auth)
        if job_results:
            results = job_results.get("results", [])
            batch_dicts = [dict(zip(csv_headers, record)) for record in batch]
            logger.debug(f"Job {job_id}: Processing {len(results)} results for {len(batch_dicts)} batch records")
            seen_record_keys = set()  # Track unique records to prevent duplicates
            batch_processed = 0
            for index, result in enumerate(results):
                if index >= len(batch_dicts):
                    logger.warning(f"Result index {index} exceeds batch size {len(batch_dicts)}. Stopping processing.")
                    break
                record = batch_dicts[index]
                zendesk_id = result.get("id")
                status = result.get("status", "")

                # Create a unique key for deduplication (e.g., zendesk_id or record hash)
                record_key = str(zendesk_id) if zendesk_id else f"{index}_{str(record)}"

                if record_key not in seen_record_keys:
                    if status in ["deleted", "Deleted"] and zendesk_id:
                        success_record = {
                            **record,
                            "Status": "Deleted",
                            "ID - preserved": record.get(id_column, "")
                        }
                        temp_success.append(success_record)
                        seen_record_keys.add(record_key)
                    else:
                        error_msg = (
                            result.get("details") or
                            result.get("error") or
                            result.get("errors") or
                            "Unknown error"
                        )
                        if isinstance(error_msg, list):
                            error_msg = "; ".join(error_msg)
                        temp_error.append({
                            **record,
                            "Error Message": error_msg
                        })
                    batch_processed += 1
                    seen_record_keys.add(record_key)  # Mark record as processed
                else:
                    logger.debug(f"Skipping duplicate record for key: {record_key}")

                with threading.Lock():
                    if batch_processed > cumulative_processed:  # Only increment if record was processed
                        cumulative_processed += 1
                        total_processed_so_far = processed_records_so_far + cumulative_processed
                        if total_processed_so_far % 200 == 0 and app_instance and hasattr(app_instance, 'progress_manager'):
                            app_instance.progress_manager.update_progress(
                                total_processed_so_far, total_records
                            )
                            logger.debug(f"Progress update: {total_processed_so_far}/{total_records} records")
                    else:
                        logger.debug(f"Skipped incrementing cumulative_processed for duplicate key: {record_key}")

            with threading.Lock():
                if temp_success:
                    success_records.extend(temp_success)
                    total_success_count += len(temp_success)
                    if len(success_records) >= chunk_size:
                        write_csv_records(success_filename, success_records, success_headers, append=True)
                        success_records.clear()
                if temp_error:
                    error_records.extend(temp_error)
                    total_error_count += len(temp_error)
                    if len(error_records) >= chunk_size:
                        write_csv_records(error_filename, error_records, error_headers, append=True)
                        error_records.clear()
                logger.info(
                    f"Batch processed: {batch_processed}, Cumulative processed: {cumulative_processed}, "
                    f"Success: {len(temp_success)}, Errors: {len(temp_error)}"
                )
        else:
            with threading.Lock():
                total_skipped_count += len(batch)
                logger.error(f"❌ Failed to retrieve job status for job {job_id}")

    try:
        submission_futures = {}
        status_futures = {}
        start_time = time.time()
        requests_this_minute = 0
        minute_interval = 60
        request_delay = 1.2

        for batch_index in range(0, len(csv_data), batch_size):
            if stop_event and stop_event.is_set():
                logger.info("🛑 Stop detected. No new batches submitted.")
                break

            batch = csv_data[batch_index:batch_index + batch_size]
            ids = []
            batch_dicts = [dict(zip(csv_headers, record)) for record in batch]
            for record in batch_dicts:
                if id_column in record:
                    try:
                        id_value = int(record[id_column])
                        ids.append(id_value)
                    except ValueError:
                        logger.error(
                            f"❌ Invalid ID format in column '{id_column}': {record[id_column]}"
                        )
                        with threading.Lock():
                            error_records.append({
                                **record,
                                "Error Message": f"Invalid ID format in '{id_column}'"
                            })
                            cumulative_processed += 1
                            if len(error_records) >= chunk_size:
                                write_csv_records(
                                    error_filename,
                                    error_records,
                                    error_headers,
                                    append=True
                                )
                                total_error_count += len(error_records)
                                error_records.clear()
                            if app_instance:
                                app_instance.safe_update_progress_bar(
                                    (cumulative_processed / total_records) * 100
                                )
                else:
                    logger.error(f"❌ Mapped ID column '{id_column}' not found in CSV headers")
                    with threading.Lock():
                        error_records.append({
                            **record,
                            "Error Message": f"Mapped ID column '{id_column}' not found"
                        })
                        cumulative_processed += 1
                        if len(error_records) >= chunk_size:
                            write_csv_records(
                                error_filename,
                                error_records,
                                error_headers,
                                append=True
                            )
                            total_error_count += len(error_records)
                            error_records.clear()
                        if app_instance:
                            app_instance.safe_update_progress_bar(
                                (cumulative_processed / total_records) * 100
                            )

            if not ids:
                logger.warning(f"Batch {batch_index // batch_size + 1} skipped: No valid IDs")
                cumulative_processed += len(batch)
                total_skipped_count += len(batch)
                if app_instance:
                    app_instance.safe_update_progress_bar(
                        (cumulative_processed / total_records) * 100
                    )
                continue

            ids_param = ",".join(map(str, ids))
            api_url = f"{api_url_base}?ids={ids_param}"

            logger.info(f"Submitting batch: {len(batch)} records")
            print(f"📡 Submitting DELETE request {batch_index // batch_size + 1} to {api_url}")

            requests_this_minute += 1
            elapsed_time = time.time() - start_time
            if elapsed_time >= minute_interval:
                pause_time = 1
                logger.info(f"⏸️ Pausing submissions for {pause_time}s")
                time.sleep(pause_time)
                start_time = time.time()
                requests_this_minute = 0

            jittered_delay = request_delay + random.uniform(0, 0.3)
            time.sleep(jittered_delay)
            logger.info(f"⏳ Applied delay of {jittered_delay:.3f}s before submission")

            future = submit_executor.submit(
                make_request_with_retries,
                "DELETE",
                api_url,
                auth,
                json=None,
                stop_event=stop_event
            )
            submission_futures[future] = batch

            for future in [f for f in submission_futures if f.done()]:
                batch = submission_futures.pop(future)
                try:
                    response = future.result()
                    if response and response.status_code in [200, 201]:
                        job_id = response.json().get("job_status", {}).get("id", "")
                        if job_id:
                            with queued_job_ids_lock:
                                if job_id not in queued_job_ids:
                                    queued_job_ids.add(job_id)
                                    status_future = status_executor.submit(
                                        process_job_status,
                                        job_id,
                                        batch,
                                        response
                                    )
                                    status_futures[status_future] = (job_id, batch)
                    else:
                        with threading.Lock():
                            error_msg = (
                                f"HTTP {response.status_code if response else 'Unknown'}: "
                                f"{response.text if response else 'No response'}"
                            )
                            for record in batch:
                                error_records.append({
                                    **dict(zip(csv_headers, record)),
                                    "Error Message": error_msg
                                })
                            cumulative_processed += len(batch)
                            total_error_count += len(batch)
                            if len(error_records) >= chunk_size:
                                write_csv_records(
                                    error_filename,
                                    error_records,
                                    error_headers,
                                    append=True
                                )
                                total_error_count += len(error_records)
                                error_records.clear()
                            if app_instance:
                                app_instance.safe_update_progress_bar(
                                    (cumulative_processed / total_records) * 100
                                )
                except Exception as e:
                    logger.error(f"Unexpected error in batch submission: {e}")
                    with threading.Lock():
                        total_skipped_count += len(batch)
                        if app_instance:
                            app_instance.safe_update_progress_bar(
                                (cumulative_processed / total_records) * 100
                            )

        for future in as_completed(submission_futures):
            batch = submission_futures[future]
            try:
                response = future.result()
                if response and response.status_code in [200, 201]:
                    job_id = response.json().get("job_status", {}).get("id", "")
                    if job_id:
                        with queued_job_ids_lock:
                            if job_id not in queued_job_ids:
                                queued_job_ids.add(job_id)
                                status_future = status_executor.submit(
                                    process_job_status,
                                    job_id,
                                    batch,
                                    response
                                )
                                status_futures[status_future] = (job_id, batch)
            except Exception as e:
                logger.error(f"Error in batch submission: {e}")
                with threading.Lock():
                    total_skipped_count += len(batch)

        for future in as_completed(status_futures):
            future.result()

    finally:
        submit_executor.shutdown(wait=True)
        status_executor.shutdown(wait=True)

        with threading.Lock():
            if success_records:
                write_csv_records(
                    success_filename,
                    success_records,
                    success_headers,
                    append=True
                )
                total_success_count += len(success_records)
                success_records.clear()
            if error_records:
                write_csv_records(
                    error_filename,
                    error_records,
                    error_headers,
                    append=True
                )
                total_error_count += len(error_records)
                error_records.clear()
            if (
                app_instance and
                total_records > 0 and
                not stop_event.is_set() and
                cumulative_processed >= total_records
            ):
                app_instance.safe_update_progress_bar(100)

        logger.info(
            f"Final counts: Success={total_success_count}, Errors={total_error_count}, "
            f"Skipped={total_skipped_count}, Cumulative Processed={cumulative_processed}"
        )
        if app_instance:
            app_instance.success_filename = success_filename
            app_instance.error_filename = error_filename

    return total_success_count, total_error_count, total_skipped_count, cumulative_processed