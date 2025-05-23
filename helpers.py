import requests
import time
import logging
import random
from datetime import datetime
import threading
import re
import csv
from decimal import Decimal
import html
from requests.auth import HTTPBasicAuth
from globals import rate_limit_event, rate_limit_resume_time, rate_limit_lock  # Import globals

logging.basicConfig(filename="app.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

job_locks = {}  # Dictionary to store job-specific locks
job_locks_lock = threading.Lock()  # Lock to manage job_locks dictionary
local_storage = threading.local()






def fetch_light_agent_role_id(subdomain, email, token):
    """Fetch the Light Agent role ID from /api/v2/custom_roles."""
    base_url = f"https://{subdomain}.zendesk.com"
    auth = (f"{email}/token", token)
    url = f"{base_url}/api/v2/custom_roles.json"
    headers = {"Content-Type": "application/json"}

    try:
        response = requests.get(url, auth=auth, headers=headers)
        response.raise_for_status()
        roles = response.json().get("custom_roles", [])
        for role in roles:
            role_name = role.get("name", "").lower()
            role_id = role.get("id")
            if role_name == "light agent" and role_id is not None:
                logger.info(f"Found Light Agent role ID: {role_id}")
                return role_id
        logger.warning("Light Agent role not found in custom roles")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch custom roles: {e}")
        return None







def is_valid_date_format(date_str, date_format):
    """Check if a string matches a given date format."""
    try:
        datetime.strptime(date_str.strip(), date_format)
        return True
    except ValueError:
        return False

def convert_to_zendesk_date(date_str, is_standard_field=False):
    """
    Convert a date or date-time string from US or EU formats to Zendesk ISO 8601 format.
    - For standard fields (e.g., created_at): YYYY-MM-DDTHH:MM:SSZ (defaults to 00:00:00Z if no time).
    - For custom date fields: YYYY-MM-DD (time ignored if present).
    """
    if not date_str or not isinstance(date_str, str):
        return date_str  # Return as-is if empty or not a string

    date_str = date_str.strip()
    logging.debug(f"Converting date string: '{date_str}' (standard: {is_standard_field})")

    # Check if already in ISO 8601 format
    iso_formats = [
        '%Y-%m-%d',             # 2025-03-26
        '%Y-%m-%dT%H:%M:%SZ',   # 2018-03-26T21:30:00Z
        '%Y-%m-%d %H:%M:%S'     # 2018-03-26 21:30:00
    ]
    for fmt in iso_formats:
        if is_valid_date_format(date_str, fmt):
            dt = datetime.strptime(date_str, fmt)
            if is_standard_field:
                if fmt == '%Y-%m-%d':
                    result = f"{date_str}T00:00:00Z"
                    logging.debug(f"Date-only ISO to standard: '{date_str}' -> '{result}'")
                    return result
                result = dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                logging.debug(f"ISO date-time preserved: '{date_str}' -> '{result}'")
                return result
            result = dt.strftime('%Y-%m-%d')
            logging.debug(f"ISO to custom field: '{date_str}' -> '{result}'")
            return result

    # Try parsing US and EU formats (with and without time)
    formats = {
        '%m/%d/%Y %H:%M': 'US (MM/DD/YYYY HH:MM)',  # 3/26/2018 21:30
        '%d/%m/%Y %H:%M': 'EU (DD/MM/YYYY HH:MM)',  # 26/3/2018 21:30
        '%m-%d-%Y %H:%M': 'US (MM-DD-YYYY HH:MM)',  # 3-26-2018 21:30
        '%d-%m-%Y %H:%M': 'EU (DD-MM-YYYY HH:MM)',  # 26-3-2018 21:30
        '%m/%d/%Y': 'US (MM/DD/YYYY)',              # 3/26/2018
        '%d/%m/%Y': 'EU (DD/MM/YYYY)',              # 26/3/2018
        '%m-%d-%Y': 'US (MM-DD-YYYY)',              # 3-26-2018
        '%d-%m-%Y': 'EU (DD-MM-YYYY)'               # 26-3-2018
    }

    for fmt, fmt_name in formats.items():
        if is_valid_date_format(date_str, fmt):
            dt = datetime.strptime(date_str, fmt)
            if is_standard_field:
                if '%H:%M' in fmt:
                    result = dt.strftime('%Y-%m-%dT%H:%M:00Z')
                    logging.debug(f"{fmt_name} with time to standard: '{date_str}' -> '{result}'")
                    return result
                result = dt.strftime('%Y-%m-%dT00:00:00Z')
                logging.debug(f"{fmt_name} date-only to standard: '{date_str}' -> '{result}'")
                return result
            result = dt.strftime('%Y-%m-%d')
            logging.debug(f"{fmt_name} to custom field: '{date_str}' -> '{result}'")
            return result

    logging.warning(f"Unrecognized date format '{date_str}'. Returning as-is.")
    return date_str





def sanitize_csv_value(value, strip_html=False, max_length=65000, is_tags_field=False):
    """
    Sanitize a value for CSV export, supporting all Unicode characters and languages.
    Ensures compatibility with RFC 4180 for CSV formatting, preserves valid UTF-8 characters,
    and removes HTML tags only if strip_html=True. Handles scientific notation, control characters,
    and proper CSV formatting for tags field.
    
    Args:
        value: The value to sanitize (string, list, or other).
        strip_html (bool): Whether to strip HTML tags from the value.
        max_length (int): Maximum length for the sanitized value to prevent API issues.
        is_tags_field (bool): Whether the value is from the tags field.
    
    Returns:
        Sanitized string, properly formatted for CSV, or empty string if None or "null".
    """
    import math  # Add import for math.isnan

    if value is None or value == "" or value == "null" or (isinstance(value, str) and value.lower() == "nan") or (isinstance(value, float) and math.isnan(value)):
        return ""  # Return empty string for None, empty, "null", "nan", or NaN values

    # Handle lists (e.g., tags) explicitly
    if isinstance(value, list):
        value = ",".join(str(v).strip() for v in value if v)
        if not value:
            return ""

    # Convert non-string values to string
    if not isinstance(value, str):
        try:
            if isinstance(value, (int, float, Decimal)):
                if isinstance(value, float) and math.isnan(value):
                    return ""  # Handle float NaN
                value = f"{value:.0f}" if isinstance(value, (float, Decimal)) and value.is_integer() else str(value)
            else:
                value = str(value)
        except Exception as e:
            logger.warning(f"Error converting value to string: {value}. Error: {e}")
            value = ""

    try:
        logger.debug(f"Sanitizing value: '{value[:100]}...'")

        # Handle scientific notation for large IDs
        scientific_pattern = r'^-?\d+\.\d+E[+-]\d+$'
        if re.match(scientific_pattern, value, re.IGNORECASE):
            try:
                sanitized_value = f"{Decimal(value):.0f}"
                logger.debug(f"Converted scientific notation: '{value}' -> '{sanitized_value}'")
                return sanitized_value
            except (ValueError, OverflowError) as e:
                logger.warning(f"Could not convert scientific notation value: {value}. Error: {e}")

        # Remove control characters (except newline and tab)
        value = re.sub(r'[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F]', ' ', value)

        # Decode HTML entities
        value = html.unescape(value)

        # Remove HTML tags if requested
        if strip_html:
            value = re.sub(r'<[^>]+>', '', value)

        # Normalize whitespace
        value = re.sub(r'\s+', ' ', value).strip()

        # Truncate long values
        if len(value) > max_length:
            logger.warning(f"Truncating value longer than {max_length} characters: '{value[:50]}...'")
            value = value[:max_length]

        # Validate UTF-8
        try:
            value.encode('utf-8').decode('utf-8')
            logger.debug(f"Value is valid UTF-8: '{value[:100]}...'")
        except (UnicodeEncodeError, UnicodeDecodeError) as e:
            logger.warning(f"Invalid UTF-8 characters in value: '{value[:50]}...'. Replacing invalid characters.")
            value = value.encode('utf-8', errors='replace').decode('utf-8')

        # Return as-is for tags or values not requiring quotes
        if is_tags_field or not any(char in value for char in ',"\n\r'):
            return value

        # Let csv.DictWriter handle quoting and escaping
        return value
    except Exception as e:
        logger.error(f"Error sanitizing CSV value '{value[:50]}...': {e}")
        return ""









def map_fields(csv_data, field_mapping, csv_headers, selected_object, custom_field_info, group_data=None, action_type=None, subdomain=None, email=None, token=None):
    """
    Maps CSV data to Zendesk fields, enforcing data types for User/Agent fields.
    """
    payload = []
    group_assignments = []
    if not csv_data or not all(isinstance(row, (list, tuple)) for row in csv_data):
        logger.error(f"Invalid csv_data: {csv_data}")
        raise ValueError("csv_data must be a list of lists or tuples")
    batch_dicts = [dict(zip(csv_headers, record)) for record in csv_data]
    logger.info(f"batch_dicts sample: {batch_dicts[:2] if batch_dicts else 'empty'}")

    standard_date_fields = {'created_at', 'updated_at', 'due_at'}
    custom_date_field_ids = {cf['id']: cf['name'] for cf in custom_field_info if cf.get('type') == 'date'}
    builtin_objects = ["Ticket", "Tickets", "User", "Users", "Agent", "Organization", "Organizations", "Macros", "Trigger"]
    is_custom_object = selected_object not in builtin_objects

    standard_fields_map = {
        "Ticket": {"id", "subject", "external_id", "description", "tags", "created_at", "priority", 
                   "status", "group_id", "assignee_id", "organization_id", "requester_id", 
                   "collaborator_ids", "follower_ids", "type", "solved_at"},
        "Tickets": {"id", "subject", "external_id", "description", "tags", "created_at", "priority", 
                    "status", "group_id", "assignee_id", "organization_id", "requester_id", 
                    "collaborator_ids", "follower_ids", "type", "solved_at"},
        "User": {"id", "external_id", "name", "email", "created_at", "details", "phone", "tags", 
                 "locale", "organization_id", "role"},
        "Users": {"id", "external_id", "name", "email", "created_at", "details", "phone", "tags", 
                  "locale", "organization_id", "role"},
        "Agent": {"id", "external_id", "name", "email", "created_at", "details", "phone", "tags", 
                  "locale", "organization_id", "role", "custom_role_id", "two_factor_auth_enabled", 
                  "ticket_restriction", "suspended", "signature", "notes", "default_group_id", 
                  "alias", "groups", "agent_brand_ids"},
        "Organization": {"id", "external_id", "name", "deleted_at", "domain_names", "details", 
                         "notes", "group_id", "tags", "created_at"},
        "Organizations": {"id", "external_id", "name", "deleted_at", "domain_names", "details", 
                          "notes", "group_id", "tags", "created_at"},
        "Macros": {"id", "title", "active", "actions", "created_at", "updated_at", "position", "restriction"},
        "Trigger": {"id", "title", "active", "position", "conditions_all", "conditions_any", "actions", 
                    "created_at", "updated_at", "category_id"}
    }
    standard_fields = standard_fields_map.get(selected_object, set())
    custom_object_standard_fields = {"id", "name", "external_id"} if is_custom_object else set()

    custom_field_key_map = {
        "Ticket": "custom_fields",
        "Tickets": "custom_fields",
        "User": "user_fields",
        "Users": "user_fields",
        "Agent": "user_fields",
        "Organization": "organization_fields",
        "Organizations": "organization_fields",
        "Macros": None,
        "Trigger": None
    }
    custom_field_key = custom_field_key_map.get(selected_object, "custom_object_fields")
    objects_to_normalize = {"Ticket", "Tickets", "User", "Users", "Agent", "Organization", "Organizations"}

    # Define expected data types for User/Agent fields
    string_fields = {"email", "notes", "details", "ticket_restriction", "external_id", "alias", "phone", "signature"}
    integer_fields = {"organization_id", "default_group_id", "custom_role_id"}
    list_fields = {"collaborator_ids", "follower_ids", "agent_brand_ids"}  # Added agent_brand_ids

    for idx, record in enumerate(batch_dicts):
        mapped_record = {}
        custom_fields_payload = {} if custom_field_key != "custom_fields" else []
        error_messages = []

        # Collect all IDs for collaborator_ids, follower_ids, and agent_brand_ids
        collaborator_ids = []
        follower_ids = []
        agent_brand_ids = []
        for csv_col, zendesk_field in field_mapping.items():
            if zendesk_field in list_fields and csv_col in record:
                value = record[csv_col]
                if isinstance(value, str) and value.strip() and value.strip().lower() != "null":
                    try:
                        ids = [int(float(v.strip())) for v in value.split(",") if v.strip() and v.strip().lower() != "null"]
                        if zendesk_field == "collaborator_ids":
                            collaborator_ids.extend(ids)
                        elif zendesk_field == "follower_ids":
                            follower_ids.extend(ids)
                        elif zendesk_field == "agent_brand_ids":
                            agent_brand_ids.extend(ids)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Invalid ID value for {zendesk_field}: {value}. Error: {e}")
                        error_messages.append(f"Invalid ID value for {zendesk_field}: {value}")
                elif not isinstance(value, list) and value is not None and str(value).strip().lower() != "null":
                    try:
                        id_value = int(float(str(value)))
                        if zendesk_field == "collaborator_ids":
                            collaborator_ids.append(id_value)
                        elif zendesk_field == "follower_ids":
                            follower_ids.append(id_value)
                        elif zendesk_field == "agent_brand_ids":
                            agent_brand_ids.append(id_value)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Invalid ID value for {zendesk_field}: {value}. Error: {e}")
                        error_messages.append(f"Invalid ID value for {zendesk_field}: {value}")
                elif isinstance(value, list):
                    try:
                        ids = [int(float(v)) for v in value if v and str(v).strip().lower() != "null"]
                        if zendesk_field == "collaborator_ids":
                            collaborator_ids.extend(ids)
                        elif zendesk_field == "follower_ids":
                            follower_ids.extend(ids)
                        elif zendesk_field == "agent_brand_ids":
                            agent_brand_ids.extend(ids)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Invalid ID value for {zendesk_field}: {value}. Error: {e}")
                        error_messages.append(f"Invalid ID value for {zendesk_field}: {value}")

        for csv_col, zendesk_field in field_mapping.items():
            if zendesk_field and zendesk_field != " " and csv_col in record:
                value = record[csv_col]
                is_custom_field = (
                    zendesk_field.startswith("custom_field_") or
                    (is_custom_object and zendesk_field not in custom_object_standard_fields)
                )

                if not is_custom_field and zendesk_field in (standard_fields | custom_object_standard_fields):
                    # Handle standard fields with explicit type enforcement
                    if zendesk_field in standard_date_fields:
                        value = convert_to_zendesk_date(value, is_standard_field=True)
                    elif zendesk_field == "tags" and value and action_type == "export":
                        value = [v.strip().replace(" ", "_").lower() for v in value.split(",") if v.strip()]
                    elif zendesk_field == "collaborator_ids":
                        value = collaborator_ids if collaborator_ids else None
                    elif zendesk_field == "follower_ids":
                        value = follower_ids if follower_ids else None
                    elif zendesk_field == "agent_brand_ids":
                        value = agent_brand_ids if agent_brand_ids else None
                    elif zendesk_field in string_fields:
                        # Ensure string fields are treated as strings
                        value = str(value) if value is not None else ""
                        if zendesk_field == "email" and value and not re.match(r"[^@]+@[^@]+\.[^@]+", value):
                            error_messages.append(f"Invalid email format: {value}")
                        elif zendesk_field == "ticket_restriction" and value and value not in ["assigned", "groups", "organization", ""]:
                            error_messages.append(f"Invalid ticket_restriction value: {value}")
                    elif zendesk_field in integer_fields:
                        # Ensure integer fields are valid integers
                        try:
                            value = int(float(value)) if value and str(value).strip().lower() != "null" else None
                        except (ValueError, TypeError):
                            error_messages.append(f"Invalid integer value for {zendesk_field}: {value}")
                            value = None
                    mapped_record[zendesk_field] = value
                else:
                    # Handle custom fields
                    field_info = next(
                        (
                            cf for cf in custom_field_info
                            if cf['name'] == zendesk_field or
                            cf['name'].replace("custom_field_", "") == zendesk_field
                        ),
                        None
                    )
                    if field_info:
                        field_id = field_info['id']
                        field_type = field_info.get('type', 'text')
                        custom_field_name = field_info.get(
                            'key', field_info['name'].replace("custom_field_", "")
                        )
                        if field_type == 'date':
                            value = convert_to_zendesk_date(value, is_standard_field=False)
                        elif field_type == 'dropdown' and value and selected_object in objects_to_normalize and action_type == "export":
                            value = str(value).replace(" ", "_").lower()
                        elif field_type == 'multiselect' and value and selected_object in objects_to_normalize and action_type == "export":
                            value = [v.strip().replace(" ", "_").lower() for v in str(value).split(",") if v.strip()]
                        if custom_field_key == "custom_fields":
                            custom_fields_payload.append({"id": field_id, "value": value})
                        else:
                            custom_fields_payload[custom_field_name] = value
                    else:
                        custom_field_name = zendesk_field.replace("custom_field_", "")
                        if "date" in custom_field_name.lower() and value:
                            value = convert_to_zendesk_date(value, is_standard_field=False)
                        if custom_field_key == "custom_fields":
                            try:
                                field_id = int(zendesk_field)
                                custom_fields_payload.append({"id": field_id, "value": value})
                            except ValueError:
                                custom_fields_payload.append({"id": zendesk_field, "value": value})
                        else:
                            custom_fields_payload[custom_field_name] = value

        if error_messages:
            # Skip record and add to error records (requires external error handling)
            logger.error(f"Record {idx} skipped due to errors: {'; '.join(error_messages)}")
            continue

        if custom_fields_payload and custom_field_key:
            mapped_record[custom_field_key] = custom_fields_payload

        # Add skip_verify_email and verified for User, Users, or Agent during import
        if selected_object in ["User", "Users", "Agent"] and action_type in ["import"]:
            mapped_record["skip_verify_email"] = True
            mapped_record["verified"] = True
            logger.debug(f"Added skip_verify_email: true and verified: true to record for {selected_object}")

        if is_custom_object:
            final_record = {}
            if custom_fields_payload:
                final_record["custom_object_fields"] = custom_fields_payload
            for csv_col, zendesk_field in field_mapping.items():
                if csv_col in record and zendesk_field and zendesk_field != " ":
                    if not (zendesk_field in custom_fields_payload or zendesk_field.startswith("custom_field_")):
                        final_record[zendesk_field] = record[csv_col]
            mapped_record = final_record

        payload.append(mapped_record)

    logger.info(f"map_fields payload sample: {payload[:2] if payload else 'empty'}")
    print(f"Generated payload for {selected_object}: {payload[:2] if payload else 'empty'}")
    return payload, group_assignments

    





def extract_ids(batch, field_mapping, csv_headers):
    """
    Extracts IDs from records for deletion, using the user-mapped column instead of hardcoding 'ID'.
    
    Args:
        batch (list): List of records to process.
        field_mapping (dict): Mapping of CSV columns to Zendesk fields.
        csv_headers (list): List of CSV header names.
    
    Returns:
        list: List of extracted IDs.
    """
    mapped_csv_column = None
    for csv_col, zendesk_field in field_mapping.items():
        if zendesk_field in ["id", "external id"]:
            mapped_csv_column = csv_col
            break

    if not mapped_csv_column:
        logging.error("❌ No valid ID mapping found in field mapping!")
        return []

    if mapped_csv_column not in csv_headers:
        logging.error(f"❌ Mapped column '{mapped_csv_column}' not found in CSV headers!")
        return []

    id_index = csv_headers.index(mapped_csv_column)
    extracted_ids = list(set(record[id_index].strip() for record in batch if record[id_index].strip()))
    return extracted_ids

def check_job_status(job_url, auth, stop_event=None):
    """
    Check the status of a Zendesk job, handling retries and rate limits.
    
    Args:
        job_url (str): URL to check job status.
        auth (tuple): Authentication credentials.
        stop_event (threading.Event, optional): Event to stop the operation.
    
    Returns:
        dict or None: Job status results or None if failed.
    """
    global rate_limit_resume_time
    max_retries = 15
    base_delay = 6
    max_delay = 15
    
    job_id = job_url.split("/")[-1]
    logging.info(f"Starting job status check for {job_id} with auth: {auth}")

    with job_locks_lock:
        if job_id not in job_locks:
            job_locks[job_id] = threading.Lock()

    with job_locks[job_id]:
        attempt = 0
        
        while attempt < max_retries:
            with rate_limit_lock:
                if not rate_limit_event.is_set():
                    current_time = time.time()
                    if current_time < rate_limit_resume_time:
                        pause_duration = rate_limit_resume_time - current_time
                        time.sleep(pause_duration)
                    rate_limit_event.set()

            try:
                logging.info(f"Attempt {attempt + 1}/{max_retries}: GET {job_url} with auth: {auth}")
                response = requests.get(job_url, auth=auth)
                print(f"🔍 Checking Response: {response}")
                logging.info(f"Response status: {response.status_code}, content: {response.text}")

                if response.status_code == 200:
                    job_status = response.json().get("job_status", {})
                    status = job_status.get("status", "unknown")
                    logging.info(f"Job {job_id} status: {status}")
                    if status in ["completed", "failed"]:
                        logging.info(f"Job {job_id} reached final status: {status}. Returning result.")
                        return job_status
                    elif status in ["queued", "working"]:
                        base = base_delay * (2 ** attempt)
                        delay = min(random.uniform(base * 0.5, base), max_delay)
                        print(f"⏳ Job {job_url} is {status}. Waiting {delay:.2f}s (attempt {attempt + 1}/{max_retries})")
                        time.sleep(delay)
                        attempt += 1
                        continue
                elif response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", base_delay))
                    print(f"⚠️ Rate limit (429) hit. Retry-After: {retry_after}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(retry_after)
                    with rate_limit_lock:
                        rate_limit_resume_time = time.time() + retry_after
                        rate_limit_event.clear()
                    attempt += 1
                    continue
                elif response.status_code == 503:
                    retry_after = int(response.headers.get("Retry-After", base_delay))
                    print(f"⚠️ Service Unavailable (503). Retry-After: {retry_after}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(min(retry_after, max_delay))
                    attempt += 1
                    continue
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(f"❌ Error checking job status: {e} (attempt {attempt + 1}/{max_retries})")
                base = base_delay * (2 ** attempt)
                delay = min(random.uniform(base * 0.5, base), max_delay)
                print(f"⏳ Retrying after error. Waiting {delay:.2f}s")
                time.sleep(delay)
                attempt += 1
                continue

        print(f"❌ Max retries reached for job status check: {job_url}")
        logging.error(f"Max retries reached for {job_url}")
        return None












def process_api_response(response, batch, success_records, error_records, email, token, csv_headers, object_type, stop_event=None):
    """
    Handles API response and sorts records into success/error lists for import, update, or delete operations.
    
    Args:
        response: HTTP response from the API.
        batch (list): List of records in the batch.
        success_records (list): List to store successful records.
        error_records (list): List to store failed records.
        email (str): Zendesk email for authentication.
        token (str): Zendesk API token.
        csv_headers (list): CSV header names.
        object_type (str): Type of object being processed.
        stop_event (threading.Event, optional): Event to stop the operation.
    
    Returns:
        None
    """
    if response.status_code not in [200, 201]:
        logging.error(f"❌ API Error: {response.status_code} - {response.text}")
        for record in batch:
            record_dict = dict(zip(csv_headers, record))
            error_records.append({
                **record_dict,
                "Error Message": response.text
            })
        return

    try:
        time.sleep(0.2)
        response_data = response.json()
        logging.info(f"🔍 API Response")
    except ValueError as e:
        logging.error(f"❌ Failed to parse JSON: {e} - {response.text}")
        for record in batch:
            record_dict = dict(zip(csv_headers, record))
            error_records.append({
                **record_dict,
                "Error Message": "Invalid JSON response"
            })
        return

    job_status = response_data.get("job_status", {})
    job_id = job_status.get("id", "")

    if not job_id:
        logging.error("❌ No job ID found in response")
        for record in batch:
            record_dict = dict(zip(csv_headers, record))
            error_records.append({
                **record_dict,
                "Error Message": "No job ID in response"
            })
        return

    job_url = f"{response.url.split('/api/v2')[0]}/api/v2/job_statuses/{job_id}.json"
    print(f"🔍 Job URL: {job_url}")
    auth = HTTPBasicAuth(f"{email}/token", token)

    job_results = check_job_status(job_url, auth, stop_event=stop_event)

    if not job_results:
        logging.error(f"❌ Failed to retrieve job status for job {job_id}")
        for record in batch:
            record_dict = dict(zip(csv_headers, record))
            error_records.append({
                **record_dict,
                "Error Message": "Job status retrieval failed"
            })
        return

    results = job_results.get("results", []) if isinstance(job_results, dict) else job_results

    batch_dicts = [dict(zip(csv_headers, record)) for record in batch]

    url = response.url.lower()
    method = response.request.method.upper()

    if "create_many" in url:
        action = "import"
        success_status = "Created"
    elif "update_many" in url:
        action = "update"
        success_status = "Updated"
    elif "destroy_many" in url or method == "DELETE":
        action = "delete"
        success_status = "Deleted"
    else:
        logging.error(f"❌ Unknown action type from URL: {url} with method: {method}")
        for record in batch:
            record_dict = dict(zip(csv_headers, record))
            error_records.append({
                **record_dict,
                "Error Message": "Unknown API operation"
            })
        return

    processed_success_ids = set()
    processed_error_ids = set()

    for index, result in enumerate(results):
        if index >= len(batch_dicts):
            break

        record = batch_dicts[index]
        record_id = result.get("id")
        status = result.get("status", "")
        record_key = record_id if record_id else str(index) + str(record)

        if "tags" in record:
            tags_value = record.get("tags", "")
            if tags_value and isinstance(tags_value, list):
                record["tags"] = ", ".join(tags_value)
            elif tags_value and isinstance(tags_value, str):
                transformed_tags = [tag.strip().replace(" ", "_") for tag in tags_value.split(",") if tag.strip()]
                record["tags"] = ", ".join(transformed_tags)

        if status == success_status:
            if record_key not in processed_success_ids:
                success_record = {
                    **record,
                    "Status": status
                }
                if action == "import":
                    success_record["ID"] = record_id
                success_records.append(success_record)
                processed_success_ids.add(record_key)
                logging.debug(f"Added success record: {record_key}")
            else:
                logging.warning(f"Duplicate success record skipped: {record_key}")
        else:
            if record_key not in processed_error_ids:
                if object_type.lower() in ["organization", "organizations"]:
                    error_msg = result.get("details") or result.get("error") or result.get("errors") or "Unknown error"
                    if isinstance(error_msg, list):
                        error_msg = "; ".join(error_msg)
                    elif not isinstance(error_msg, str):
                        error_msg = str(error_msg)
                elif object_type.lower() in ["user", "users", "agent"]:
                    error_msg = result.get("details") or result.get("error") or result.get("errors") or result.get("message")
                    if not error_msg and status != success_status:
                        error_msg = str(result)
                    error_msg = error_msg or "Unknown error"
                elif object_type.lower() in ["ticket", "tickets"]:
                    error_msg = result.get("errors") or result.get("details") or result.get("error") or result.get("message")
                    if isinstance(error_msg, list):
                        error_msg = "; ".join(error_msg)
                    error_msg = error_msg or "Unknown error"
                else:
                    error_msg = result.get("error", "Unknown error")

                if isinstance(error_msg, list):
                    error_msg = "; ".join(error_msg)

                error_records.append({
                    **record,
                    "Error Message": error_msg
                })
                processed_error_ids.add(record_key)
                logging.debug(f"Added error record: {record_key}")
            else:
                logging.warning(f"Duplicate error record skipped: {record_key}")

    if len(results) < len(batch_dicts):
        logging.warning(f"⚠️ Fewer results ({len(results)}) than batch size ({len(batch_dicts)})")
        for i in range(len(results), len(batch_dicts)):
            record = batch_dicts[i]
            if "tags" in record:
                tags_value = record.get("tags", "")
                if tags_value and isinstance(tags_value, list):
                    record["tags"] = ", ".join(tags_value)
                elif tags_value and isinstance(tags_value, str):
                    transformed_tags = [tag.strip().replace(" ", "_") for tag in tags_value.split(",") if tag.strip()]
                    record["tags"] = ", ".join(transformed_tags)
            
            record_key = str(i) + str(record)
            if record_key not in processed_error_ids:
                error_records.append({
                    **record,
                    "Error Message": "No result from job"
                })
                processed_error_ids.add(record_key)
                logging.debug(f"Added error record for unprocessed: {record_key}")
            else:
                logging.warning(f"Duplicate unprocessed error record skipped: {record_key}")