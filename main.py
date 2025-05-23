# main.py
import webview
from auth import API as AuthAPI
from csv_handler import API as CsvAPI
from zendesk_fields import fetch_zendesk_fields, fetch_custom_objects
from zendesk_bulk import execute_bulk_operation
import threading
import os
import requests
import tempfile
import logging
import json
import numpy as np
import csv
import time
import pandas as pd
from progress_manager import ProgressManager
import sys
import traceback

# Setup Logging to both file and console
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG for more detail
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler(sys.stdout)  # Output to console for immediate feedback
    ]
)
logger = logging.getLogger(__name__)

class AppAPI:
    def __init__(self):
        logger.debug("Initializing AppAPI")
        try:
            self.auth_api = AuthAPI()
            self.csv_api = CsvAPI()
            self.window = None
            self.subdomain = None
            self.email = None
            self.token = None
            self.save_credentials = False
            self.selected_object = None
            self.action_type = None
            self.csv_headers = []
            self.csv_data = []
            self.csv_file_path = ""
            self.comments_csv_headers = []
            self.comments_csv_data = []
            self.comments_csv_file_path = ""
            self.attachments_csv_headers = []
            self.attachments_csv_data = []
            self.attachments_csv_file_path = ""
            self.field_mapping = {}
            self.comment_field_mapping = {}
            self.attachment_field_mapping = {}
            self.zendesk_fields = []
            self.custom_field_info = []
            self.ticket_id_field = None
            self.comment_ticket_id_field = None
            self.attachment_comment_id_field = None
            self.success_filename = None
            self.error_filename = None
            self.operation_running = False
            self.stop_event = threading.Event()
            self.progress_manager = None
            self.save_directory = os.path.join(tempfile.gettempdir(), "zendesk_data_loader")
            self.comment_zendesk_fields = ['author_id', 'body', 'html_body', 'public', 'created_at']
            self.attachment_zendesk_fields = ['file_name']
            self.trigger_zendesk_fields = ['id', 'title', 'active', 'position', 'conditions_all', 'conditions_any', 'actions', 'created_at', 'updated_at', 'category_id']
            self.custom_objects = []
            self.group_data = {}
            self.attachment_directory = None
            logger.debug("AppAPI initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize AppAPI: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def get_unique_filename(self, base_filename):
        """Ensures a new unique filename is created if one already exists."""
        file_dir, file_name = os.path.split(base_filename)
        name, ext = os.path.splitext(file_name)
        counter = 1
        new_filename = base_filename
        while os.path.exists(new_filename):
            new_filename = os.path.join(file_dir, f"{name} ({counter}){ext}")
            counter += 1
        return new_filename

    def fetch_groups(self):
        logger.debug("Fetching groups from Zendesk")
        try:
            base_url = f"https://{self.subdomain}.zendesk.com/api/v2/groups.json"
            auth = (f"{self.email}/token", self.token)
            headers = {"Content-Type": "application/json"}
            groups = {}
            url = base_url

            while url:
                response = requests.get(url, auth=auth, headers=headers)
                response.raise_for_status()
                data = response.json()
                for group in data.get("groups", []):
                    groups[group["name"].lower()] = group["id"]
                url = data.get("next_page")
            self.group_data = groups
            logger.info(f"Fetched {len(groups)} groups: {list(groups.keys())}")
            return {"success": True, "groups": groups}
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch groups: {str(e)}")
            return {"success": False, "message": str(e)}

    def authenticate(self, credentials):
        logger.debug(f"authenticate called with credentials: {credentials}")
        logger.debug(f"save_credentials value: {credentials.get('save_credentials')}")
        try:
            self.save_credentials = credentials.get("save_credentials", False)
            result = self.auth_api.authenticate(credentials)
            if result["success"]:
                self.subdomain = credentials.get("subdomain")
                self.email = credentials.get("email")
                self.token = credentials.get("api_token")
                self.custom_objects = fetch_custom_objects(self.subdomain, self.email, self.token)
                self.fetch_groups()
                logger.info(f"Fetched custom objects: {self.custom_objects}")
            return result
        except Exception as e:
            logger.error(f"Authentication failed: {str(e)}")
            return {"success": False, "message": str(e)}

    def get_custom_objects(self):
        logger.debug("get_custom_objects called")
        return {"success": True, "custom_objects": self.custom_objects}

    def select_csv_file(self):
        logger.debug("select_csv_file called")
        try:
            file_paths = webview.windows[0].create_file_dialog(
                dialog_type=webview.OPEN_DIALOG,
                file_types=('CSV Files (*.csv)', 'All Files (*.*)')
            )
            if file_paths and len(file_paths) > 0:
                file_path = file_paths[0]
                logger.info(f"CSV file selected: {file_path}")
                return {"success": True, "file_path": file_path}
            logger.warning("No file selected in select_csv_file")
            return {"success": False, "message": "No file selected"}
        except Exception as e:
            logger.error(f"Error in select_csv_file: {str(e)}")
            return {"success": False, "message": str(e)}

    def load_csv(self, data):
        logger.debug(f"load_csv: Called with filePath: {data.get('filePath')}, headers_only: {data.get('headers_only')}")
        try:
            file_path = data['filePath']
            headers_only = data.get('headers_only', False)
            if not os.path.exists(file_path):
                logger.error(f"CSV file does not exist: {file_path}")
                return {"success": False, "message": f"CSV file does not exist: {file_path}"}

            # Read only headers if headers_only is True
            if headers_only:
                df = pd.read_csv(file_path, encoding='utf-8-sig', nrows=0)  # Read only headers
                headers = df.columns.tolist()
                data_rows = []
            else:
                # Read first 1000 rows for data preview (if needed elsewhere)
                df = pd.read_csv(file_path, encoding='utf-8-sig', nrows=1000)
                df = df.replace({np.nan: None})  # Replace NaN with None
                headers = df.columns.tolist()
                data_rows = df.values.tolist()

            if not headers:
                logger.error("No valid headers found in CSV")
                return {"success": False, "message": "No valid headers found in CSV"}

            self.csv_headers = headers
            self.csv_data = data_rows
            self.csv_file_path = file_path
            normalized_object = self.selected_object
            if self.selected_object in ["Tickets + Comments", "Tickets + Comments + Attachments"]:
                normalized_object = "Tickets"
            elif self.selected_object == "Triggers":
                normalized_object = "Trigger"
            self.zendesk_fields, self.custom_field_info = fetch_zendesk_fields(
                self.subdomain, self.email, self.token, normalized_object, self.action_type
            )
            logger.info(f"load_csv: Success, headers: {headers}, rows: {len(data_rows)}")
            return {
                "success": True,
                "fields": headers,
                "data_rows": data_rows,
                "row_count": len(data_rows)
            }
        except Exception as e:
            logger.error(f"Error in load_csv: {str(e)}")
            return {"success": False, "message": str(e)}

    def load_ticket_csv(self, data):
        logger.debug(f"load_ticket_csv: Called with filePath: {data.get('filePath')}, headers_only: {data.get('headers_only')}")
        try:
            file_path = data['filePath']
            headers_only = data.get('headers_only', False)
            if not os.path.exists(file_path):
                logger.error(f"CSV file does not exist: {file_path}")
                return {"success": False, "message": f"CSV file does not exist: {file_path}"}

            # Read only headers if headers_only is True
            if headers_only:
                df = pd.read_csv(file_path, encoding='utf-8-sig', nrows=0)
                headers = df.columns.tolist()
                data_rows = []
            else:
                # Read first 1000 rows for data preview
                df = pd.read_csv(file_path, encoding='utf-8-sig', nrows=1000)
                df = df.replace({np.nan: None})  # Replace NaN with None
                headers = df.columns.tolist()
                data_rows = df.values.tolist()

            if not headers:
                logger.error("No valid headers found in CSV")
                return {"success": False, "message": "No valid headers found in CSV"}

            self.csv_headers = headers
            self.csv_data = data_rows
            self.csv_file_path = file_path
            self.zendesk_fields, self.custom_field_info = fetch_zendesk_fields(
                self.subdomain, self.email, self.token, "Tickets", self.action_type
            )
            logger.info(f"load_ticket_csv: Success, headers: {headers}, rows: {len(data_rows)}")
            return {
                "success": True,
                "fields": headers,
                "data_rows": data_rows,
                "row_count": len(data_rows)
            }
        except Exception as e:
            logger.error(f"Error in load_ticket_csv: {str(e)}")
            return {"success": False, "message": str(e)}

    def load_comments_csv(self, data):
        logger.debug(f"load_comments_csv: Called with filePath: {data.get('filePath')}, headers_only: {data.get('headers_only')}")
        try:
            file_path = data['filePath']
            headers_only = data.get('headers_only', False)
            if not os.path.exists(file_path):
                logger.error(f"CSV file does not exist: {file_path}")
                return {"success": False, "message": f"CSV file does not exist: {file_path}"}

            # Read only headers if headers_only is True
            if headers_only:
                df = pd.read_csv(file_path, encoding='utf-8-sig', nrows=0)
                headers = df.columns.tolist()
                data_rows = []
            else:
                # Read first 1000 rows for data preview
                df = pd.read_csv(file_path, encoding='utf-8-sig', nrows=1000)
                df = df.replace({np.nan: None})  # Replace NaN with None
                headers = df.columns.tolist()
                data_rows = df.values.tolist()

            if not headers:
                logger.error("No valid headers found in CSV")
                return {"success": False, "message": "No valid headers found in CSV"}

            self.comments_csv_headers = headers
            self.comments_csv_data = data_rows
            self.comments_csv_file_path = file_path
            self.zendesk_fields = self.comment_zendesk_fields
            logger.info(f"load_comments_csv: Success, headers: {headers}, rows: {len(data_rows)}")
            return {
                "success": True,
                "fields": headers,
                "data_rows": data_rows,
                "row_count": len(data_rows)
            }
        except Exception as e:
            logger.error(f"Error in load_comments_csv: {str(e)}")
            return {"success": False, "message": str(e)}

    def load_attachments_csv(self, data):
        logger.debug(f"load_attachments_csv: Called with filePath: {data.get('filePath')}, headers_only: {data.get('headers_only')}")
        try:
            file_path = data['filePath']
            headers_only = data.get('headers_only', False)
            if not os.path.exists(file_path):
                logger.error(f"CSV file does not exist: {file_path}")
                return {"success": False, "message": f"CSV file does not exist: {file_path}"}

            # Read only headers if headers_only is True
            if headers_only:
                df = pd.read_csv(file_path, encoding='utf-8-sig', nrows=0)
                headers = df.columns.tolist()
                data_rows = []
            else:
                # Read first 1000 rows for data preview
                df = pd.read_csv(file_path, encoding='utf-8-sig', nrows=1000)
                df = df.replace({np.nan: None})  # Replace NaN with None
                headers = df.columns.tolist()
                data_rows = df.values.tolist()

            if not headers:
                logger.error("No valid headers found in CSV")
                return {"success": False, "message": "No valid headers found in CSV"}

            self.attachments_csv_headers = headers
            self.attachments_csv_data = data_rows
            self.attachments_csv_file_path = file_path
            self.zendesk_fields = self.attachment_zendesk_fields
            logger.info(f"load_attachments_csv: Success, headers: {headers}, rows: {len(data_rows)}")
            return {
                "success": True,
                "fields": headers,
                "data_rows": data_rows,
                "row_count": len(data_rows)
            }
        except Exception as e:
            logger.error(f"Error in load_attachments_csv: {str(e)}")
            return {"success": False, "message": str(e)}




    
    def build_csv_indices(self):
        """
        Builds indices for comments and attachments CSVs, grouping comments by ticket_id and comment_id.
        Indexes include comment row data, file names, and row numbers for html_body fetching.
        Supports nested attachment directory structure: Attachments/ticket_XXX/comment_XXX/{attachment}.
        Logs indexing progress and errors to app.log and prints status updates.
        
        Returns:
            tuple: (comments_index, attachments_index)
                - comments_index: Dict mapping ticket_id to comment_id to list of dicts with row, file_name, and row_number.
                - attachments_index: Empty dict (reserved for future use).
        """
        logger.debug("Building indices for comments and attachments CSVs")
        comments_index = {}

        try:
            # Initialize column indices
            comment_ticket_id_idx = None
            comment_id_idx = None
            comment_html_body_idx = None
            file_name_idx = None

            # Validate and set indices
            if self.comments_csv_headers and self.comment_ticket_id_field:
                if self.comment_ticket_id_field not in self.comments_csv_headers:
                    logger.error(f"Comment ticket ID field '{self.comment_ticket_id_field}' not in comments CSV headers")
                    raise ValueError(f"Comment ticket ID field '{self.comment_ticket_id_field}' not in headers")
                comment_ticket_id_idx = self.comments_csv_headers.index(self.comment_ticket_id_field)

                # Find html_body column if mapped
                for csv_field, zendesk_field in self.comment_field_mapping.items():
                    if zendesk_field == "html_body" and csv_field in self.comments_csv_headers:
                        comment_html_body_idx = self.comments_csv_headers.index(csv_field)
                        break

                # Find attachment_comment_id_field
                if self.attachment_comment_id_field and self.attachment_comment_id_field in self.comments_csv_headers:
                    comment_id_idx = self.comments_csv_headers.index(self.attachment_comment_id_field)
                else:
                    logger.error(f"Attachment comment ID field '{self.attachment_comment_id_field}' not found in comments CSV headers")
                    raise ValueError(f"Attachment comment ID field '{self.attachment_comment_id_field}' not found in comments CSV headers")

                # Find file_name column
                if self.selected_object == "Tickets + Comments + Attachments":
                    for csv_field, zendesk_field in self.attachment_field_mapping.items():
                        if zendesk_field == "file_name" and csv_field in self.comments_csv_headers:
                            file_name_idx = self.comments_csv_headers.index(csv_field)
                            break
                    if file_name_idx is None:
                        logger.error("No 'file_name' field mapped in comments CSV for attachments")
                        raise ValueError("No 'file_name' field mapped in comments CSV for attachments")

            # Build comments index
            if self.comments_csv_file_path and os.path.exists(self.comments_csv_file_path):
                print(f"📋 Indexing comments from {self.comments_csv_file_path}...")
                with open(self.comments_csv_file_path, 'r', encoding='utf-8-sig') as f:
                    reader = csv.reader(f)
                    headers = next(reader)  # Skip header
                    for row_number, row_data in enumerate(reader, start=1):
                        ticket_id = str(row_data[comment_ticket_id_idx]).strip() if comment_ticket_id_idx is not None and len(row_data) > comment_ticket_id_idx and row_data[comment_ticket_id_idx] else None
                        comment_id = str(row_data[comment_id_idx]).strip() if comment_id_idx is not None and len(row_data) > comment_id_idx and row_data[comment_id_idx] else None
                        file_name = str(row_data[file_name_idx]).strip() if file_name_idx is not None and len(row_data) > file_name_idx and row_data[file_name_idx] else None
                        if ticket_id and comment_id:
                            # Store row data, file name, and row number for html_body fetching
                            comments_index.setdefault(ticket_id, {}).setdefault(comment_id, []).append({
                                "row": row_data,
                                "file_name": file_name,
                                "row_number": row_number
                            })
                            logger.debug(f"Indexed comment: ticket_id={ticket_id}, comment_id={comment_id}, file_name={file_name}, row={row_number}")
                        else:
                            logger.debug(f"Skipped comment row {row_number}: ticket_id={ticket_id}, comment_id={comment_id}")
                print(f"✅ Indexed {sum(len(comments) for comments in comments_index.values())} comments across {len(comments_index)} tickets")

            logger.info(f"Built comments index with {len(comments_index)} ticket IDs")
            return comments_index, {}  # Return empty attachments_index for future use
        except Exception as e:
            logger.error(f"Error building CSV indices: {e}")
            print(f"❌ Error building CSV indices: {e}")
            raise
    




    def fetch_html_body_values(self, row_numbers, html_body_column):
        """
        Fetch html_body values from the comments CSV for specific row numbers.
        Args:
            row_numbers (set): Set of 1-based row numbers to fetch.
            html_body_column (str): CSV column name mapped to html_body.
        Returns:
            dict: Mapping of row_number to html_body value.
        """
        logger.debug(f"Fetching html_body for {len(row_numbers)} comment rows")
        html_body_idx = self.comments_csv_headers.index(html_body_column) if html_body_column in self.comments_csv_headers else None
        if html_body_idx is None:
            logger.error(f"html_body column '{html_body_column}' not in comments CSV headers")
            raise ValueError(f"html_body column '{html_body_column}' not in headers")

        result = {}
        try:
            with open(self.comments_csv_file_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.reader(f)
                headers = next(reader)  # Skip header
                for current_row, row_data in enumerate(reader, start=1):
                    if current_row in row_numbers:
                        html_body = row_data[html_body_idx] if html_body_idx < len(row_data) else None
                        result[current_row] = html_body
                    if current_row > max(row_numbers, default=0):
                        break  # Stop reading once all needed rows are fetched
            logger.debug(f"Fetched html_body for {len(result)} rows")
            return result
        except Exception as e:
            logger.error(f"Error fetching html_body values: {str(e)}")
            raise

    def get_csv_data(self):
        logger.debug("get_csv_data called")
        return {
            "headers": self.csv_headers,
            "file_path": self.csv_file_path,
            "comments_headers": self.comments_csv_headers,
            "comments_file_path": self.comments_csv_file_path,
            "attachments_headers": self.attachments_csv_headers,
            "attachments_file_path": self.attachments_csv_file_path
        }

    def clear_csv_data(self):
        logger.debug("clear_csv_data called")
        try:
            self.csv_headers = []
            self.csv_data = []
            self.csv_file_path = ""
            self.comments_csv_headers = []
            self.comments_csv_data = []
            self.comments_csv_file_path = ""
            self.attachments_csv_headers = []
            self.attachments_csv_data = []
            self.attachments_csv_file_path = ""
            self.ticket_id_field = None
            self.comment_ticket_id_field = None
            self.attachment_comment_id_field = None
            self.field_mapping = {}
            self.comment_field_mapping = {}
            self.attachment_field_mapping = {}
            self.attachment_directory = None
            logger.info("CSV data and mappings cleared")
            return {"success": True, "message": "CSV data and mappings cleared"}
        except Exception as e:
            logger.error(f"Error in clear_csv_data: {str(e)}")
            return {"success": False, "message": str(e)}



    def get_zendesk_fields(self, object_type=None):
        logger.debug(f"get_zendesk_fields called with object_type: {object_type}, selected_object: {self.selected_object}, action_type: {self.action_type}")
        try:
            if not object_type:
                if not self.selected_object:
                    logger.error("No object_type provided and self.selected_object is not set")
                    return {"success": False, "message": "No object type selected"}
                object_type = self.selected_object

            # Normalize object_type
            normalized_object = object_type
            if object_type in ["Tickets + Comments", "Tickets + Comments + Attachments"]:
                normalized_object = "Tickets"
                logger.info(f"Normalized object_type to 'Tickets' for field fetching")
            elif object_type == "Triggers":
                normalized_object = "Trigger"
                logger.info(f"Normalized object_type to 'Trigger' for field fetching")

            if not self.action_type:
                logger.warning("action_type not set; defaulting to 'import'")
                self.action_type = "import"

            fields, custom_field_info = fetch_zendesk_fields(
                self.subdomain, self.email, self.token, normalized_object, self.action_type
            )
            if not fields and not custom_field_info:
                logger.warning(f"No fields or custom_field_info returned for {normalized_object}")
                return {"success": True, "fields": []}

            builtin_objects = ["User", "Users", "Agent", "Organization", "Organizations", "Ticket", "Tickets", "Ticket Comments", "Attachments", "Trigger", "Macros"]
            is_custom_object = normalized_object not in builtin_objects

            standard_api_names = {field for field in fields if not field.startswith("custom_field_")}
            custom_api_names = {field for field in fields if field.startswith("custom_field_")}

            if normalized_object == "Attachments":
                standard_fields = [
                    {"api_name": "file_name", "display_name": "File Name", "type": "text"}
                ]
                custom_fields = []
            elif normalized_object == "Trigger":
                standard_fields = [
                    {"api_name": field, "display_name": field.replace("_", " ").title(), "type": "text"}
                    for field in ['id', 'title', 'active', 'position', 'conditions_all', 'conditions_any', 'actions', 'created_at', 'updated_at', 'category_id']
                ]
                custom_fields = []
            elif is_custom_object and self.action_type == "delete":
                standard_fields = [
                    {"api_name": "id", "display_name": "ID", "type": "text"}
                ]
                custom_fields = []
            else:
                standard_fields = [
                    {"api_name": field, "display_name": field.replace("_", " ").title(), "type": "text"}
                    for field in standard_api_names
                ]

                if is_custom_object:
                    required_standard_fields = [
                        {"api_name": "id", "display_name": "ID", "type": "text"},
                        {"api_name": "name", "display_name": "Name", "type": "text"},
                        {"api_name": "external_id", "display_name": "External ID", "type": "text"}
                    ]
                    for req_field in required_standard_fields:
                        if req_field["api_name"] not in [f["api_name"] for f in standard_fields]:
                            standard_fields.append(req_field)

                custom_fields = []
                for cf in custom_field_info:
                    api_name = cf["name"]
                    display_name = cf.get("display_name", cf["name"].replace("custom_field_", "").replace("_", " ").title())
                    field_type = cf.get("type", "text")

                    if normalized_object in ["Ticket", "Tickets"]:
                        api_name = str(cf["id"])

                    if api_name:
                        custom_fields.append({
                            "api_name": api_name,
                            "display_name": display_name,
                            "type": field_type
                        })

            comment_fields = [
                {"api_name": "author_id", "display_name": "Author ID", "type": "text"},
                {"api_name": "body", "display_name": "Body", "type": "text"},
                {"api_name": "html_body", "display_name": "HTML Body", "type": "text"},
                {"api_name": "public", "display_name": "Public", "type": "boolean"},
                {"api_name": "created_at", "display_name": "Created At", "type": "date"}
            ] if self.selected_object in ["Tickets + Comments", "Tickets + Comments + Attachments"] else []

            all_fields = standard_fields + custom_fields + comment_fields

            all_fields = [
                field for field in all_fields
                if field["api_name"] and field["api_name"] not in [">>>", "<<<<<<<"]
            ]

            logger.info(f"Retrieved {len(all_fields)} Zendesk fields for {normalized_object}")
            return {"success": True, "fields": all_fields}
        except Exception as e:
            logger.error(f"Failed to fetch Zendesk fields: {str(e)}")
            return {"success": False, "message": str(e)}

    def auto_map_fields(self, csv_headers, field_type="ticket"):
        logger.debug(f"auto_map_fields called with headers: {csv_headers}, type: {field_type}")
        try:
            if not csv_headers:
                logger.error("No CSV headers available")
                return {"success": False, "message": "No CSV headers available"}

            comment_zendesk_fields = ['author_id', 'body', 'html_body', 'public', 'created_at']
            attachment_zendesk_fields = ['file_name']
            trigger_zendesk_fields = ['id', 'title', 'active', 'position', 'conditions_all', 'conditions_any', 'actions', 'created_at', 'updated_at']

            if field_type == "ticket":
                if not self.zendesk_fields:
                    logger.error("No Zendesk fields available for tickets")
                    return {"success": False, "message": "No Zendesk fields available for tickets"}
                fields = self.zendesk_fields
                full_fields = self.get_zendesk_fields(self.selected_object)["fields"]
            elif field_type == "comment":
                fields = comment_zendesk_fields
                full_fields = [{'api_name': f, 'display_name': f.replace('_', ' ').title(), 'type': 'unknown'} for f in fields]
            elif field_type == "attachment":
                fields = attachment_zendesk_fields
                full_fields = [{'api_name': f, 'display_name': f.replace('_', ' ').title(), 'type': 'text'} for f in fields]
            elif field_type == "trigger":
                fields = trigger_zendesk_fields
                full_fields = [{'api_name': f, 'display_name': f.replace('_', ' ').title(), 'type': 'text'} for f in fields]
            else:
                logger.error(f"Invalid field_type: {field_type}")
                return {"success": False, "message": f"Invalid field_type: {field_type}"}

            field_map = {}
            for field in full_fields:
                api_name = field['api_name'].lower()
                display_name = field['display_name'].lower()
                field_map[api_name] = field['api_name']
                if display_name != api_name:
                    field_map[display_name] = field['api_name']

            mapping = {}
            used_fields = set()

            def normalize_text(text):
                return ' '.join(text.lower().strip().split())

            for csv_field in csv_headers:
                if not csv_field:
                    mapping[csv_field] = " "
                    continue

                norm_csv_field = normalize_text(csv_field)

                if norm_csv_field in field_map and field_map[norm_csv_field] not in used_fields:
                    mapping[csv_field] = field_map[norm_csv_field]
                    used_fields.add(field_map[norm_csv_field])
                    continue

                best_match = None
                best_score = -1
                csv_words = set(norm_csv_field.split())
                csv_word_count = len(csv_words)

                for field_key, zendesk_field in field_map.items():
                    if zendesk_field in used_fields:
                        continue
                    norm_field_key = normalize_text(field_key)
                    field_words = set(norm_field_key.split())
                    field_word_count = len(field_words)

                    if csv_words.issubset(field_words):
                        score = len(csv_words)
                        if csv_word_count == field_word_count:
                            score += 10
                        if score > best_score:
                            best_match = zendesk_field
                            best_score = score

                if best_match:
                    mapping[csv_field] = best_match
                    used_fields.add(best_match)
                else:
                    mapping[csv_field] = " "

            if field_type == "ticket":
                self.field_mapping = mapping
            elif field_type == "comment":
                self.comment_field_mapping = mapping
            elif field_type == "attachment":
                self.attachment_field_mapping = mapping
            elif field_type == "trigger":
                self.field_mapping = mapping

            logger.info(f"Auto-map result for {field_type}: {mapping}")
            return {"success": True, "mapping": mapping}
        except Exception as e:
            logger.error(f"Error in auto_map_fields: {str(e)}")
            return {"success": False, "message": str(e)}

    def save_mapping(self, mapping, mapping_type="ticket"):
        logger.debug(f"save_mapping called with mapping: {mapping}, type: {mapping_type}")
        try:
            if mapping_type == "ticket":
                self.field_mapping = mapping
            elif mapping_type == "comment":
                self.comment_field_mapping = mapping
            elif mapping_type == "attachment":
                self.attachment_field_mapping = mapping
            elif mapping_type == "trigger":
                self.field_mapping = mapping
            logger.info(f"Mapping saved for {mapping_type}")
            return {"success": True, "message": "Mapping saved"}
        except Exception as e:
            logger.error(f"Error in save_mapping: {str(e)}")
            return {"success": False, "message": str(e)}

    def select_mapping_file(self):
        logger.debug("select_mapping_file called")
        try:
            file_paths = webview.windows[0].create_file_dialog(
                dialog_type=webview.OPEN_DIALOG,
                file_types=('JSON Files (*.json)', 'All Files (*.*)')
            )
            if file_paths and len(file_paths) > 0:
                file_path = file_paths[0]
                logger.info(f"Mapping file selected: {file_path}")
                return {"success": True, "file_path": file_path}
            logger.warning("No file selected in select_mapping_file")
            return {"success": False, "message": "No file selected"}
        except Exception as e:
            logger.error(f"Error in select_mapping_file: {str(e)}")
            return {"success": False, "message": str(e)}

    def load_mapping_from_file(self, file_path, mapping_type="ticket"):
        logger.debug(f"load_mapping_from_file called with file_path: {file_path}, type: {mapping_type}")
        try:
            if isinstance(file_path, list):
                if len(file_path) == 0:
                    logger.error("No file path provided in list")
                    return {"success": False, "message": "No file path provided in list"}
                file_path = file_path[0]

            if not isinstance(file_path, str):
                logger.error(f"Invalid file path type: {type(file_path)}")
                return {"success": False, "message": f"Invalid file path type: {type(file_path)}"}

            if not os.path.exists(file_path):
                logger.error(f"File does not exist: {file_path}")
                return {"success": False, "message": f"File not found: {file_path}"}

            with open(file_path, 'r', encoding='utf-8') as f:
                mapping = json.load(f)

            if not isinstance(mapping, dict):
                logger.error(f"Invalid mapping format in {file_path}: not a dictionary")
                return {"success": False, "message": "Invalid mapping format: must be a dictionary"}

            if mapping_type == "ticket":
                self.field_mapping = mapping
            elif mapping_type == "comment":
                self.comment_field_mapping = mapping
            elif mapping_type == "attachment":
                self.attachment_field_mapping = mapping
            elif mapping_type == "trigger":
                self.field_mapping = mapping

            logger.info(f"Mapping loaded for {mapping_type}")
            return {
                "success": True,
                "mapping": mapping,
                "message": f"Mapping loaded!"
            }
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from {file_path}: {str(e)}")
            return {"success": False, "message": f"Invalid JSON format: {str(e)}"}
        except Exception as e:
            logger.error(f"Error loading mapping from file {file_path}: {str(e)}")
            return {"success": False, "message": f"Error loading mapping: {str(e)}"}

    def update_progress_bar(self, progress, record_count=None):
        logger.debug(f"update_progress_bar called with progress: {progress}, record_count: {record_count}")
        try:
            js_code = f"updateProgressBar({progress}"
            if record_count is not None:
                js_code += f", {record_count}"
            js_code += ")"
            webview.windows[0].evaluate_js(js_code)
        except Exception as e:
            logger.error(f"Error updating progress bar: {str(e)}")

    def select_save_directory(self):
        logger.debug("select_save_directory called")
        try:
            directory = webview.windows[0].create_file_dialog(webview.FOLDER_DIALOG)
            if directory:
                self.save_directory = directory[0]
                logger.info(f"Save directory set to: {self.save_directory}")
                return {"success": True, "directory": self.save_directory}
            logger.warning("No directory selected")
            return {"success": False, "message": "No directory selected"}
        except Exception as e:
            logger.error(f"Error in select_save_directory: {str(e)}")
            return {"success": False, "message": str(e)}

    def select_save_file(self, default_name="mapping.json"):
        logger.debug(f"select_save_file called with default_name: {default_name}")
        try:
            file_path = webview.windows[0].create_file_dialog(
                dialog_type=webview.SAVE_DIALOG,
                file_types=('JSON Files (*.json)', 'All Files (*.*)'),
                save_filename=default_name
            )
            if file_path:
                logger.info(f"Save file selected: {file_path}")
                return {"success": True, "file_path": file_path}
            logger.warning("No file selected")
            return {"success": False, "message": "No file selected"}
        except Exception as e:
            logger.error(f"Error in select_save_file: {str(e)}")
            return {"success": False, "message": str(e)}

    def save_mapping_to_file(self, mapping, file_path, mapping_type="ticket"):
        logger.debug(f"save_mapping_to_file called with mapping: {mapping}, file_path: {file_path}, type: {mapping_type}")
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(mapping, f, indent=4)
            if mapping_type == "ticket":
                self.field_mapping = mapping
            elif mapping_type == "comment":
                self.comment_field_mapping = mapping
            elif mapping_type == "attachment":
                self.attachment_field_mapping = mapping
            elif mapping_type == "trigger":
                self.field_mapping = mapping
            logger.info(f"Mapping saved for {mapping_type}")
            return {"success": True, "message": f"Mapping saved!"}
        except Exception as e:
            logger.error(f"Error saving mapping to file: {str(e)}")
            return {"success": False, "message": f"Error saving mapping: {str(e)}"}

    def set_field_mapping(self, mapping, mapping_type="ticket"):
        logger.debug(f"set_field_mapping called with mapping: {mapping}, type: {mapping_type}")
        try:
            if mapping_type == "ticket":
                self.field_mapping = mapping
            elif mapping_type == "comment":
                self.comment_field_mapping = mapping
            elif mapping_type == "attachment":
                self.attachment_field_mapping = mapping
            elif mapping_type == "trigger":
                self.field_mapping = mapping
            logger.info(f"Field mapping set for {mapping_type}")
            return {"success": True, "message": "Field mapping set"}
        except Exception as e:
            logger.error(f"Error in set_field_mapping: {str(e)}")
            return {"success": False, "message": str(e)}

    def set_ticket_id_fields(self, ticket_id_field, comment_ticket_id_field, attachment_comment_id_field=None):
        logger.debug(f"set_ticket_id_fields called with ticket_id: {ticket_id_field}, comment_id: {comment_ticket_id_field}, attachment_comment_id: {attachment_comment_id_field}")
        try:
            self.ticket_id_field = ticket_id_field
            self.comment_ticket_id_field = comment_ticket_id_field
            self.attachment_comment_id_field = attachment_comment_id_field
            logger.info("Ticket ID fields set")
            return {"success": True}
        except Exception as e:
            logger.error(f"Error in set_ticket_id_fields: {str(e)}")
            return {"success": False, "message": str(e)}

    def set_action_type(self, action_type):
        logger.debug(f"set_action_type called with action_type: {action_type}")
        try:
            self.action_type = action_type.lower()
            logger.info(f"Action type set to: {action_type}")
            return {"success": True}
        except Exception as e:
            logger.error(f"Error in set_action_type: {str(e)}")
            return {"success": False, "message": str(e)}

    def set_selected_object(self, selected_object):
        logger.debug(f"set_selected_object called with selected_object: {selected_object}")
        try:
            self.selected_object = selected_object
            logger.info(f"Selected object set to: {selected_object}")
            return {"success": True}
        except Exception as e:
            logger.error(f"Error in set_selected_object: {str(e)}")
            return {"success": False, "message": str(e)}

    def get_selected_object(self):
        logger.debug("get_selected_object called")
        return {"selected_object": self.selected_object}

    def get_action_type(self):
        logger.debug("get_action_type called")
        return {"action_type": self.action_type or "Not Selected"}

    def safe_update_progress_bar(self, progress, record_count=None):
        logger.debug(f"safe_update_progress_bar called with progress: {progress}, record_count: {record_count}")
        try:
            self.update_progress_bar(progress, record_count)
        except Exception as e:
            logger.error(f"Error in safe_update_progress_bar: {str(e)}")

    def select_attachment_directory(self):
        logger.debug("select_attachment_directory called")
        try:
            directory = webview.windows[0].create_file_dialog(webview.FOLDER_DIALOG)
            if directory:
                self.attachment_directory = directory[0]
                logger.info(f"Attachment directory set to: {self.attachment_directory}")
                return {"success": True, "directory": self.attachment_directory}
            logger.warning("No directory selected")
            return {"success": False, "message": "No directory selected"}
        except Exception as e:
            logger.error(f"Error in select_attachment_directory: {str(e)}")
            return {"success": False, "message": str(e)}

    def navigate_to(self, page):
        logger.debug(f"navigate_to called with page: {page}")
        try:
            pages = {
                "login": "ui/login.html",
                "action_selection": "ui/select_action.html",
                "object_selection": "ui/select_object.html",
                "field_mapping": "ui/field_mapping.html",
                "ticket_field_mapping": "ui/ticket_field_mapping.html",
                "comment_mapping": "ui/comment_mapping.html",
                "attachment_mapping": "ui/attachment_mapping.html",
                "confirm_import": "ui/confirm_import.html"
            }
            if page == "object_selection":
                self.clear_csv_data()
            if page in pages:
                if not os.path.exists(pages[page]):
                    logger.error(f"Page file not found: {pages[page]}")
                    raise FileNotFoundError(f"Page file not found: {pages[page]}")
                logger.info(f"Loading page: {pages[page]}")
                webview.windows[0].load_url(pages[page])
                time.sleep(0.5)
                if page in ["field_mapping", "ticket_field_mapping", "comment_mapping", "attachment_mapping"]:
                    try:
                        is_ready = webview.windows[0].evaluate_js("typeof window.pywebview !== 'undefined'")
                        if is_ready:
                            if page == "field_mapping" and self.selected_object in ["Tickets + Comments", "Tickets + Comments + Attachments"]:
                                self._send_mappings_to_frontend("ticket")
                            elif page == "attachment_mapping" and self.selected_object != "Tickets + Comments + Attachments":
                                webview.windows[0].load_url(pages["field_mapping"])
                                time.sleep(0.5)
                                is_ready = webview.windows[0].evaluate_js("typeof window.pywebview !== 'undefined'")
                                if is_ready:
                                    self._send_mappings_to_frontend("ticket")
                            else:
                                if page == "field_mapping":
                                    self._send_mappings_to_frontend("ticket")
                                elif page == "ticket_field_mapping":
                                    self._send_mappings_to_frontend("ticket")
                                elif page == "comment_mapping":
                                    self._send_mappings_to_frontend("comment")
                                elif page == "attachment_mapping":
                                    self._send_mappings_to_frontend("attachment")
                        else:
                            logger.warning(f"pywebview not ready on page {page}, skipping JavaScript evaluation")
                    except webview.errors.JavascriptException as e:
                        logger.error(f"Error evaluating pywebview readiness on page {page}: {str(e)}")
            else:
                logger.error(f"Invalid page: {page}")
                raise ValueError(f"Invalid page: {page}")
            logger.info(f"Navigated to page: {page}")
            return {"success": True}
        except Exception as e:
            logger.error(f"Error in navigate_to: {str(e)}")
            return {"success": False, "message": str(e)}

    def _send_mappings_to_frontend(self, mapping_type):
        logger.debug(f"Sending {mapping_type} mappings to frontend")
        try:
            mapping = {
                "ticket": self.field_mapping,
                "comment": self.comment_field_mapping,
                "attachment": self.attachment_field_mapping,
                "trigger": self.field_mapping
            }.get(mapping_type, {})
            js_code = f"""
            if (typeof window.pywebview !== 'undefined' && typeof window.updateFieldMapping === 'function') {{
                window.updateFieldMapping({json.dumps(mapping)}, '{mapping_type}');
            }} else {{
                console.warn('pywebview or updateFieldMapping not ready, skipping mapping update');
            }}
            """
            webview.windows[0].evaluate_js(js_code)
            logger.info(f"Sent {mapping_type} mappings to frontend")
        except webview.errors.JavascriptException as e:
            logger.error(f"Error sending {mapping_type} mappings: {str(e)}")

    def get_field_mapping(self, mapping_type="ticket"):
        logger.debug(f"get_field_mapping called with type: {mapping_type}")
        try:
            mapping = {
                "ticket": self.field_mapping,
                "comment": self.comment_field_mapping,
                "attachment": self.attachment_field_mapping,
                "trigger": self.field_mapping
            }.get(mapping_type, {})
            logger.info(f"Retrieved field mapping for {mapping_type}")
            return {"success": True, "mapping": mapping}
        except Exception as e:
            logger.error(f"Error in get_field_mapping: {str(e)}")
            return {"success": False, "message": str(e)}








    def execute_import(self):
        logger.debug("execute_import called")
        try:
            # Validate save directory
            if not self.save_directory or not os.path.exists(self.save_directory):
                logger.error("No valid save directory selected")
                return {"success": False, "message": "Please select a save directory"}

            # Validate CSV headers and file paths for non-export actions
            if self.action_type != "export":
                if not self.csv_headers or not self.csv_file_path or not os.path.exists(self.csv_file_path):
                    logger.error("No CSV headers or valid file path")
                    return {"success": False, "message": "No CSV headers or valid file path"}
                if self.selected_object in ["Tickets + Comments", "Tickets + Comments + Attachments"]:
                    if not self.comments_csv_headers or not self.comments_csv_file_path or not os.path.exists(self.comments_csv_file_path):
                        logger.error("No CSV headers or valid file path for comments")
                        return {"success": False, "message": "No CSV headers or valid file path for comments"}
                    if self.selected_object == "Tickets + Comments + Attachments":
                        if not self.attachment_field_mapping or 'file_name' not in self.attachment_field_mapping.values():
                            logger.error("Attachment field mapping must include 'file_name'")
                            return {"success": False, "message": "Attachment field mapping must include 'file_name'"}
                        if not self.attachment_directory:
                            logger.error("Attachment directory not selected")
                            return {"success": False, "message": "Attachment directory not selected"}
                        if not self.attachment_comment_id_field:
                            logger.error("Comment ID field for attachments not selected")
                            return {"success": False, "message": "Comment ID field for attachments not selected"}
                        if self.attachment_comment_id_field not in self.comments_csv_headers:
                            logger.error(f"Comment ID field '{self.attachment_comment_id_field}' not found in comments CSV headers")
                            return {"success": False, "message": f"Comment ID field '{self.attachment_comment_id_field}' not found in comments CSV headers"}

            self.operation_running = True
            self.stop_event.clear()
            os.makedirs(self.save_directory, exist_ok=True)

            # Generate unique filenames for all operations
            normalized_object = self.selected_object
            if self.selected_object == "Tickets + Comments":
                normalized_object = "Tickets_Comments"
            elif self.selected_object == "Triggers":
                normalized_object = "Trigger"

            success_filename = None
            error_filename = None
            attachment_success_filename = None
            attachment_error_filename = None
            comment_success_filename = None
            comment_error_filename = None

            if self.action_type != "export":
                if self.selected_object == "Tickets + Comments + Attachments":
                    success_filename = self.get_unique_filename(
                        os.path.join(self.save_directory, "Import_success.csv")
                    )
                    error_filename = self.get_unique_filename(
                        os.path.join(self.save_directory, "Import_errors.csv")
                    )
                    attachment_success_filename = self.get_unique_filename(
                        os.path.join(self.save_directory, "Attachment_success.csv")
                    )
                    attachment_error_filename = self.get_unique_filename(
                        os.path.join(self.save_directory, "Attachment_errors.csv")
                    )
                    comment_success_filename = self.get_unique_filename(
                        os.path.join(self.save_directory, "Comment_success.csv")
                    )
                    comment_error_filename = self.get_unique_filename(
                        os.path.join(self.save_directory, "Comment_errors.csv")
                    )
                    logger.info(f"Generated filenames for Tickets + Comments + Attachments: "
                                f"Ticket Success: {success_filename}, Ticket Error: {error_filename}, "
                                f"Comment Success: {comment_success_filename}, Comment Error: {comment_error_filename}, "
                                f"Attachment Success: {attachment_success_filename}, Attachment Error: {attachment_error_filename}")
                else:
                    success_filename = self.get_unique_filename(
                        os.path.join(self.save_directory, f"{normalized_object}_{self.action_type}_success.csv")
                    )
                    error_filename = self.get_unique_filename(
                        os.path.join(self.save_directory, f"{normalized_object}_{self.action_type}_errors.csv")
                    )
                    logger.info(f"Generated filenames for {self.action_type}: {success_filename}, {error_filename}")
                    if self.selected_object == "Tickets + Comments":
                        attachment_success_filename = self.get_unique_filename(
                            os.path.join(self.save_directory, f"{normalized_object}_attachments_success.csv")
                        )
                        attachment_error_filename = self.get_unique_filename(
                            os.path.join(self.save_directory, f"{normalized_object}_attachments_errors.csv")
                        )
                        logger.info(f"Generated attachment filenames: {attachment_success_filename}, {attachment_error_filename}")

            # Initialize CSV files with headers for non-export operations
            def write_csv_records(filename, headers, records=None, append=False):
                mode = 'a' if append else 'w'
                with open(filename, mode, newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, fieldnames=headers)
                    if not append:
                        writer.writeheader()
                    if records:
                        writer.writerows(records)

            if self.action_type != "export":
                ticket_success_headers = self.csv_headers + ["ID", "Status"] if self.action_type == "import" else self.csv_headers + ["Status", "ID - preserved"]
                ticket_error_headers = self.csv_headers + ["Error Message"]
                if self.selected_object == "Tickets + Comments + Attachments":
                    attachment_success_headers = [self.comment_ticket_id_field, "comment_id", "file_name", "upload_token"]
                    attachment_error_headers = [self.comment_ticket_id_field, "comment_id", "file_name", "error_message"]
                    comment_success_headers = self.comments_csv_headers + ["html_body", "Status"] if "html_body" not in self.comments_csv_headers else self.comments_csv_headers + ["Status"]
                    comment_error_headers = self.comments_csv_headers + ["html_body", "Error Message"] if "html_body" not in self.comments_csv_headers else self.comments_csv_headers + ["Error Message"]
                    if success_filename:
                        write_csv_records(success_filename, ticket_success_headers)
                    if error_filename:
                        write_csv_records(error_filename, ticket_error_headers)
                    if attachment_success_filename:
                        write_csv_records(attachment_success_filename, attachment_success_headers)
                    if attachment_error_filename:
                        write_csv_records(attachment_error_filename, attachment_error_headers)
                    if comment_success_filename:
                        write_csv_records(comment_success_filename, comment_success_headers)
                    if comment_error_filename:
                        write_csv_records(comment_error_filename, comment_error_headers)
                else:
                    attachment_success_headers = [self.comment_ticket_id_field, "comment_id", "file_name", "upload_token"] if self.selected_object == "Tickets + Comments" else []
                    attachment_error_headers = [self.comment_ticket_id_field, "comment_id", "file_name", "error_message"] if self.selected_object == "Tickets + Comments" else []
                    if success_filename:
                        write_csv_records(success_filename, ticket_success_headers)
                    if error_filename:
                        write_csv_records(error_filename, ticket_error_headers)
                    if self.selected_object == "Tickets + Comments":
                        if attachment_success_filename:
                            write_csv_records(attachment_success_filename, attachment_success_headers)
                        if attachment_error_filename:
                            write_csv_records(attachment_error_filename, attachment_error_headers)

            # Initialize progress tracking
            total_records = 1
            if self.action_type != "export":
                if self.csv_file_path and os.path.exists(self.csv_file_path):
                    total_records = sum(1 for _ in open(self.csv_file_path, 'r', encoding='utf-8-sig')) - 1
                total_records = max(total_records, 1)
            else:
                from zendesk_bulk import fetch_total_records, get_total_records
                normalized_object_export = self.selected_object
                if self.selected_object == "Triggers":
                    normalized_object_export = "Trigger"
                fetch_total_records(self.subdomain, self.email, self.token, normalized_object_export)
                total_records = get_total_records() or 1
                if self.selected_object == "Comments":
                    total_records *= 10
                logger.info(f"Estimated total records for export: {total_records}")

            logger.info(f"ProgressManager initializing with {total_records} records")
            self.progress_manager = ProgressManager(self, total_records, update_interval=200)
            self.progress_manager.start()

            success_count = 0
            error_count = 0
            skipped_count = 0
            exported_count = 0
            total_processed = 0
            processed_records = 0  # Cumulative processed records across chunks

            if self.action_type == "export":
                result = execute_bulk_operation(
                    subdomain=self.subdomain,
                    email=self.email,
                    token=self.token,
                    selected_object=self.selected_object,
                    csv_data=self.csv_data,
                    csv_headers=self.csv_headers,
                    field_mapping=self.field_mapping,
                    action_type=self.action_type,
                    save_directory=self.save_directory,
                    stop_event=self.stop_event,
                    app_instance=self,
                    success_filename=success_filename,
                    error_filename=error_filename
                )
                _, _, exported_count, total_processed = result
                message = f"Export completed successfully!\nExported: {exported_count}"
                logger.info(message)
                self.safe_update_progress_bar(100, total_records)
                return {"success": True, "message": message}
            else:
                comments_index = {}
                attachments_index = {}
                html_body_column = None
                if self.selected_object in ["Tickets + Comments", "Tickets + Comments + Attachments"]:
                    comments_index, attachments_index = self.build_csv_indices()
                    for csv_field, zendesk_field in self.comment_field_mapping.items():
                        if zendesk_field == "html_body" and csv_field in self.comments_csv_headers:
                            html_body_column = csv_field
                            break

                chunk_size = 1000

                def read_csv_chunks(file_path, headers, chunk_size):
                    if not file_path or not os.path.exists(file_path):
                        logger.warning(f"File path {file_path} does not exist")
                        return
                    for chunk in pd.read_csv(file_path, chunksize=chunk_size, encoding='utf-8-sig'):
                        chunk = chunk.replace({np.nan: None})
                        logger.debug(f"Read chunk with {len(chunk)} rows, headers: {chunk.columns.tolist()}")
                        yield chunk.values.tolist()

                ticket_chunks = read_csv_chunks(self.csv_file_path, self.csv_headers, chunk_size) if self.csv_file_path else []

                for ticket_chunk in ticket_chunks:
                    if self.stop_event.is_set():
                        logger.info("Stop event detected, aborting operation")
                        break
                    comments_data = []
                    comment_row_numbers = []
                    attachments_data = []
                    ticket_id_idx = self.csv_headers.index(self.ticket_id_field) if self.ticket_id_field in self.csv_headers else None

                    if self.selected_object in ["Tickets + Comments", "Tickets + Comments + Attachments"] and ticket_id_idx is not None:
                        for ticket_row in ticket_chunk:
                            ticket_id = str(ticket_row[ticket_id_idx]).strip() if ticket_row[ticket_id_idx] else None
                            if ticket_id and ticket_id in comments_index:
                                for comment_id, comments in comments_index[ticket_id].items():
                                    for comment in comments:
                                        comments_data.append(comment["row"])
                                        comment_row_numbers.append(comment["row_number"])
                                        if self.selected_object == "Tickets + Comments + Attachments" and comment["file_name"]:
                                            attachment_row = {self.attachment_comment_id_field: comment_id}
                                            for csv_field, zendesk_field in self.attachment_field_mapping.items():
                                                if zendesk_field == "file_name":
                                                    attachment_row[csv_field] = comment["file_name"]
                                            attachments_data.append([attachment_row.get(h, "") for h in self.comments_csv_headers])
                                            logger.debug(f"Added attachment: ticket_id={ticket_id}, comment_id={comment_id}, file_name={comment['file_name']}")

                    result = execute_bulk_operation(
                        subdomain=self.subdomain,
                        email=self.email,
                        token=self.token,
                        selected_object=self.selected_object,
                        csv_data=ticket_chunk,
                        csv_headers=self.csv_headers,
                        field_mapping=self.field_mapping,
                        action_type=self.action_type,
                        save_directory=self.save_directory,
                        stop_event=self.stop_event,
                        comments_csv_data=comments_data,
                        comments_csv_headers=self.comments_csv_headers,
                        ticket_id_field=self.ticket_id_field,
                        comment_ticket_id_field=self.comment_ticket_id_field,
                        comment_field_mapping=self.comment_field_mapping,
                        app_instance=self,
                        attachments_csv_data=attachments_data if self.selected_object == "Tickets + Comments + Attachments" else [],
                        attachments_csv_headers=self.comments_csv_headers if self.selected_object == "Tickets + Comments + Attachments" else [],
                        attachment_comment_id_field=self.attachment_comment_id_field,
                        base_file_dir=self.attachment_directory if self.selected_object == "Tickets + Comments + Attachments" else None,
                        attachment_field_mapping=self.attachment_field_mapping if self.selected_object == "Tickets + Comments + Attachments" else {},
                        chunk_size=100,
                        comment_row_numbers=comment_row_numbers,
                        html_body_column=html_body_column,
                        success_filename=success_filename,
                        error_filename=error_filename,
                        attachment_success_filename=attachment_success_filename,
                        attachment_error_filename=attachment_error_filename,
                        comment_success_filename=comment_success_filename,
                        comment_error_filename=comment_error_filename,
                        processed_records_so_far=processed_records
                    )
                    logger.debug(f"execute_bulk_operation returned: {result} (length: {len(result)})")
                    if len(result) >= 4:
                        chunk_success, chunk_error, chunk_skipped, chunk_processed = result[:4]
                    else:
                        logger.error(f"execute_bulk_operation returned too few values: {result}")
                        chunk_success, chunk_error, chunk_skipped, chunk_processed = 0, 0, 0, 0
                    success_count += chunk_success
                    error_count += chunk_error
                    skipped_count += chunk_skipped
                    processed_records += chunk_processed  # Accumulate processed records
                    total_processed += chunk_processed
                    logger.debug(f"Chunk processed: {chunk_processed}, Cumulative processed: {processed_records}/{total_records}")
                    # Update progress bar with cumulative progress
                    self.safe_update_progress_bar((processed_records / total_records) * 100, processed_records)

                # Final progress update to reflect actual processed records
                if not self.stop_event.is_set():
                    logger.info(f"Operation completed, final processed records: {processed_records}/{total_records}")
                    self.safe_update_progress_bar((processed_records / total_records) * 100, processed_records)

                message = f"Operation completed successfully!\nSuccesses: {success_count}\nFailures: {error_count}"
                response_dict = {"success": True, "message": message}

                # Remove duplicates from output files
                if self.selected_object == "Tickets + Comments + Attachments":
                    if success_filename and self.csv_api.remove_duplicates_from_csv(success_filename)["success"]:
                        logger.info(f"Duplicates removed from {success_filename}")
                    if error_filename and self.csv_api.remove_duplicates_from_csv(error_filename)["success"]:
                        logger.info(f"Duplicates removed from {error_filename}")
                    if attachment_success_filename and self.csv_api.remove_duplicates_from_csv(attachment_success_filename)["success"]:
                        logger.info(f"Duplicates removed from {attachment_success_filename}")
                    if attachment_error_filename and self.csv_api.remove_duplicates_from_csv(attachment_error_filename)["success"]:
                        logger.info(f"Duplicates removed from {attachment_error_filename}")
                    if comment_success_filename and self.csv_api.remove_duplicates_from_csv(comment_success_filename)["success"]:
                        logger.info(f"Duplicates removed from {comment_success_filename}")
                    if comment_error_filename and self.csv_api.remove_duplicates_from_csv(comment_error_filename)["success"]:
                        logger.info(f"Duplicates removed from {comment_error_filename}")
                else:
                    if success_filename and self.csv_api.remove_duplicates_from_csv(success_filename)["success"]:
                        logger.info(f"Duplicates removed from {success_filename}")
                    if error_filename and self.csv_api.remove_duplicates_from_csv(error_filename)["success"]:
                        logger.info(f"Duplicates removed from {error_filename}")
                    if self.selected_object == "Tickets + Comments":
                        if attachment_success_filename and self.csv_api.remove_duplicates_from_csv(attachment_success_filename)["success"]:
                            logger.info(f"Duplicates removed from {attachment_success_filename}")
                        if attachment_error_filename and self.csv_api.remove_duplicates_from_csv(attachment_error_filename)["success"]:
                            logger.info(f"Duplicates removed from {attachment_error_filename}")

                logger.info(message)
                return response_dict

        except Exception as e:
            self.stop_event.set()
            error_message = str(e)
            logger.error(f"Error in execute_import: {error_message}")
            return {"success": False, "message": error_message}

        finally:
            self.operation_running = False
            self.stop_event.clear()
            if hasattr(self, 'progress_manager') and self.progress_manager is not None:
                logger.info("Cleaning up ProgressManager in execute_import")
                self.progress_manager.stop(stopped_by_user=self.stop_event.is_set())






    def stop_operation(self):
        logger.debug("stop_operation called")
        try:
            logger.info("🛑 Stop button pressed. Attempting to halt operations.")
            self.stop_event.set()
            if self.progress_manager:
                self.progress_manager.stop(stopped_by_user=True)
            return {"success": True, "message": "Operation stop requested"}
        except Exception as e:
            logger.error(f"Error in stop_operation: {str(e)}")
            return {"success": False, "message": str(e)}

    def load_credentials(self):
        logger.debug("load_credentials called")
        try:
            credentials = self.auth_api.load_credentials()
            safe_credentials = credentials.copy()
            if safe_credentials["api_token"]:
                safe_credentials["api_token"] = "****"
            logger.info(f"Sending credentials to frontend: {safe_credentials}")
            return credentials
        except Exception as e:
            logger.error(f"Error in load_credentials: {str(e)}")
            return {"success": False, "message": str(e)}

    def shutdown(self):
        logger.debug("shutdown called")
        try:
            if not self.save_credentials:
                self.auth_api.clear_credentials()
            logger.info("App shutdown completed")
        except Exception as e:
            logger.error(f"Error in shutdown: {str(e)}")

def start_app():
    logger.debug("Starting application")
    try:
        # Verify critical files exist
        ui_path = "ui/login.html"
        if not os.path.exists(ui_path):
            logger.error(f"UI file not found: {ui_path}")
            raise FileNotFoundError(f"UI file not found: {ui_path}")

        api = AppAPI()
        window = webview.create_window(
            "Zendesk Data Loader",
            ui_path,
            js_api=api,
            width=625,
            height=720
        )
        def on_closed():
            logger.debug("Window closed, shutting down")
            api.shutdown()
        window.events.closed += on_closed
        logger.info("Starting pywebview with debug mode")
        webview.start(debug=False)
        logger.info("Application started successfully")
    except Exception as e:
        logger.error(f"Failed to start application: {str(e)}")
        logger.error(traceback.format_exc())
        raise

if __name__ == "__main__":
    logger.debug("Main script execution started")
    try:
        start_app()
    except Exception as e:
        logger.error(f"Main script failed: {str(e)}")
        logger.error(traceback.format_exc())
        sys.exit(1)