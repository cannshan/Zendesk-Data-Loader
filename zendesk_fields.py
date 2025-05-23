import requests
import logging

logging.basicConfig(filename="app.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def fetch_zendesk_fields(subdomain, email, token, selected_object, action_type):
    """
    Fetch Zendesk fields for a given object type, separating standard and custom fields.
    Returns a tuple: (fields, custom_field_info)
    - fields: List of standard field API names (e.g., 'id', 'title', or schema-defined fields for custom objects)
    - custom_field_info: List of dictionaries with custom field details (id, name, type, display_name)
    """
    standard_fields = {
        "User": [
            "ID", "External ID", "Name", "Email", "Created At", "Details", "Phone", "Tags", "Locale", "Organization ID"
        ],
        "Users": [
            "ID", "External ID", "Name", "Email", "Created At", "Details", "Phone", "Tags", "Locale", "Organization ID"
        ],
        "Agent": [
            "ID", "External ID", "Name", "Email", "Created At", "Details", "Phone", "Tags", "Locale", "Organization ID",
            "Custom Role ID", "Ticket Restriction", "Suspended", "Signature", "Notes", "Default Group ID", "Groups", "Alias", "Agent Brand IDs"
        ],
        "Agents": [
            "ID", "External ID", "Name", "Email", "Created At", "Details", "Phone", "Tags", "Locale", "Organization ID",
            "Custom Role ID", "Ticket Restriction", "Suspended", "Signature", "Notes", "Default Group ID", "Groups", "Alias", "Agent Brand IDs"
        ],
        "Organization": [
            "ID", "External ID", "Name", "Domain Names", "Details", "Notes", "Group ID", "Tags", "Created At"
        ],
        "Organizations": [
            "ID", "External ID", "Name", "Domain Names", "Details", "Notes", "Group ID", "Tags", "Created At"
        ],
        "Ticket": [
            "ID", "Subject", "External_ID", "Description", "Tags", "Created At", "Priority", "Type", "Organization ID",
            "Requester ID", "Assignee ID", "Group ID", "Status", "Solved At", "Follower IDs", "Collaborator IDs"
        ],
        "Tickets": [
            "ID", "Subject", "External_ID", "Description", "Tags", "Created At", "Priority", "Type", "Organization ID",
            "Requester ID", "Assignee ID", "Group ID", "Status", "Solved At", "Follower IDs", "Collaborator IDs"
        ],
        "Ticket Comments": [
            "ticket_id", "event_id", "author_id", "body", "html_body", "public", "created_at", "via_channel",
            "via_source_from", "via_source_to"
        ],
        "Attachments": ["file_name"],
        "Trigger": [
            "id", "title", "active", "position", "conditions_all", "conditions_any", "actions", "created_at", "updated_at"
        ],
        "Triggers": [
            "id", "title", "active", "position", "conditions_all", "conditions_any", "actions", "created_at", "updated_at"
        ],
        "Macros": [
            "ID", "Title", "Active", "Actions", "Created At", "Updated At", "Position", "Restriction"
        ]
    }

    object_field_keys = {
        "User": "user_fields", "Users": "user_fields", "Agent": "user_fields",
        "Organization": "organization_fields", "Organizations": "organization_fields",
        "Ticket": "ticket_fields", "Tickets": "ticket_fields",
        "Ticket Comments": None,
        "Attachments": None,
        "Trigger": None,
        "Triggers": None,
        "Macros": None
    }

    field_endpoints = {
        "User": "/api/v2/user_fields.json", "Users": "/api/v2/user_fields.json", "Agent": "/api/v2/user_fields.json",
        "Organization": "/api/v2/organization_fields.json", "Organizations": "/api/v2/organization_fields.json",
        "Ticket": "/api/v2/ticket_fields.json", "Tickets": "/api/v2/ticket_fields.json",
        "Ticket Comments": None,
        "Attachments": None,
        "Trigger": None,
        "Triggers": None,
        "Macros": None
    }

    base_url = f"https://{subdomain}.zendesk.com"
    auth = (f"{email}/token", token)
    fields = []
    custom_field_info = []

    system_field_titles = {
        "subject", "description", "priority", "status", "type", "tags",
        "group", "assignee", "organization", "requester", "collaborator",
        "external id", "created at", "solved at", "follower_ids", "collaborator_ids"
    }

    # Normalize selected_object
    normalized_object = selected_object
    if selected_object in ["Tickets + Comments", "Tickets + Comments + Attachments"]:
        normalized_object = "Tickets"
        logging.info(f"Normalized selected_object from {selected_object} to {normalized_object}")
    elif selected_object == "Triggers":
        normalized_object = "Trigger"
        logging.info(f"Normalized selected_object from {selected_object} to {normalized_object}")
    elif selected_object == "Macros":
        normalized_object = "Macros"
        logging.info(f"Normalized selected_object to {normalized_object}")

    builtin_objects = ["User", "Users", "Agent", "Agents", "Organization", "Organizations", "Ticket", "Tickets", "Ticket Comments", "Attachments", "Trigger", "Macros"]
    is_custom_object = normalized_object not in builtin_objects

    if is_custom_object:
        schema_endpoint = f"{base_url}/api/v2/custom_objects/{normalized_object}"
        logging.info(f"Fetching custom object schema from: {schema_endpoint}")
        try:
            schema_response = requests.get(schema_endpoint, auth=auth)
            schema_response.raise_for_status()
            schema_data = schema_response.json().get("custom_object", {})
            required_fields = schema_data.get("required_fields", [])
            standard_fields_list = ["id"]
            if "external_id" in schema_data.get("fields", []):
                standard_fields_list.append("external_id")
            for field in required_fields:
                field_key = field.get("key", "").lower().replace(" ", "_")
                if field_key and field_key not in standard_fields_list:
                    standard_fields_list.append(field_key)
            fields = standard_fields_list
            logging.info(f"Standard fields from schema for {normalized_object}: {fields}")
        except requests.exceptions.RequestException as e:
            logging.warning(f"Failed to fetch custom object schema for {normalized_object}: {e}")
            fields = ["id"]
            logging.info(f"Falling back to minimal standard fields: {fields}")

        custom_field_endpoint = f"{base_url}/api/v2/custom_objects/{normalized_object}/fields"
        logging.info(f"Fetching custom object fields from: {custom_field_endpoint}")
        try:
            response = requests.get(custom_field_endpoint, auth=auth)
            response.raise_for_status()
            data = response.json()
            custom_field_info = [
                {
                    "name": field["key"].lower().replace(" ", "_"),
                    "id": field["id"],
                    "key": field["key"],
                    "type": field["type"],
                    "display_name": field.get("title", field["key"])
                }
                for field in data.get("custom_object_fields", [])
                if "key" in field and "type" in field and "id" in field
            ]
            fields.extend([cf["name"] for cf in custom_field_info])
            logging.info(f"Custom object fields fetched: {fields}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch custom object fields for {normalized_object}: {e}")
            custom_field_info = []
    else:
        # Retrieve standard fields for the normalized object
        fields = [field.lower().replace(" ", "_") for field in standard_fields.get(normalized_object, [])]
        logging.info(f"Initial standard fields for {normalized_object}: {fields}")

        if normalized_object == "Attachments":
            logging.info("Attachments selected: restricting fields to 'file_name'")
        elif normalized_object in ["Trigger", "Macros"]:
            logging.info(f"{normalized_object} selected: using standard fields only, no custom fields")
        elif normalized_object in field_endpoints and field_endpoints[normalized_object]:
            url = f"{base_url}{field_endpoints[normalized_object]}"
            logging.info(f"Fetching fields for {normalized_object} from: {url}")
            try:
                response = requests.get(url, auth=auth)
                response.raise_for_status()
                data = response.json()
                field_key = object_field_keys[normalized_object]
                if field_key in data:
                    if normalized_object in ["Ticket", "Tickets"]:
                        custom_field_info = [
                            {
                                "name": f"custom_field_{field['title'].lower().replace(' ', '_')}",
                                "id": field["id"],
                                "key": field.get("key"),
                                "type": field["type"],
                                "display_name": field.get("title", f"Custom Field {field['id']}")
                            }
                            for field in data[field_key]
                            if "title" in field and "type" in field and "id" in field
                            and field["title"].lower() not in system_field_titles
                            and field["type"] not in ["subject", "description", "priority", "status"]
                        ]
                    else:
                        custom_field_info = [
                            {
                                "name": field.get("key", field["title"].lower().replace(" ", "_")),
                                "id": field["id"],
                                "key": field.get("key"),
                                "type": field["type"],
                                "display_name": (
                                    field.get("raw_title", field.get("title", f"Custom Field {field['id']}"))
                                    if normalized_object in ["User", "Users", "Agent", "Agents", "Organization", "Organizations"]
                                    else field.get("title", f"Custom Field {field['id']}")
                                )
                            }
                            for field in data[field_key]
                            if "title" in field and "type" in field and "id" in field
                        ]
                    extracted_fields = [field["name"] for field in custom_field_info]
                    fields.extend(extracted_fields)
                    logging.info(f"Custom fields fetched: {extracted_fields}")
                else:
                    logging.warning(f"No custom fields found in response for {normalized_object}")
            except requests.exceptions.RequestException as e:
                logging.error(f"Failed to fetch fields for {normalized_object}: {e}")
                logging.info(f"Returning standard fields only: {fields}")
        elif normalized_object in ["Ticket Comments", "Attachments", "Trigger", "Macros"]:
            logging.info(f"No custom field endpoint for {normalized_object}; using standard fields only")

    # Adjust fields based on action type
    if action_type == "import":
        fields = [field for field in fields if field.lower() != "id"]
    elif action_type == "delete":
        fields = ["id"]
    # For "update" and "export", keep "id" in the list - no filtering needed

    # Sort fields for consistency
    standard_fields_list = [field for field in fields if not field.startswith("custom_field_")]
    custom_fields = [field for field in fields if field.startswith("custom_field_")]
    standard_fields_list.sort()
    custom_fields.sort()
    fields = standard_fields_list + custom_fields

    # Log the final fields returned
    logging.info(f"Returning fields for {normalized_object}: {fields}")
    logging.info(f"Returning custom_field_info: {[cf['name'] for cf in custom_field_info]}")

    return fields, custom_field_info

def fetch_custom_objects(subdomain, email, token):
    """Fetch all custom objects from Zendesk."""
    base_url = f"https://{subdomain}.zendesk.com"
    auth = (f"{email}/token", token)
    url = f"{base_url}/api/v2/custom_objects"
    
    try:
        response = requests.get(url, auth=auth)
        response.raise_for_status()
        custom_objects = response.json().get("custom_objects", [])
        return [obj["key"] for obj in custom_objects]
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch custom objects: {e}")
        return []