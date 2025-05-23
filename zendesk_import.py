from zendesk_bulk import execute_bulk_operation
from globals import logger


def execute_import(subdomain, email, token, selected_object, csv_data, csv_headers, field_mapping, update_progress=None):

    execute_bulk_operation(
        subdomain=subdomain,
        email=email,
        token=token,
        selected_object=selected_object,
        csv_data=csv_data,
        csv_headers=csv_headers,
        field_mapping=field_mapping,
        action_type="import",
        update_progress=update_progress  # Pass the progress callback
    )