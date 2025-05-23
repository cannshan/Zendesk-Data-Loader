from zendesk_bulk import execute_bulk_operation

def execute_update(subdomain, email, token, selected_object, csv_data, csv_headers, field_mapping, update_progress=None):
    """
    Executes the update operation by calling the bulk operation function.
    """
    execute_bulk_operation(
        subdomain=subdomain,
        email=email,
        token=token,
        selected_object=selected_object,
        csv_data=csv_data,
        csv_headers=csv_headers,
        field_mapping=field_mapping,
        action_type="update",
        update_progress=update_progress  # Pass the progress callback
    )