from zendesk_bulk import execute_bulk_operation

def execute_export(subdomain, email, token, selected_object, csv_data, csv_headers, field_mapping, update_progress=None):

    execute_bulk_operation(
        subdomain=subdomain,
        email=email,
        token=token,
        selected_object=selected_object,
        csv_data=csv_data,
        csv_headers=csv_headers,
        field_mapping=field_mapping,
        action_type="export",
        update_progress=update_progress,
        save_directory=save_directory
    )
