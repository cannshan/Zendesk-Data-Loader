from zendesk_bulk import execute_bulk_operation
from tkinter import messagebox

def execute_delete(subdomain, email, token, selected_object, csv_data, csv_headers, field_mapping, update_progress=None):
    """
    Executes the delete operation by calling the bulk operation function.

    Args:
        subdomain (str): The Zendesk subdomain.
        email (str): The Zendesk email address.
        token (str): The Zendesk API token.
        selected_object (str): The object to delete (e.g., "Users", "Organizations", "Tickets").
        csv_data (list of lists): The data loaded from the CSV file.
        csv_headers (list): The headers from the CSV file.
        field_mapping (dict): A dictionary mapping CSV fields to Zendesk fields.
        update_progress (function, optional): A callback function to update the progress bar.
    """

    # Ensure field mapping extracts selected values, not widget objects
    resolved_field_mapping = {csv_field: combobox.get().lower() for csv_field, combobox in field_mapping.items()}  

    # Identify the mapped ID or External ID field
    mapped_id_field = None
    for csv_field, zendesk_field in resolved_field_mapping.items():
        if zendesk_field in ["id", "external_id"]:  # Allow either ID or External ID
            mapped_id_field = csv_field
            break

    if not mapped_id_field:
        messagebox.showerror(
            "Error", 
            "No valid ID field mapped for deletion. Ensure a CSV field is mapped to either 'id' or 'external_id' in Zendesk."
        )
        return

    # Get the index of the mapped ID field in the CSV headers
    try:
        id_index = csv_headers.index(mapped_id_field)
    except ValueError:
        messagebox.showerror("Error", f"Mapped field '{mapped_id_field}' not found in CSV headers.")
        return

    # Filter out rows without an ID value
    filtered_csv_data = [row for row in csv_data if row[id_index].strip()]  # Ensure it's not empty

    if not filtered_csv_data:
        messagebox.showerror(
            "Error", 
            f"No valid records found with a mapped {mapped_id_field}. Please check your CSV and field mapping."
        )
        return

    # Execute the delete operation using bulk API
    execute_bulk_operation(
        subdomain=subdomain,
        email=email,
        token=token,
        selected_object=selected_object,
        csv_data=filtered_csv_data,
        csv_headers=csv_headers,
        field_mapping=field_mapping,
        action_type="delete",
        update_progress=update_progress  # Pass the progress callback
    )
