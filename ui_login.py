import tkinter as tk
from tkinter import messagebox
from auth import authenticate_user, save_credentials_to_env, load_credentials
from ui_action import create_action_selection_page

def create_login_page(root, app_state):
    """Creates the login page where users enter their Zendesk credentials."""
    clear_window(root)
    tk.Label(root, text="Enter Zendesk Credentials", font=("Arial", 16)).pack(pady=20)

    tk.Label(root, text="Subdomain").pack()
    app_state["subdomain_entry"] = tk.Entry(root)
    app_state["subdomain_entry"].pack(pady=5)

    tk.Label(root, text="Email").pack()
    app_state["email_entry"] = tk.Entry(root)
    app_state["email_entry"].pack(pady=5)

    tk.Label(root, text="API Token").pack()
    app_state["token_entry"] = tk.Entry(root, show="*")
    app_state["token_entry"].pack(pady=5)

    app_state["save_credentials_var"] = tk.BooleanVar()
    tk.Checkbutton(root, text="Save Credentials", variable=app_state["save_credentials_var"]).pack(pady=5)

    tk.Button(root, text="Login", command=lambda: authenticate_user_ui(root, app_state)).pack(pady=20)

    saved_credentials = load_credentials()
    if saved_credentials:
        app_state["subdomain_entry"].insert(0, saved_credentials.get("subdomain", ""))
        app_state["email_entry"].insert(0, saved_credentials.get("email", ""))
        app_state["token_entry"].insert(0, saved_credentials.get("token", ""))
        app_state["save_credentials_var"].set(True)

def authenticate_user_ui(root, app_state):
    """Handles user authentication."""
    subdomain = app_state["subdomain_entry"].get().strip()
    email = app_state["email_entry"].get().strip()
    token = app_state["token_entry"].get().strip()

    if authenticate_user(subdomain, email, token):
        app_state.update({"subdomain": subdomain, "email": email, "token": token})
        if app_state["save_credentials_var"].get():
            save_credentials_to_env(subdomain, email, token)
        messagebox.showinfo("Success", "Authentication successful!")
        create_action_selection_page(root, app_state)
