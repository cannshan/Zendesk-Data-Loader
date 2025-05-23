import keyring
import requests

SERVICE_NAME = "ZendeskDataLoader"

class API:
    def authenticate(self, credentials):
        subdomain = credentials.get("subdomain", "").strip()
        email = credentials.get("email", "").strip()
        api_token = credentials.get("api_token", "").strip()
        save_credentials = credentials.get("save_credentials", False)

        if not subdomain or not email or not api_token:
            return {"success": False, "message": "All fields are required."}

        zendesk_url = f"https://{subdomain}.zendesk.com/api/v2/users/me.json"
        headers = {
            "Authorization": f"Basic {requests.auth._basic_auth_str(email + '/token', api_token)}",
            "Content-Type": "application/json"
        }

        try:
            response = requests.get(zendesk_url, headers=headers)
            if response.status_code == 200:
                if save_credentials:
                    self.save_credentials(subdomain, email, api_token)
                else:
                    self.clear_credentials()  # Clear credentials if not saving
                return {"success": True, "message": "Authenticated successfully."}
            else:
                return {"success": False, "message": f"Authentication failed: {response.status_code}"}
        except requests.exceptions.RequestException as e:
            return {"success": False, "message": f"Error: {str(e)}"}

    def save_credentials(self, subdomain, email, api_token):
        """Saves credentials to the system keychain."""
        try:
            keyring.set_password(SERVICE_NAME, "subdomain", subdomain)
            keyring.set_password(SERVICE_NAME, "email", email)
            keyring.set_password(SERVICE_NAME, "api_token", api_token)
            print("Credentials saved securely to keychain.")
        except keyring.errors.KeyringError as e:
            print(f"Failed to save credentials to keychain: {e}")
            raise Exception(f"Failed to save credentials: {e}")

    def load_credentials(self):
        """Loads credentials from the system keychain."""
        try:
            subdomain = keyring.get_password(SERVICE_NAME, "subdomain") or ""
            email = keyring.get_password(SERVICE_NAME, "email") or ""
            api_token = keyring.get_password(SERVICE_NAME, "api_token") or ""
            credentials = {
                "subdomain": subdomain,
                "email": email,
                "api_token": api_token
            }
            print("Loaded credentials from keychain:", {
                "subdomain": credentials["subdomain"],
                "email": credentials["email"],
                "api_token": "****" if credentials["api_token"] else ""
            })
            return credentials
        except keyring.errors.KeyringError as e:
            print(f"Failed to load credentials from keychain: {e}")
            return {"subdomain": "", "email": "", "api_token": ""}

    def clear_credentials(self):
        """Clears credentials from the system keychain."""
        try:
            keyring.delete_password(SERVICE_NAME, "subdomain")
            keyring.delete_password(SERVICE_NAME, "email")
            keyring.delete_password(SERVICE_NAME, "api_token")
            print("Credentials cleared from keychain.")
        except keyring.errors.NoPasswordError:
            print("No credentials found to clear.")
        except keyring.errors.KeyringError as e:
            print(f"Failed to clear credentials: {e}")