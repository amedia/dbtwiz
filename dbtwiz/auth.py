import json
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from dbtwiz.interact import confirm

from .config import user_config
from .logging import fatal, warn

CREDENTIALS_JSON = Path("gcloud", "application_default_credentials.json")


def ensure_app_default_auth() -> None:
    """Ensures application-default authorization is active."""
    if not user_config().getboolean("general", "auth_check"):
        return

    if (
        "APPDATA" in os.environ
        and Path(os.environ["APPDATA"], CREDENTIALS_JSON).exists()
    ):
        credentials_file = Path(os.environ["APPDATA"], CREDENTIALS_JSON)
    else:
        credentials_file = Path.home() / ".config" / CREDENTIALS_JSON

    if credentials_file.exists():
        expiry = datetime.fromtimestamp(credentials_file.stat().st_mtime) + timedelta(
            hours=18
        )
        remaining = expiry - datetime.now()
        if remaining.total_seconds() > 0:
            if remaining >= timedelta(minutes=5):
                # debug(f"GCP credentials seem to be valid until {expiry.strftime('%H:%M:%S')}.")
                return
            else:
                warn("GCP application-default credentials seem to expire within the next five minutes.")
        else:
            warn(
                f"GCP application-default credentials seem to have expired at {expiry.strftime('%Y-%m-%d %H:%M:%S')}."
            )
    else:
        warn("No GCP application-default authentication credentials found.")

    if confirm("Do you wish to reauthenticate now?"):
        subprocess.run("gcloud auth application-default login", shell=True)


def ensure_gcloud_auth() -> None:
    """Ensures gcloud authorization is active."""
    allowed_domains = ["amedia.no"]
    # Get list of authenticated accounts
    result = subprocess.run(
        "gcloud auth list --format=json",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        shell=True,
    )
    if result.returncode != 0:
        fatal(
            "Error checking gcloud authentication. Ensure gcloud is installed correctly."
        )

    # Parse the output and check if there are any accounts
    accounts = json.loads(result.stdout.strip())
    if not accounts or len(accounts) == 0:
        warn("No GCP accounts authenticated.")

    # Check if an allowed domain account is authenticated
    for account in accounts:
        if account.get("status") == "ACTIVE" and any(
            account.get("account").lower().endswith(f"@{domain}") for domain in allowed_domains
        ):
            return
    warn("No valid GCP account is authenticated.")

    if confirm("Do you wish to reauthenticate now?"):
        subprocess.run("gcloud auth login", check=True, shell=True)


def ensure_auth(
    check_app_default_auth: bool = True,
    check_gcloud_auth: bool = False,
) -> None:
    """Main authentication function."""
    if check_app_default_auth:
        ensure_app_default_auth()
    if check_gcloud_auth:
        ensure_gcloud_auth()
