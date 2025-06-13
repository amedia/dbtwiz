import os
import shutil
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from dbtwiz.config.project import project_config
from dbtwiz.config.user import user_config
from dbtwiz.helpers.logger import fatal, warn
from dbtwiz.ui.interact import confirm

CREDENTIALS_JSON = Path("gcloud", "application_default_credentials.json")


def ensure_app_default_auth() -> None:
    """Ensures application-default authorization is active."""
    if not user_config().auth_check:
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
                warn(
                    "GCP application-default credentials seem to expire within the next five minutes."
                )
        else:
            warn(
                f"GCP application-default credentials seem to have expired at {expiry.strftime('%Y-%m-%d %H:%M:%S')}."
            )
    else:
        warn("No GCP application-default authentication credentials found.")

    if confirm("Do you wish to reauthenticate now?"):
        subprocess.run("gcloud auth application-default login", shell=True)


def check_gcloud_installed():
    """Check if gcloud CLI is installed."""
    return shutil.which("gcloud") is not None


def ensure_gcloud_auth() -> None:
    """Ensures gcloud authorization is active."""
    if not check_gcloud_installed():
        fatal(
            "Error checking gcloud authentication. Ensure gcloud is installed correctly."
        )

    # Check access token
    result = subprocess.run(
        "gcloud auth print-access-token",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        shell=True,
    )

    if (
        "Reauthentication required" in result.stderr
        or "There was a problem refreshing your current auth tokens" in result.stderr):
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
