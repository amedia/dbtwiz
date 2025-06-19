import shutil
import subprocess

from google.auth import default
from google.auth.exceptions import DefaultCredentialsError, RefreshError
from google.auth.transport import requests

from dbtwiz.config.project import project_config
from dbtwiz.config.user import user_config
from dbtwiz.helpers.logger import fatal, warn
from dbtwiz.ui.interact import confirm


def check_gcloud_installed():
    """Check if gcloud CLI is installed."""
    return shutil.which("gcloud") is not None


def app_default_auth_login():
    """Triggers application default authorization and sets quota project."""
    if confirm("Do you wish to reauthenticate now?"):
        subprocess.run("gcloud auth application-default login", shell=True)
        if project_config().user_project:
            subprocess.run(
                f"gcloud auth application-default set-quota-project {project_config().user_project}",
                shell=True,
            )


def ensure_app_default_auth() -> None:
    """Ensures application-default authorization is active."""
    if not user_config().auth_check:
        return

    if not check_gcloud_installed():
        fatal(
            "Error checking gcloud authentication. Ensure gcloud is installed correctly."
        )

    try:
        credentials, _ = default()

        if not credentials.valid:
            if hasattr(credentials, "refresh"):
                # Attempt to refresh credentials
                request = requests.Request()
                credentials.refresh(request)
            else:
                app_default_auth_login()
    except (DefaultCredentialsError, RefreshError) as e:
        warn(str(e))
        app_default_auth_login()


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
        or "There was a problem refreshing your current auth tokens" in result.stderr
    ):
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
