from datetime import datetime, timedelta
from pathlib import Path
import os
import subprocess
import time

from .logging import info, warn
from .config import user_config


CREDENTIALS_JSON = Path("gcloud", "application_default_credentials.json")


def ensure_auth():
    if not user_config().getboolean("general", "auth_check"):
        return

    if "APPDATA" in os.environ and Path(os.environ["APPDATA"], CREDENTIALS_JSON).exists():
        credentials_file = Path(os.environ["APPDATA"], CREDENTIALS_JSON)
    else:
        credentials_file = Path.home() / ".config" / CREDENTIALS_JSON

    if credentials_file.exists():
        expiry = datetime.fromtimestamp(credentials_file.stat().st_mtime) + timedelta(hours=18)
        remaining = expiry - datetime.now()
        if remaining.total_seconds() > 0:
            if remaining >= timedelta(minutes=5):
                # debug(f"GCP credentials seem to be valid until {expiry.strftime('%H:%M:%S')}.")
                return
            else:
                warn("GCP credentials seem to expire within the next five minutes.")
        else:
            warn(f"GCP credentials seem to have expired at {expiry.strftime('%Y-%m-%d %H:%M:%S')}.")
    else:
        warn("No GCP authentication credentials found.")

    info("Do you wish to reauthenticate now (y/n)?")
    answer = input()
    if answer[0:1].lower() == "n":
        info("So be it.")
        time.sleep(1)
        return

    subprocess.run(["gcloud", "auth", "application-default", "login"])
