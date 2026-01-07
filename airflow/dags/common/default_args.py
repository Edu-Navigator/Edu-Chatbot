from datetime import timedelta

DEFAULT_ARGS = {
    "owner": "team7",
    "email": ["sosoj1552@gmail.com", "symoon1007@gmail.com", "wonba1128@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
