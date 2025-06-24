import re

SUBPAGE_PATHS = ["/contact", "/about", "/mentions-legales", "/impressum"]
EMAIL_EXCLUDE_PATTERNS = [
    r"noreply@", r"no-reply@", r"do-not-reply@", r"facebook.com", r"twitter.com"
]
CSV_FIELDNAMES = ["place_name", "website_url", "email", "category"]
DEFAULT_THREAD_COUNT = 6
DEFAULT_TIMEOUT = 10
DEFAULT_MAX_RETRIES = 2
EMAIL_PATTERN = re.compile(
    r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
    re.IGNORECASE
)
