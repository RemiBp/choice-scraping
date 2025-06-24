import re
from .constants import EMAIL_EXCLUDE_PATTERNS


def filter_emails(emails):
    if not emails:
        return set()
    filtered_emails = set()
    for email in emails:
        email_lower = email.lower()
        if '@' not in email or email.endswith(
            ('.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp')
        ):
            continue
        if any(re.search(pattern, email_lower) for pattern in EMAIL_EXCLUDE_PATTERNS):
            continue
        if any(domain in email_lower for domain in [
            'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com',
            'example.com', 'test.com', 'localhost'
        ]):
            continue
        if any(placeholder in email_lower for placeholder in [
            'example', 'test', 'demo', 'sample', 'placeholder', 'nom@domain'
        ]):
            continue
        filtered_emails.add(email)
    return filtered_emails
