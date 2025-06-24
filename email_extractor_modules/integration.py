"""
Integration helpers for email_extractor_modules to be used by other scripts.
Provides easy-to-use functions for wellness.py and billetreduc_shotgun_mistral.py.
"""

import logging
from .email_extraction import extract_emails_from_website
from .filtering import filter_emails
from .constants import DEFAULT_TIMEOUT, DEFAULT_MAX_RETRIES


def extract_and_filter_emails(website_url, timeout=DEFAULT_TIMEOUT, 
                             max_retries=DEFAULT_MAX_RETRIES, 
                             use_selenium_primary=True):
    """
    One-shot function to extract and filter emails from a website.
    Perfect for integration into other scripts.
    
    Args:
        website_url (str): Website URL to extract emails from
        timeout (int): Request timeout in seconds
        max_retries (int): Maximum retry attempts
        use_selenium_primary (bool): Use Selenium as primary method
        
    Returns:
        set: Filtered, valid business emails
    """
    logger = logging.getLogger("EmailExtractor")
    
    if not website_url:
        return set()
    
    try:
        # Extract raw emails
        raw_emails = extract_emails_from_website(
            website_url, timeout, max_retries, use_selenium_primary
        )
        
        # Filter and return valid emails
        valid_emails = filter_emails(raw_emails)
        
        logger.info(f"Extracted {len(valid_emails)} valid emails from {website_url}")
        return valid_emails
        
    except Exception as e:
        logger.error(f"Email extraction failed for {website_url}: {e}")
        return set()


def extract_emails_for_wellness_place(place_data):
    """
    Extract emails specifically for wellness.py integration.
    
    Args:
        place_data (dict): Place data with 'website' or 'website_url' field
        
    Returns:
        list: List of valid email addresses
    """
    website_url = place_data.get('website') or place_data.get('website_url')
    
    if not website_url:
        return []
    
    emails = extract_and_filter_emails(website_url, use_selenium_primary=True)
    return list(emails)


def extract_emails_for_venue(venue_data):
    """
    Extract emails specifically for billetreduc_shotgun_mistral.py integration.
    
    Args:
        venue_data (dict): Venue data with website information
        
    Returns:
        list: List of valid email addresses
    """
    # Try multiple possible website field names
    website_url = (venue_data.get('website_url') or 
                  venue_data.get('website') or 
                  venue_data.get('site_web') or
                  venue_data.get('url'))
    
    if not website_url:
        return []
    
    emails = extract_and_filter_emails(website_url, use_selenium_primary=True)
    return list(emails)


def batch_extract_emails(items, website_field='website_url', max_workers=3):
    """
    Extract emails from multiple items in parallel.
    
    Args:
        items (list): List of items with website fields
        website_field (str): Field name containing website URL
        max_workers (int): Maximum parallel workers
        
    Returns:
        dict: Mapping of item index to extracted emails
    """
    from concurrent.futures import ThreadPoolExecutor
    
    results = {}
    
    def extract_for_item(index_item):
        index, item = index_item
        website_url = item.get(website_field)
        if website_url:
            emails = extract_and_filter_emails(website_url)
            return index, list(emails)
        return index, []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(extract_for_item, (i, item)) 
                  for i, item in enumerate(items)]
        
        for future in futures:
            try:
                index, emails = future.result()
                results[index] = emails
            except Exception as e:
                logging.error(f"Batch email extraction error: {e}")
    
    return results 