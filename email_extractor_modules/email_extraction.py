import logging
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import contextlib
import os
import aiohttp
from .constants import (
    SUBPAGE_PATHS, EMAIL_PATTERN, DEFAULT_TIMEOUT, DEFAULT_MAX_RETRIES
)


def normalize_url(url):
    if not url:
        return None
    url = url.strip()
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    return url


def fetch_page(url, timeout=DEFAULT_TIMEOUT, max_retries=DEFAULT_MAX_RETRIES):
    """
    Fetch a webpage with retries and return the text content.
    Returns None if the page cannot be fetched.
    """
    logger = logging.getLogger("EmailExtractor")
    for attempt in range(max_retries + 1):
        try:
            response = requests.get(
                url, timeout=timeout, verify=False,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                    'AppleWebKit/537.36'
                }
            )
            response.raise_for_status()
            return response.text
        except Exception as e:
            if attempt < max_retries:
                logger.debug(f"Attempt {attempt + 1} failed for {url}: {e}")
            else:
                logger.debug(f"All attempts failed for {url}: {e}")
    return None


def extract_emails_with_library(
    url, timeout=DEFAULT_TIMEOUT, max_retries=DEFAULT_MAX_RETRIES
):
    """Extract emails using requests + BeautifulSoup."""
    logger = logging.getLogger("EmailExtractor")
    emails = set()
    try:
        text = fetch_page(url, timeout, max_retries)
        if text:
            emails.update(EMAIL_PATTERN.findall(text))
            logger.debug(
                f"Library extraction from {url}: {len(emails)} emails")
    except Exception as e:
        logger.debug(f"Library extraction failed for {url}: {e}")
    return emails


def extract_emails_from_text(text):
    if not text:
        return set()
    return set(EMAIL_PATTERN.findall(text))


def find_subpage_links(base_url, page_text):
    """Find relevant subpage links from the main page."""
    if not page_text:
        return []

    subpage_links = []
    try:
        soup = BeautifulSoup(page_text, 'html.parser')
        for link in soup.find_all('a', href=True):
            href = link['href'].lower()
            if any(subpath in href for subpath in SUBPAGE_PATHS):
                full_url = urljoin(base_url, link['href'])
                subpage_links.append(full_url)
    except Exception:
        pass
    return list(set(subpage_links))


def extract_emails_from_website(
    url, timeout=DEFAULT_TIMEOUT, max_retries=DEFAULT_MAX_RETRIES,
    use_selenium_primary=True
):
    """
    Extract emails from a website using the specified method.
    Returns a set of email addresses found.
    """
    logger = logging.getLogger("EmailExtractor")
    url = normalize_url(url)
    if not url:
        return set()

    all_emails = set()

    if use_selenium_primary:
        selenium_emails = extract_emails_with_selenium(
            url, timeout, max_retries)
        all_emails.update(selenium_emails)
        if selenium_emails:
            logger.debug(
                f"Selenium found {len(selenium_emails)} emails from {url}"
            )
            return all_emails

    library_emails = extract_emails_with_library(url, timeout, max_retries)
    all_emails.update(library_emails)

    return all_emails


def extract_emails_with_selenium(
    url, timeout=DEFAULT_TIMEOUT, max_retries=DEFAULT_MAX_RETRIES
):
    """Extract emails using Selenium WebDriver."""
    emails = set()

    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from webdriver_manager.chrome import ChromeDriverManager

        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--disable-logging')
        chrome_options.add_argument('--log-level=3')
        chrome_options.add_experimental_option(
            'excludeSwitches', ['enable-logging']
        )
        chrome_options.add_experimental_option('useAutomationExtension', False)

        with contextlib.redirect_stderr(open(os.devnull, 'w')):
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)

        try:
            driver.set_page_load_timeout(timeout)
            driver.get(url)

            page_source = driver.page_source
            emails.update(extract_emails_from_text(page_source))

            subpage_links = find_subpage_links(url, page_source)
            for subpage_url in subpage_links[:3]:
                try:
                    driver.get(subpage_url)
                    subpage_source = driver.page_source
                    emails.update(extract_emails_from_text(subpage_source))
                except Exception:
                    continue
        finally:
            driver.quit()

    except Exception:
        pass

    return emails


def extract_emails_with_shared_selenium(url, driver, timeout=DEFAULT_TIMEOUT):
    """Extract emails using a shared Selenium WebDriver instance."""
    emails = set()
    try:
        driver.set_page_load_timeout(timeout)
        driver.get(url)

        page_source = driver.page_source
        emails.update(extract_emails_from_text(page_source))

        subpage_links = find_subpage_links(url, page_source)
        for subpage_url in subpage_links[:2]:
            try:
                driver.get(subpage_url)
                subpage_source = driver.page_source
                emails.update(extract_emails_from_text(subpage_source))
            except Exception:
                continue

    except Exception:
        pass

    return emails


async def extract_emails_async(
    url, timeout=DEFAULT_TIMEOUT, max_retries=DEFAULT_MAX_RETRIES,
    use_selenium=False
):
    """Extract emails asynchronously."""
    logger = logging.getLogger("EmailExtractor")
    url = normalize_url(url)
    if not url:
        return set()

    emails = set()

    if use_selenium:
        return extract_emails_with_selenium(url, timeout, max_retries)

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=timeout) as response:
                if response.status == 200:
                    text = await response.text()
                    emails.update(extract_emails_from_text(text))

                    subpage_links = find_subpage_links(url, text)
                    for subpage_url in subpage_links[:2]:
                        try:
                            async with session.get(
                                subpage_url, timeout=timeout
                            ) as sub_response:
                                if sub_response.status == 200:
                                    sub_text = await sub_response.text()
                                    emails.update(
                                        extract_emails_from_text(sub_text)
                                    )
                        except Exception:
                            continue
    except Exception as e:
        logger.debug(f"Async extraction failed for {url}: {e}")

    return emails


async def fetch_page_async(session, url, timeout=DEFAULT_TIMEOUT):
    """Fetch a page asynchronously."""
    try:
        async with session.get(url, timeout=timeout) as response:
            if response.status == 200:
                return await response.text()
    except Exception:
        pass
    return None
