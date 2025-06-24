import logging
import asyncio
from .email_extraction import extract_emails_from_website, extract_emails_async
from .filtering import filter_emails


def process_place(place, timeout, max_retries, use_selenium_primary=True):
    website_url = place.get('website_url')
    if not website_url:
        return []
    try:
        raw_emails = extract_emails_from_website(
            website_url, timeout, max_retries, use_selenium_primary
        )
        valid_emails = filter_emails(raw_emails)
        results = []
        for email in valid_emails:
            results.append({
                'place_name': place.get('place_name', 'unknown'),
                'website_url': website_url,
                'email': email,
                'category': place.get('category', 'unknown')
            })
        return results
    except Exception as e:
        logger = logging.getLogger("EmailExtractor")
        logger.error(f"Error processing {website_url}: {e}")
        return []


async def process_place_async(place, timeout, max_retries, use_selenium=False):
    website_url = place.get('website_url')
    if not website_url:
        return []
    try:
        raw_emails = await extract_emails_async(
            website_url, timeout, max_retries, use_selenium
        )
        valid_emails = filter_emails(raw_emails)
        results = []
        for email in valid_emails:
            results.append({
                'place_name': place.get('place_name', 'unknown'),
                'website_url': website_url,
                'email': email,
                'category': place.get('category', 'unknown')
            })
        return results
    except Exception as e:
        logger = logging.getLogger("EmailExtractor")
        logger.error(f"Error processing {website_url}: {e}")
        return []


def process_batch_threaded(places, max_workers=6, use_selenium=True):
    import concurrent.futures
    logger = logging.getLogger("EmailExtractor")
    all_results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_place = {
            executor.submit(
                process_place, place, 10, 2, use_selenium
            ): place for place in places
        }
        for future in concurrent.futures.as_completed(future_to_place):
            place = future_to_place[future]
            try:
                result = future.result()
                all_results.extend(result)
                if result:
                    logger.info(
                        f"✓ {place.get('place_name', 'Unknown')}: "
                        f"{len(result)} emails found"
                    )
                else:
                    logger.info(
                        f"✗ {place.get('place_name', 'Unknown')}: No emails found"
                    )
            except Exception as exc:
                logger.error(
                    f"Place {place.get('place_name', 'Unknown')} "
                    f"generated an exception: {exc}"
                )
    return all_results


async def process_batch_async(places, max_concurrent=10, use_selenium=False):
    logger = logging.getLogger("EmailExtractor")
    semaphore = asyncio.Semaphore(max_concurrent)

    async def process_with_semaphore(place):
        async with semaphore:
            return await process_place_async(place, 10, 2, use_selenium)

    tasks = [process_with_semaphore(place) for place in places]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    all_results = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(
                f"Place {places[i].get('place_name', 'Unknown')} "
                f"generated an exception: {result}"
            )
        else:
            all_results.extend(result)
            if result:
                logger.info(
                    f"✓ {places[i].get('place_name', 'Unknown')}: "
                    f"{len(result)} emails found"
                )
            else:
                logger.info(
                    f"✗ {places[i].get('place_name', 'Unknown')}: No emails found"
                )
    return all_results


def test_single_entry(
    place_name, website_url, timeout=10, max_retries=2,
    use_selenium_primary=True, use_async=False
):
    logger = logging.getLogger("EmailExtractor")
    place = {
        'place_name': place_name,
        'website_url': website_url,
        'category': 'test'
    }

    logger.info(f"Testing single entry: {place_name} - {website_url}")

    if use_async:
        logger.info("Using async processing...")
        import asyncio
        results = asyncio.run(
            process_place_async(place, timeout, max_retries, False)
        )
    else:
        logger.info("Using Selenium processing...")
        results = process_place(
            place, timeout, max_retries, use_selenium_primary)

    if results:
        logger.info(f"Found {len(results)} valid emails:")
        for result in results:
            logger.info(f"  • {result['email']}")
    else:
        logger.info("No valid business emails found")

    return results


async def test_single_entry_async(
    place_name, website_url, timeout=10, max_retries=2, use_selenium=False
):
    logger = logging.getLogger("EmailExtractor")
    place = {
        'place_name': place_name,
        'website_url': website_url,
        'category': 'test'
    }

    logger.info(f"Testing single entry (async): {place_name} - {website_url}")
    results = await process_place_async(place, timeout, max_retries, use_selenium)

    if results:
        logger.info(f"Found {len(results)} valid emails:")
        for result in results:
            logger.info(f"  • {result['email']}")
    else:
        logger.info("No valid business emails found")

    return results
