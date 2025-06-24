"""
email_extractor.py
------------------
Extracts business emails from a list of place websites.
Crawls homepage and key subpages, filters out non-biz addresses, outputs deduped CSV.
"""

import email_extractor_modules.env  # noqa: F401 (side effects)
import argparse
import logging
import sys
import time
import asyncio
from email_extractor_modules.constants import (
    DEFAULT_THREAD_COUNT, DEFAULT_TIMEOUT, DEFAULT_MAX_RETRIES
)
from email_extractor_modules.io_utils import load_input, write_output
from email_extractor_modules.pipeline import (
    process_batch_threaded, process_batch_async, test_single_entry
)


def main():
    parser = argparse.ArgumentParser(
        description="Extract business emails from place websites."
    )
    parser.add_argument(
        '--input', type=str,
        help='Input CSV file (or leave blank to use MongoDB)'
    )
    parser.add_argument('--output', type=str, help='Output CSV file')
    parser.add_argument(
        '--threads', type=int, default=DEFAULT_THREAD_COUNT,
        help='Number of threads for Selenium processing'
    )
    parser.add_argument(
        '--async-concurrent', type=int, default=10,
        help='Max concurrent requests for async processing'
    )
    parser.add_argument(
        '--mongo-uri', type=str, help='MongoDB URI (if using MongoDB input)'
    )
    parser.add_argument(
        '--mongo-db', type=str,
        help='MongoDB database name (if using MongoDB input)'
    )
    parser.add_argument(
        '--mongo-collection', type=str,
        help='MongoDB collection name (if using MongoDB input)'
    )
    parser.add_argument(
        '--timeout', type=int, default=DEFAULT_TIMEOUT,
        help='HTTP request timeout (seconds)'
    )
    parser.add_argument(
        '--max-retries', type=int, default=DEFAULT_MAX_RETRIES,
        help='Max HTTP retries per request'
    )
    parser.add_argument(
        '--test', nargs=2, metavar=('NAME', 'URL'),
        help='Test single entry: --test "Place Name" "website.com"'
    )
    parser.add_argument(
        '--production', action='store_true',
        help='Run in production mode (default is test mode)'
    )
    parser.add_argument(
        '--use-async',
        action='store_true',
        help='Use async processing instead of Selenium (faster but less capable)')
    parser.add_argument(
        '--use-selenium', action='store_true',
        help='Force use of Selenium (default, more capable but slower)'
    )
    parser.add_argument(
        '--batch-size', type=int, default=100,
        help='Batch size for processing large datasets'
    )

    if len(sys.argv) == 1:
        logging.info(
            "No arguments provided, running parallel test with first 10 "
            "MongoDB records..."
        )
        logging.info(
            f"Using parallel Selenium extraction with {DEFAULT_THREAD_COUNT} "
            "threads"
        )
        test_mongo_uri = (
            "mongodb+srv://remibarbier:Calvi8Pierc2@lieuxrestauration.szq31."
            "mongodb.net/?retryWrites=true&w=majority&appName=lieuxrestauration")
        test_mongo_db = "Restauration_Officielle"
        test_mongo_collection = "producers"
        logging.info(
            f"Loading records from {test_mongo_db}.{test_mongo_collection}"
        )
        places = load_input(
            None, test_mongo_uri, test_mongo_db, test_mongo_collection
        )
        if not places:
            logging.error(
                "No records found in MongoDB. Check connection and collection."
            )
            return
        test_places = places[:10]
        logging.info(
            f"Processing {len(test_places)} sites in parallel with "
            f"{DEFAULT_THREAD_COUNT} Chrome drivers..."
        )
        start_time = time.time()
        results = process_batch_threaded(
            test_places, max_workers=DEFAULT_THREAD_COUNT, use_selenium=True
        )
        end_time = time.time()
        print("\n🎯 PARALLEL PROCESSING SUMMARY")
        print("=" * 50)
        print(f"Sites processed: {len(test_places)}")
        print(f"Total emails found: {len(results)}")
        sites_with_emails = len(set(r['website_url'] for r in results))
        success_rate = sites_with_emails / len(test_places) * 100
        print(
            f"Sites with emails: {sites_with_emails}/{len(test_places)} "
            f"({success_rate:.0f}%)"
        )
        print(f"Processing time: {end_time - start_time:.1f}s")
        print(
            f"Average per site: "
            f"{(end_time - start_time) / len(test_places):.1f}s"
        )
        print(
            f"Parallel efficiency: ~{6*5.4:.0f}s sequential vs "
            f"{end_time - start_time:.0f}s parallel"
        )
        if results:
            print("\n📧 VALID EMAILS EXTRACTED:")
            for result in results:
                print(f"  • {result['place_name']}: {result['email']}")
        else:
            print("\n❌ No valid business emails found")
        print("=" * 50)
        
        # Write test results to CSV
        test_output_file = "test_emails_output.csv"
        write_output(results, test_output_file)
        print(f"\n💾 Test results saved to: {test_output_file}")
        
        return

    args = parser.parse_args()
    use_selenium = not args.use_async
    if args.use_selenium:
        use_selenium = True
    if args.test:
        test_single_entry(
            args.test[0], args.test[1], timeout=args.timeout,
            max_retries=args.max_retries, use_selenium_primary=use_selenium,
            use_async=args.use_async
        )
        return
    if not args.production or not args.output:
        logging.error(
            "Production mode requires --production flag and --output argument"
        )
        return
    logging.info("Starting email extraction...")
    if use_selenium:
        logging.info(f"Using Selenium with {args.threads} threads")
    else:
        logging.info(
            f"Using async processing with {args.async_concurrent} "
            "concurrent requests"
        )
    places = load_input(
        args.input, args.mongo_uri, args.mongo_db, args.mongo_collection
    )
    if not places:
        logging.error("No places loaded from input")
        return
    all_results = []
    batch_size = args.batch_size
    for i in range(0, len(places), batch_size):
        batch = places[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (len(places) + batch_size - 1) // batch_size
        logging.info(
            f"Processing batch {batch_num}/{total_batches} "
            f"({len(batch)} places)"
        )
        if use_selenium:
            batch_results = process_batch_threaded(
                batch, max_workers=args.threads, use_selenium=True
            )
        else:
            batch_results = asyncio.run(
                process_batch_async(
                    batch, max_concurrent=args.async_concurrent,
                    use_selenium=False
                )
            )
        all_results.extend(batch_results)
        total_processed = min(i + batch_size, len(places))
        logging.info(
            f"Completed {total_processed}/{len(places)} places. "
            f"Found {len(batch_results)} emails in this batch."
        )
        if i + batch_size < len(places):
            time.sleep(1)
    write_output(all_results, args.output)
    logging.info(
        f"Extraction complete. {len(all_results)} emails written to "
        f"{args.output}"
    )
    total_places_with_emails = len(
        set(result['website_url'] for result in all_results)
    )
    success_percentage = total_places_with_emails / len(places) * 100
    logging.info(
        f"Summary: {total_places_with_emails}/{len(places)} places had "
        f"valid emails ({success_percentage:.1f}%)"
    )


if __name__ == "__main__":
    main()
