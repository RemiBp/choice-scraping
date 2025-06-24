import logging
import csv
from .constants import CSV_FIELDNAMES


def load_input(input_path=None, mongo_uri=None, mongo_db=None, mongo_collection=None):
    """
    Load input data from MongoDB.
    Returns a list of dicts with 'place_name', 'website_url', 'category'.
    Field normalization is handled for restaurant, leisure, and beauty collections.
    """
    logger = logging.getLogger("EmailExtractor")
    if not (mongo_uri and mongo_db and mongo_collection):
        logger.error(
            "MongoDB URI, DB, and collection must be provided for MongoDB input."
        )
        return []
    try:
        from pymongo import MongoClient
        client = MongoClient(mongo_uri)
        db = client[mongo_db]
        collection = db[mongo_collection]
        logger.info(
            f"Connected to MongoDB: {mongo_db}.{mongo_collection}"
        )
        cursor = collection.find({})
        places = []
        for doc in cursor:
            place_name = doc.get('name') or doc.get('place_name') or 'Unknown'
            website_url = (
                doc.get('website') or doc.get('website_url') or
                doc.get('site_web') or doc.get('url')
            )
            category = doc.get('category') or doc.get('type') or 'Unknown'
            if website_url:
                places.append({
                    'place_name': place_name,
                    'website_url': website_url,
                    'category': category
                })
        logger.info(f"Loaded {len(places)} places from MongoDB")
        return places
    except Exception as e:
        logger.error(f"Error loading from MongoDB: {e}")
        return []


def write_output(results, output_path):
    if not results:
        return
    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        for result in results:
            writer.writerow(result)
