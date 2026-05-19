#!/usr/bin/env python3
"""
Migrate scraped data from MongoDB → PostgreSQL via the Choice backend API.
Run this after scraping completes, from the Hetzner server OR locally
(the backend must be reachable at BACKEND_URL).

Usage:
  python3 migrate_to_backend.py --backend http://localhost:6543
  python3 migrate_to_backend.py --backend https://your-deployed-backend.com
  python3 migrate_to_backend.py --type restaurant --limit 100   # partial run
"""

import os
import sys
import json
import time
import argparse
import logging
import requests
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger("migrate")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")

def get_mongo_client():
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

def _safe_rating(val):
    """Extract a float rating from either a number or a dict."""
    if isinstance(val, (int, float)):
        return val
    if isinstance(val, dict):
        return val.get("average") or val.get("rating") or 0
    return 0

def _safe_photos(val):
    """Ensure photos is a list of strings."""
    if not val:
        return []
    if isinstance(val, list):
        return [str(p) for p in val if p]
    return []

def _safe_hours(val):
    if not val:
        return []
    if isinstance(val, list):
        return [str(h) for h in val if h]
    return []

def push_restaurant(session, backend_url, doc):
    """Push one restaurant document to the backend scrapper API."""
    name = doc.get("name", "")
    if not name or not name.strip():
        return 0, {"skip": "no name"}
    payload = {
        "name": name,
        "address": doc.get("address", ""),
        "placeId": doc.get("place_id") or doc.get("placeId", ""),
        "mapsUrl": doc.get("maps_url") or doc.get("mapsUrl", ""),
        "latitude": doc.get("latitude") or doc.get("lat"),
        "longitude": doc.get("longitude") or doc.get("lon"),
        "rating": _safe_rating(doc.get("rating", 0)),
        "phoneNumber": doc.get("phone_number") or doc.get("phoneNumber", ""),
        "website": doc.get("website", ""),
        "description": doc.get("description", ""),
        "type": "restaurant",
        "cuisineType": doc.get("cuisine_type") or (doc.get("categories") or [None])[0],
        "priceRange": doc.get("price_range"),
        "photos": _safe_photos(doc.get("photos")),
        "openingHours": _safe_hours(doc.get("opening_hours") or doc.get("horaires")),
    }
    payload = {k: v for k, v in payload.items() if v is not None}
    r = session.post(f"{backend_url}/api/producer/scrapper/createProducer", json=payload, timeout=15)
    return r.status_code, r.json() if r.content else {}

def push_wellness(session, backend_url, doc):
    """Push one wellness place to backend."""
    name = doc.get("name", "")
    if not name or not name.strip():
        return 0, {"skip": "no name"}
    payload = {
        "name": name,
        "address": doc.get("address", ""),
        "placeId": doc.get("place_id") or doc.get("placeId", ""),
        "mapsUrl": doc.get("maps_url") or doc.get("mapsUrl", ""),
        "latitude": doc.get("latitude") or doc.get("lat"),
        "longitude": doc.get("longitude") or doc.get("lon"),
        "rating": _safe_rating(doc.get("rating", 0)),
        "phoneNumber": doc.get("phone") or doc.get("phoneNumber", ""),
        "website": doc.get("website", ""),
        "description": doc.get("description", ""),
        "type": "wellness",
        "category": doc.get("category", ""),
        "photos": _safe_photos(doc.get("photos")),
        "openingHours": _safe_hours(doc.get("opening_hours") or doc.get("openingHours")),
    }
    payload = {k: v for k, v in payload.items() if v is not None}
    r = session.post(f"{backend_url}/api/producer/scrapper/createProducer", json=payload, timeout=15)
    return r.status_code, r.json() if r.content else {}

def push_leisure(session, backend_url, doc):
    """Push one leisure venue/event to backend."""
    name = doc.get("name", "")
    if not name or not name.strip():
        return 0, {"skip": "no name"}
    payload = {
        "name": name,
        "address": doc.get("address", ""),
        "latitude": doc.get("latitude") or doc.get("lat"),
        "longitude": doc.get("longitude") or doc.get("lon"),
        "rating": _safe_rating(doc.get("rating", 0)),
        "placeId": doc.get("place_id") or doc.get("placeId", ""),
        "mapsUrl": doc.get("maps_url") or doc.get("mapsUrl", ""),
        "phoneNumber": doc.get("phone_number") or doc.get("phoneNumber", ""),
        "website": doc.get("website", ""),
        "description": doc.get("description", ""),
        "type": "leisure",
        "category": doc.get("category", ""),
        "photos": _safe_photos(doc.get("photos")),
        "openingHours": _safe_hours(doc.get("opening_hours") or doc.get("openingHours")),
    }
    payload = {k: v for k, v in payload.items() if v is not None}
    r = session.post(f"{backend_url}/api/producer/scrapper/createProducer", json=payload, timeout=15)
    return r.status_code, r.json() if r.content else {}

def migrate(backend_url, types=None, limit=None, dry_run=False):
    client = get_mongo_client()
    session = requests.Session()
    session.headers.update({"Content-Type": "application/json"})

    stats = {"restaurant": 0, "wellness": 0, "leisure": 0, "errors": 0}

    if not types or "restaurant" in types:
        logger.info("Migrating restaurants...")
        col = client["Restauration_Officielle"]["producers"]
        docs = list(col.find({}, {"_id": 0}).limit(limit or 0))
        logger.info(f"Found {len(docs)} restaurants in MongoDB")
        for i, doc in enumerate(docs):
            if dry_run:
                logger.info(f"[DRY RUN] Would push: {doc.get('name')}")
                continue
            code, resp = push_restaurant(session, backend_url, doc)
            if code in (200, 201):
                stats["restaurant"] += 1
                if i % 50 == 0:
                    logger.info(f"Restaurants: {i+1}/{len(docs)} pushed")
            elif code == 0:
                pass  # skipped (no name)
            else:
                logger.warning(f"Failed {doc.get('name')}: {code} {resp}")
                stats["errors"] += 1
            time.sleep(0.1)  # rate limit

    if not types or "wellness" in types:
        logger.info("Migrating wellness places...")
        col = client["Beauty_Wellness"]["BeautyPlaces"]
        docs = list(col.find({}, {"_id": 0}).limit(limit or 0))
        logger.info(f"Found {len(docs)} wellness places in MongoDB")
        for i, doc in enumerate(docs):
            if dry_run:
                logger.info(f"[DRY RUN] Would push: {doc.get('name')}")
                continue
            code, resp = push_wellness(session, backend_url, doc)
            if code in (200, 201):
                stats["wellness"] += 1
                if i % 200 == 0:
                    logger.info(f"Wellness: {i+1}/{len(docs)} pushed")
            elif code == 0:
                pass  # skipped (no name)
            else:
                logger.warning(f"Failed {doc.get('name')}: {code} {resp}")
                stats["errors"] += 1
            time.sleep(0.1)

    if not types or "leisure" in types:
        logger.info("Migrating leisure venues...")
        col = client["Loisir&Culture"]["Loisir_Paris_Producers"]
        docs = list(col.find({}, {"_id": 0}).limit(limit or 0))
        logger.info(f"Found {len(docs)} leisure venues in MongoDB")
        for i, doc in enumerate(docs):
            if dry_run:
                logger.info(f"[DRY RUN] Would push: {doc.get('name')}")
                continue
            code, resp = push_leisure(session, backend_url, doc)
            if code in (200, 201):
                stats["leisure"] += 1
                if i % 200 == 0:
                    logger.info(f"Leisure: {i+1}/{len(docs)} pushed")
            elif code == 0:
                pass  # skipped (no name)
            else:
                logger.warning(f"Failed {doc.get('name')}: {code} {resp}")
                stats["errors"] += 1
            time.sleep(0.1)

    logger.info(f"\n=== Migration complete ===")
    logger.info(f"Restaurants pushed: {stats['restaurant']}")
    logger.info(f"Wellness pushed:    {stats['wellness']}")
    logger.info(f"Leisure pushed:     {stats['leisure']}")
    logger.info(f"Errors:             {stats['errors']}")
    return stats

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate MongoDB scraped data to PostgreSQL via backend API")
    parser.add_argument("--backend", default="http://localhost:6543", help="Backend base URL")
    parser.add_argument("--type", nargs="+", choices=["restaurant", "wellness", "leisure"], help="Types to migrate")
    parser.add_argument("--limit", type=int, default=None, help="Max documents per type")
    parser.add_argument("--dry-run", action="store_true", help="Don't push, just log what would be sent")
    args = parser.parse_args()

    # Test connection
    try:
        r = requests.get(f"{args.backend}/api/health", timeout=5)
        logger.info(f"Backend reachable at {args.backend} ({r.status_code})")
    except Exception as e:
        logger.warning(f"Backend health check failed: {e}. Continuing anyway...")

    migrate(
        backend_url=args.backend,
        types=args.type,
        limit=args.limit,
        dry_run=args.dry_run,
    )
