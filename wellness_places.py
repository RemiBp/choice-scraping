#!/usr/bin/env python3
"""
Fast wellness/beauty venue scraper for Paris using Google Places API.
Writes directly to PostgreSQL via backend scrapper API.
Falls back to MongoDB if backend not reachable.
"""
import os, time, logging, argparse, requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger("WellnessPlaces")

GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:6543")

WELLNESS_TYPES = [
    "spa", "beauty_salon", "hair_salon", "barber_shop",
    "nail_salon", "gym", "physiotherapist", "massage_therapist",
    "dentist", "doctor", "pharmacy",
    "tattoo_shop", "skin_care_clinic",
    "yoga_studio", "pilates_studio",
    "hair_removal_service", "tanning_salon",
]

# Dense grid: Paris + inner suburbs
PARIS_GRID = []
lat = 48.75
while lat <= 48.97:
    lng = 2.20
    while lng <= 2.56:
        PARIS_GRID.append((round(lat, 3), round(lng, 3)))
        lng += 0.030
    lat += 0.025
logger.info(f"Grid has {len(PARIS_GRID)} points")


def places_nearby(lat, lng, radius, place_type, page_token=None):
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {"location": f"{lat},{lng}", "radius": radius, "type": place_type,
              "language": "fr", "key": GOOGLE_MAPS_API_KEY}
    if page_token:
        params = {"pagetoken": page_token, "key": GOOGLE_MAPS_API_KEY}
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    return r.json()


def place_details(place_id):
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "fields": "name,formatted_address,geometry,rating,formatted_phone_number,"
                  "website,opening_hours,photos,types,editorial_summary",
        "language": "fr", "key": GOOGLE_MAPS_API_KEY,
    }
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    return r.json().get("result", {})


def photo_url(ref, w=800):
    return f"https://maps.googleapis.com/maps/api/place/photo?maxwidth={w}&photoreference={ref}&key={GOOGLE_MAPS_API_KEY}"


def push_to_backend(session, doc):
    payload = {
        "name": doc["name"],
        "address": doc.get("address", ""),
        "placeId": doc.get("place_id", ""),
        "mapsUrl": doc.get("maps_url", ""),
        "latitude": doc.get("latitude"),
        "longitude": doc.get("longitude"),
        "rating": doc.get("rating", 0),
        "phoneNumber": doc.get("phone_number", ""),
        "website": doc.get("website", ""),
        "description": doc.get("description", ""),
        "type": "wellness",
        "photos": doc.get("photos", []),
        "openingHours": doc.get("opening_hours", []),
    }
    payload = {k: v for k, v in payload.items() if v is not None and v != ""}
    r = session.post(f"{BACKEND_URL}/api/producer/scrapper/createProducer", json=payload, timeout=15)
    return r.status_code


def save_to_mongo(col, doc):
    try:
        col.update_one({"place_id": doc["place_id"]}, {"$setOnInsert": doc}, upsert=True)
        return True
    except Exception as e:
        logger.warning(f"Mongo: {e}")
        return False


def scrape_all(radius=2500, limit=0, use_backend=True, use_mongo=True):
    session = requests.Session()
    session.headers.update({"Content-Type": "application/json"})

    if use_backend:
        try:
            r = requests.get(f"{BACKEND_URL}/api/health", timeout=5)
            logger.info(f"Backend reachable (status {r.status_code})")
        except Exception as e:
            logger.warning(f"Backend not reachable: {e}. MongoDB only.")
            use_backend = False

    col = None
    seen_ids = set()
    if use_mongo:
        try:
            from pymongo import MongoClient, ASCENDING
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            col = client["Beauty_Wellness"]["BeautyPlaces"]
            col.create_index([("place_id", ASCENDING)], unique=True, sparse=True)
            seen_ids = set(d["place_id"] for d in col.find({}, {"place_id": 1}) if d.get("place_id"))
            logger.info(f"MongoDB connected — {len(seen_ids)} existing")
        except Exception as e:
            logger.warning(f"MongoDB unavailable: {e}")
            col = None

    saved = 0
    errors = 0

    for (lat, lng) in PARIS_GRID:
        for place_type in WELLNESS_TYPES:
            token = None
            while True:
                try:
                    data = places_nearby(lat, lng, radius, place_type, page_token=token)
                    status = data.get("status", "")
                    if status not in ("OK", "ZERO_RESULTS"):
                        if status == "REQUEST_DENIED":
                            logger.error("API key invalid — aborting")
                            return saved
                        if status == "OVER_QUERY_LIMIT":
                            logger.warning("Rate limit — sleeping 60s")
                            time.sleep(60)
                            continue
                        break

                    for p in data.get("results", []):
                        pid = p.get("place_id", "")
                        if not pid or pid in seen_ids:
                            continue
                        seen_ids.add(pid)

                        try:
                            details = place_details(pid)
                            time.sleep(0.05)
                        except Exception:
                            details = p

                        geom = details.get("geometry", {}).get("location", {})
                        photos = [photo_url(ph["photo_reference"]) for ph in details.get("photos", [])[:5]]
                        hours = details.get("opening_hours", {}).get("weekday_text", [])

                        doc = {
                            "place_id": pid,
                            "name": details.get("name", p.get("name", "")),
                            "address": details.get("formatted_address", p.get("vicinity", "")),
                            "latitude": geom.get("lat"),
                            "longitude": geom.get("lng"),
                            "rating": details.get("rating", p.get("rating", 0)),
                            "phone_number": details.get("formatted_phone_number", ""),
                            "website": details.get("website", ""),
                            "description": details.get("editorial_summary", {}).get("overview", ""),
                            "opening_hours": hours,
                            "photos": photos,
                            "category": place_type,
                            "maps_url": f"https://www.google.com/maps/place/?q=place_id:{pid}",
                            "type": "wellness",
                        }

                        ok = False
                        if use_backend:
                            try:
                                code = push_to_backend(session, doc)
                                ok = code in (200, 201)
                                if not ok:
                                    logger.warning(f"Backend {code} for {doc['name']}")
                            except Exception as e:
                                logger.warning(f"Backend push failed: {e}")

                        if col is not None:
                            save_to_mongo(col, doc)
                            ok = True

                        if ok:
                            saved += 1
                            if saved % 100 == 0:
                                logger.info(f"Saved {saved} wellness venues")

                        if limit and saved >= limit:
                            return saved

                    token = data.get("next_page_token")
                    if not token:
                        break
                    time.sleep(2)

                except KeyboardInterrupt:
                    logger.info(f"Interrupted — saved {saved}")
                    return saved
                except Exception as e:
                    errors += 1
                    logger.warning(f"Error {place_type} @ {lat},{lng}: {e}")
                    break

    logger.info(f"Done. Saved: {saved}, errors: {errors}")
    return saved


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--radius", type=int, default=2500)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--no-backend", action="store_true")
    parser.add_argument("--no-mongo", action="store_true")
    args = parser.parse_args()

    if not GOOGLE_MAPS_API_KEY:
        logger.error("GOOGLE_MAPS_API_KEY not set")
        exit(1)

    logger.info(f"Wellness scrape — radius={args.radius}m, backend={BACKEND_URL}")
    n = scrape_all(radius=args.radius, limit=args.limit,
                   use_backend=not args.no_backend, use_mongo=not args.no_mongo)
    logger.info(f"Total saved: {n}")
