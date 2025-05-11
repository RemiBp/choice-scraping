import os
import requests
import time
import re
import json
from bs4 import BeautifulSoup
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pymongo import MongoClient
from collections import OrderedDict
import logging
import base64
from datetime import datetime, timedelta
import httpx
import sys
import argparse  # Pour les arguments de ligne de commande
import hashlib
import shutil
from io import BytesIO
from PIL import Image
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import random
import unicodedata
from pprint import pformat
import Levenshtein
import traceback
from math import cos, sin, sqrt, atan2, radians, degrees

# Limiter le nombre de threads pour éviter le "Resource temporarily unavailable"
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"

# Si torch est utilisé, limiter également le nombre de threads pour torch
try:
    import torch
    torch.set_num_threads(1)
except ImportError:
    pass  # torch n'est pas installé, on ignore

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("BeautyWellnessProcessor")

# Configuration API et MongoDB
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")
MONGO_URI = "mongodb+srv://remibarbier:Calvi8Pierc2@lieuxrestauration.szq31.mongodb.net/?retryWrites=true&w=majority&appName=lieuxrestauration"
DB_NAME = "Beauty_Wellness"

# Configuration Brightdata
BRIGHTDATA_TOKEN = os.getenv("BRIGHTDATA_TOKEN")
BRIGHTDATA_ZONE = "web_unlocker1"
BRIGHTDATA_ENABLED = bool(BRIGHTDATA_TOKEN)

# Configuration pour Bing Search
BING_SEARCH_CACHE = {}

# --- AJOUT : Fonction utilitaire OpenAI GPT-3.5-turbo ---
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"

def generate_ai_response_openai(prompt, model="gpt-3.5-turbo", temperature=0.2, max_tokens=512):
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json"
    }
    data = {
        "model": model,
        "messages": [
            {"role": "system", "content": "Tu es un assistant expert en analyse de reviews pour la beauté et le bien-être. Réponds toujours en français."},
            {"role": "user", "content": prompt}
        ],
        "temperature": temperature,
        "max_tokens": max_tokens
    }
    try:
        response = requests.post(OPENAI_API_URL, headers=headers, json=data, timeout=60)
        response.raise_for_status()
        result = response.json()
        return result["choices"][0]["message"]["content"].strip()
    except Exception as e:
        logger.error(f"Erreur OpenAI: {e}")
        return None
# --- FIN AJOUT ---

# Classe pour simuler MongoDB en mode local/test
class MockCollection:
    def __init__(self, name):
        self.name = name
        self._data = {}  # Stockage en mémoire
        
    def find_one(self, query):
        query_key = str(query)
        return self._data.get(query_key, None)
    
    def count_documents(self, query):
        return len([doc for key, doc in self._data.items() if all(item in doc.items() for item in query.items())])
    
    def update_one(self, query, update, upsert=False):
        query_key = str(query)
        if query_key in self._data or upsert:
            doc = self._data.get(query_key, {})
            if "$set" in update:
                for k, v in update["$set"].items():
                    doc[k] = v
            self._data[query_key] = doc
        return type('obj', (object,), {'matched_count': 1, 'modified_count': 1, 'inserted_id': '123'})
    
    def insert_one(self, doc):
        doc_id = str(doc.get('_id', hash(str(doc))))
        self._data[doc_id] = doc
        return type('obj', (object,), {'inserted_id': doc_id})
    
    def find(self, query=None, limit=None):
        results = []
        for doc in self._data.values():
            if query is None or all(item in doc.items() for item in query.items()):
                results.append(doc)
        if limit:
            results = results[:limit]
        return results
    
    def aggregate(self, pipeline):
        # Simulation très simplifiée de l'agrégation
        return []

class MockDatabase:
    def __init__(self, name):
        self.name = name
        self._collections = {}
    
    def __getitem__(self, name):
        if name not in self._collections:
            self._collections[name] = MockCollection(name)
        return self._collections[name]
    
    def __getattr__(self, name):
        return self[name]

class MockMongoClient:
    def __init__(self, *args, **kwargs):
        self._dbs = {}
        logger.info("Initialisation du client MongoDB simulé (mode test)")
    
    def __getitem__(self, name):
        if name not in self._dbs:
            self._dbs[name] = MockDatabase(name)
        return self._dbs[name]
    
    def server_info(self):
        return {'version': '4.4.0', 'gitVersion': 'mock-version'}
    
    def __getattr__(self, name):
        if name == 'admin':
            return type('obj', (object,), {'command': lambda x: None})
        return self[name]

# Vérifier si on doit utiliser MongoDB simulé
TEST_MODE = False  # Sera mis à jour par les arguments CLI

# Le client MongoDB sera initialisé plus tard, après le parsing des arguments
client = None
db = None
analyzer = SentimentIntensityAnalyzer()

# Définition des catégories, sous-catégories et critères d'évaluation
CATEGORIES = {
    "Soins esthétiques et bien-être": {
        "sous_categories": [
            "Institut de beauté", "Spa", "Salon de massage", 
            "Centre d'épilation", "Clinique de soins de la peau", "Salon de bronzage"
        ],
        "google_types": ["spa", "beauty_salon", "massage_therapist", "hair_removal_service", "skin_care_clinic", "tanning_salon"],
        "criteres_evaluation": [
            "Qualité des soins", "Propreté", "Accueil", "Rapport qualité/prix", 
            "Ambiance", "Expertise du personnel"
        ]
    },
    "Coiffure et soins capillaires": {
        "sous_categories": ["Salon de coiffure", "Barbier"],
        "google_types": ["hair_salon", "barber_shop"],
        "criteres_evaluation": [
            "Qualité de la coupe", "Respect des attentes", "Conseil", 
            "Produits utilisés", "Tarifs", "Ponctualité"
        ]
    },
    "Onglerie et modifications corporelles": {
        "sous_categories": ["Salon de manucure", "Salon de tatouage", "Salon de piercing"],
        "google_types": ["nail_salon", "tattoo_shop", "piercing_shop"],
        "criteres_evaluation": [
            "Précision", "Hygiène", "Créativité", "Durabilité", 
            "Conseil", "Douleur ressentie"
        ]
    }
}

# Expressions et termes à rechercher dans les commentaires pour chaque critère
MOTS_CLES = {
    "Soins esthétiques et bien-être": {
        "Qualité des soins": ["soin", "massage", "traitement", "qualité", "professionnel"],
        "Propreté": ["propre", "hygiène", "hygiénique", "nettoyage", "sanitaire"],
        "Accueil": ["accueil", "réception", "amabilité", "gentil", "sympathique"],
        "Rapport qualité/prix": ["prix", "tarif", "cher", "abordable", "valeur", "qualité-prix"],
        "Ambiance": ["ambiance", "atmosphère", "décor", "calme", "relaxant", "musique"],
        "Expertise du personnel": ["expert", "compétent", "professionnel", "expérience", "savoir-faire"]
    },
    "Coiffure et soins capillaires": {
        "Qualité de la coupe": ["coupe", "coiffure", "style", "résultat", "satisfait"],
        "Respect des attentes": ["attente", "demande", "photo", "souhaité", "voulu", "imaginé"],
        "Conseil": ["conseil", "suggestion", "recommandation", "avis", "guider"],
        "Produits utilisés": ["produit", "shampooing", "soin", "coloration", "marque"],
        "Tarifs": ["prix", "tarif", "cher", "abordable", "supplément", "coût"],
        "Ponctualité": ["heure", "attente", "retard", "rendez-vous", "ponctuel", "rapidité"]
    },
    "Onglerie et modifications corporelles": {
        "Précision": ["précis", "détail", "fin", "minutieux", "exact", "ligne"],
        "Hygiène": ["propre", "stérile", "gant", "aiguille", "hygiène", "désinfecté"],
        "Créativité": ["créatif", "original", "idée", "design", "motif", "artistique"],
        "Durabilité": ["tenir", "durer", "longtemps", "solide", "permanent", "semaine"],
        "Conseil": ["conseil", "suggestion", "recommandation", "avis", "information"],
        "Douleur ressentie": ["douleur", "mal", "doux", "supportable", "indolore", "aie"]
    }
}

# Classe pour gérer les requêtes Brightdata
def get_brightdata_request(url, timeout=30):
    """
    Effectue une requête via Bright Data Web Unlocker API.
    
    Args:
        url: URL à scraper
        timeout: Délai d'attente maximum en secondes
        
    Returns:
        Texte HTML de la page ou None en cas d'erreur
    """
    if not BRIGHTDATA_ENABLED:
        logger.warning("BrightData n'est pas activé (token manquant)")
        return None
        
    try:
        # Configuration complète pour Web Unlocker API
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {BRIGHTDATA_TOKEN}"
        }
        
        # Configuration des paramètres - retirer "device" qui cause l'erreur 400
        payload = {
            "zone": BRIGHTDATA_ZONE,
            "url": url,
            "format": "raw",
            "country": "fr",
            # "device": "desktop",  # Supprimé car non autorisé
            "render": True
        }
        
        logger.info(f"Requête BrightData vers: {url}")
        
        # Requête avec BrightData Web Unlocker API
        response = requests.post(
            "https://api.brightdata.com/request", 
            headers=headers, 
            json=payload,
            timeout=timeout
        )
        
        # Log détaillé en cas d'erreur
        if response.status_code != 200:
            logger.error(f"Réponse BrightData {response.status_code}: {response.text[:200]}")
            return None
            
        return response.text
    except Exception as e:
        logger.error(f"Erreur de requête BrightData: {e}")
        return None

# --- FONCTION UTILITAIRE : Générer une grille de points autour d'un centre ---
def generate_grid_points(center_lat, center_lng, radius_m, spacing_m=500):
    """
    Génère une grille de points (lat, lng) couvrant un cercle de rayon radius_m autour du centre.
    spacing_m : distance entre les points de la grille (en mètres)
    """
    points = []
    R = 6378137  # Rayon de la Terre en mètres
    d = spacing_m
    # Nombre de points à couvrir sur le rayon
    steps = int(radius_m // d)
    for dx in range(-steps, steps + 1):
        for dy in range(-steps, steps + 1):
            dist = sqrt(dx ** 2 + dy ** 2) * d
            if dist > radius_m:
                continue
            # Calcul du décalage en latitude/longitude
            delta_lat = (dy * d) / R
            delta_lng = (dx * d) / (R * cos(radians(center_lat)))
            lat = center_lat + degrees(delta_lat)
            lng = center_lng + degrees(delta_lng)
            points.append((lat, lng))
    return points

# --- FONCTION UTILITAIRE : Charger et sauvegarder la progression ---
import json as _json
def save_progress_resume(filename, idx):
    with open(filename, 'w') as f:
        _json.dump({'last_point_idx': idx}, f)

def load_progress_resume(filename):
    try:
        with open(filename, 'r') as f:
            data = _json.load(f)
            return data.get('last_point_idx', 0)
    except Exception:
        return 0

# --- NOUVELLE FONCTION PRINCIPALE DE COLLECTE AVEC QUADRILLAGE PAR POINT ---
def collect_places_grid(categories_to_process, google_types_by_cat, lat, lng, radius, grid_spacing, limit, resume=False, progress_file="progress_resume.json"):
    grid_points = generate_grid_points(lat, lng, radius, spacing_m=grid_spacing)
    logger.info(f"Quadrillage de la zone : {len(grid_points)} points à explorer (espacement {grid_spacing}m)")
    all_places = []
    seen_place_ids = set()
    start_idx = 0
    if resume:
        start_idx = load_progress_resume(progress_file)
        logger.info(f"Mode reprise activé : reprise à partir du point {start_idx+1}/{len(grid_points)}")
    for idx, (grid_lat, grid_lng) in enumerate(grid_points):
        if idx < start_idx:
            continue  # Déjà traité
        logger.info(f"Traitement du point {idx+1}/{len(grid_points)} : {grid_lat:.5f}, {grid_lng:.5f}")
        for category_name in categories_to_process:
            google_types = google_types_by_cat[category_name]
            for type_name in google_types:
                cache_collection = db.PlacesCache
                cache_key = f"{grid_lat}_{grid_lng}_{radius}_{type_name}"
                cached_results = cache_collection.find_one({"key": cache_key})
                if cached_results and (datetime.now() - cached_results["timestamp"]).days < 7:
                    logger.info(f"Utilisation des résultats en cache pour {type_name} @ {grid_lat:.5f},{grid_lng:.5f}")
                    place_results = cached_results["results"]
                else:
                    logger.info(f"Récupération des lieux de type {type_name} depuis Google Maps API @ {grid_lat:.5f},{grid_lng:.5f}")
                    url = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={grid_lat},{grid_lng}&radius={radius}&type={type_name}&key={GOOGLE_MAPS_API_KEY}"
                    all_results = []
                    next_page_token = None
                    for _ in range(3):  # Jusqu'à 3 pages (max 60 résultats)
                        if next_page_token:
                            url_page = url + f"&pagetoken={next_page_token}"
                            response = requests.get(url_page)
                        else:
                            response = requests.get(url)
                        data = response.json()
                        if 'results' in data:
                            all_results.extend(data['results'])
                        next_page_token = data.get('next_page_token')
                        if not next_page_token:
                            break
                        time.sleep(2)
                    place_results = all_results
                    cache_collection.update_one(
                        {"key": cache_key},
                        {"$set": {
                            "key": cache_key,
                            "results": place_results,
                            "timestamp": datetime.now()
                        }},
                        upsert=True
                    )
                logger.info(f"{len(place_results)} résultats bruts récupérés pour {type_name} @ {grid_lat:.5f},{grid_lng:.5f}")
                for place in place_results:
                    # Vérifier la limite AVANT d'ajouter le lieu
                    if limit and len(all_places) >= limit:
                        logger.info(f"Limite exacte de {limit} lieux atteinte. Arrêt au point {idx+1}/{len(grid_points)} ({grid_lat:.5f},{grid_lng:.5f}).")
                        save_progress_resume(progress_file, idx) # Sauvegarder où on s'est arrêté
                        return all_places # Arrêter immédiatement la collecte

                    name = place.get("name", "").strip()
                    place_id = place.get("place_id")
                    if not place_id or place_id in seen_place_ids:
                        continue  # Éviter les doublons
                    sous_categorie = determiner_sous_categorie(name, category_name)
                    all_places.append({
                        "place_id": place_id,
                        "name": name,
                        "address": place.get("vicinity"),
                        "gps_coordinates": place.get("geometry", {}).get("location"),
                        "rating": place.get("rating"),
                        "user_ratings_total": place.get("user_ratings_total"),
                        "category": category_name,
                        "sous_categorie": sous_categorie,
                        "google_type": type_name,
                        "photos": place.get("photos", [])
                    })
                    seen_place_ids.add(place_id)
        # Sauvegarder la progression à chaque point
        save_progress_resume(progress_file, idx)
    logger.info(f"{len(all_places)} lieux uniques trouvés pour la zone.")
    return all_places

def determiner_sous_categorie(place_name, category_name):
    """Détermine la sous-catégorie en fonction du nom du lieu et de la catégorie principale."""
    place_name_lower = place_name.lower()
    
    for sous_cat in CATEGORIES[category_name]["sous_categories"]:
        sous_cat_mots = sous_cat.lower().split()
        # Vérifier si un des mots-clés de la sous-catégorie est dans le nom du lieu
        if any(mot in place_name_lower for mot in sous_cat_mots):
            return sous_cat
    
    # Par défaut, retourner la première sous-catégorie
    return CATEGORIES[category_name]["sous_categories"][0]

def extract_place_details(place_id):
    """Récupère les détails d'un lieu depuis Google Maps, y compris les avis, website, phone et horaires."""
    # Vérifier si déjà en cache
    cache_collection = db.PlaceDetailsCache
    cached_place = cache_collection.find_one({"place_id": place_id})
    
    if cached_place and (datetime.now() - cached_place["timestamp"]).days < 7:
        logger.info(f"Utilisation des détails en cache pour {place_id}")
        return cached_place["details"]
    
    # Retrait de 'photos' des champs demandés pour économiser les quotas
    fields = "name,formatted_address,rating,reviews,website,formatted_phone_number,opening_hours,geometry,place_id,user_ratings_total"
    url = f"https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&fields={fields}&key={GOOGLE_MAPS_API_KEY}&language=fr"
    
    try:
        response = requests.get(url)
        data = response.json()
        
        if "result" in data:
            place_details = data["result"]
            
            # Log des champs spécifiques souvent manquants
            log_msg = f"Détails extraits pour {place_id}: "
            log_msg += f"Website={'Oui' if 'website' in place_details else 'Non'}, "
            log_msg += f"Phone={'Oui' if 'formatted_phone_number' in place_details else 'Non'}, "
            log_msg += f"Hours={'Oui' if 'opening_hours' in place_details else 'Non'}"
            logger.info(log_msg)
            
            # Mise en cache des détails
            cache_collection.update_one(
                {"place_id": place_id},
                {"$set": {
                    "place_id": place_id,
                    "details": place_details,
                    "timestamp": datetime.now()
                }},
                upsert=True
            )
            
            return place_details
        
        return None
    except Exception as e:
        logger.error(f"Erreur lors de la requête API: {e}")
        return None

def extract_reviews(place_details):
    """Extrait et analyse les avis d'un lieu depuis les détails Google Maps."""
    reviews = []
    
    if not place_details or "reviews" not in place_details:
        return reviews
    
    for review in place_details["reviews"][:10]:  # Limite à 10 avis
        sentiment = analyzer.polarity_scores(review["text"])['compound']
        sentiment_label = "Positif" if sentiment >= 0.05 else "Négatif" if sentiment <= -0.05 else "Neutre"
        reviews.append({
            "source": "Google Maps",
            "author_name": review.get("author_name", ""),
            "text": review["text"],
            "sentiment": sentiment_label,
            "sentiment_score": sentiment,
            "rating": review.get("rating", 0),
            "time": review.get("time", 0)
        })
    
    return reviews

def extract_photo_urls(place_details, max_photos=5):
    """Extrait les URLs des photos depuis les détails Google Maps."""
    photos = []
    
    if not place_details or "photos" not in place_details:
        return photos
    
    for photo in place_details["photos"][:max_photos]:
        if "photo_reference" in photo:
            photo_url = f"https://maps.googleapis.com/maps/api/place/photo?maxwidth=800&photoreference={photo['photo_reference']}&key={GOOGLE_MAPS_API_KEY}"
            photos.append(photo_url)
    
    return photos

def search_links_bing(name, city="Paris"):
    """
    Recherche une URL Tripadvisor via Bing pour un établissement
    Implémentation améliorée pour extraire correctement les URLs
    
    Args:
        name: Nom de l'établissement
        city: Ville (par défaut Paris)
        
    Returns:
        URL Tripadvisor ou None si rien n'est trouvé
    """
    # Nettoyage du nom et de la ville
    name = name.strip()
    city = city.strip()
    
    # Construction de la requête Bing
    query = f"{name} {city} site:tripadvisor.fr"
    search_url = f"https://www.bing.com/search?q={requests.utils.quote(query)}"
    
    logger.info(f"Recherche Bing pour Tripadvisor: {query}")
    
    try:
        # Headers réalistes pour éviter d'être bloqué
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Referer": "https://www.bing.com/",
            "Sec-Ch-Ua": '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1"
        }
        
        response = requests.get(search_url, headers=headers, timeout=10)
        
        if response.status_code != 200:
            logger.error(f"Échec de la requête Bing: code {response.status_code}")
            return None
        
        # Parser les résultats
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Méthode 1: Chercher dans les éléments <a> avec href
        tripadvisor_urls = []
        
        # Chercher d'abord les citations directes
        for cite in soup.find_all('cite'):
            cite_text = cite.text.strip()
            if 'tripadvisor.fr' in cite_text:
                url_parts = cite_text.split()
                for part in url_parts:
                    if part.startswith('https://') or part.startswith('http://'):
                        if 'tripadvisor.fr' in part and ('Restaurant_Review' in part or 'Attraction_Review' in part or 'Hotel_Review' in part):
                            tripadvisor_urls.append(part)
        
        # Ensuite chercher dans les liens
        for link in soup.find_all('a'):
            href = link.get('href', '')
            # Chercher des URLs Tripadvisor dans href
            if 'tripadvisor.fr' in href and ('bing.com/ck' in href or '/search?q=' not in href):
                # Soit extraire directement le href
                if 'http' in href and 'tripadvisor.fr' in href:
                    tripadvisor_urls.append(href)
                # Soit chercher dans le texte de l'attribut data-url si disponible
                elif link.get('data-url') and 'tripadvisor.fr' in link.get('data-url'):
                    tripadvisor_urls.append(link.get('data-url'))
                # Soit chercher dans le texte du lien
                elif 'tripadvisor.fr' in link.text:
                    url_match = re.search(r'(https?://(?:www\.)?tripadvisor\.fr/[^\s]+)', link.text)
                    if url_match:
                        tripadvisor_urls.append(url_match.group(1))
        
        # Méthode 2: Extraire les URLs avec regex du HTML complet (fallback)
        if not tripadvisor_urls:
            # Regex pour trouver des URLs Tripadvisor
            url_pattern = r'(https?://(?:www\.)?tripadvisor\.fr/(?:Restaurant|Attraction|Hotel)_Review[^"\s\'&]+)'
            urls = re.findall(url_pattern, response.text)
            tripadvisor_urls.extend(urls)
        
        # Nettoyer et trier les URLs
        cleaned_urls = []
        for url in tripadvisor_urls:
            # Nettoyer l'URL (enlever les paramètres et fragments)
            clean_url = url.split('#')[0].split('?')[0]
            if clean_url not in cleaned_urls:
                cleaned_urls.append(clean_url)
        
        # Filtrer pour ne garder que les URLs de type Review
        review_urls = [url for url in cleaned_urls if 'Review' in url]
        
        if review_urls:
            best_url = review_urls[0]  # Prendre la première URL
            logger.info(f"URL Tripadvisor trouvée pour {name}: {best_url}")
            return best_url
        
        logger.warning(f"Aucune URL Tripadvisor trouvée pour {name} {city}")
        return None
            
    except Exception as e:
        logger.error(f"Erreur lors de la recherche Bing: {e}")
        return None

def find_tripadvisor_url(place_name, address=None):
    """Version simplifiée pour trouver l'URL Tripadvisor d'un lieu."""
    # Extraire la ville de l'adresse ou utiliser Paris par défaut
    city = "Paris"
    if address:
        address_parts = address.split(',')
        if len(address_parts) > 1:
            city = address_parts[-1].strip()
    
    # Rechercher l'URL directement via Bing
    return search_links_bing(place_name, city)

def extract_google_maps_reviews(place_id):
    """
    Extrait les commentaires directement depuis l'API Google Maps.
    
    Args:
        place_id: Identifiant Google Maps du lieu
        
    Returns:
        list: Liste des textes des commentaires
    """
    logger.info(f"Extraction des commentaires Google Maps pour place_id: {place_id}")
    
    try:
        place_details = extract_place_details(place_id)
        
        if not place_details or "reviews" not in place_details:
            logger.warning(f"Aucun commentaire trouvé dans les détails de place_id: {place_id}")
            return []
        
        comments = []
        for review in place_details.get("reviews", []):
            if "text" in review and review["text"].strip():
                comments.append(review["text"].strip())
        
        logger.info(f"Extraction réussie: {len(comments)} commentaires trouvés")
        return comments
        
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction des commentaires Google Maps: {e}")
        return []

def extract_place_screenshot(place_id, place_name):
    """
    Génère une capture d'écran du lieu via Google Maps avec Chrome headless
    en utilisant la même méthode que billetreduc_shotgun_mistral.py
    
    Args:
        place_id: Identifiant Google Maps du lieu
        place_name: Nom du lieu pour le log
        
    Returns:
        URL de l'image en format data:image/jpeg;base64 ou URL statique
    """
    try:
        # Vérification du cache
        cache_collection = db.ScreenshotCache
        cache_key = place_id
        cached_screenshot = cache_collection.find_one({"place_id": cache_key})
        
        if cached_screenshot and cached_screenshot.get("screenshot_url"):
            logger.info(f"Utilisation du screenshot en cache pour {place_name}")
            return cached_screenshot.get("screenshot_url")
            
        # Construire l'URL Google Maps
        maps_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}"
        
        # En mode test, on utilise une URL statique si spécifié
        if '--test' in sys.argv and not '--real-screenshots' in sys.argv:
            static_url = f"https://maps.googleapis.com/maps/api/staticmap?center=place_id:{place_id}&zoom=17&size=800x600&maptype=roadmap&markers=color:red%7Cplace_id:{place_id}&key={GOOGLE_MAPS_API_KEY}"
            
            try:
                # Télécharger l'image et la convertir en base64
                import base64 as b64_module  # Renommer l'import pour éviter la confusion
                response = requests.get(static_url, timeout=10)
                if response.status_code == 200:
                    image_base64 = b64_module.b64encode(response.content).decode('utf-8')
                    data_url = f"data:image/jpeg;base64,{image_base64}"
                    
                    # Mise en cache
                    cache_collection.update_one(
                        {"place_id": cache_key},
                        {"$set": {
                            "place_id": cache_key,
                            "screenshot_url": data_url,
                            "is_test_data": True,
                            "timestamp": datetime.now()
                        }},
                        upsert=True
                    )
                    
                    logger.info(f"Image statique convertie en base64 pour {place_name}")
                    return data_url
            except Exception as e:
                logger.error(f"Erreur lors de la conversion de l'image statique: {e}")
                return static_url
        
        # MÉTHODE DE BILLETREDUC: Utiliser Selenium pour naviguer et capturer l'écran
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from webdriver_manager.chrome import ChromeDriverManager
        import time
        from PIL import Image
        import io
        import base64 as b64_module  # Correction de la référence base64
        
        # Options Chrome - EXACTEMENT comme dans billetreduc
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1280,800")
        
        # Ne pas ajouter trop d'options qui peuvent causer des problèmes
        chrome_options.add_argument("--lang=fr-FR")
        chrome_options.add_argument("--mute-audio")
        
        # Génération d'un ID unique pour ce screenshot
        screenshot_id = hashlib.md5(f"{place_id}_{place_name}".encode()).hexdigest()[:10]
        
        # Créer le répertoire des images s'il n'existe pas
        workspace_dir = os.path.dirname(os.path.abspath(__file__))
        image_dir = os.path.join(workspace_dir, "venue_images")
        os.makedirs(image_dir, exist_ok=True)
        
        # Chemins des fichiers
        screenshot_path = os.path.join(image_dir, f"maps_raw_{screenshot_id}.png")
        cropped_path = os.path.join(image_dir, f"maps_{screenshot_id}.jpg")
        
        # Initialisation du driver
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        
        try:
            # Définir un timeout plus long
            driver.set_page_load_timeout(30)
            
            logger.info(f"Navigation vers Google Maps pour {place_name}: {maps_url}")
            driver.get(maps_url)
            
            # Attendre que la page se charge (10 secondes max)
            time.sleep(2)
            
            # IMPORTANT: Gestion des cookies EXACTEMENT comme dans billetreduc_shotgun_mistral.py
            try:
                # Recherche et acceptation de la boîte de dialogue des cookies
                logger.info("Recherche et acceptation de la boîte de dialogue des cookies")
                
                try:
                    # Utiliser des doubles quotes pour l'expression XPath qui contient des apostrophes
                    consent = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, '//button[contains(., "Accept") or contains(., "Accepter") or contains(., "J\'accepte")]')) # Corrected XPath
                    )
                    consent.click()
                    logger.info("Consentement cookies accepté")
                    time.sleep(3)
                except Exception as cookie_err:
                    logger.info(f"Pas de bandeau de cookies détecté ou erreur: {cookie_err}")
            except Exception as e:
                logger.warning(f"Impossible d'accepter les cookies: {e}")
            
            # Attendre que la carte se charge complètement
            time.sleep(5)
            
            # Capturer l'écran entier
            logger.info("Capture de l'écran complet")
            driver.save_screenshot(screenshot_path)
            logger.info(f"Screenshot brut capturé et sauvegardé: {screenshot_path}")
            
            # Ouvrir l'image pour la recadrer
            img = Image.open(screenshot_path)
            
            # Coordonnées du crop (à ajuster si nécessaire selon la mise en page de Google Maps)
            left = 72
            top = 70
            right = 350
            bottom = 230
            
            crop_box = (left, top, right, bottom)
            logger.info(f"Recadrage de l'image: {crop_box}")
            cropped_img = img.crop(crop_box)
            
            # Sauvegarder l'image recadrée
            cropped_img.save(cropped_path, format="JPEG", quality=90)
            logger.info(f"Image recadrée sauvegardée: {cropped_path}")
            
            # Convertir l'image en base64 pour l'application web
            buffer = io.BytesIO()
            cropped_img.save(buffer, format="JPEG", quality=90)
            buffer.seek(0)
            img_base64 = b64_module.b64encode(buffer.read()).decode()
            data_url = f"data:image/jpeg;base64,{img_base64}"
            
            # Mise en cache
            cache_collection.update_one(
                {"place_id": cache_key},
                {"$set": {
                    "place_id": cache_key,
                    "screenshot_url": data_url,
                    "local_path": cropped_path,
                    "timestamp": datetime.now()
                }},
                upsert=True
            )
            
            logger.info(f"Screenshot converti en base64 pour {place_name}")
            return data_url
            
        except Exception as e:
            logger.error(f"Erreur lors de la capture d'écran pour {place_name}: {e}")
            
            # Fallback vers l'URL statique en cas d'erreur
            static_url = f"https://maps.googleapis.com/maps/api/staticmap?center=place_id:{place_id}&zoom=17&size=800x600&maptype=roadmap&markers=color:red%7Cplace_id:{place_id}&key={GOOGLE_MAPS_API_KEY}"
            
            # Mise en cache de l'URL statique
            cache_collection.update_one(
                {"place_id": cache_key},
                {"$set": {
                    "place_id": cache_key,
                    "screenshot_url": static_url,
                    "error": str(e),
                    "timestamp": datetime.now()
                }},
                upsert=True
            )
            
            return static_url
            
        finally:
            # Fermer le navigateur
            driver.quit()
            
            # Supprimer le screenshot brut pour économiser de l'espace (facultatif)
            try:
                if os.path.exists(screenshot_path):
                    os.remove(screenshot_path)
            except:
                pass
    
    except Exception as e:
        logger.error(f"Erreur générale lors de la génération du screenshot pour {place_name}: {e}")
        # Fallback vers URL statique en cas d'erreur
        return f"https://maps.googleapis.com/maps/api/staticmap?center=place_id:{place_id}&zoom=17&size=800x600&maptype=roadmap&markers=color:red%7Cplace_id:{place_id}&key={GOOGLE_MAPS_API_KEY}"


# Fonction d'aide pour décider quelle méthode de screenshot utiliser
def get_place_screenshot(place_id, place_name):
    """
    Récupère un screenshot en utilisant la méthode appropriée selon les options
    """
    # Vérifier si l'option billetreduc est active
    if '--billetreduc-screenshots' in sys.argv:
        logger.info(f"Utilisation de la méthode billetreduc pour {place_name}")
        return extract_screenshot_billetreduc_method(place_id, place_name)
    else:
        # Méthode standard
        return extract_place_screenshot(place_id, place_name)

def screenshot_photo(driver, prefix, max_retries=2):
    """
    Capture la photo principale du lieu sur Google Maps
    
    Args:
        driver: WebDriver Selenium
        prefix: Préfixe pour le nom du fichier
        max_retries: Nombre maximum de tentatives
    
    Returns:
        Tuple (chemin de l'image, version base64, image PIL)
    """
    last_error = None
    for attempt in range(max_retries):
        try:
            if attempt > 0 and DEBUG_MODE:
                print(f"  ↳ Tentative {attempt + 1}/{max_retries} de capture photo")
            
            # Attendre que la page soit complètement chargée
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "DUwDvf"))
            )
            
            # Prendre la capture d'écran
            screenshot = driver.get_screenshot_as_png()
            image = Image.open(BytesIO(screenshot))
            
            # Coordonnées du crop (à ajuster si nécessaire selon la mise en page de Google Maps)
            left = 30
            top = 70
            right = 330
            bottom = 230
            
            cropped = image.crop((left, top, right, bottom))
            
            # Vérifier que l'image n'est pas vide ou trop petite
            if cropped.size[0] < 100 or cropped.size[1] < 100:
                raise ValueError("Image trop petite, possible erreur de capture")
            
            path = f"{prefix}_photo.png"
            cropped.save(path)
            
            if DEBUG_MODE and attempt > 0:
                print(f"  ↳ Capture photo réussie après {attempt + 1} tentative(s)")
            
            return path, encode_image_base64(cropped), cropped
            
        except Exception as e:
            last_error = e
            if DEBUG_MODE:
                print(f"⚠️ Échec de capture photo (tentative {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2)  # Pause avant nouvelle tentative
                continue
    
    raise RuntimeError(f"Impossible de capturer la photo après {max_retries} tentatives") from last_error

def main():
    """Fonction principale pour l'exécution du script"""
    # Récupérer les arguments
    if args.place_id and args.place_name:
        logger.info(f"Mode test avec place_id: {args.place_id}, nom: {args.place_name}")
        # Construire un objet place factice
        place = {
            "place_id": args.place_id,
            "name": args.place_name,
            "category": "Soins esthétiques et bien-être",
            "sous_categorie": "Institut de beauté"
        }
        # Traiter ce lieu spécifique
        processed_place = process_place(place)
        if processed_place:
            logger.info(f"Traitement réussi pour {args.place_name}")
            return [processed_place]
        else:
            logger.error(f"Échec du traitement pour {args.place_name}")
            return []
    elif args.area:
        # Obtenir les coordonnées GPS de la zone
        from geopy.geocoders import Nominatim
        from geopy.exc import GeocoderTimedOut
        
        # Configurer geocoder avec un timeout plus long et activer le cache
        geolocator = Nominatim(user_agent="wellness_script", timeout=10)
        
        # Fonction avec retries pour la géolocalisation
        def geocode_with_retry(query, max_retries=3):
            for attempt in range(max_retries):
                try:
                    logger.info(f"Tentative de géolocalisation ({attempt+1}/{max_retries}) : {query}")
                    return geolocator.geocode(query)
                except GeocoderTimedOut:
                    logger.warning(f"Timeout lors de la géolocalisation (tentative {attempt+1}/{max_retries})")
                    if attempt < max_retries - 1:
                        time.sleep(2)  # Pause avant nouvelle tentative
                    else:
                        logger.error(f"Toutes les tentatives de géolocalisation ont échoué pour {query}")
                        return None
        
        # Essayer d'abord avec format exact
        location = geocode_with_retry(args.area)
        
        # Si échec, essayer avec un format alternatif
        if not location and "arrondissement" in args.area.lower():
            # Essayer format alternatif: "Paris 4e" au lieu de "Paris, 4ème arrondissement"
            alt_query = args.area.replace("ème arrondissement", "e").replace(", ", " ")
            logger.info(f"Tentative avec format alternatif: {alt_query}")
            location = geocode_with_retry(alt_query)
            
        # Dernier essai avec format très simplifié
        if not location:
            simple_query = args.area.split(",")[0].strip()
            if simple_query != args.area:
                logger.info(f"Tentative avec format simplifié: {simple_query}")
                location = geocode_with_retry(simple_query)
                
        if not location:
            logger.error(f"Zone introuvable: {args.area}")
            return []
        
        logger.info(f"Zone trouvée: {args.area} à {location.latitude}, {location.longitude}")
        
        # Déterminer les catégories à traiter
        categories_to_process = []
        if args.categories:
            category_names = args.categories.split(',')
            for name in category_names:
                if name.strip() in CATEGORIES:
                    categories_to_process.append(name.strip())
        else:
            # Utiliser toutes les catégories par défaut
            categories_to_process = list(CATEGORIES.keys())
        
        logger.info(f"Catégories à traiter: {categories_to_process}")
        
        # Nouvelle collecte par quadrillage point par point, toutes catégories confondues
        google_types_by_cat = {cat: CATEGORIES[cat]["google_types"] for cat in categories_to_process}
        all_places = collect_places_grid(
            categories_to_process,
            google_types_by_cat,
            location.latitude,
            location.longitude,
            args.radius,
            grid_spacing=500,
            limit=args.limit,
            resume=getattr(args, 'resume', False),
            progress_file="progress_resume.json"
        )
        
        logger.info(f"Traitement de {len(all_places)} établissements")
        
        # Traiter chaque lieu
        processed_places = []
        for place in all_places:
            logger.info(f"Traitement de: {place['name']}")
            processed_place = process_place(place)
            if processed_place:
                processed_places.append(processed_place)
        
        logger.info(f"Terminé! {len(processed_places)}/{len(all_places)} lieux traités.")
        return processed_places
    else:
        # Mode par défaut
        return test_small_zone()

def analyze_reviews_with_mistral(reviews, category):
    """Analyse les reviews avec OpenAI GPT-3.5-turbo en fonction de la catégorie pour obtenir uniquement des notes"""
    def normalize_critere(s):
        s = unicodedata.normalize('NFKD', s)
        s = ''.join(c for c in s if not unicodedata.combining(c))
        s = s.lower().replace(' ', '').replace('-', '').replace('_', '')
        return s
    try:
        review_texts = []
        for review in reviews:
            if "text" in review and review["text"].strip():
                review_texts.append(f"- {review['text'].strip()}")
        logger.info(f"Nombre de reviews transmises à GPT pour analyse : {len(review_texts)}")
        # Sécuriser la récupération des critères d'évaluation
        try:
            criteria = CATEGORIES[category]["criteres_evaluation"]
        except KeyError:
            logger.error(f"Catégorie inconnue pour l'analyse GPT : {category}")
            # Prendre la première catégorie par défaut
            criteria = list(CATEGORIES.values())[0]["criteres_evaluation"]
        if not review_texts:
            logger.warning("Aucun texte de review à analyser")
            return {c: 2.5 for c in criteria}
        reviews_text = "\n".join(review_texts)
        
        # Générer la définition de chaque critère avec les mots-clés
        criteres_def = []
        for crit in criteria:
            mots = MOTS_CLES.get(category, {}).get(crit, [])
            if mots:
                criteres_def.append(f"- {crit} : {', '.join(mots)}")
            else:
                criteres_def.append(f"- {crit}")
        criteres_def_text = "\n".join(criteres_def)
        
        # Prompt amélioré et plus directif
        prompt = f"""Voici des avis clients pour un établissement de type {category}.

CRITÈRES À ÉVALUER:
{criteres_def_text}

AVIS À ANALYSER:
{reviews_text}

INSTRUCTIONS:
1. Analyse les avis et attribue une note sur 5 pour CHACUN des critères listés ci-dessus.
2. Base-toi sur l'ensemble des avis, même si un critère n'est pas explicitement mentionné.
3. Si tu ne peux absolument pas juger un critère, attribue la note par défaut 2.5.
4. Ne donne pas la même note à tous les critères sauf si c'est absolument justifié par les avis.
5. Utilise EXACTEMENT le format suivant pour ta réponse:

{criteria[0]} : [NOTE]
{criteria[1]} : [NOTE]
{criteria[2] if len(criteria) > 2 else ''}{"" if len(criteria) <= 2 else " : [NOTE]"}
{criteria[3] if len(criteria) > 3 else ''}{"" if len(criteria) <= 3 else " : [NOTE]"}
{criteria[4] if len(criteria) > 4 else ''}{"" if len(criteria) <= 4 else " : [NOTE]"}

ATTENTION: Respecte scrupuleusement ce format avec uniquement les critères demandés et leurs notes.
"""
        logger.info(f"Prompt global envoyé à GPT :\n{prompt}")
        response = generate_ai_response_openai(prompt, max_tokens=600, temperature=0.1)
        time.sleep(1)
        logger.info(f"Réponse brute reçue de GPT :\n{response}")
        if not response:
            logger.error("Erreur lors de l'analyse des reviews")
            return {c: 2.5 for c in criteria}
            
        # Parsing amélioré des notes
        notes = {}
        missing_criteria = []
        
        # Création d'un dictionnaire de correspondance pour les critères normalisés
        normalized_criteria = {normalize_critere(crit): crit for crit in criteria}
        
        # Parsing ligne par ligne
        for line in response.strip().split('\n'):
            line = line.strip()
            if ':' in line:
                aspect, note_str = line.split(':', 1)
                aspect = aspect.strip()
                
                # Normaliser l'aspect pour la comparaison
                aspect_norm = normalize_critere(aspect)
                
                # Trouver le critère correspondant
                matching_criterion = None
                for norm_crit, original_crit in normalized_criteria.items():
                    if norm_crit in aspect_norm or aspect_norm in norm_crit:
                        matching_criterion = original_crit
                        break
                
                try:
                    # Extraction de la note (accepte plusieurs formats)
                    note_str = note_str.strip()
                    # Recherche d'un nombre flottant dans la chaîne
                    match = re.search(r'(\d+[.,]?\d*)', note_str)
                    if match:
                        note = float(match.group(1).replace(',', '.'))
                        if 0 <= note <= 5:
                            if matching_criterion:
                                notes[matching_criterion] = note
                            else:
                                # Tenter une correspondance approximative si pas de match exact
                                closest_match = None
                                min_distance = float('inf')
                                for norm_crit, original_crit in normalized_criteria.items():
                                    distance = Levenshtein.distance(aspect_norm, norm_crit)
                                    if distance < min_distance and distance <= 3:  # Seuil de distance
                                        min_distance = distance
                                        closest_match = original_crit
                                
                                if closest_match:
                                    notes[closest_match] = note
                                    logger.info(f"Correspondance approximative trouvée: '{aspect}' -> '{closest_match}'")
                except Exception as e:
                    logger.warning(f"Erreur lors du parsing de la note pour '{aspect}': {e}")
                    continue
        
        # Vérification des critères manquants
        for criterion in criteria:
            if criterion not in notes:
                notes[criterion] = 2.5
                missing_criteria.append(criterion)
                
        if missing_criteria:
            logger.warning(f"Critères non trouvés dans la réponse GPT (note par défaut 2.5): {', '.join(missing_criteria)}")
            
        # Calcul de la moyenne
        if notes:
            notes_sum = sum(notes.values())
            notes_count = len(notes)
            notes["average_score"] = round(notes_sum / notes_count, 2)
            
        return notes
    except Exception as e:
        logger.error(f"Erreur lors de l'analyse des reviews: {e}")
        traceback.print_exc()
        return {c: 2.5 for c in criteria}

# --- AJOUT : Fonction process_place robuste ---
def process_place(place):
    """
    Traite un lieu et prépare ses données selon le schéma WellnessPlaceSchema pour MongoDB.
    Retourne le dictionnaire « place_data » prêt à être inséré / mis à jour, ou None en cas d'échec.
    """
    try:
        
        place_id = place.get("place_id")
        place_name = place.get("name", "Lieu inconnu")
        logger.info(f"--- Début traitement: {place_name} ({place_id}) ---")

        # ------------------------------------------------------------------
        # 1. Récupération du lien Tripadvisor
        # ------------------------------------------------------------------
        tripadvisor_url = find_tripadvisor_url(place_name, place.get("address"))

        # ------------------------------------------------------------------
        # 2. Récupération des détails via Google Places
        # ------------------------------------------------------------------
        details = extract_place_details(place_id)
        if not details:
            logger.error(
                f"Impossible de récupérer les détails pour {place_name}. Traitement annulé."
            )
            return None

        # ------------------------------------------------------------------
        # 3. Construction de la structure de base
        # ------------------------------------------------------------------
        place_data = {
            "place_id": place_id,
            "name": details.get("name", place_name),
            "category": place.get("google_type", "other"),
        }

        # ------------------------------------------------------------------
        # 4. Récupération des commentaires Google Maps + Tripadvisor
        # ------------------------------------------------------------------
        comments: list[dict[str, str]] = []

        # Google Maps
        google_reviews = extract_google_maps_reviews(place_id)
        if google_reviews:
            comments.extend({"source": "Google Maps", "text": txt} for txt in google_reviews)

        # Tripadvisor
        if tripadvisor_url:
            tripadvisor_comments = get_full_tripadvisor_comments(tripadvisor_url)
            if tripadvisor_comments:
                comments.extend(
                    {"source": "Tripadvisor", "text": txt} for txt in tripadvisor_comments
                )

        # Stockage des commentaires / URL pour utilisation future
        if comments:
            place_data["comments"] = comments
        if tripadvisor_url:
            place_data["tripadvisor_url"] = tripadvisor_url

        # ------------------------------------------------------------------
        # 5. Informations de localisation
        # ------------------------------------------------------------------
        location_info = {"type": "Point", "coordinates": None}

        # Coordonnées géographiques
        g_loc = details.get("geometry", {}).get("location", {})
        if g_loc:
            location_info["coordinates"] = [g_loc.get("lng"), g_loc.get("lat")]

        # Adresse détaillée
        address_details = parse_full_address(details.get("formatted_address"))
        location_info.update(
            {
                "address": address_details.get("address"),
                "city": address_details.get("city"),
                "postal_code": address_details.get("postal_code"),
                "country": address_details.get("country"),
            }
        )

        if (
            location_info["coordinates"]
            and None not in location_info["coordinates"]
        ):
            place_data["location"] = location_info

        # ------------------------------------------------------------------
        # 6. Informations de contact
        # ------------------------------------------------------------------
        contact_info = {
            "phone": details.get("formatted_phone_number"),
            "email": None,  # Non fourni par Google API
            "website": details.get("website"),
            "social_media": {"facebook": None, "instagram": None, "twitter": None},
        }
        if contact_info["phone"] or contact_info["website"]:
            place_data["contact"] = contact_info

        # ------------------------------------------------------------------
        # 7. Horaires d'ouverture
        # ------------------------------------------------------------------
        if details.get("opening_hours"):
            place_data["business_hours"] = parse_opening_hours(details["opening_hours"])

        # ------------------------------------------------------------------
        # 8. Initialisation des champs optionnels
        # ------------------------------------------------------------------
        place_data["services"] = []
        place_data["images"] = []
        place_data["profile_photo"] = None

        # ------------------------------------------------------------------
        # 9. Notes Google brutes
        # ------------------------------------------------------------------
        if "rating" in details:
            place_data["rating"] = {
                "average": details.get("rating", 0),
                "count": details.get("user_ratings_total", 0),
            }
        else:
            place_data["rating"] = {"average": 0, "count": 0}

        # ------------------------------------------------------------------
        # 10. Analyse des reviews via GPT / Mistral
        # ------------------------------------------------------------------
        gpt_notes = None # Initialiser à None
        if comments:
            # On passe la catégorie globale (ex: place["category"]) si disponible, sinon une valeur par défaut
            gpt_category = place.get("category", "Soins esthétiques et bien-être")
            if gpt_category not in CATEGORIES:
                # Essayer de retrouver la catégorie globale à partir du type Google
                google_type = place.get("google_type")
                for cat_name, cat_info in CATEGORIES.items():
                    if google_type in cat_info.get("google_types", []):
                        gpt_category = cat_name
                        break
            gpt_notes = analyze_reviews_with_mistral(comments, gpt_category)
            # *** AJOUT DE LA LIGNE CI-DESSOUS ***
            if gpt_notes: # S'assurer qu'on a bien des notes
                place_data["criteria_ratings"] = gpt_notes

        # ------------------------------------------------------------------
        # 11. Génération de description via OpenAI
        # ------------------------------------------------------------------
        logger.info(f"Génération de description AI pour {place_name}")
        place_data["description"] = generate_place_description(place)

        # ------------------------------------------------------------------
        # 12. Génération du screenshot principal pour l'UI
        # ------------------------------------------------------------------
        try:
            place_data["profile_photo"] = get_place_screenshot(place_id, place_name)
        except Exception as e:
            logger.error(f"Erreur lors de la génération du screenshot pour {place_name}: {e}")
            place_data["profile_photo"] = None

        # ------------------------------------------------------------------
        # 13. Sauvegarde dans MongoDB
        # ------------------------------------------------------------------
        save_to_mongo(place_data)
        logger.info(f"--- Fin traitement: {place_name} ---")

        return place_data

    except Exception as e:  # noqa: BLE001
        logger.error(f"Erreur lors du traitement de {place.get('name', 'Lieu inconnu')}: {e}")
        traceback.print_exc()
        return None

# --- AJOUT : Fonction de sauvegarde MongoDB robuste ---
def save_to_mongo(place):
    """Insère ou met à jour un lieu dans la collection BeautyPlaces de MongoDB selon le schéma WellnessPlaceSchema."""
    try:
        if not place or "place_id" not in place:
            logger.warning("Données de lieu invalides ou place_id manquant, impossible de sauvegarder")
            return False
            
        # S'assurer que tous les champs essentiels sont présents
        required_fields = ["name", "category", "location", "description"]
        for field in required_fields:
            if field not in place:
                logger.warning(f"Champ {field} manquant dans les données du lieu")
                if field == "description":
                    # Générer une description par défaut si manquante
                    place["description"] = f"Établissement de {place.get('category', 'beauté et bien-être')} situé à {place.get('location', {}).get('city', 'Paris')}."
        
        collection = db["BeautyPlaces"]
        existing = collection.find_one({"place_id": place["place_id"]})
        
        if existing:
            # Mise à jour
            logger.info(f"Mise à jour de '{place.get('name', 'Lieu inconnu')}' dans MongoDB")
            logger.debug(f"Champs disponibles pour mise à jour: {', '.join(place.keys())}")
            
            # Exclure _id de la mise à jour s'il existe
            update_data = {k: v for k, v in place.items() if k != '_id'}
            collection.update_one(
                {"_id": existing["_id"]}, 
                {"$set": update_data}
            )
        else:
            # Nouvel enregistrement
            logger.info(f"Ajout de '{place.get('name', 'Lieu inconnu')}' dans MongoDB")
            collection.insert_one(place)
            
        return True
    except Exception as e:
        logger.error(f"Erreur lors de la sauvegarde du lieu {place.get('name', 'inconnu')}: {e}")
        traceback.print_exc()
        return False
# --- FIN AJOUT ---

def parse_full_address(address_string):
    """Tente de parser une adresse complète en composants (adresse, ville, code postal, pays)."""
    if not address_string:
        return {"address": None, "city": None, "postal_code": None, "country": "France"} # Default France

    parts = [part.strip() for part in address_string.split(',')]
    address = parts[0] if parts else None
    city = None
    postal_code = None
    country = "France" # Default

    if len(parts) > 1:
        # Recherche du code postal et de la ville dans les dernières parties
        for part in reversed(parts[1:]):
            # Regex pour code postal français (5 chiffres)
            cp_match = re.search(r'\b(\d{5})\b', part)
            if cp_match and not postal_code:
                postal_code = cp_match.group(1)
                # Essayer d'extraire la ville de cette partie
                city_part = re.sub(r'\b\d{5}\b', '', part).strip()
                if city_part and not city:
                    city = city_part
            elif not city: # Si pas de code postal trouvé, la dernière partie est probablement la ville
                 # Ignorer si c'est juste "France"
                 if part.lower() != "france":
                     city = part

    # Si la ville est toujours manquante mais qu'on a le code postal, essayer de l'isoler
    if not city and postal_code and len(parts) > 1:
        potential_city_part = re.sub(r'\b\d{5}\b', '', parts[-1]).strip()
        if potential_city_part:
            city = potential_city_part
        elif len(parts) > 2: # Essayer l'avant-dernière partie
             potential_city_part = re.sub(r'\b\d{5}\b', '', parts[-2]).strip()
             if potential_city_part:
                city = potential_city_part

    # Si la ville est toujours "Paris" dans l'adresse mais pas extraite, la forcer
    if not city and postal_code and postal_code.startswith('75') and 'paris' in address_string.lower():
        city = 'Paris'

    # Nettoyage final de l'adresse principale
    if city and city in address:
        address = address.replace(city, '').strip().rstrip(',')
    if postal_code and postal_code in address:
         address = address.replace(postal_code, '').strip().rstrip(',')

    return {
        "address": address,
        "city": city,
        "postal_code": postal_code,
        "country": country
    }

def parse_opening_hours(opening_hours_data):
    """Convertit les données opening_hours de Google API au format du schéma."""
    if not opening_hours_data or "periods" not in opening_hours_data:
        return None

    days_map = {0: "sunday", 1: "monday", 2: "tuesday", 3: "wednesday", 4: "thursday", 5: "friday", 6: "saturday"}
    business_hours = {day: {"open": None, "close": None} for day in days_map.values()}

    for period in opening_hours_data["periods"]:
        open_info = period.get("open")
        close_info = period.get("close")

        if open_info and "day" in open_info and "time" in open_info:
            day_index = open_info["day"]
            day_name = days_map.get(day_index)
            if day_name:
                # Formater l'heure HHMM en HH:MM
                open_time = open_info["time"]
                open_time_formatted = f"{open_time[:2]}:{open_time[2:]}"

                # Si plusieurs horaires pour un jour, on prend le premier trouvé (simplification)
                if business_hours[day_name]["open"] is None:
                     business_hours[day_name]["open"] = open_time_formatted

                if close_info and "time" in close_info:
                    close_time = close_info["time"]
                    close_time_formatted = f"{close_time[:2]}:{close_time[2:]}"
                    if business_hours[day_name]["close"] is None:
                         business_hours[day_name]["close"] = close_time_formatted
                else: # Cas ouvert 24h (rare pour ce secteur, mais possible)
                    if business_hours[day_name]["close"] is None:
                         business_hours[day_name]["close"] = "24:00" # Approximation

    # Mettre null pour les jours sans horaires (fermés)
    for day in business_hours:
        if business_hours[day]["open"] is None:
             business_hours[day] = None # Mettre tout le jour à null si fermé

    return business_hours

def generate_place_description(place):
    """
    Génère une description concise du lieu à l'aide de l'API OpenAI.
    Utilise les informations disponibles (nom, catégorie, avis, etc.)
    
    Args:
        place: Dictionnaire avec les informations du lieu
        
    Returns:
        str: Description générée par l'IA (max 1000 caractères)
    """
    try:
        # Extraire les informations disponibles
        name = place.get('name', '') if place else ''
        category = place.get('category', '') if place else ''
        sous_category = place.get('sous_categorie', '') if place else ''
        address = place.get('address', '') if place else ''
        # Sécuriser l'accès à location
        location = place.get('location') if place else None
        city = (location or {}).get('city', 'Paris')
        rating = place.get('rating') or (place.get('rating', {}).get('average', '') if place else '')
        
        # Services spécifiques (si disponibles)
        services = []
        if sous_category and ("soins" in sous_category.lower() or "institut" in sous_category.lower()):
            services = ["soins du visage", "soins du corps", "épilation", "manucure", "massage"]
        elif sous_category and "spa" in sous_category.lower():
            services = ["massage", "sauna", "hammam", "soins du corps", "relaxation"]
        elif (category and ("coiffure" in category.lower() or "salon" in category.lower())):
            services = ["coupe", "coloration", "coiffage", "soins capillaires"]
        
        # Extraire quelques avis pour le contexte
        comment_texts = []
        if place and 'comments' in place and place['comments']:
            for comment in place['comments'][:3]:  # Max 3 commentaires pour le contexte
                if isinstance(comment, dict) and 'text' in comment:
                    comment_texts.append(comment['text'][:200] + '...' if len(comment['text']) > 200 else comment['text'])
        
        comments_str = "\n".join([f"- {c}" for c in comment_texts])
        
        # Préparer la partie des commentaires
        avis_section = ""
        if comment_texts:
            avis_section = "Quelques avis clients :\n" + comments_str + "\n\n"
        
        # Construire le prompt sans f-string imbriqué
        prompt = f"""Rédige une description attrayante et informative pour un établissement de beauté ou bien-être en 3-4 phrases (maximum 1000 caractères).

Informations sur l'établissement :
- Nom : {name}
- Catégorie : {category}
- Type : {sous_category if sous_category else category}
- Adresse : {address}, {city}
- Note : {rating}
- Services potentiels : {', '.join(services) if services else 'non spécifiés'}

{avis_section}La description doit être écrite avec un ton professionnel mais chaleureux, en français, et mettre en valeur l'établissement. N'invente pas d'informations spécifiques comme les horaires, les prix exacts ou des services très spécifiques qui ne sont pas mentionnés. Évite tout cliché ou lieu commun sur le bien-être ou la beauté.

Description (max 1000 caractères) :"""

        # Appel à l'API OpenAI
        logger.info(f"Génération de description pour {name}")
        description = generate_ai_response_openai(prompt, max_tokens=300, temperature=0.7)
        
        # Nettoyer et limiter la description
        if description:
            description = description.strip()
            if len(description) > 1000:
                description = description[:997] + "..."
            logger.info(f"Description générée pour {name}: {len(description)} caractères")
            return description
        
        # Description par défaut si échec
        default_desc = f"{sous_category if sous_category else category} situé à {city}."
        if services:
            default_desc += f" Offre des services tels que {', '.join(services[:3])}."
        return default_desc
        
    except Exception as e:
        logger.error(f"Erreur lors de la génération de description: {e}")
        city = 'Paris'
        try:
            location = place.get('location') if place else None
            city = (location or {}).get('city', 'Paris')
        except Exception:
            pass
        return f"Établissement de bien-être et de beauté situé à {city}."

# Point d'entrée principal
if __name__ == "__main__":
    try:
        # Configuration des arguments de ligne de commande
        parser = argparse.ArgumentParser(description="Script de scraping pour les établissements de beauté et bien-être")
        parser.add_argument("--test", action="store_true", help="Mode test")
        parser.add_argument("--real-screenshots", action="store_true", help="Activer les screenshots réels")
        parser.add_argument("--billetreduc-screenshots", action="store_true", help="Activer les screenshots avec billetreduc")
        parser.add_argument("--force-analysis", action="store_true", help="Forcer l'analyse avec Mistral")
        parser.add_argument("--place-id", type=str, help="ID du lieu à scraper")
        parser.add_argument("--place-name", type=str, help="Nom du lieu à scraper")
        parser.add_argument("--area", type=str, help="Zone à analyser (ex: 'Paris, France')")
        parser.add_argument("--radius", type=int, default=1000, help="Rayon de recherche en mètres")
        parser.add_argument("--categories", type=str, help="Catégories à analyser, séparées par des virgules")
        parser.add_argument("--limit", type=int, default=5, help="Nombre maximum de lieux à traiter")
        parser.add_argument("--mock-db", action="store_true", help="Utiliser MongoDB simulé (sans connexion réseau)")
        parser.add_argument("--resume", action="store_true", help="Reprendre la collecte à partir du dernier point traité (progress_resume.json)")
        args = parser.parse_args()

        # Configuration des paramètres globaux
        TEST_MODE = args.test
        REAL_SCREENSHOTS = args.real_screenshots
        BILLETREDUC_SCREENSHOTS = args.billetreduc_screenshots
        FORCE_ANALYSIS = args.force_analysis
        
        # Initialiser MongoDB réel ou simulé
        if args.mock_db or TEST_MODE:
            logger.info("Utilisation de MongoDB simulé (en mémoire)")
            client = MockMongoClient()
        else:
            logger.info("Connexion à MongoDB Atlas")
            client = MongoClient(MONGO_URI)
            
        # Initialiser la base de données
        db = client[DB_NAME]
        
        # Exécuter la fonction principale
        main()
        
    except Exception as error:
        logger.error(f"ERREUR CRITIQUE pendant l'exécution: {error}")
        traceback.print_exc()
        sys.exit(1)