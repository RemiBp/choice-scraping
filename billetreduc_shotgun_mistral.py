#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script combiné pour le traitement des données BilletReduc et Shotgun.
Utilise désormais GPT pour l'analyse IA (si activée).
Cette version combine les deux sources de données en un seul script pour faciliter la maintenance.
"""

import requests
from bs4 import BeautifulSoup
import re
import json
import os
from datetime import datetime
import time
import traceback  # Pour le suivi des erreurs dans run_shotgun_scraper
from pymongo import MongoClient, GEOSPHERE
from bson.objectid import ObjectId
import nest_asyncio
import asyncio
from playwright.async_api import async_playwright
import logging
import sys
import hashlib  # Pour le hachage des prompts pour le caching
import pickle   # Pour le checkpointing
import argparse # Pour les arguments en ligne de commande
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from PIL import Image

# Ajouter le chemin scripts/Restauration/ au PYTHONPATH
import sys
import os
restauration_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'Restauration')
if restauration_path not in sys.path:
    sys.path.append(restauration_path)

# ---- Configuration du logger ----
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("billetreduc_shotgun_gpt.log"), # Nom de log mis à jour
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("LoisirsEventsProcessor")

# ---- Configuration MongoDB ----
MONGO_URI = "mongodb+srv://remibarbier:Calvi8Pierc2@lieuxrestauration.szq31.mongodb.net/?retryWrites=true&w=majority&appName=lieuxrestauration"
client = MongoClient(MONGO_URI)
db_loisir = client["Loisir&Culture"]
collection_evenements = db_loisir["Loisir_Paris_Evenements"]
collection_producers = db_loisir["Loisir_Paris_Producers"]
collection_config = db_loisir["Configuration"]  # Collection pour les configurations

# ---- Création des index géospatiaux ----
try:
    collection_evenements.create_index([("location", GEOSPHERE)])
    collection_producers.create_index([("location", GEOSPHERE)])
    logger.info("Index géospatiaux 2dsphere créés ou déjà existants pour 'location'.")
except Exception as e:
    logger.error(f"Erreur lors de la création des index géospatiaux : {e}")

# ---- Configuration API de géocodage et Google Places ----
GEOCODING_API_URL = "https://maps.googleapis.com/maps/api/geocode/json"
PLACES_API_URL = "https://maps.googleapis.com/maps/api/place"
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

# ---- Configuration pour Selenium (remplacement de l'API Google Maps) ----
CHROME_OPTIONS = webdriver.ChromeOptions()
CHROME_OPTIONS.binary_location = "/usr/bin/google-chrome"
CHROME_OPTIONS.add_argument("--no-sandbox")
CHROME_OPTIONS.add_argument("--disable-dev-shm-usage")
CHROME_OPTIONS.add_argument("--disable-gpu")
CHROME_OPTIONS.add_argument("--disable-software-rasterizer")
CHROME_OPTIONS.add_argument("--disable-background-networking")
CHROME_OPTIONS.add_argument("--disable-default-apps")
CHROME_OPTIONS.add_argument("--disable-extensions")
CHROME_OPTIONS.add_argument("--disable-sync")
CHROME_OPTIONS.add_argument("--metrics-recording-only")
CHROME_OPTIONS.add_argument("--mute-audio")
CHROME_OPTIONS.add_argument("--headless=new")

# ---- URL par défaut pour les images de lieux ----
DEFAULT_IMAGE_URL = "https://via.placeholder.com/800x600.png?text=Venue+Image+Not+Available"

# ---- Configuration pour le stockage des images ----
# Utilisation de ImgBB comme service de stockage gratuit
IMGBB_API_KEY = os.getenv("IMGBB_API_KEY")  # Clé API gratuite avec limite d'upload
IMGBB_UPLOAD_URL = "https://api.imgbb.com/1/upload"

# ---- Configuration des sources de données ----
SOURCES = {
    "BILLETREDUC": "billetreduc",
    "SHOTGUN": "shotgun"
}

# ---- Mappage des catégories pour standardisation ----
CATEGORY_MAPPING = {
    # Mappage général pour standardiser les catégories
    "default": "Autre",
    
    # Catégories Shotgun -> Standard
    "deep": "Musique » Électronique",
    "techno": "Musique » Électronique",
    "house": "Musique » Électronique",
    "hip hop": "Musique » Hip-Hop",
    "rap": "Musique » Hip-Hop",
    "rock": "Musique » Rock",
    "indie": "Musique » Indie",
    "pop": "Musique » Pop",
    "jazz": "Musique » Jazz",
    "soul": "Musique » Soul",
    "funk": "Musique » Funk",
    "dj set": "Musique » DJ Set",
    "club": "Musique » Club",
    "festival": "Festival",
    "concert": "Concert",
    "live": "Concert",
    "comédie": "Théâtre » Comédie",
    "spectacle": "Spectacles",
    "danse": "Spectacles » Danse",
    "exposition": "Exposition",
    "conférence": "Conférence",
    "stand-up": "Spectacles » One-man-show",
    "one-man-show": "Spectacles » One-man-show",
    "théâtre": "Théâtre",
    "cinéma": "Cinéma",
    "projection": "Cinéma",
}

# Liste des catégories principales pour la carte
MAIN_CATEGORIES = [
    "Théâtre",
    "Musique",
    "Spectacles",
    "Cinéma",
    "Exposition",
    "Festival",
    "Concert",
    "Conférence"
]

# ---- Mappings pour la traduction des dates ----
JOURS_FR_EN = {
    "lundi": "Monday", "mardi": "Tuesday", "mercredi": "Wednesday",
    "jeudi": "Thursday", "vendredi": "Friday", "samedi": "Saturday", "dimanche": "Sunday"
}
MOIS_FR_EN = {
    "janvier": "January", "février": "February", "mars": "March", "avril": "April",
    "mai": "May", "juin": "June", "juillet": "July", "août": "August",
    "septembre": "September", "octobre": "October", "novembre": "November", "décembre": "December"
}

# Mappings des abréviations des mois
MOIS_ABBR_FR = {
    "janv.": "janvier", "févr.": "février", "mars": "mars", "avr.": "avril",
    "mai": "mai", "juin": "juin", "juil.": "juillet", "août": "août",
    "sept.": "septembre", "oct.": "octobre", "nov.": "novembre", "déc.": "décembre"
}

# ---- Shotgun scraping settings ----
MAX_SHOTGUN_PAGES = 10  # Maximum number of venue pages to scrape
SHOTGUN_BASE_URL = "https://shotgun.live"
SHOTGUN_VENUES_URL_TEMPLATE = "https://shotgun.live/fr/venues/-/france/{page_num}"

# ---- Mappings pour l'analyse AI par catégorie ----
CATEGORY_MAPPINGS = {
    "Théâtre": {
        "aspects": ["mise en scène", "jeu des acteurs", "texte", "scénographie"],
        "emotions": ["intense", "émouvant", "captivant", "enrichissant", "profond"]
    },
    "Théâtre contemporain": {
        "aspects": ["mise en scène", "jeu des acteurs", "texte", "originalité", "message"],
        "emotions": ["provocant", "dérangeant", "stimulant", "actuel", "profond"]
    },
    "Comédie": {
        "aspects": ["humour", "jeu des acteurs", "rythme", "dialogue"],
        "emotions": ["drôle", "amusant", "divertissant", "léger", "enjoué"]
    },
    "Spectacle musical": {
        "aspects": ["performance musicale", "mise en scène", "chant", "chorégraphie"],
        "emotions": ["entraînant", "mélodieux", "festif", "rythmé", "touchant"]
    },
    "One-man-show": {
        "aspects": ["humour", "présence scénique", "texte", "interaction"],
        "emotions": ["drôle", "mordant", "spontané", "énergique", "incisif"]
    },
    "Concert": {
        "aspects": ["performance", "répertoire", "son", "ambiance"],
        "emotions": ["électrisant", "envoûtant", "festif", "énergique", "intense"]
    },
    "Musique électronique": {
        "aspects": ["dj", "ambiance", "son", "rythme"],
        "emotions": ["festif", "énergique", "immersif", "exaltant", "hypnotique"]
    },
    "Danse": {
        "aspects": ["chorégraphie", "technique", "expressivité", "musique"],
        "emotions": ["gracieux", "puissant", "fluide", "émouvant", "esthétique"]
    },
    "Cirque": {
        "aspects": ["performance", "mise en scène", "acrobaties", "créativité"],
        "emotions": ["impressionnant", "magique", "époustouflant", "spectaculaire", "poétique"]
    },
    "Default": {  # Catégorie par défaut si non reconnue
        "aspects": ["qualité générale", "intérêt", "originalité"],
        "emotions": ["agréable", "intéressant", "divertissant", "satisfaisant"]
    }
}

# ---- Fonctions utilitaires ----
def standardize_category(category):
    """
    Standardise les catégories pour assurer une cohérence entre les sources
    """
    if not category or category == "Catégorie non disponible":
        return CATEGORY_MAPPING["default"]
        
    # Recherche exacte
    if category in CATEGORY_MAPPING:
        return CATEGORY_MAPPING[category]
        
    # Recherche partielle (mots-clés dans la catégorie)
    category_lower = category.lower()
    for key, value in CATEGORY_MAPPING.items():
        if key.lower() in category_lower:
            return value
            
    # Recherche dans les catégories principales
    for main_cat in MAIN_CATEGORIES:
        if main_cat.lower() in category_lower:
            return main_cat
            
    # Par défaut, retourner la catégorie originale
    return category

def extract_main_category(category):
    """
    Extrait la catégorie principale (pour le filtrage sur la carte)
    """
    if not category or category == "Catégorie non disponible":
        return CATEGORY_MAPPING["default"]
        
    # Si la catégorie contient un séparateur (comme "Théâtre » Comédie")
    if "»" in category:
        return category.split("»")[0].strip()
        
    # Vérifier si la catégorie correspond à une principale
    for main_cat in MAIN_CATEGORIES:
        if main_cat.lower() in category.lower():
            return main_cat
            
    return CATEGORY_MAPPING["default"]

# ---- Fonctions de gestion des flags pour l'exécution AI ----
def check_ai_processed():
    """Vérifie si l'analyse AI a déjà été effectuée"""
    config = collection_config.find_one({"type": "billetreduc_ai_status"})
    if config and config.get("ai_processed", False):
        logger.info("L'analyse AI a déjà été effectuée le %s", config.get("processed_date", "date inconnue"))
        return True
    logger.info("L'analyse AI n'a pas encore été effectuée")
    return False

def mark_ai_processed():
    """Marque l'analyse AI comme effectuée"""
    collection_config.update_one(
        {"type": "billetreduc_ai_status"},
        {"$set": {
            "ai_processed": True, 
            "processed_date": datetime.now()
        }},
        upsert=True
    )
    logger.info("Traitement AI marqué comme effectué")

# ---- Fonctions pour le traitement des dates ----
def translate_date_to_english(date_text):
    """Traduit les jours et mois français en anglais"""
    # Remplacer d'abord les abréviations par les noms complets
    for abbr, full in MOIS_ABBR_FR.items():
        date_text = date_text.replace(abbr, full)
        
    # Traduire les jours et mois en anglais
    for jour_fr, jour_en in JOURS_FR_EN.items():
        date_text = date_text.replace(jour_fr, jour_en)
    for mois_fr, mois_en in MOIS_FR_EN.items():
        date_text = date_text.replace(mois_fr, mois_en)
    return date_text

def format_dates(date_text):
    """Formate les dates extraites de différentes sources"""
    date_text = date_text.strip().lower()
    
    # Format "Du [date] au [date]"
    if "au" in date_text:
        match = re.search(r"Du (\w+ \d+ \w+ \d+) au (\w+ \d+ \w+ \d+)", date_text)
        if match:
            start_date_str = translate_date_to_english(match.group(1))
            end_date_str = translate_date_to_english(match.group(2))
            try:
                start_date = datetime.strptime(start_date_str, "%A %d %B %Y").strftime("%d/%m/%Y")
                end_date = datetime.strptime(end_date_str, "%A %d %B %Y").strftime("%d/%m/%Y")
                return f"{start_date} au {end_date}"
            except ValueError:
                pass
    
    # Format "mer 12 févr." (Shotgun)
    shotgun_pattern = r"(\w+) (\d+) (\w+\.?)"
    match = re.search(shotgun_pattern, date_text)
    if match:
        day_of_week = match.group(1)
        day_num = match.group(2)
        month = match.group(3)
        
        # Compléter l'année si non précisée
        current_year = datetime.now().year
        
        # Traduire en anglais pour le parsing
        full_date_fr = f"{day_of_week} {day_num} {month} {current_year}"
        full_date_en = translate_date_to_english(full_date_fr)
        
        try:
            # Essayer différents formats en fonction de l'input
            formats_to_try = [
                "%A %d %B %Y",  # Wednesday 12 February 2025
                "%a %d %B %Y",  # Wed 12 February 2025
            ]
            
            parsed_date = None
            for fmt in formats_to_try:
                try:
                    parsed_date = datetime.strptime(full_date_en, fmt)
                    break
                except ValueError:
                    continue
                    
            if parsed_date:
                return parsed_date.strftime("%d/%m/%Y")
        except Exception as e:
            logger.warning(f"Erreur lors du parsing de la date {full_date_en}: {e}")
    
    # Si aucun format reconnu, retourner tel quel
    return date_text.strip()

def parse_horaires(horaires_text):
    """
    Analyse et extrait les horaires depuis différentes sources
    """
    if not horaires_text:
        return []
        
    horaires = []
    
    # Format Shotgun: "mer 12 févr."
    shotgun_pattern = r"(\w+) (\d+) (\w+\.?)"
    match = re.search(shotgun_pattern, horaires_text)
    if match:
        jour = match.group(1)
        # Convertir le jour en format complet
        jours_map = {
            "lun": "lundi", "mar": "mardi", "mer": "mercredi", 
            "jeu": "jeudi", "ven": "vendredi", "sam": "samedi", 
            "dim": "dimanche"
        }
        jour_complet = jours_map.get(jour, jour)
        
        # Essayer de trouver l'heure dans le texte
        heure_pattern = r"(\d{1,2})[h:](\d{2})?"
        heure_match = re.search(heure_pattern, horaires_text)
        heure = "20h00"  # Heure par défaut
        if heure_match:
            h = heure_match.group(1)
            m = heure_match.group(2) or "00"
            heure = f"{h}h{m}"
            
        horaires.append({"jour": jour_complet, "heure": heure})
    
    # Si aucun format reconnu et pas d'horaires créés, ajouter un horaire par défaut
    if not horaires:
        horaires.append({"jour": "vendredi", "heure": "20h00"})
        
    return horaires

# ---- Fonctions pour la géolocalisation et récupération de photos ----
def get_coordinates_from_address(address):
    """Obtient les coordonnées géographiques à partir d'une adresse"""
    try:
        # Si vide ou trop court, impossible de géocoder correctement
        if not address or len(address) < 5:
            logger.warning(f"Adresse trop courte ou manquante: '{address}'")
            return None
            
        # ---- Google Geocoding API call removed ----
        # The original script used Google Geocoding API here.
        # This has been removed to minimize API costs as requested.
        # Consider using Selenium-based search or another geocoding method if needed.
        logger.warning(f"Google Geocoding API call skipped for address: {address}. Returning None.")
        # Returning None to indicate coordinates were not fetched via this method.
        # Downstream code should ideally use fetch_place_details_and_photos_selenium
        # or extract_location_from_lieu_name as fallbacks.
        return None

        # --- Original Google API Call Logic (commented out for reference) ---
        # params = {
        #     "address": address,
        #     "key": GOOGLE_MAPS_API_KEY
        # }
        # response = requests.get(GEOCODING_API_URL, params=params)
        # response.raise_for_status()
        # data = response.json()
        # if data["status"] == "OK" and len(data["results"]) > 0:
        #     location = data["results"][0]["geometry"]["location"]
        #     return {
        #         "type": "Point",  # Format GeoJSON pour MongoDB
        #         "coordinates": [location["lng"], location["lat"]]  # Longitude, Latitude
        #     }
        # else:
        #     logger.warning("Échec du géocodage pour l'adresse : %s (%s)", address, data.get("status"))
        #     return None
        # --- End of Original Logic ---

    except Exception as e:
        logger.error("Erreur inattendue dans get_coordinates_from_address pour %s : %s", address, e)
        return None

def is_similar(lieu, nom_api):
    """Détermine si le nom de lieu est similaire au nom retourné par l'API."""
    from difflib import SequenceMatcher
    
    lieu_clean = re.sub(r'\W+', '', lieu.lower())
    nom_clean = re.sub(r'\W+', '', nom_api.lower())
    matcher = SequenceMatcher(None, lieu_clean, nom_clean)
    return matcher.ratio() > 0.5 or lieu_clean in nom_clean

def get_photo_urls(photo_data):
    """Récupère les URLs des photos depuis l'API Google Places."""
    if not photo_data:
        return []
    return [
        f"{PLACES_API_URL}/photo?maxwidth=800&photoreference={photo['photo_reference']}&key={GOOGLE_MAPS_API_KEY}"
        for photo in photo_data
    ]

# URL d'images de placeholder thématiques pour divers types de lieux
VENUE_TYPE_IMAGES = {
    "théâtre": "https://images.unsplash.com/photo-1503095396549-807759245b35?q=80&w=2071&auto=format&fit=crop",
    "cinéma": "https://images.unsplash.com/photo-1489599849927-2ee91cede3ba?q=80&w=2070&auto=format&fit=crop",
    "salle de concert": "https://images.unsplash.com/photo-1514525253161-7a46d19cd819?q=80&w=2074&auto=format&fit=crop",
    "musée": "https://images.unsplash.com/photo-1566127992631-137a642a90f4?q=80&w=2070&auto=format&fit=crop",
    "bar": "https://images.unsplash.com/photo-1514933651103-005eec06c04b?q=80&w=1974&auto=format&fit=crop",
    "club": "https://images.unsplash.com/photo-1571702168369-e9418d49cce2?q=80&w=2070&auto=format&fit=crop",
    "default": "https://images.unsplash.com/photo-1603190287605-e6ade32fa852?q=80&w=2070&auto=format&fit=crop"
}

def get_venue_image_url(lieu_name, image_path=None):
    """
    Génère une URL d'image fiable pour un lieu en utilisant soit une image locale
    convertie en base64, soit une image thématique d'Unsplash selon le type de lieu.
    
    Args:
        lieu_name: Nom du lieu pour déterminer le type de venue
        image_path: Chemin local vers l'image (optionnel)
        
    Returns:
        URL d'image utilisable directement dans l'application
    """
    # Si un chemin d'image local est fourni, essayer d'encoder en base64
    if image_path and os.path.exists(image_path):
        try:
            # Vérifier la taille de l'image (limite à 2MB pour être pratique)
            file_size = os.path.getsize(image_path) / (1024 * 1024)  # Taille en MB
            if file_size > 2:
                logger.warning(f"Image trop volumineuse ({file_size:.1f}MB), utilisation d'une image Unsplash")
            else:
                # Ouvrir et redimensionner l'image pour réduire la taille
                img = Image.open(image_path)
                max_size = (800, 600)  # Taille maximale raisonnable
                img.thumbnail(max_size, Image.Resampling.LANCZOS)
                
                # Convertir en RGB si nécessaire (pour les images avec transparence)
                if img.mode in ('RGBA', 'LA'):
                    background = Image.new(img.mode[:-1], img.size, (255, 255, 255))
                    background.paste(img, img.split()[-1])
                    img = background
                    
                # Sauvegarder en mémoire en format JPEG
                import io
                import base64
                buffer = io.BytesIO()
                img.save(buffer, format="JPEG", quality=90)
                buffer.seek(0)
                
                # Encoder en base64 pour une URL data
                img_str = base64.b64encode(buffer.read()).decode()
                data_url = f"data:image/jpeg;base64,{img_str}"
                
                logger.info(f"Image convertie en URL data pour: {lieu_name}")
                return data_url
        except Exception as e:
            logger.error(f"Erreur lors de la conversion de l'image en base64: {e}")
    
    # Si la conversion échoue ou si aucune image n'est fournie, utiliser une image thématique
    # Déterminer le type de lieu basé sur le nom
    lieu_lower = lieu_name.lower()
    
    # Correspondance avec les types de lieux connus
    for venue_type, url in VENUE_TYPE_IMAGES.items():
        if venue_type in lieu_lower:
            logger.info(f"Utilisation d'une image Unsplash de type '{venue_type}' pour: {lieu_name}")
            return url
    
    # Par défaut, utiliser une image générique de lieu
    logger.info(f"Utilisation de l'image par défaut pour: {lieu_name}")
    return VENUE_TYPE_IMAGES["default"]

def fetch_place_details_and_photos_selenium(lieu_name, address=None):
    """
    Recherche des informations sur un lieu via Selenium sur Google Maps,
    incluant capture d'écran et extraction de photo.
    Convertit directement l'image en URL data pour une accessibilité sans dépendance.

    Args:
        lieu_name: Nom du lieu à rechercher
        address: Adresse optionnelle pour préciser la recherche
        
    Returns:
        Dict contenant les détails du lieu et l'URL de la photo
        Utilise une image thématique ou par défaut en cas d'échec
    """
    driver = None
    try:
        logger.info(f"Recherche du lieu '{lieu_name}' via Selenium")
        
        # Construction de la requête (plus précise si une adresse est fournie)
        query = lieu_name
        if address and len(address) > 5 and address != "Adresse non disponible":
            query = f"{lieu_name} {address}"
            
        # Initialiser le driver Chrome
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=CHROME_OPTIONS)
        
        # Accéder à Google Maps avec la recherche
        maps_url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
        driver.get(maps_url)
        time.sleep(7)  # Attendre le chargement complet
        
        # Gérer les cookies si nécessaire
        try:
            # Utiliser des doubles quotes pour l'expression XPath qui contient des apostrophes
            consent = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, '//button[contains(., "Accept") or contains(., "Accepter") or contains(., "J\'accepte")]'))
            )
            consent.click()
            logger.info("Consentement cookies accepté")
            time.sleep(3)
        except Exception as cookie_err:
            logger.info(f"Pas de bandeau de cookies détecté ou erreur: {cookie_err}")
            
        # Extraire les informations basiques
        wait = WebDriverWait(driver, 10)
        name = lieu_name  # Par défaut, utiliser le nom fourni
        rating = "Non trouvée"
        
        try:
            name_element = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "DUwDvf")))
            name = name_element.text
        except Exception as name_err:
            logger.warning(f"Erreur lors de l'extraction du nom: {name_err}")
            
        try:
            rating_element = driver.find_element(By.CLASS_NAME, "F7nice")
            rating = rating_element.text
        except Exception as rating_err:
            logger.warning(f"Erreur lors de l'extraction de la note: {rating_err}")
        
        # Créer le répertoire pour les images si nécessaire
        workspace_dir = os.path.dirname(os.path.abspath(__file__))
        image_dir = os.path.join(workspace_dir, "venue_images")
        os.makedirs(image_dir, exist_ok=True)
        
        # Capturer et sauvegarder le screenshot
        venue_id = hashlib.md5(lieu_name.encode()).hexdigest()[:10]
        screenshot_path = os.path.join(image_dir, f"maps_{venue_id}.png")
        driver.save_screenshot(screenshot_path)
        logger.info(f"Screenshot pris: {screenshot_path}")
        
        # Traitement de l'image: crop de la partie contenant l'image du lieu
        img = Image.open(screenshot_path)
        
        # Coordonnées du crop (à ajuster si nécessaire selon la mise en page de Google Maps)
        left = 30
        top = 70
        right = 330
        bottom = 230
        
        cropped_img = img.crop((left, top, right, bottom))
        cropped_path = os.path.join(image_dir, f"lieu_{venue_id}.png")
        cropped_img.save(cropped_path)
        logger.info(f"Image croppée sauvegardée: {cropped_path}")
        
        # Utiliser la nouvelle fonction pour obtenir une URL d'image fiable
        image_url = get_venue_image_url(lieu_name, cropped_path)
        
        # Construire et retourner les détails du lieu
        place_details = {
            "name": name,
            "address": address or "Adresse extraite de la recherche",
            "rating": rating,
            "maps_url": maps_url,
            "photos": [image_url],
            "profile_image": image_url,
            "image": image_url,  # Champ "image" standardisé pour tous les lieux
            "photo": image_url   # Pour compatibilité avec le backend leisureProducers.js
        }
        
        logger.info(f"Informations extraites pour '{lieu_name}': Nom={name}, Note={rating}")
        return place_details
        
    except Exception as e:
        logger.error(f"Erreur lors de la recherche Selenium pour '{lieu_name}': {e}")
        # Obtenir une URL d'image thématique basée sur le type de lieu
        image_url = get_venue_image_url(lieu_name)
        
        # Retourner un dictionnaire avec l'image thématique
        return {
            "name": lieu_name,
            "address": address or "Adresse non disponible",
            "rating": "Non trouvée",
            "maps_url": f"https://www.google.com/maps/search/{lieu_name.replace(' ', '+')}",
            "photos": [image_url],
            "profile_image": image_url,
            "image": image_url,
            "photo": image_url
        }
        
    finally:
        if driver:
            driver.quit()

def extract_location_from_lieu_name(lieu_name):
    """
    Tente d'extraire une localisation approximative à partir du nom du lieu.
    Utilisé comme dernier recours lorsque l'adresse est manquante.
    """
    PARIS_LOCATION = {
        "type": "Point",
        "coordinates": [2.3522, 48.8566]  # Coordonnées de Paris
    }
    
    # Si lieu contient un arrondissement parisien, utiliser les coordonnées approximatives
    if "paris" in lieu_name.lower():
        arr_match = re.search(r"paris.*?(\d{1,2})", lieu_name.lower())
        if arr_match:
            arr = int(arr_match.group(1))
            # Coordonnées approximatives des arrondissements parisiens
            # Ceci est une simplification, mais suffit pour l'affichage sur la carte
            ARR_COORDS = {
                1: [2.3417, 48.8626], 2: [2.3470, 48.8697], 3: [2.3615, 48.8639],
                4: [2.3542, 48.8553], 5: [2.3470, 48.8448], 6: [2.3359, 48.8487],
                7: [2.3137, 48.8560], 8: [2.3124, 48.8725], 9: [2.3387, 48.8740],
                10: [2.3551, 48.8790], 11: [2.3800, 48.8580], 12: [2.3956, 48.8394],
                13: [2.3554, 48.8315], 14: [2.3264, 48.8339], 15: [2.2966, 48.8417],
                16: [2.2635, 48.8583], 17: [2.3127, 48.8825], 18: [2.3431, 48.8917],
                19: [2.3824, 48.8817], 20: [2.4010, 48.8654]
            }
            if arr in ARR_COORDS:
                return {
                    "type": "Point",
                    "coordinates": ARR_COORDS[arr]
                }
    
    # Si pas d'arrondissement spécifique, utiliser les coordonnées de Paris
    return PARIS_LOCATION

# ---- Fonctions de scraping ----
def scrape_lieu_details(lien_lieu):
    """Scrape les détails d'un lieu depuis sa page web"""
    try:
        # Vérifier si le lien est valide
        if not lien_lieu or lien_lieu == "Lien non disponible":
            return {
                "adresse": "Adresse non disponible",
                "description": "Description non disponible",
                "lien_lieu": lien_lieu
            }
            
        # Parser l'URL pour déterminer la source
        source = SOURCES["BILLETREDUC"]  # Par défaut
        if "shotgun" in lien_lieu.lower():
            source = SOURCES["SHOTGUN"]
            
        if source == SOURCES["BILLETREDUC"]:
            return scrape_billetreduc_lieu(lien_lieu)
        elif source == SOURCES["SHOTGUN"]:
            return scrape_shotgun_lieu(lien_lieu)
        else:
            return {
                "adresse": "Source non prise en charge",
                "description": "Source non prise en charge",
                "lien_lieu": lien_lieu
            }
    except Exception as e:
        logger.error("Erreur lors du scraping de %s : %s", lien_lieu, e)
        return {
            "adresse": "Erreur lors du scraping",
            "description": "Erreur lors du scraping",
            "lien_lieu": lien_lieu
        }
        
def scrape_billetreduc_lieu(lien_lieu):
    """Scrape les détails d'un lieu depuis BilletReduc"""
    try:
        response = requests.get(lien_lieu)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Récupération de l'adresse
        h5_elements = soup.find_all('h5')
        adresse = h5_elements[1].text.strip() if len(h5_elements) > 1 else "Adresse non disponible"
        
        # Récupération de la description
        description_elem = soup.find('h6')
        description = description_elem.text.strip() if description_elem else "Description non disponible"
        

        return {
            "adresse": adresse,
            "description": description,
            "lien_lieu": lien_lieu,
            "source": SOURCES["BILLETREDUC"]
        }

    except Exception as e:
        logger.error(f"Erreur lors du scraping BilletReduc: {e}")
        return {
            "adresse": adresse,
            "description": description,
            "lien_lieu": lien_lieu,
            "source": SOURCES["BILLETREDUC"]
        }
        
def extract_venue_urls(base_url, page_url):
    """Récupère les URLs des lieux à partir des balises <a> ayant la classe ciblée."""
    try:
        response = requests.get(page_url)
        if response.status_code != 200:
            logger.warning(f"Échec de la récupération de la page: {page_url} (status: {response.status_code})")
            return []

        soup = BeautifulSoup(response.text, 'html.parser')
        elements = soup.find_all(class_="bg-card flex items-center gap-4 rounded-sm p-6")
        hrefs = [re.search(r'href="([^"]+)"', str(elem)).group(1) for elem in elements if re.search(r'href="([^"]+)"', str(elem))]

        venue_urls = [base_url.rstrip("/") + "/" + href.lstrip("/") for href in hrefs]
        logger.info(f"Extraction de {len(venue_urls)} URLs de lieux depuis {page_url}")
        return venue_urls
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction des URLs: {e}")
        return []

def scrape_venue_details(page_url):
    """Récupère les informations détaillées d'un lieu et ses événements avec leurs URLs exacts."""
    try:
        response = requests.get(page_url)
        if response.status_code != 200:
            logger.warning(f"Échec de la récupération du lieu: {page_url} (status: {response.status_code})")
            return {}

        soup = BeautifulSoup(response.text, 'html.parser')

        # Extraire uniquement le nom du lieu
        lieu_elements = soup.find_all(class_="text-muted-foreground")
        lieu = lieu_elements[1].text.strip() if len(lieu_elements) > 1 else "Lieu non disponible"

        # Extraire la description
        description_elem = soup.find(class_="line-clamp-3 text-balance max-md:text-center")
        description = description_elem.text.strip() if description_elem else "Description non disponible"

        # Extraire l'adresse et convertir en coordonnées
        adresse_elem = soup.find(class_="flex items-center gap-2")
        adresse = adresse_elem.text.strip() if adresse_elem else "Adresse non disponible"
        # --- Coordinates Fix Start ---
        coordinates = None
        if adresse != "Adresse non disponible":
            # Attempt to get coordinates from address (will return None now)
            coordinates = get_coordinates_from_address(adresse)
            
        # Fallback to extracting from venue name if address geocoding failed
        if not coordinates:
            logger.info(f"Coordinates not found for address '{adresse}'. Falling back to name-based extraction for '{lieu}'.")
            coordinates = extract_location_from_lieu_name(lieu)
        # --- Coordinates Fix End ---

        # Extraire l'image principale
        image_elem = soup.find(class_="aspect-square h-full w-full bg-black object-contain")
        main_image = image_elem['src'] if image_elem and 'src' in image_elem.attrs else DEFAULT_IMAGE_URL

        # Extraire les événements et leurs liens
        events_div = soup.find("div", class_="gap grid grid-cols-1 gap-x-4 gap-y-8 sm:grid-cols-2 lg:grid-cols-3 lg:gap-x-8")

        events_list = []
        if events_div:
            event_links = [a["href"] for a in events_div.find_all("a", href=True)]
            event_names = [e.text.strip() for e in events_div.find_all(class_="line-clamp-2 text-lg font-bold leading-tight")]
            event_prices = [p.text.strip() for p in events_div.find_all(class_="text-foreground")]

            # Associer chaque nom d'événement à son URL et prix
            for index, event_name in enumerate(event_names):
                event_url = "https://shotgun.live" + event_links[index] if index < len(event_links) else "URL non disponible"
                event_image = main_image if index == 0 else DEFAULT_IMAGE_URL
                event_price = event_prices[index] if index < len(event_prices) else "Prix non disponible"

                events_list.append({
                    "intitulé": event_name,
                    "image": event_image,
                    "lien_url": event_url,
                    "catégorie": "",  # Sera rempli lors du scraping détaillé
                    "prix": event_price
                })

        venue_data = {
            "lieu": lieu,
            "adresse": adresse,
            "description": description,
            "evenements": events_list,
            "lien_lieu": page_url,
            "location": coordinates,
            "nombre_evenements": len(events_list),
            "image": main_image,
            "source": SOURCES["SHOTGUN"]
        }

        logger.info(f"Détails récupérés pour le lieu: {lieu} avec {len(events_list)} événements")
        return venue_data
    except Exception as e:
        logger.error(f"Erreur lors du scraping du lieu {page_url}: {e}")
        return {}

def save_venue_to_mongo(venue_data):
    """Insère ou met à jour un lieu dans MongoDB."""
    try:
        if not venue_data or "lieu" not in venue_data:
            logger.warning("Données de lieu invalides, impossible de sauvegarder")
            return False

        existing_venue = collection_producers.find_one({"lieu": venue_data["lieu"]})
        if existing_venue:
            collection_producers.update_one({"_id": existing_venue["_id"]}, {"$set": venue_data})
            logger.info(f"Mise à jour de '{venue_data['lieu']}' dans MongoDB.")
        else:
            collection_producers.insert_one(venue_data)
            logger.info(f"Ajout de '{venue_data['lieu']}' dans MongoDB.")
        return True
    except Exception as e:
        logger.error(f"Erreur lors de la sauvegarde du lieu {venue_data.get('lieu', 'inconnu')}: {e}")
        return False

def scrape_event_details(lien_url, lieu, lien_lieu, prix_reduit="Prix non disponible"):
    """Scrape les détails d'un événement Shotgun et les stocke dans MongoDB."""
    try:
        response = requests.get(lien_url)
        if response.status_code != 200:
            logger.warning(f"Échec de la récupération de l'événement: {lien_url} (status: {response.status_code})")
            return None

        soup = BeautifulSoup(response.text, "html.parser")

        title_elem = soup.find(class_="font-title font-black uppercase text-xl md:text-[1.75rem]")
        title = title_elem.text.strip() if title_elem else "Titre non disponible"

        schedule_elem = soup.find(class_="text-accent-foreground")
        prochaines_dates = schedule_elem.text.strip() if schedule_elem else "Dates non disponibles"

        horaires_elem = soup.find_all(class_="text-white")
        horaires = [horaire.text.strip() for horaire in horaires_elem]

        address_elem = soup.find(class_="flex-1 py-4 text-foreground")
        address = address_elem.text.strip() if address_elem else "Adresse non disponible"

        location_name_elem = soup.find(class_="text-foreground font-bold")
        location_name = location_name_elem.text.strip() if location_name_elem else lieu  # Utiliser le nom du lieu passé en paramètre si non trouvé

        details_elem = soup.find(class_="whitespace-pre-wrap break-words")
        details = details_elem.text.strip() if details_elem else "Détails non disponibles"

        category_elem = soup.find(class_="flex flex-wrap gap-2")
        category = category_elem.find(class_="text-sm tracking-wider h-11 rounded-full px-5 focus-visible:outline-hidden inline-flex cursor-pointer items-center justify-center whitespace-nowrap font-bold uppercase transition-colors disabled:pointer-events-none disabled:opacity-50 text-primary border-border hover:border-primary/20 focus:border-primary/20 border bg-transparent") if category_elem else None
        category = category.text.strip().split()[0] if category else "Catégorie non disponible"
        
        
        # Standardiser la catégorie
        standardized_category = standardize_category(category)
        main_category = extract_main_category(standardized_category)

        images = [img['src'] for img in soup.find_all('img', src=True)]
        main_image = images[0] if images else DEFAULT_IMAGE_URL

        # Extraire le lineup
        lineup = []
        lineup_elems = soup.find_all(class_="flex flex-col gap-1.5")
        lineup_images = soup.find_all("img", class_="object-cover aspect-square rounded-sm transition duration-200 hover:contrast-150")

        for i, elem in enumerate(lineup_elems):
            lineup_entry = {
                "nom": elem.text.strip(),
                "image": lineup_images[i]["src"] if i < len(lineup_images) else DEFAULT_IMAGE_URL
            }
            lineup.append(lineup_entry)

        # Extraire les horaires à partir du texte des dates
        formatted_horaires = []
        if prochaines_dates != "Dates non disponibles":
            parsed_horaires = parse_horaires(prochaines_dates)
            if parsed_horaires:
                formatted_horaires = parsed_horaires
            else:
                # Format basique si parse_horaires ne trouve rien
                formatted_horaires = [{"jour": prochaines_dates.split()[0], "heure": " - ".join(horaires) if horaires else "20h00"}]
        else:
            # Horaire par défaut si aucune date n'est trouvée
            formatted_horaires = [{"jour": "vendredi", "heure": "20h00"}]

        # Obtenir les coordonnées géographiques si non disponibles
        coordinates = get_coordinates_from_address(address) if address != "Adresse non disponible" else None
        if not coordinates:
            coordinates = extract_location_from_lieu_name(location_name)

        event_data = {
            "intitulé": title,
            "catégorie": standardized_category,
            "catégorie_principale": main_category,
            "détail": details,
            "lieu": location_name,
            "lien_lieu": lien_lieu,
            "prochaines_dates": prochaines_dates,
            "prix_reduit": prix_reduit,
            "ancien_prix": "",
            "note": "Note non disponible",
            "image": main_image,
            "site_url": lien_url,
            "purchase_url": lien_url,
            "commentaires": [],
            "catégories_prix": [],
            "location": coordinates,
            "horaires": formatted_horaires,
            "lineup": lineup,
            "source": SOURCES["SHOTGUN"]
        }

        # Ajout ou mise à jour dans la base de données des événements
        event_id = None
        existing_event = collection_evenements.find_one({"site_url": lien_url})
        if existing_event:
            collection_evenements.update_one(
                {"_id": existing_event["_id"]},
                {"$set": event_data}
            )
            event_id = existing_event["_id"]
            logger.info(f"Mise à jour de l'événement: {title}")
        else:
            result = collection_evenements.insert_one(event_data)
            event_id = result.inserted_id
            logger.info(f"Ajout de l'événement: {title}")

        # Mise à jour dans la base des producteurs si l'événement est déjà lié
        producer_doc = collection_producers.find_one(
            {"lieu": location_name, "evenements.lien_url": lien_url},
            {"evenements.$": 1}
        )

        if producer_doc:
            collection_producers.update_one(
                {"lieu": location_name, "evenements.lien_url": lien_url},
                {"$set": {
                    "evenements.$.catégorie": standardized_category,
                    "evenements.$.image": main_image,
                    "evenements.$.lien_evenement": f"/Loisir_Paris_Evenements/{event_id}"
                }}
            )
            logger.info(f"Mise à jour de l'événement '{title}' dans le producteur")
            
        return event_data
    except Exception as e:
        logger.error(f"Erreur lors du scraping de l'événement {lien_url}: {e}")
        return None

def run_shotgun_scraper(max_pages=MAX_SHOTGUN_PAGES, resume=False):
    """Exécute le processus complet de scraping Shotgun: lieux et événements."""
    try:
        logger.info(f"Démarrage du processus de scraping Shotgun à {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Charger checkpoint si disponible et reprise demandée
        checkpoint = None
        if resume:
            checkpoint = load_checkpoint(checkpoint_name="shotgun_scraping")
            
        # ÉTAPE 1: Scraping des lieux (venues)
        logger.info("\n[SCRAPING] ETAPE 1: SCRAPING DES LIEUX (VENUES)")
        
        base_url = SHOTGUN_BASE_URL
        page_num = 1
        venues_scraped = 0
        
        # Si checkpoint existe, reprendre à partir de la dernière page scrappée
        if checkpoint and "last_page" in checkpoint:
            page_num = checkpoint.get("last_page")
            venues_scraped = checkpoint.get("venues_scraped", 0)
            logger.info(f"Reprise du scraping à partir de la page {page_num}, {venues_scraped} lieux déjà traités")

        while page_num <= max_pages:
            page_url = SHOTGUN_VENUES_URL_TEMPLATE.format(page_num=page_num)
            logger.info(f"Scraping page {page_num}: {page_url}")
            
            venue_urls = extract_venue_urls(base_url, page_url)
            
            if not venue_urls:
                logger.info(f"Aucune donnée trouvée sur la page {page_num}. Fin du scraping des lieux.")
                break

            for url_idx, url in enumerate(venue_urls):
                logger.info(f"Scraping {url_idx+1}/{len(venue_urls)}: {url}")
                venue_details = scrape_venue_details(url)
                if venue_details:
                    save_venue_to_mongo(venue_details)
                    venues_scraped += 1
                
                # Sauvegarder le checkpoint après chaque lieu
                save_checkpoint({
                    "last_page": page_num,
                    "venues_scraped": venues_scraped,
                    "timestamp": datetime.now().isoformat()
                }, checkpoint_name="shotgun_scraping")
                
                time.sleep(1)  # Pause pour éviter de surcharger le serveur

            page_num += 1
            time.sleep(2)  # Pause entre les pages
            
        logger.info(f"Scraping des lieux terminé! {venues_scraped} lieux traités.")

        # ETAPE 2: Scraping détaillé des événements
        logger.info("\n[SCRAPING] ETAPE 2: SCRAPING DETAILLE DES EVENEMENTS")
        
        # Récupération des producteurs de type Shotgun
        producers_cursor = collection_producers.find(
            {"source": SOURCES["SHOTGUN"]},
            {"lieu": 1, "lien_lieu": 1, "evenements": 1}
        )
        
        events_scraped = 0
        events_total = 0
        
        # Si checkpoint existe pour les événements, reprendre à partir du dernier lieu et événement
        last_lieu = None
        last_event_url = None
        if checkpoint and "last_lieu" in checkpoint and "events_scraped" in checkpoint:
            last_lieu = checkpoint.get("last_lieu")
            last_event_url = checkpoint.get("last_event_url")
            events_scraped = checkpoint.get("events_scraped", 0)
            logger.info(f"Reprise du scraping des événements à partir de {last_lieu}, {events_scraped} événements déjà traités")
        
        # Comptage du nombre total d'événements
        for producer in producers_cursor:
            if "evenements" in producer and producer["evenements"]:
                events_total += len(producer["evenements"])
        
        logger.info(f"Total de {events_total} événements Shotgun à traiter")
        
        # Réinitialiser le curseur
        producers_cursor = collection_producers.find(
            {"source": SOURCES["SHOTGUN"]},
            {"lieu": 1, "lien_lieu": 1, "evenements": 1}
        )
        
        skip_until_lieu = False if last_lieu is None else True
        for producer in producers_cursor:
            lieu = producer.get("lieu", "Lieu non disponible")
            lien_lieu = producer.get("lien_lieu", "Lien non disponible")
            
            # Si on doit reprendre après un certain lieu, on saute jusqu'à ce qu'on le trouve
            if skip_until_lieu and lieu != last_lieu:
                continue
            elif skip_until_lieu and lieu == last_lieu:
                skip_until_lieu = False  # On a trouvé le lieu où reprendre
            
            logger.info(f"Traitement des événements pour '{lieu}'")
            
            if "evenements" not in producer or not producer["evenements"]:
                logger.info(f"Aucun événement trouvé pour '{lieu}', passage au suivant...")
                continue
                
            logger.info(f"{len(producer['evenements'])} événements trouvés")
            
            skip_until_event = False if last_event_url is None else True
            for ev in producer["evenements"]:
                lien_url = ev.get("lien_url")
                prix_reduit = ev.get("prix", "Prix non disponible")
                
                # Si on doit reprendre après un certain événement, on saute jusqu'à ce qu'on le trouve
                if skip_until_event and lien_url != last_event_url:
                    continue
                elif skip_until_event and lien_url == last_event_url:
                    skip_until_event = False  # On a trouvé l'événement où reprendre
                    continue  # On saute cet événement car déjà traité
                
                if lien_url and lien_url != "URL non disponible":
                    logger.info(f"Scraping de l'événement: {lien_url}")
                    event_data = scrape_event_details(lien_url, lieu, lien_lieu, prix_reduit)
                    if event_data:
                        events_scraped += 1
                        
                        # Sauvegarder le checkpoint après chaque événement
                        save_checkpoint({
                            "last_page": page_num,
                            "venues_scraped": venues_scraped,
                            "last_lieu": lieu,
                            "last_event_url": lien_url,
                            "events_scraped": events_scraped,
                            "timestamp": datetime.now().isoformat()
                        }, checkpoint_name="shotgun_scraping")
                    
                    time.sleep(1)  # Pause pour éviter de surcharger le serveur
            
            # Réinitialiser last_event_url après avoir traité tous les événements d'un lieu
            last_event_url = None
            # Pause après chaque lieu pour éviter de surcharger le serveur
            time.sleep(2)

        logger.info(f"Scraping des événements terminé! {events_scraped} événements traités.")
        logger.info(f"PROCESSUS COMPLET TERMINE à {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        return {"venues_scraped": venues_scraped, "events_scraped": events_scraped}

    except Exception as e:
        logger.error(f"ERREUR CRITIQUE dans le scraping Shotgun: {e}")
        # Sauvegarder le checkpoint d'erreur
        save_checkpoint({
            "error": str(e),
            "last_page": page_num if 'page_num' in locals() else 1,
            "venues_scraped": venues_scraped if 'venues_scraped' in locals() else 0,
            "last_lieu": lieu if 'lieu' in locals() else None,
            "last_event_url": lien_url if 'lien_url' in locals() else None,
            "events_scraped": events_scraped if 'events_scraped' in locals() else 0,
            "timestamp": datetime.now().isoformat(),
            "error_traceback": traceback.format_exc()
        }, checkpoint_name="shotgun_scraping_error")
        return None

def scrape_shotgun_lieu(lien_lieu):
    """Scrape les détails d'un lieu depuis Shotgun (version simplifiée pour compatibilité)"""
    try:
        # Vérifier si le lien est valide pour un scraping détaillé
        if not lien_lieu or lien_lieu == "Lien non disponible" or "shotgun.live" not in lien_lieu:
            return {
                "adresse": "Adresse non disponible",
                "description": "Description non disponible",
                "lien_lieu": lien_lieu,
                "source": SOURCES["SHOTGUN"]
            }
            
        # Tenter d'utiliser la fonction détaillée pour récupérer toutes les infos
        venue_details = scrape_venue_details(lien_lieu)
        if venue_details and "adresse" in venue_details and "description" in venue_details:
            return {
                "adresse": venue_details["adresse"],
                "description": venue_details["description"],
                "lien_lieu": lien_lieu,
                "source": SOURCES["SHOTGUN"],
                "image": venue_details.get("image", DEFAULT_IMAGE_URL)
            }
        
        # Si la fonction détaillée échoue, retomber sur la méthode simple
        response = requests.get(lien_lieu)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Récupération de l'adresse (structure différente sur Shotgun)
        adresse = "Adresse non disponible"
        description = "Description non disponible"
        
        # Chercher l'adresse dans les balises qui contiennent souvent cette info
        adresse_candidates = soup.select('.venue-details address, .venue-info address, .venue-address')
        if adresse_candidates:
            adresse = adresse_candidates[0].text.strip()
        
        # Chercher la description
        description_candidates = soup.select('.venue-description, .venue-about, .venue-details p')
        if description_candidates:
            description = description_candidates[0].text.strip()
        
        # Si l'adresse est vide, essayer d'extraire la ville du titre
        if adresse == "Adresse non disponible":
            title_elem = soup.select('.venue-name, h1')
            if title_elem:
                venue_name = title_elem[0].text.strip()
                # Vérifier si le nom contient "Paris" ou un autre lieu connu
                if "paris" in venue_name.lower():
                    adresse = "Paris"
        
        return {
            "adresse": adresse,
            "description": description,
            "lien_lieu": lien_lieu,
            "source": SOURCES["SHOTGUN"]
        }
    except Exception as e:
        logger.error(f"Erreur lors du scraping Shotgun: {e}")
        return {
            "adresse": "Erreur lors du scraping",
            "description": "Erreur lors du scraping",
            "lien_lieu": lien_lieu,
            "source": SOURCES["SHOTGUN"]
        }

def scrape_spectacles(page_url):
    """Scrape les informations des spectacles depuis une page de BilletReduc"""
    try:
        response = requests.get(page_url)
        if response.status_code != 200:
            logger.error("Erreur lors de la récupération de la page %s : %s", page_url, response.status_code)
            return []
            
        soup = BeautifulSoup(response.text, 'html.parser')
        spectacle_sections = soup.find_all('td', class_='bgbeige')
        spectacles = []
        
        for section in spectacle_sections:
            title_elem = section.find('a', class_='head')
            title = title_elem.text.strip() if title_elem else None
            if not title:
                continue
                
            link_href = title_elem['href'] if title_elem else None
            numero_spectacle = link_href.split('/')[1] if link_href else "Numéro non disponible"
            
            category_elem = section.find('span', class_='small')
            category = category_elem.text.strip() if category_elem else "Catégorie non disponible"
            
            # Standardiser la catégorie
            standardized_category = standardize_category(category)
            main_category = extract_main_category(standardized_category)
            
            detail_elem = section.find('div', class_='libellepreliste')
            detail = detail_elem.text.strip() if detail_elem else "Détail non disponible"
            
            lieu_elem = section.find('span', class_='lieu')
            if lieu_elem:
                lieu_link_elem = lieu_elem.find('a')
                lieu = lieu_link_elem.text.strip() if lieu_link_elem else "Lieu non disponible"
                lieu_url = f"https://www.billetreduc.com{lieu_link_elem['href']}" if lieu_link_elem else "Lien non disponible"
            else:
                lieu = "Lieu non disponible"
                lieu_url = "Lien non disponible"
                
            dates_text = lieu_elem.text if lieu_elem else ""
            if "Prochaines dates:" in dates_text:
                raw_dates = dates_text.split("Prochaines dates:")[-1].strip()
                dates = format_dates(raw_dates)
            else:
                dates = "Dates non disponibles"
                
            prix_reduit_elem = section.find_next('span', class_='prixli')
            prix_reduit = prix_reduit_elem.text.strip() if prix_reduit_elem else "Prix réduit non disponible"
            
            ancien_prix_elem = section.find_next('strike')
            ancien_prix = ancien_prix_elem.text.strip() if ancien_prix_elem else "Ancien prix non disponible"
            
            note_elem = section.find('b', class_=re.compile(r'note\d+ tooltip'))
            note = note_elem.get('class', [None])[0].replace('note', '').replace('tooltip', '') if note_elem else "Note non disponible"
            
            image_url = f"https://www.billetreduc.com/zg/n100/{numero_spectacle}.jpeg"
            site_url = f"https://www.billetreduc.com{title_elem['href']}" if title_elem else "Lien non disponible"
            purchase_url = f"https://www.billetreduc.com/v2/PurchaseTunnel#/ShowSelection?eventId={numero_spectacle}"
            
                    # Récupération de la description
            description_elem = soup.find('speDescription')
            description = description_elem.text.strip() if description_elem else "Description non disponible"
            
            soustitre_elem = soup.find('span', class_='h6')
            soustitre = soustitre_elem.text.strip() if soustitre_elem else "Sous-titre non disponible"
            
            # Récupération du lineup
            lineup = []
            credits_elem = soup.find('div', class_='evtCredits')
            if credits_elem:
                credit_links = credits_elem.find_all('a')
                seen = set()
                for link in credit_links:
                    nom = link.text.strip()
                    if nom and nom not in seen:
                        lineup.append({
                            "nom": nom,
                            "image": None  # Pas d'image disponible sur BilletReduc
                        })
                        seen.add(nom)
            
            # Extraire les dates et horaires à partir du texte
            horaires = []
            if dates != "Dates non disponibles":
                horaires = parse_horaires(dates)
            
            spectacles.append({
                "intitulé": title,
                "catégorie": standardized_category,
                "catégorie_principale": main_category,
                "catégorie_originale": category,
                "détail": detail,
                "lieu": lieu,
                "lien_lieu": lieu_url,
                "prochaines_dates": dates,
                "prix_reduit": prix_reduit,
                "ancien_prix": ancien_prix,
                "note": note,
                "image": image_url,
                "site_url": site_url,
                "purchase_url": purchase_url,
                "description": description,
                "soustitre": soustitre,
                "lineup": lineup,"source": SOURCES["BILLETREDUC"],
                "horaires": horaires
                                
            })
        
        return spectacles
    except Exception as e:
        logger.error("Erreur lors du scraping de %s : %s", page_url, e)
        return []

def scrape_billetreduc_page(page_url):
    """Scrape une page de commentaires pour un événement"""
    try:
        response = requests.get(page_url)
        if response.status_code != 200:
            logger.error("Erreur lors de la récupération de la page %s : %s", page_url, response.status_code)
            return []
            
        soup = BeautifulSoup(response.text, 'html.parser')
        critique_divs = soup.find_all('div', class_='crit')
        commentaires = []
        
        for crit in critique_divs:
            # Extraire la note
            note_elem = crit.find('b', class_='tooltip')
            note = note_elem['title'] if note_elem and 'title' in note_elem.attrs else "Note non disponible"
            
            # Extraire le titre du commentaire
            titre_elem = crit.find('b')
            titre = titre_elem.text.strip() if titre_elem else "Titre non disponible"
            
            # Extraire le contenu du commentaire
            commentaire_brut = ""
            contenu_elements = crit.find_all(string=True, recursive=False)
            for elem in contenu_elements:
                commentaire_brut += elem.strip() + " "
                
            commentaires.append({
                "titre": titre,
                "note": note,
                "contenu": commentaire_brut.strip()
            })
            
        return commentaires
    except Exception as e:
        logger.error("Erreur lors du scraping des commentaires de %s : %s", page_url, e)
        return []

def scrape_comments_for_event(event_id, base_url):
    """Scrape les commentaires pour un événement spécifique"""
    all_comments = []
    last_comments_content = set()  # Pour détecter les doublons
    
    for page_num in range(1, 6):  # Limité aux 5 premières pages
        page_url = f"{base_url}/evtcrit.htm?CRITIQUESpg={page_num}"
        logger.info("Scraping page %s for event %s: %s", page_num, event_id, page_url)
        comments = scrape_billetreduc_page(page_url)
        
        if not comments:
            logger.info("Aucun commentaire trouvé sur cette page, arrêt du scraping des commentaires.")
            break  # Arrêt si plus de commentaires trouvés
            
        # Créer une signature pour la page actuelle basée sur le contenu des commentaires
        current_comments_content = set(c.get("contenu", "")[:50] for c in comments)  # Utiliser les 50 premiers caractères
        
        # Vérifier si on a déjà ces commentaires (pagination qui renvoie la même page)
        if current_comments_content and current_comments_content == last_comments_content:
            logger.info("Mêmes commentaires détectés que la page précédente, arrêt du scraping des commentaires.")
            break
            
        # Mémoriser cette page pour comparaison future
        last_comments_content = current_comments_content
        
        # Ajouter les nouveaux commentaires
        all_comments.extend(comments)
        
    return all_comments

async def scrape_categories_and_prices(url):
    """Scrape les catégories et prix via Playwright"""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url)
        
        # Accepter les cookies si nécessaire
        try:
            cookie_button = await page.query_selector('button#onetrust-accept-btn-handler')
            if cookie_button:
                await cookie_button.click()
                await page.wait_for_timeout(1000)
        except Exception as e:
            logger.info("Pas de bandeau de cookies détecté")
            
        # Trouver les sections de catégories
        category_sections = await page.query_selector_all('.category-content')
        all_data = []
        
        for section in category_sections:
            title_elem = await section.query_selector('.filter-title .label-name')
            title = await title_elem.inner_text() if title_elem else "Catégorie non disponible"
            
            price_elements = await section.query_selector_all('.price.final-price')
            prices = [await price.inner_text() for price in price_elements] if price_elements else []
            
            all_data.append({
                "Catégorie": title.strip(),
                "Prix": [price.strip() for price in prices]
            })
            
        await browser.close()
        return all_data

# ---- Fonctions d'analyse AI ----
def analyze_comment_by_category(comment, category):
    """Analyse un commentaire en fonction de sa catégorie en utilisant l'IA OpenAI"""
    if not AI_ENABLED:
        logger.debug("Analyse AI désactivée, retour de valeurs par défaut.")
        return None # ou retourner une structure par défaut si nécessaire
    
    try:
        # Standardiser la catégorie
        category = standardize_category(category)
        
        # Extraire la catégorie principale
        main_category = extract_main_category(category)
        
        # Récupérer la configuration pour cette catégorie
        category_config = CATEGORY_MAPPINGS.get(main_category, CATEGORY_MAPPINGS["default"])
        
        # Construire le prompt pour l'analyse
        prompt = f"""Analyse le commentaire suivant pour un événement de type {category} :
        
        Commentaire : \"{comment}\"
        
        Donne uniquement une note sur 5 pour chaque aspect mentionné dans le commentaire.
        Ne génère pas de texte explicatif, uniquement les notes.
        """
        
        # Générer la réponse avec OpenAI
        response = generate_ai_response_openai(prompt)
        
        if not response:
            logger.error("Erreur lors de l'analyse du commentaire")
            return None
        
        # Nettoyer et parser la réponse
        try:
            # Extraire les notes de la réponse
            notes = {}
            for line in response.split('\n'):
                if ':' in line:
                    aspect, note = line.split(':', 1)
                    aspect = aspect.strip().lower()
                    try:
                        note = float(note.strip())
                        if 0 <= note <= 5:
                            notes[aspect] = note
                    except ValueError:
                        continue
            
            return notes
            
        except Exception as e:
            logger.error(f"Erreur lors du parsing de la réponse: {e}")
            return None
            
    except Exception as e:
        logger.error(f"Erreur lors de l'analyse du commentaire: {e}")
        return None

def run_ai_analysis(batch_size=5, resume=False):
    """
    Exécute l'analyse AI sur les commentaires des événements avec possibilité de reprendre.
    """
    if check_ai_processed() and not resume:
        logger.info("L'analyse AI a déjà été effectuée, étape ignorée")
        return
        
    logger.info("Début de l'analyse AI des commentaires...")
    
    # Charger checkpoint si disponible et reprise demandée
    checkpoint = None
    if resume:
        checkpoint = load_checkpoint(checkpoint_name="ai_analysis")
    
    try:
        # Récupérer les événements avec des commentaires
        query = {"commentaires": {"$exists": True, "$ne": []}}
        
        # Si checkpoint existe, filtrer pour ne reprendre qu'à partir du dernier traité
        if checkpoint and "last_event_id" in checkpoint:
            last_id = checkpoint["last_event_id"]
            logger.info(f"Reprise depuis l'événement avec ID: {last_id}")
            query["_id"] = {"$gt": ObjectId(last_id)}
        
        events = list(collection_evenements.find(query))
        logger.info("Nombre d'événements à analyser: %s", len(events))
        
        # Si aucun événement à traiter mais que nous avons un checkpoint, marquer comme terminé
        if not events and checkpoint:
            mark_ai_processed()
            logger.info("Tous les événements ont déjà été traités!")
            return
        
        # Traiter par lots pour gérer la mémoire et permettre des reprises
        for i in range(0, len(events), batch_size):
            batch = events[i:i+batch_size]
            logger.info("Traitement du lot %s/%s", i//batch_size + 1, (len(events) + batch_size - 1)//batch_size)
            
            for event in batch:
                category = event.get("catégorie", "Catégorie non disponible")
                commentaires = event.get("commentaires", [])
                
                if not commentaires:
                    continue
                    
                # Analyser seulement un échantillon de commentaires (3 max) pour économiser les ressources
                sample_size = min(3, len(commentaires))
                sample_comments = commentaires[:sample_size]
                
                aspects_scores = {}
                emotions_set = set()
                appreciations = []
                
                for comment in sample_comments:
                    result = analyze_comment_by_category(comment, category)
                    if not result:
                        continue
                        
                    # Collecter les scores des aspects
                    for key, value in result.items():
                        if isinstance(value, (int, float)) and key != "Appréciation globale":
                            aspects_scores[key] = aspects_scores.get(key, []) + [value]
                            
                    # Collecter les émotions
                    if "Émotions" in result and isinstance(result["Émotions"], list):
                        emotions_set.update(result["Émotions"])
                    elif "Émotions" in result and isinstance(result["Émotions"], str):
                        emotions_set.add(result["Émotions"])
                        
                    # Collecter l'appréciation globale
                    if "Appréciation globale" in result:
                        appreciations.append(result["Appréciation globale"])
                
                # Calculer les moyennes des aspects
                avg_aspects = {}
                for aspect, scores in aspects_scores.items():
                    if scores:
                        avg_aspects[aspect] = sum(scores) / len(scores)
                
                # Calculer une note globale moyenne
                if avg_aspects:
                    global_score = sum(avg_aspects.values()) / len(avg_aspects)
                else:
                    global_score = None
                
                # Mettre à jour l'événement dans MongoDB
                update_data = {
                    "notes_globales": {
                        "aspects": avg_aspects,
                        "emotions": list(emotions_set),
                        "appréciation_globale": appreciations[0] if appreciations else "Pas de synthèse disponible"
                    }
                }
                
                if global_score:
                    update_data["note_ai"] = global_score
                
                collection_evenements.update_one(
                    {"_id": event["_id"]},
                    {"$set": update_data}
                )
                
                logger.info("Analyse AI effectuée pour l'événement: %s", event.get("intitulé", "Sans titre"))
                
                # Sauvegarder le checkpoint après chaque événement
                save_checkpoint({
                    "last_event_id": str(event["_id"]),
                    "timestamp": datetime.now().isoformat(),
                    "events_processed": i + batch.index(event) + 1
                }, checkpoint_name="ai_analysis")
                
            # Pause entre les lots pour éviter de surcharger le système
            if i + batch_size < len(events):
                logger.info("Pause de 2 secondes entre les lots...")
                time.sleep(2)
        
        # Marquer l'analyse comme effectuée
        mark_ai_processed()
        logger.info("Analyse AI terminée avec succès!")
        
    except Exception as e:
        logger.error("Erreur lors de l'analyse AI: %s", e)
        logger.info("L'analyse pourra être reprise à partir du dernier checkpoint")

# ---- Fonctions pour la génération des producteurs et ajout de coordonnées ----
def generate_producers_with_coordinates(resume=False):
    """
    Génère un document par lieu dans Loisir_Paris_Producers,
    et ajoute les coordonnées aux événements dans Loisir_Paris_Evenements.
    Avec support de checkpointing pour reprendre après interruption.
    """
    logger.info("Début de la génération des producteurs avec coordonnées...")
    
    # Charger checkpoint si disponible et reprise demandée
    checkpoint = None
    if resume:
        checkpoint = load_checkpoint(checkpoint_name="producers_generation")
    
    try:
        # Récupérer les lieux uniques
        lieux = collection_evenements.distinct("lieu")
        logger.info("Nombre de lieux à traiter: %s", len(lieux))
        
        # Si nous avons un checkpoint, filtrer pour ne reprendre qu'à partir du dernier traité
        if checkpoint and "last_processed_lieu" in checkpoint:
            last_lieu = checkpoint["last_processed_lieu"]
            try:
                start_idx = lieux.index(last_lieu) + 1
                lieux = lieux[start_idx:]
                logger.info(f"Reprise depuis le lieu: {last_lieu}, restant {len(lieux)} lieux")
            except ValueError:
                logger.warning(f"Le lieu précédent {last_lieu} n'a pas été trouvé, traitement depuis le début")
        
        for i, lieu in enumerate(lieux):
            if not lieu or lieu == "Lieu non disponible":
                logger.warning(f"Lieu manquant ou invalide, ignoré")
                continue
                
            # Récupérer les événements pour ce lieu
            evenements = list(collection_evenements.find({"lieu": lieu}))
            if not evenements:
                continue
                
            # Récupérer le lien du lieu et déterminer la source
            premier_evenement = evenements[0]
            lien_lieu = premier_evenement.get("lien_lieu", "Lien non disponible")
            source = SOURCES["BILLETREDUC"]  # Par défaut
            
            if "shotgun" in lien_lieu.lower():
                source = SOURCES["SHOTGUN"]
            elif premier_evenement.get("source") == SOURCES["SHOTGUN"]:
                source = SOURCES["SHOTGUN"]
                
            # Scraper les détails du lieu selon la source
            lieu_details = scrape_lieu_details(lien_lieu)
            
            # Obtenir les coordonnées géographiques
            coordinates = get_coordinates_from_address(lieu_details["adresse"])
            
            # Si pas de coordonnées via l'adresse, essayer via le nom du lieu
            if not coordinates:
                logger.warning(f"Pas de coordonnées pour l'adresse: {lieu_details['adresse']}, tentative via le nom du lieu")
                coordinates = extract_location_from_lieu_name(lieu)
            
            # Rechercher les photos et détails complémentaires
            # Utiliser Selenium pour BilletReduc, et conserver les images pour Shotgun
            place_details = None
            if source == SOURCES["BILLETREDUC"]:
                logger.info(f"Utilisation de Selenium pour récupérer l'image du lieu BilletReduc: {lieu}")
                place_details = fetch_place_details_and_photos_selenium(lieu, lieu_details["adresse"])
            else:
                # Pour Shotgun, récupérer l'image de l'événement
                shotgun_event = next((e for e in evenements if e.get("image")), None)
                if shotgun_event and "image" in shotgun_event:
                    # Créer un dictionnaire minimal avec l'image de l'événement Shotgun
                    place_details = {
                        "image": shotgun_event["image"]
                    }
                    logger.info(f"Image Shotgun récupérée pour: {lieu}")
                else:
                    logger.info(f"Pas d'image Shotgun trouvée, utilisation de l'image par défaut pour: {lieu}")
                    place_details = {
                        "image": DEFAULT_IMAGE_URL
                    }
            
            # Ajouter les coordonnées aux événements
            if coordinates:
                for evenement in evenements:
                    if "location" not in evenement or evenement["location"] is None:
                        collection_evenements.update_one(
                            {"_id": evenement["_id"]},
                            {"$set": {"location": coordinates}}
                        )
                        logger.info(f"Coordonnées ajoutées pour l'événement: {evenement.get('intitulé', 'Sans titre')}")
            else:
                logger.warning(f"Impossible de trouver des coordonnées pour le lieu: {lieu}")
                continue  # Passer au lieu suivant si pas de coordonnées
            
            # Standardiser les catégories des événements
            for evenement in evenements:
                category = evenement.get("catégorie", "Catégorie non disponible")
                if category == "Catégorie non disponible" or category == "":
                    standardized_category = standardize_category(category)
                    main_category = extract_main_category(standardized_category)
                    
                    collection_evenements.update_one(
                        {"_id": evenement["_id"]},
                        {"$set": {
                            "catégorie": standardized_category,
                            "catégorie_principale": main_category
                        }}
                    )
                    logger.info(f"Catégorie standardisée pour l'événement: {evenement.get('intitulé', 'Sans titre')}")
            
            # Récupérer les événements mis à jour avec catégories standardisées
            evenements = list(collection_evenements.find({"lieu": lieu}))
            
            # Créer la liste des événements pour le producteur
            events_list = [{
                "intitulé": e["intitulé"],
                "catégorie": e.get("catégorie", "Catégorie non disponible"),
                "lien_evenement": f"/Loisir_Paris_Evenements/{str(e['_id'])}"
            } for e in evenements]
            
            # Construire le document producteur
            producer_doc = {
                "lieu": lieu,
                "adresse": lieu_details["adresse"],
                "description": lieu_details["description"],
                "nombre_evenements": len(events_list),
                "evenements": events_list,
                "lien_lieu": lieu_details["lien_lieu"],
                "location": coordinates,
                "source": source
            }
            
            # Ajouter les informations de photos si disponibles
            if place_details:
                # S'assurer que l'image est toujours disponible en utilisant le champ image standardisé
                if "image" in place_details:
                    producer_doc["image"] = place_details["image"]
                else:
                    # Fallback au DEFAULT_IMAGE_URL si aucune image n'est disponible
                    producer_doc["image"] = DEFAULT_IMAGE_URL
                
                # Pour la compatibilité avec d'autres sections du code
                producer_doc["photos"] = place_details.get("photos", [place_details.get("image", DEFAULT_IMAGE_URL)])
                
                # Informations complémentaires potentiellement utiles
                if place_details.get("phone_number"):
                    producer_doc["telephone"] = place_details["phone_number"]
                if place_details.get("website"):
                    producer_doc["site_web"] = place_details["website"]
                if place_details.get("rating"):
                    producer_doc["note_google"] = place_details["rating"]
                if place_details.get("maps_url"):
                    producer_doc["lien_google_maps"] = place_details["maps_url"]
            else:
                # Assurer qu'une image par défaut est toujours assignée
                producer_doc["image"] = DEFAULT_IMAGE_URL
            
            # Mettre à jour ou créer le producteur
            query = {"lieu": lieu}
            update_result = collection_producers.update_one(
                query,
                {"$set": producer_doc},
                upsert=True
            )
            
            if update_result.matched_count > 0:
                logger.info(f"Mise à jour effectuée pour le lieu: {lieu}")
            elif update_result.upserted_id:
                logger.info(f"Nouveau lieu inséré: {lieu}")
            else:
                logger.info(f"Aucune modification pour le lieu: {lieu}")
            
            # Sauvegarder le checkpoint après chaque lieu
            save_checkpoint({
                "last_processed_lieu": lieu,
                "timestamp": datetime.now().isoformat(),
                "processed_count": i + 1,
                "total_count": len(lieux) + i + 1  # Total initial + index actuel
            }, checkpoint_name="producers_generation")
                
        logger.info("Génération des producteurs terminée avec succès!")
        
    except Exception as e:
        logger.error(f"Erreur lors de la génération des producteurs: {e}")
        logger.info("La génération pourra être reprise à partir du dernier checkpoint")

def process_shotgun_events(resume=False):
    """
    Fonction spécifique pour traiter les événements Shotgun avec support de checkpointing.
    """
    logger.info("Traitement des événements Shotgun...")
    
    # Charger checkpoint si disponible et reprise demandée
    checkpoint = None
    if resume:
        checkpoint = load_checkpoint(checkpoint_name="shotgun_processing")
    
    try:
        # Récupérer tous les événements Shotgun qui n'ont pas de location
        query = {
            "$or": [
                {"site_url": {"$regex": "shotgun", "$options": "i"}},
                {"lien_lieu": {"$regex": "shotgun", "$options": "i"}},
                {"source": SOURCES["SHOTGUN"]}
            ],
            "$or": [
                {"location": {"$exists": False}},
                {"location": None}
            ]
        }
        
        # Si nous avons un checkpoint, filtrer pour ne reprendre qu'à partir du dernier traité
        if checkpoint and "last_event_id" in checkpoint:
            last_id = checkpoint["last_event_id"]
            logger.info(f"Reprise depuis l'événement avec ID: {last_id}")
            query["_id"] = {"$gt": ObjectId(last_id)}
        
        shotgun_events = list(collection_evenements.find(query))
        
        logger.info(f"Nombre d'événements Shotgun sans location: {len(shotgun_events)}")
        
        for i, event in enumerate(shotgun_events):
            # Extraire le lieu et le lien
            lieu = event.get("lieu", "Lieu non disponible")
            lien_lieu = event.get("lien_lieu", "Lien non disponible")
            
            # Si pas de catégorie ou "Catégorie non disponible", essayer de déduire une catégorie
            category = event.get("catégorie", "Catégorie non disponible")
            if category == "Catégorie non disponible" or category == "":
                # Chercher des indices dans le titre ou détail
                title = event.get("intitulé", "")
                detail = event.get("détail", "")
                
                # Chercher des mots-clés dans le titre et détail
                for keyword, mapped_category in CATEGORY_MAPPING.items():
                    if keyword.lower() in title.lower() or keyword.lower() in detail.lower():
                        category = mapped_category
                        break
                        
                # Si aucun mot-clé trouvé, utiliser "Concert" comme catégorie par défaut pour Shotgun
                if category == "Catégorie non disponible":
                    category = "Musique » Concert"
                    
                # Mettre à jour la catégorie
                collection_evenements.update_one(
                    {"_id": event["_id"]},
                    {"$set": {
                        "catégorie": category,
                        "catégorie_principale": extract_main_category(category),
                        "source": SOURCES["SHOTGUN"]
                    }}
                )
                logger.info(f"Catégorie définie pour l'événement Shotgun: {event.get('intitulé', 'Sans titre')}")
            
            # Scraper les détails du lieu
            lieu_details = scrape_shotgun_lieu(lien_lieu)
            
            # Obtenir les coordonnées géographiques
            coordinates = get_coordinates_from_address(lieu_details["adresse"])
            
            # Si pas de coordonnées via l'adresse, essayer via le nom du lieu
            if not coordinates:
                logger.warning(f"Pas de coordonnées pour l'adresse Shotgun: {lieu_details['adresse']}, tentative via le nom du lieu")
                coordinates = extract_location_from_lieu_name(lieu)
            
            # Si toujours pas de coordonnées, utiliser Paris
            if not coordinates:
                logger.warning(f"Utilisation des coordonnées de Paris par défaut pour: {lieu}")
                coordinates = {
                    "type": "Point",
                    "coordinates": [2.3522, 48.8566]  # Paris
                }
            
            # Mettre à jour l'événement avec les coordonnées
            collection_evenements.update_one(
                {"_id": event["_id"]},
                {"$set": {"location": coordinates}}
            )
            logger.info(f"Coordonnées ajoutées pour l'événement Shotgun: {event.get('intitulé', 'Sans titre')}")
            
            # Mettre à jour ou créer les horaires si manquants
            if "horaires" not in event or not event["horaires"]:
                dates_text = event.get("prochaines_dates", "")
                horaires = parse_horaires(dates_text)
                if horaires:
                    collection_evenements.update_one(
                        {"_id": event["_id"]},
                        {"$set": {"horaires": horaires}}
                    )
                    logger.info(f"Horaires ajoutés pour l'événement Shotgun: {event.get('intitulé', 'Sans titre')}")
            
            # Sauvegarder le checkpoint après chaque événement
            save_checkpoint({
                "last_event_id": str(event["_id"]),
                "timestamp": datetime.now().isoformat(),
                "events_processed": i + 1
            }, checkpoint_name="shotgun_processing")
        
        logger.info("Traitement des événements Shotgun terminé")
        
    except Exception as e:
        logger.error(f"Erreur lors du traitement des événements Shotgun: {e}")
        logger.info("Le traitement pourra être repris à partir du dernier checkpoint")

# ---- Fonction de scraping et sauvegarde des événements ----
def save_to_mongo(spectacles, resume=False):
    """Sauvegarde les spectacles dans MongoDB avec support de checkpointing"""
    
    # Charger checkpoint si disponible et reprise demandée
    checkpoint = None
    if resume:
        checkpoint = load_checkpoint(checkpoint_name="save_to_mongo")
    
    # Déterminer l'indice de départ
    start_idx = 0
    if checkpoint and "last_spectacle_index" in checkpoint and "batch_id" in checkpoint:
        saved_batch_id = checkpoint["batch_id"]
        # Générer un identifiant unique pour ce lot de spectacles
        current_batch_id = hashlib.md5(json.dumps([s["intitulé"] for s in spectacles]).encode()).hexdigest()
        
        # Si le batch_id correspond, reprendre à partir du dernier indice
        if saved_batch_id == current_batch_id:
            start_idx = checkpoint["last_spectacle_index"] + 1
            logger.info(f"Reprise de la sauvegarde à partir de l'indice {start_idx}")
        else:
            logger.info("Nouveau lot de spectacles détecté, démarrage depuis le début")
    
    for i, spectacle in enumerate(spectacles[start_idx:], start=start_idx):
        try:
            # Standardiser la catégorie si ce n'est pas déjà fait
            if "catégorie_principale" not in spectacle:
                category = spectacle.get("catégorie", "Catégorie non disponible")
                main_category = extract_main_category(category)
                spectacle["catégorie_principale"] = main_category
                
            # Extraire le numéro de spectacle
            numero_spectacle = spectacle["purchase_url"].split("eventId=")[-1]
            
            # Scraper les informations complémentaires
            purchase_info = asyncio.run(scrape_categories_and_prices(spectacle["purchase_url"]))
            spectacle["catégories_prix"] = purchase_info
            
            # Scraper les commentaires
            base_url = spectacle["site_url"].split("/evt.htm")[0]
            comments = scrape_comments_for_event(numero_spectacle, base_url)
            spectacle["commentaires"] = comments
            
            # Vérifier si l'événement existe déjà
            existing_event = collection_evenements.find_one({"purchase_url": {"$regex": f"eventId={numero_spectacle}"}})
            
            if existing_event:
                # Mise à jour de l'événement existant
                collection_evenements.update_one(
                    {"_id": existing_event["_id"]},
                    {"$set": spectacle}
                )
                logger.info(f"Mise à jour effectuée pour: {spectacle['intitulé']}")
            else:
                # Insertion d'un nouvel événement
                collection_evenements.insert_one(spectacle)
                logger.info(f"Ajouté: {spectacle['intitulé']}")
            
            # Sauvegarder le checkpoint après chaque spectacle
            batch_id = hashlib.md5(json.dumps([s["intitulé"] for s in spectacles]).encode()).hexdigest()
            save_checkpoint({
                "last_spectacle_index": i,
                "batch_id": batch_id,
                "total_spectacles": len(spectacles),
                "timestamp": datetime.now().isoformat()
            }, checkpoint_name="save_to_mongo")
                
        except Exception as e:
            logger.error(f"Erreur pour {spectacle.get('intitulé', 'Inconnu')}: {e}")
            # Sauvegarder quand même le checkpoint pour continuer après cette erreur
            batch_id = hashlib.md5(json.dumps([s["intitulé"] for s in spectacles]).encode()).hexdigest()
            save_checkpoint({
                "last_spectacle_index": i,
                "batch_id": batch_id,
                "total_spectacles": len(spectacles),
                "timestamp": datetime.now().isoformat(),
                "last_error": str(e)
            }, checkpoint_name="save_to_mongo")

def scrape_billetreduc_events(resume=False):
    """Scrape les événements de BilletReduc pour toutes les régions et rubriques avec support de checkpointing"""
    logger.info("Début du scraping des événements BilletReduc...")

    # Initialisation pour asyncio
    nest_asyncio.apply()

    # Liste des IDs de région disponibles sur BilletReduc
    region_ids = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V"]

    # Rubriques d'événements à explorer
    rubriques = [4, 36, 187, 241]  # Ids correspondant à différentes catégories

    # Charger le checkpoint si nécessaire
    checkpoint = load_checkpoint(checkpoint_name="billetreduc_scraping") if resume else None
    region_index = checkpoint.get("region_index", 0) if checkpoint else 0
    rubrique_index = checkpoint.get("rubrique_index", 0) if checkpoint else 0
    page = checkpoint.get("page", 1) if checkpoint else 1

    for i in range(region_index, len(region_ids)):
        region_id = region_ids[i]
        for j in range(rubrique_index if i == region_index else 0, len(rubriques)):
            rubrique_id = rubriques[j]
            current_page = page if (i == region_index and j == rubrique_index) else 1
            
            # Pour détecter les pages dupliquées
            last_spectacles_ids = set()
            consecutive_duplicates = 0  # Compteur de doublons consécutifs

            while True:
                url = f"https://www.billetreduc.com/search.htm?idrub={rubrique_id}&prix=0&region={region_id}&tri=r&type=3&LISTEPEpg={current_page}"
                logger.info(f"Scraping URL: {url}")

                try:
                    response = requests.get(url, allow_redirects=True)

                    # Si redirection, arrêter cette rubrique + région
                    if f"region={region_id}" not in response.url:
                        logger.info(f"Redirection détectée. Arrêt pour région {region_id}, rubrique {rubrique_id}.")
                        break

                    spectacles = scrape_spectacles(url)
                    if not spectacles:
                        logger.info(f"Aucun spectacle trouvé page {current_page}.")
                        break
                    
                    # Obtenir les identifiants des spectacles de cette page
                    # Utiliser purchase_url car il contient l'ID unique de l'événement
                    current_spectacles_ids = set(s.get("purchase_url", "").split("eventId=")[-1] for s in spectacles)
                    
                    # Vérifier si on a les mêmes spectacles que la page précédente
                    if current_spectacles_ids and current_spectacles_ids == last_spectacles_ids:
                        consecutive_duplicates += 1
                        logger.info(f"Mêmes spectacles que la page précédente détectés ({consecutive_duplicates} fois consécutives).")
                        
                        # Si on a les mêmes spectacles, passer immédiatement à la rubrique suivante
                        logger.info("Détection de boucle, passage à la rubrique/région suivante.")
                        break
                    else:
                        consecutive_duplicates = 0  # Réinitialiser le compteur si les spectacles sont différents
                    
                    # Mémoriser les IDs pour la prochaine comparaison
                    last_spectacles_ids = current_spectacles_ids

                    save_to_mongo(spectacles)

                    # Sauvegarder le checkpoint après chaque page
                    save_checkpoint({
                        "region_index": i,
                        "rubrique_index": j,
                        "page": current_page + 1,
                        "timestamp": datetime.now().isoformat()
                    }, checkpoint_name="billetreduc_scraping")

                    current_page += 1

                except Exception as e:
                    logger.error(f"Erreur lors du scraping de {url}: {e}")
                    save_checkpoint({
                        "region_index": i,
                        "rubrique_index": j,
                        "page": current_page,
                        "timestamp": datetime.now().isoformat(),
                        "last_error": str(e)
                    }, checkpoint_name="billetreduc_scraping")
                    return  # Stop temporaire pour reprise plus tard

    logger.info("Scraping BilletReduc terminé pour toutes les régions et rubriques.")

def run_full_pipeline():
    print("▶️ Scraping Shotgun...")
    run_shotgun_scraper(max_pages=10, resume=False)

    print("▶️ Scraping BilletReduc (toutes régions & rubriques)...")
    scrape_billetreduc_events(resume=False)

    print("🧠 Analyse IA des commentaires...")
    run_ai_analysis(batch_size=5, resume=False)

    print("🏗️ Génération des producteurs avec coordonnées...")
    generate_producers_with_coordinates(resume=False)

    print("✅ Pipeline complet terminé.")

# ---- Fonctions principales ----
def process_all_data(run_ai=True, resume=False):
    """
    Fonction principale qui exécute toutes les étapes du traitement des données avec support de reprise.
    Si run_ai est False, l'analyse AI sera ignorée même si elle n'a pas été effectuée auparavant.
    Si resume est False, chaque étape sera redémarrée depuis le début.
    """
    try:
        # Charger checkpoint global si disponible
        checkpoint = None
        if resume:
            checkpoint = load_checkpoint(checkpoint_name="global_process")
        
        # Déterminer l'étape de départ
        start_step = 0
        if checkpoint and "last_completed_step" in checkpoint:
            start_step = checkpoint["last_completed_step"] + 1
        
        logger.info(f"Début du processus complet de traitement des données (étape {start_step})...")
        
        steps = [
            ("Scraping des événements BilletReduc", scrape_billetreduc_events),
            ("Traitement des événements Shotgun", process_shotgun_events),
            ("Génération des producteurs avec coordonnées", generate_producers_with_coordinates),
            ("Analyse AI", run_ai_analysis if run_ai else lambda resume=True: logger.info("Étape AI ignorée"))
        ]
        
        # Exécuter les étapes à partir de l'étape de départ
        for i, (step_name, step_function) in enumerate(steps[start_step:], start=start_step):
            logger.info(f"Exécution de l'étape {i}: {step_name}")
            
            try:
                if step_name == "Analyse AI" and not run_ai:
                    # Ignorer l'étape AI si désactivée
                    logger.info("Étape AI ignorée sur demande")
                else:
                    # Exécuter la fonction d'étape avec le paramètre resume
                    step_function(resume=resume)
                
                # Mettre à jour le checkpoint global après chaque étape réussie
                save_checkpoint({
                    "last_completed_step": i,
                    "timestamp": datetime.now().isoformat()
                }, checkpoint_name="global_process")
                
            except Exception as e:
                logger.error(f"Erreur lors de l'étape {i} ({step_name}): {e}")
                logger.info(f"Le processus pourra être repris à partir de l'étape {i}")
                # Sauvegarder quand même le checkpoint pour permettre une reprise à cette étape
                save_checkpoint({
                    "last_completed_step": i - 1,  # Pas complètement terminé cette étape
                    "last_failed_step": i,
                    "timestamp": datetime.now().isoformat(),
                    "last_error": str(e)
                }, checkpoint_name="global_process")
                raise
            
        logger.info("Processus complet de traitement des données terminé avec succès!")
        
    except Exception as e:
        logger.error(f"Erreur lors du processus complet: {e}")

def verify_database_consistency():
    """
    Vérifie et corrige les incohérences dans la base de données
    """
    logger.info("Vérification de la cohérence de la base de données...")
    
    try:
        # 1. Vérifier les événements sans coordonnées
        events_without_location = collection_evenements.count_documents({
            "$or": [
                {"location": {"$exists": False}},
                {"location": None}
            ]
        })
        logger.info(f"Événements sans coordonnées: {events_without_location}")
        
        # 2. Vérifier les événements avec catégorie non disponible
        events_with_missing_category = collection_evenements.count_documents({
            "$or": [
                {"catégorie": "Catégorie non disponible"},
                {"catégorie": {"$exists": False}},
                {"catégorie": ""}
            ]
        })
        logger.info(f"Événements avec catégorie manquante: {events_with_missing_category}")
        
        # 3. Vérifier les producteurs sans coordonnées
        producers_without_location = collection_producers.count_documents({
            "$or": [
                {"location": {"$exists": False}},
                {"location": None}
            ]
        })
        logger.info(f"Producteurs sans coordonnées: {producers_without_location}")
        
        # 4. Vérifier la cohérence entre producteurs et événements
        all_venues = collection_evenements.distinct("lieu")
        producer_venues = collection_producers.distinct("lieu")
        
        missing_producers = []
        for venue in all_venues:
            if venue not in producer_venues and venue != "Lieu non disponible":
                missing_producers.append(venue)
                
        logger.info(f"Lieux d'événements sans producteur correspondant: {len(missing_producers)}")
        
        # Corriger les incohérences
        if events_without_location > 0 or events_with_missing_category > 0 or producers_without_location > 0 or missing_producers:
            logger.info("Des incohérences ont été détectées, lancement des corrections...")
            
            # Corriger les événements sans catégorie standardisée
            if events_with_missing_category > 0:
                events_to_fix = collection_evenements.find({
                    "$or": [
                        {"catégorie": "Catégorie non disponible"},
                        {"catégorie": {"$exists": False}},
                        {"catégorie": ""}
                    ]
                })
                
                for event in events_to_fix:
                    # Essayer de déduire la catégorie à partir du titre ou des détails
                    title = event.get("intitulé", "")
                    detail = event.get("détail", "")
                    
                    # Rechercher des mots-clés dans le titre et les détails
                    found_category = False
                    for keyword, category in CATEGORY_MAPPING.items():
                        if keyword.lower() in title.lower() or keyword.lower() in detail.lower():
                            standardized_category = category
                            main_category = extract_main_category(standardized_category)
                            
                            collection_evenements.update_one(
                                {"_id": event["_id"]},
                                {"$set": {
                                    "catégorie": standardized_category,
                                    "catégorie_principale": main_category
                                }}
                            )
                            found_category = True
                            break
                            
                    # Si aucune catégorie n'a été trouvée, attribuer une catégorie par défaut
                    if not found_category:
                        # Déterminer la catégorie par défaut selon la source
                        if "shotgun" in event.get("site_url", "").lower() or "shotgun" in event.get("lien_lieu", "").lower():
                            default_category = "Musique » Concert"
                        else:
                            default_category = "Théâtre"
                            
                        main_category = extract_main_category(default_category)
                        
                        collection_evenements.update_one(
                            {"_id": event["_id"]},
                            {"$set": {
                                "catégorie": default_category,
                                "catégorie_principale": main_category
                            }}
                        )
            
            # Générer les producteurs manquants
            generate_producers_with_coordinates()
            
            logger.info("Corrections terminées avec succès")
        else:
            logger.info("Aucune incohérence majeure détectée dans la base de données")
            
    except Exception as e:
        logger.error(f"Erreur lors de la vérification de la cohérence: {e}")

# AJOUT : Clé OpenAI et fonction utilitaire GPT-3.5-turbo
OPENAI_API_KEY = "sk-..."  # <-- Mets ta clé ici
OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"

def generate_ai_response_openai(prompt, model="gpt-3.5-turbo", temperature=0.2, max_tokens=512):
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json"
    }
    data = {
        "model": model,
        "messages": [
            {"role": "system", "content": "Tu es un assistant expert en analyse de reviews pour les événements, spectacles et loisirs. Réponds toujours en français."},
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

def save_checkpoint(data, checkpoint_name="checkpoint"):
    """Sauvegarde un checkpoint (pickle) dans le dossier courant."""
    filename = f"{checkpoint_name}.pkl"
    with open(filename, "wb") as f:
        pickle.dump(data, f)

def load_checkpoint(checkpoint_name="checkpoint"):
    """Charge un checkpoint (pickle) depuis le dossier courant."""
    filename = f"{checkpoint_name}.pkl"
    if os.path.exists(filename):
        with open(filename, "rb") as f:
            return pickle.load(f)
    return None

if __name__ == "__main__":
    print("Script started")  # Added print statement
    parser = argparse.ArgumentParser(description='Script de traitement des donnees Evenements & Loisirs (GPT)') # Description mise à jour
    
    parser.add_argument('--no-resume', action='store_true', help='Redémarrer le processus depuis le début sans utiliser de checkpoints')
    parser.add_argument('--force-ai', action='store_true', help="Forcer l'execution de l'analyse AI meme si deja effectuee")
    parser.add_argument('--skip-ai', action='store_true', help="Ignorer l'analyse AI meme si jamais effectuee")
    parser.add_argument('--verify-db', action='store_true', help='Verifier et corriger les incoherences dans la base de donnees')
    parser.add_argument('--shotgun-only', action='store_true', help='Traiter uniquement les evenements Shotgun déjà dans la base de données')
    parser.add_argument('--run-shotgun-scraper', action='store_true', help='Exécuter le processus complet de scraping Shotgun (venues et événements)')
    parser.add_argument('--max-shotgun-pages', type=int, default=MAX_SHOTGUN_PAGES, help=f'Nombre maximum de pages Shotgun à scraper (défaut: {MAX_SHOTGUN_PAGES})')
    parser.add_argument('--step', type=int, help='Exécuter une étape spécifique (1: billetreduc, 2: shotgun, 3: producteurs, 4: AI)')
    parser.add_argument("--all", action="store_true", help="Lance tout le pipeline complet BilletReduc + Shotgun + IA + producteurs")
    
    args = parser.parse_args()
      
    # --- Suppression de l'appel à print_gpu_memory_info ---
    # print_gpu_memory_info()
    
    # Configurer l'activation/désactivation de l'IA
    if args.skip_ai:
        set_ai_enabled(False)
        logger.info("Fonctionnalité IA désactivée par option --skip-ai")

    if args.all:
        run_full_pipeline()
        exit()

    # Si --force-ai est spécifié, réinitialiser le flag AI processed
    if args.force_ai:
        collection_config.update_one(
            {"type": "billetreduc_ai_status"},
            {"$set": {"ai_processed": False}},
            upsert=True
        )
        logger.info("Réinitialisation du statut AI forcée!")
    
    # Si --verify-db est spécifié, vérifier la cohérence de la base de données
    if args.verify_db:
        logger.info("Vérification de la cohérence de la base de données...")
        verify_database_consistency()
    # Si --step est spécifié, exécuter uniquement cette étape
    elif args.step:
        logger.info(f"Exécution de l'étape {args.step} uniquement...")
        steps = [
            scrape_billetreduc_events,
            process_shotgun_events,
            generate_producers_with_coordinates,
            lambda resume=True: run_ai_analysis(resume=False) if not args.skip_ai else logger.info("Étape AI ignorée")
        ]
        
        if 1 <= args.step <= len(steps):
            steps[args.step-1](resume=not args.no_resume)
        else:
            logger.error(f"Étape invalide: {args.step}. Doit être entre 1 et {len(steps)}")
    # Si --run-shotgun-scraper est spécifié, exécuter le processus complet de scraping Shotgun
    elif args.run_shotgun_scraper:
        logger.info(f"Exécution du processus complet de scraping Shotgun (max {args.max_shotgun_pages} pages)...")
        run_shotgun_scraper(max_pages=args.max_shotgun_pages, resume=not args.no_resume)
        logger.info("Génération des producteurs pour les événements Shotgun...")
        generate_producers_with_coordinates(resume=not args.no_resume)
    # Si --shotgun-only est spécifié, traiter uniquement les événements Shotgun déjà dans la base
    elif args.shotgun_only:
        logger.info("Traitement des événements Shotgun déjà dans la base uniquement...")
        process_shotgun_events(resume=not args.no_resume)
        logger.info("Génération des producteurs pour les événements Shotgun...")
        generate_producers_with_coordinates(resume=not args.no_resume)
    # Sinon, exécution normale du processus complet
    else:
        logger.info("Exécution du processus complet (version GPT)...") # Message mis à jour
        process_all_data(run_ai=not args.skip_ai, resume=not args.no_resume)
