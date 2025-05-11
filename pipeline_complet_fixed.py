"""
Pipeline complet remplaçant Google Places API pour la récupération de données de restaurants
Ce script combine:
1. Google Maps Nearby Search API pour la liste initiale des restaurants
2. Capture d'écran Google Maps + OCR
3. Recherche Bing pour trouver les liens vers TheFork/TripAdvisor
4. Scraping des plateformes avec support BrightData pour anti-bot
5. Sauvegarde en MongoDB

Auteur: Kodu
"""

import os
import time
import json
import urllib.parse
import base64
import re
import requests
import tempfile
import argparse
import functools
import uuid
from io import BytesIO
from collections import OrderedDict, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo import MongoClient
import openai
import random
from requests.auth import HTTPBasicAuth
import glob
import shutil
import atexit
from datetime import datetime, timedelta
import traceback
from urllib.parse import urlparse
from bson.objectid import ObjectId
import sys
import pandas as pd
import hashlib

# Selenium et outils web
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException, WebDriverException

# Traitement d'image
from PIL import Image
import pytesseract

# Parsing HTML
from bs4 import BeautifulSoup

# Configuration des API et paramètres
# Clés API en dur pour le test
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
BRIGHTDATA_TOKEN = os.getenv("BRIGHTDATA_TOKEN")
BRIGHTDATA_ENABLED = bool(BRIGHTDATA_TOKEN)
MONGODB_URI = "mongodb+srv://remibarbier:Calvi8Pierc2@lieuxrestauration.szq31.mongodb.net/?retryWrites=true&w=majority&appName=lieuxrestauration"
DB_NAME = "Restauration_Officielle"
COLLECTION_NAME = "producers"
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")  # Clé API pour Google Maps
NUM_THREADS = 4  # Nombre de threads par défaut pour le traitement parallèle
MAPS_API_REQUEST_COUNT = 0  # Compteur de requêtes Google Maps API
MAX_MAPS_API_REQUESTS = 500  # Limite quotidienne de requêtes Google Maps API
MAX_RETRIES = 3
TIMEOUT = 30
DEBUG = False

# Configurer OpenAI API
openai.api_key = OPENAI_API_KEY

# Variables globales pour statistiques de performance
TIMING_STATS = defaultdict(list)
DEBUG_MODE = False  # Mode debug avec logs détaillés
USE_BRIGHTDATA = False  # Utilisation de BrightData pour contourner les mesures anti-bot
BRIGHTDATA_ENABLED = True  # Si le service BrightData est activé

# Cache pour les résultats de recherche
SEARCH_CACHE = {}
HTML_CACHE = {}
BING_SEARCH_CACHE = {}
MAX_CACHE_SIZE = 1000
CACHE_TIMEOUT = 3600  # 1 heure en secondes

# Définition des catégories de restaurant pour Google Maps API
RESTAURANT_CATEGORIES = {
    "restaurant", "cafe", "bar", "meal_takeaway", "bakery", "fast_food",
    "sushi_restaurant", "pizza_restaurant", "chinese_restaurant",
    "indian_restaurant", "french_restaurant", "italian_restaurant",
    "thai_restaurant", "mexican_restaurant", "vietnamese_restaurant",
    "seafood_restaurant", "steakhouse", "burger_restaurant",
    "ice_cream_shop", "brewery", "pub"
}

# Ajouter la variable globale de cache pour les recherches Bing
BING_SEARCH_CACHE = {}

# Décorateur pour mesurer le temps d'exécution des fonctions
def timing_decorator(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if not DEBUG_MODE:
            return func(*args, **kwargs)
        
        # Mesurer le temps pour cette fonction
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        
        # Calculer la durée
        duration = end_time - start_time
        
        # Récupérer le nom du restaurant si disponible dans les arguments
        restaurant_name = None
        for arg in args:
            if isinstance(arg, dict) and arg.get("name"):
                restaurant_name = arg["name"]
                break
        
        # Afficher les informations de timing
        log_prefix = f"[{restaurant_name}] " if restaurant_name else ""
        print(f"⏱️ {log_prefix}{func.__name__}: {duration:.2f} secondes")
        
        # Stocker les stats
        TIMING_STATS[func.__name__].append(duration)
        
        return result
    return wrapper

# Configuration du navigateur Chrome pour Selenium
# Ne pas mettre dans une variable globale pour éviter les problèmes avec copy()
@timing_decorator
def get_chrome_options():
    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1280,1800")
    options.add_argument("--lang=fr")  # Passer en français pour une meilleure extraction
    return options

# Connexion à MongoDB
@timing_decorator
def get_mongo_client():
    """
    Obtient une connexion à MongoDB
    """
    try:
        # Utiliser l'URI MongoDB Atlas
        client = MongoClient(MONGODB_URI)
        # Tester la connexion
        client.server_info()
        return client
    except Exception as e:
        print(f"❌ Erreur lors de la connexion à MongoDB: {str(e)}")
        return None

# =============================================
# ÉTAPE 1: RÉCUPÉRATION DES RESTAURANTS VIA GOOGLE MAPS API
# =============================================

def generate_zones(divisions=5):
    """
    Divise Paris en une grille de zones pour les requêtes Google Maps API
    
    Args:
        divisions: Nombre de divisions par côté pour la grille
    
    Returns:
        Liste de dictionnaires définissant chaque zone
    """
    # Limites géographiques de Paris
    lat_min, lat_max = 48.8156, 48.9022
    lng_min, lng_max = 2.2242, 2.4699
    
    lat_range = lat_max - lat_min
    lng_range = lng_max - lng_min
    
    zones = []
    for i in range(divisions):
        for j in range(divisions):
            zones.append({
                "lat_min": lat_min + (i * lat_range / divisions),
                "lat_max": lat_min + ((i + 1) * lat_range / divisions),
                "lng_min": lng_min + (j * lng_range / divisions),
                "lng_max": lng_min + ((j + 1) * lng_range / divisions)
            })
    
    return zones

@timing_decorator
def get_restaurants_in_zone(zone):
    """
    Récupère tous les restaurants dans une zone définie en utilisant Google Maps Nearby Search API
    
    Args:
        zone: Dictionnaire définissant les limites de la zone (lat_min, lat_max, lng_min, lng_max)
    
    Returns:
        Liste de restaurants avec leurs informations de base
    """
    global MAPS_API_REQUEST_COUNT
    
    # Vérifier si on a atteint la limite quotidienne
    if MAPS_API_REQUEST_COUNT >= MAX_MAPS_API_REQUESTS:
        print("⚠️ Limite quotidienne de l'API Google Maps atteinte (500 requêtes)")
        return []
    
    lat_min, lat_max = zone["lat_min"], zone["lat_max"]
    lng_min, lng_max = zone["lng_min"], zone["lng_max"]
    
    # Étape de la grille (en degrés) pour un espacement d'environ 500 m
    step = 0.005  # Environ 500 mètres
    
    # Points latitudes et longitudes
    lat_points = [lat_min + i * step for i in range(int((lat_max - lat_min) / step) + 1)]
    lng_points = [lng_min + i * step for i in range(int((lng_max - lng_min) / step) + 1)]
    
    all_restaurants = []  # Liste pour stocker tous les résultats
    
    # Parcourir chaque point de la grille
    for lat in lat_points:
        for lng in lng_points:
            # Vérifier si on a atteint la limite
            if MAPS_API_REQUEST_COUNT >= MAX_MAPS_API_REQUESTS:
                print("⚠️ Limite quotidienne de l'API Google Maps atteinte pendant le traitement")
                break
                
            url = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={lat},{lng}&radius=200&type=restaurant&key={GOOGLE_MAPS_API_KEY}"
            MAPS_API_REQUEST_COUNT += 1
            
            try:
                response = requests.get(url)
                data = response.json()
                
                # Vérifiez si des résultats sont retournés
                if 'results' in data:
                    # Filtrer les lieux pour inclure uniquement les catégories pertinentes
                    filtered_places = [
                        place for place in data['results']
                        if set(place.get("types", [])).intersection(RESTAURANT_CATEGORIES)
                    ]
                    all_restaurants.extend(filtered_places)
                
                # Récupérer le token pour la page suivante, s'il existe
                next_page_token = data.get("next_page_token")
                if next_page_token and MAPS_API_REQUEST_COUNT < MAX_MAPS_API_REQUESTS:
                    # Pause pour attendre que la page suivante soit prête
                    time.sleep(2)
                    url = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?pagetoken={next_page_token}&key={GOOGLE_MAPS_API_KEY}"
                    MAPS_API_REQUEST_COUNT += 1
                    
                    response = requests.get(url)
                    data = response.json()
                    
                    if 'results' in data:
                        filtered_places = [
                            place for place in data['results']
                            if set(place.get("types", [])).intersection(RESTAURANT_CATEGORIES)
                        ]
                        all_restaurants.extend(filtered_places)
            except Exception as e:
                print(f"❌ Erreur Google Maps API: {e}")
    
    # Supprimer les doublons en utilisant place_id comme clé unique
    unique_restaurants = {}
    for restaurant in all_restaurants:
        place_id = restaurant.get("place_id")
        if place_id and place_id not in unique_restaurants:
            unique_restaurants[place_id] = restaurant
    
    restaurants_list = list(unique_restaurants.values())
    print(f"✅ {len(restaurants_list)} restaurants récupérés via Google Maps API dans la zone")
    return restaurants_list

def convert_nearby_to_restaurant(place):
    """
    Convertit le résultat de l'API Nearby Search en format restaurant compatible
    
    Args:
        place: Dictionnaire du restaurant issu de l'API Nearby Search
    
    Returns:
        Dictionnaire formaté pour le pipeline
    """
    place_id = place.get("place_id", "")
    maps_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}" if place_id else None
    
    # Extraire les coordonnées
    location = place.get("geometry", {}).get("location", {})
    lat = location.get("lat")
    lng = location.get("lng")
    
    # Construire une structure pour chaque restaurant
    restaurant = {
        "name": place.get("name"),
        "place_id": place_id,
        "maps_url": maps_url,
        "address": place.get("vicinity", ""),
        "lat": lat,
        "lon": lng,
        "rating": place.get("rating"),
        "user_ratings_total": place.get("user_ratings_total"),
        "business_status": place.get("business_status"),
        "types": place.get("types", []),
        "open_now": place.get("opening_hours", {}).get("open_now"),
        "photos": place.get("photos", [])
    }
    
    return restaurant

def is_valid_restaurant(restaurant):
    """
    Vérifie si un restaurant a les données minimales nécessaires
    
    Args:
        restaurant: Dictionnaire du restaurant
    
    Returns:
        Boolean indiquant si le restaurant est valide
    """
    # Vérifier le nom et place_id
    if not restaurant.get("name") or not restaurant.get("place_id"):
        return False
    
    # Vérifier les coordonnées GPS
    has_coordinates = restaurant.get("lat") is not None and restaurant.get("lon") is not None
    
    return has_coordinates

def get_all_paris_restaurants(max_zones=None):
    """
    Récupère tous les restaurants de Paris en divisant la ville en grille
    
    Args:
        max_zones: Nombre maximum de zones à traiter (ou None pour toutes)
    
    Returns:
        Liste complète des restaurants
    """
    all_restaurants = []
    zones = generate_zones(divisions=5)  # Diviser Paris en 25 zones (5x5)
    
    # Limiter le nombre de zones si spécifié
    if max_zones and max_zones < len(zones):
        zones = zones[:max_zones]
        print(f"ℹ️ Traitement limité à {max_zones} zones sur {len(zones)} disponibles")
    
    for i, zone in enumerate(zones):
        print(f"📍 Traitement de la zone {i+1}/{len(zones)}")
        zone_restaurants = get_restaurants_in_zone(zone)
        
        # Convertir au format compatible avec le reste du pipeline
        formatted_restaurants = [convert_nearby_to_restaurant(place) for place in zone_restaurants]
        
        # Filtrer les restaurants valides
        valid_restaurants = [r for r in formatted_restaurants if is_valid_restaurant(r)]
        
        all_restaurants.extend(valid_restaurants)
        
        # Vérifier si on a atteint la limite quotidienne
        if MAPS_API_REQUEST_COUNT >= MAX_MAPS_API_REQUESTS:
            print("⚠️ Limite quotidienne de l'API Google Maps atteinte, arrêt du traitement de zones")
            break
        
        time.sleep(2)  # Pause pour éviter de surcharger l'API
    
    print(f"📊 Total: {len(all_restaurants)} restaurants uniques récupérés via Google Maps API")
    print(f"📊 Requêtes Google Maps API utilisées: {MAPS_API_REQUEST_COUNT}/{MAX_MAPS_API_REQUESTS}")
    
    return all_restaurants

def is_restaurant_in_mongodb(name, maps_url=None, place_id=None):
    """
    Vérifie si un restaurant existe déjà dans MongoDB
    """
    try:
        client = get_mongo_client()
        if not client:
            print("❌ Impossible de se connecter à MongoDB")
            return False
            
        # Utiliser la bonne base de données
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        
        print(f"🔍 Vérification dans MongoDB pour: {name}")
        print(f"  Base de données: {DB_NAME}")
        print(f"  Collection: {COLLECTION_NAME}")
        
        # Construire la requête
        query = {"name": name}
        
        if place_id:
            query["place_id"] = place_id
        elif maps_url:
            query["maps_url"] = maps_url
        
        print(f"  Requête: {query}")
        
        # Vérifier si le restaurant existe
        exists = collection.find_one(query)
        
        if exists:
            print(f"  ✅ Restaurant trouvé dans MongoDB (ID: {exists.get('_id')})")
        else:
            print(f"  ❌ Restaurant non trouvé dans MongoDB")
            
        client.close()
        
        return exists is not None
    except Exception as e:
        print(f"❌ Erreur lors de la vérification MongoDB: {str(e)}")
        traceback.print_exc()
        return False

def convert_to_12h_format(time_str):
    """Convertit une heure au format 24h en format 12h AM/PM"""
    try:
        # Nettoyer l'entrée
        time_str = time_str.strip().replace("h", ":").replace("H", ":")
        if ":" not in time_str:
            time_str = time_str.zfill(4)
            time_str = f"{time_str[:2]}:{time_str[2:]}"
        
        # Parsing de l'heure
        hour = int(time_str.split(":")[0])
        minute = int(time_str.split(":")[1]) if ":" in time_str else 0
        
        # Conversion en format 12h
        if hour == 0:
            return f"12:{minute:02d} AM"
        elif hour < 12:
            return f"{hour}:{minute:02d} AM"
        elif hour == 12:
            return f"12:{minute:02d} PM"
        else:
            return f"{hour-12}:{minute:02d} PM"
    except:
        return time_str

def extract_opening_hours_thefork(soup):
    """
    Extrait les horaires depuis TheFork avec le nouveau format
    """
    horaires = []
    
    # Liste des jours pour mapping
    jours = {
        "monday": "Lundi", "tuesday": "Mardi", "wednesday": "Mercredi",
        "thursday": "Jeudi", "friday": "Vendredi", "saturday": "Samedi", 
        "sunday": "Dimanche"
    }
    
    for tag in soup.select('div[data-testid^="interval-line-"]'):
        # Exemple : "interval-line-wednesday" -> "wednesday"
        testid = tag.get("data-testid")
        jour = testid.replace("interval-line-", "")
        jour_fr = jours.get(jour, jour.capitalize())
        
        heures = [div.get_text(strip=True) for div in tag.find_all("div") 
                 if div.get_text(strip=True) and not any(j in div.get_text(strip=True).lower() 
                 for j in ["aujourd'hui", "today"])]
        
        if heures:
            # Nettoyer et formater les heures
            clean_hours = []
            for h in heures:
                if "-" in h:
                    clean_hours.append(h)
            if clean_hours:
                horaires.append(f"{jour_fr} : {' / '.join(clean_hours)}")
    
    return format_opening_hours(horaires)

def extract_opening_hours_tripadvisor(soup):
    """
    Extrait les horaires depuis TripAdvisor avec le nouveau format
    """
    horaires = []
    days_order = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"]
    
    # Chercher les horaires dans différents formats possibles
    hours_div = soup.find("div", class_=lambda x: x and "hours" in x.lower())
    if not hours_div:
        return format_opening_hours([])  # Retourner format standard si pas trouvé
    
    # Extraire le texte et nettoyer
    hours_text = hours_div.get_text(" ", strip=True)
    
    # Parser chaque jour
    current_day = None
    for line in hours_text.split("\n"):
        line = line.strip()
        if not line:
            continue
        
        # Détecter le jour
        for day in days_order:
            if line.lower().startswith(day.lower()):
                current_day = day
                # Extraire les heures
                hours_part = line[len(day):].strip(" :")
                if hours_part:
                    horaires.append(f"{current_day} : {hours_part}")
                break
    
    # Trier les jours dans l'ordre
    sorted_hours = []
    for day in days_order:
        matching_hours = [h for h in horaires if h.startswith(day)]
        if matching_hours:
            sorted_hours.append(matching_hours[0])
    
    return format_opening_hours(sorted_hours)

def extract_images_and_menus(soup):
    """
    Extrait les images et menus d'une page
    
    Args:
        soup: BeautifulSoup object de la page
    
    Returns:
        Tuple (liste des URLs d'images, liste des URLs des menus)
    """
    # Extraction des images
    image_urls = []
    for img in soup.find_all("img"):
        url = img.get("src") or img.get("data-src") or img.get("data-lazyurl")
        if url and url not in image_urls:
            image_urls.append(url)
    
    # Limiter à 5 images
    image_urls = image_urls[:5]
    
    # Extraction des menus
    menu_urls = []
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if any(k in href.lower() for k in ["menu", "carte", "pdf"]):
            menu_urls.append(href)
    
    # Dédupliquer les URLs
    menu_urls = list(set(menu_urls))
    
    return image_urls, menu_urls

@timing_decorator
def extract_platform_data(url, platform, name=None):
    """
    Extrait les données d'une plateforme spécifique
    
    Args:
        url: URL de la page
        platform: Nom de la plateforme ('thefork' ou 'tripadvisor')
        name: Nom du restaurant (pour les logs)
    
    Returns:
        Dictionnaire des données extraites
    """
    log_prefix = f"[{name}] " if name else ""
    
    # Log de debug pour vérifier l'URL passée
    print(f"{log_prefix}🔗 URL utilisée pour {platform}: {url}")
    
    # Récupérer le HTML via BrightData
    html = fetch_html_with_brightdata(url=url, name=name, platform=platform)
    if not html:
        return {}
    
    # Parser le HTML
    soup = BeautifulSoup(html, 'html.parser')
    
    # Extraire les données selon la plateforme
    data = {}
    
    # Horaires d'ouverture
    if platform == 'thefork':
        data['opening_hours'] = extract_opening_hours_thefork(soup)
    elif platform == 'tripadvisor':
        data['opening_hours'] = extract_opening_hours_tripadvisor(soup)
    
    # Images et menus (commun aux deux plateformes)
    images, menus = extract_images_and_menus(soup)
    data['images'] = images
    data['menus'] = menus
    
    # Ajouter l'URL source
    data['source_url'] = url
    
    return data

def extract_opening_hours_structured(soup):
    """
    Extrait les horaires depuis TheFork et les convertit au format MongoDB
    
    Args:
        soup: BeautifulSoup object de la page TheFork
    
    Returns:
        Liste des horaires au format MongoDB ["Monday: 9:00 AM – 11:00 PM", etc.]
    """
    # Ordre standard des jours pour MongoDB
    days_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    
    # Mapping français -> anglais
    fr_to_en = {
        "lundi": "Monday",
        "mardi": "Tuesday", 
        "mercredi": "Wednesday",
        "jeudi": "Thursday", 
        "vendredi": "Friday",
        "samedi": "Saturday",
        "dimanche": "Sunday"
    }
    
    # Initialiser les horaires par défaut
    formatted_hours = {day: f"{day}: Not specified" for day in days_order}
    
    # Extraire les horaires depuis les balises data-testid
    for tag in soup.select('div[data-testid^="interval-line-"]'):
        # Exemple : "interval-line-wednesday" -> "wednesday"
        testid = tag.get("data-testid")
        jour = testid.replace("interval-line-", "")
        
        # Extraire les heures en ignorant les mentions "aujourd'hui"
        heures = []
        for div in tag.find_all("div"):
            text = div.get_text(strip=True)
            if text and not any(x in text.lower() for x in ["aujourd'hui", "today"]):
                # Nettoyer et formater l'heure
                if "-" in text:
                    try:
                        start, end = text.split("-")
                        # Convertir en format 12h
                        start_time = convert_to_12h_format(start.strip())
                        end_time = convert_to_12h_format(end.strip())
                        heures.append(f"{start_time} – {end_time}")
                    except:
                        continue
        
        if heures:
            # Convertir le jour en anglais
            en_day = None
            for fr_day, en_day_name in fr_to_en.items():
                if fr_day in jour.lower():
                    en_day = en_day_name
                    break
            
            if en_day:
                formatted_hours[en_day] = f"{en_day}: {' / '.join(heures)}"
    
    # Retourner les horaires dans l'ordre standard
    return [formatted_hours[day] for day in days_order]

def extract_thefork_data(lafourchette_url, restaurant_name=None):
    """
    Extraction des données depuis LaFourchette/TheFork via BrightData
    
    Args:
        lafourchette_url: URL de la page LaFourchette
        restaurant_name: Nom du restaurant (pour les logs)
    
    Returns:
        Dictionnaire des données extraites
    """
    log_prefix = f"[{restaurant_name}] " if restaurant_name else ""
    
    # Validation stricte de l'URL
    if not lafourchette_url or not isinstance(lafourchette_url, str):
        print(f"{log_prefix}⚠️ URL LaFourchette invalide ou manquante")
        return {}
    
    # S'assurer que l'URL commence par http/https
    if not lafourchette_url.lower().startswith(("http://", "https://")):
        print(f"{log_prefix}⚠️ URL LaFourchette invalide: {lafourchette_url}")
        return {}
    
    print(f"🍴 Extraction des données LaFourchette pour {restaurant_name}")
    
    try:
        # Utiliser BrightData pour contourner les protections anti-bot
        html = fetch_html_with_brightdata(url=lafourchette_url, name=restaurant_name, platform="lafourchette")
        if not html:
            print(f"{log_prefix}❌ Erreur lors de l'extraction des données LaFourchette: Pas de HTML récupéré")
            return {}
        
        # Parser le HTML avec BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')
        
        # Initialiser le dictionnaire de résultats
        result = {
            "opening_hours": [],
            "photos": [],
            "phone_number": "",
            "website": "",
            "rating": 0,
            "price_level": "",
            "description": ""
        }
        
        # Extraire les horaires d'ouverture (plusieurs méthodes possibles)
        hours_container = soup.find('div', class_=lambda c: c and ('timeslots' in c.lower() or 'hours' in c.lower() or 'horaires' in c.lower()))
        if hours_container:
            days_fr = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"]
            days_en = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            all_days = days_fr + days_en
            
            formatted_hours = []
            day_elements = hours_container.find_all(['div', 'p', 'li', 'span'], text=lambda t: t and any(day.lower() in t.lower() for day in all_days))
            
            if day_elements:
                for day_el in day_elements:
                    day_text = day_el.get_text(strip=True)
                    if day_text and any(day.lower() in day_text.lower() for day in all_days):
                        formatted_hours.append(day_text)
            
            if not formatted_hours:
                # Méthode alternative: extraire tout le texte des horaires
                hours_text = hours_container.get_text(strip=True)
                if hours_text:
                    # Diviser par des séparateurs communs
                    for separator in [',', '.', ';', '\n']:
                        if separator in hours_text:
                            formatted_hours = [part.strip() for part in hours_text.split(separator) if part.strip()]
                            break
                    
                    if not formatted_hours:
                        formatted_hours = [hours_text]
            
            result["opening_hours"] = formatted_hours
            print(f"{log_prefix}✅ Horaires extraits: {len(formatted_hours)} entrées")
        
        # Extraire les images (plusieurs sélecteurs possibles)
        photo_urls = []
        
        # 1. Photos dans le carrousel principal
        carousel_images = soup.select('.carousel img[src], .gallery img[src], .photos img[src], [data-test="restaurant-cover"] img[src]')
        if carousel_images:
            for img in carousel_images:
                src = img.get('src')
                if src:
                    if src.startswith('//'):
                        src = 'https:' + src
                    photo_urls.append(src)
        
        # 2. Photos dans les miniatures ou galeries
        thumbnail_images = soup.select('.restaurant-photos img[src], .thumbnail img[src], [data-test="restaurant-photos"] img[src]')
        if thumbnail_images:
            for img in thumbnail_images:
                src = img.get('src')
                if src:
                    if src.startswith('//'):
                        src = 'https:' + src
                    photo_urls.append(src)
        
        # 3. Recherche générique d'images
        if not photo_urls:
            for img in soup.find_all("img"):
                src = img.get("src") or img.get("data-src")
                if src and not src.startswith("data:"):
                    # Filtrer les petites images et les icônes
                    is_icon = "icon" in src.lower() or "logo" in src.lower() or "avatar" in src.lower()
                    if not is_icon:
                        if src.startswith("//"):
                            src = "https:" + src
                        photo_urls.append(src)
        
        # Dédupliquer et limiter à 5 photos (URLs)
        unique_photo_urls = list(dict.fromkeys(photo_urls))[:5]
        
        # Convertir les URLs en Base64
        photos_base64 = []
        if unique_photo_urls:
            print(f"{log_prefix}⚙️ Conversion de {len(unique_photo_urls)} photos en Base64...")
            for photo_url in unique_photo_urls:
                base64_data = url_to_base64(photo_url)
                if base64_data:
                    photos_base64.append(base64_data)
            
            result["photos"] = photos_base64
            print(f"{log_prefix}✅ Photos converties en Base64: {len(photos_base64)}")
        
        # Extraire le numéro de téléphone
        phone_element = soup.find('a', href=lambda h: h and h.startswith('tel:'))
        if phone_element:
            phone = phone_element.get_text(strip=True)
            result["phone_number"] = phone
            print(f"{log_prefix}✅ Téléphone extrait: {phone}")
        
        # Extraire le site web
        website_element = soup.find('a', href=lambda h: h and (h.startswith('http') and not ('lafourchette' in h or 'thefork' in h)))
        if website_element:
            website = website_element.get('href')
            result["website"] = website
            print(f"{log_prefix}✅ Site web extrait: {website}")
        
        # Extraire la note
        rating_elements = soup.select('[data-test="restaurant-rating"], .restaurant-rating, .rating, .score')
        for rating_element in rating_elements:
            rating_text = rating_element.get_text(strip=True)
            # Chercher un nombre de 1 à 10 ou de 1 à 5 avec éventuellement une décimale
            rating_match = re.search(r'(\d+[.,]\d+|\d+)(?:\s*[/|]?\s*(?:5|10))?', rating_text)
            if rating_match:
                try:
                    rating_value = float(rating_match.group(1).replace(',', '.'))
                    # Normaliser sur 5
                    if rating_value > 5:
                        rating_value /= 2
                    result["rating"] = rating_value
                    print(f"{log_prefix}✅ Note extraite: {rating_value}")
                    break
                except:
                    pass
        
        # Extraire le niveau de prix (€, €€, €€€)
        price_elements = soup.select('[data-test="restaurant-price"], .restaurant-price, .price')
        for price_element in price_elements:
            price_text = price_element.get_text(strip=True)
            if '€' in price_text:
                # Compter le nombre de symboles € pour déterminer le niveau de prix
                price_level = price_text.count('€')
                result["price_level"] = price_level
                print(f"{log_prefix}✅ Niveau de prix extrait: {price_level}")
                break
        
        return result  # Correction: Cette ligne doit être au même niveau que la boucle for, pas à l'intérieur
        
    except Exception as e:
        print(f"{log_prefix}❌ Erreur lors de l'extraction des données TripAdvisor: {str(e)}")
        traceback.print_exc()
        return {}

def extract_tripadvisor_data(tripadvisor_url, restaurant_name=None):
    """
    Extraction des données depuis TripAdvisor via BrightData
    
    Args:
        tripadvisor_url: URL de la page TripAdvisor
        restaurant_name: Nom du restaurant (pour les logs)
    
    Returns:
        Dictionnaire des données extraites (notamment photos en Base64)
    """
    log_prefix = f"[{restaurant_name}] " if restaurant_name else ""
    
    # Validation de l'URL
    if not tripadvisor_url or not isinstance(tripadvisor_url, str) or not tripadvisor_url.lower().startswith(("http://", "https://")):
        print(f"{log_prefix}⚠️ URL TripAdvisor invalide ou manquante: {tripadvisor_url}")
        return {}
        
    print(f"🌐 Extraction des données TripAdvisor pour {restaurant_name}")
    
    try:
        # Utiliser BrightData
        html = fetch_html_with_brightdata(url=tripadvisor_url, name=restaurant_name, platform="tripadvisor")
        if not html:
            print(f"{log_prefix}❌ Erreur lors de l'extraction des données TripAdvisor: Pas de HTML récupéré")
            return {}
            
        soup = BeautifulSoup(html, 'html.parser')
        
        result = {
            "photos": [],
            "opening_hours": [], # Placeholder, could reuse extract_opening_hours_tripadvisor if needed
            "rating": 0, # Placeholder
            # Ajouter d'autres champs si nécessaire
        }
        
        # Extraire les URLs des photos (TripAdvisor selectors might vary)
        photo_urls = []
        # Common selectors for TripAdvisor images
        photo_selectors = [
            'img.basicImg', 
            'div[data-gallery-photos] img', 
            'img[data-photoid]', 
            'div.photo-grid img[src]', 
            'img[data-lazyurl]'
        ]
        
        for selector in photo_selectors:
            images = soup.select(selector)
            for img in images:
                src = img.get('src') or img.get('data-src') or img.get('data-lazyurl')
                if src and not src.startswith('data:'):
                    if src.startswith('//'):
                        src = 'https:' + src
                    # Basic check to avoid tiny icons/logos
                    if 'avatar' not in src.lower() and 'icon' not in src.lower(): 
                         photo_urls.append(src)
            if photo_urls: # Stop if found with one selector
                 break 
                 
        # Dédupliquer et limiter (e.g., à 5 photos)
        unique_photo_urls = list(dict.fromkeys(photo_urls))[:5]
        
        # Convertir les URLs en Base64
        photos_base64 = []
        if unique_photo_urls:
            print(f"{log_prefix}⚙️ Conversion de {len(unique_photo_urls)} photos TripAdvisor en Base64...")
            for photo_url in unique_photo_urls:
                base64_data = url_to_base64(photo_url)
                if base64_data:
                    photos_base64.append(base64_data)
            
            result["photos"] = photos_base64
            print(f"{log_prefix}✅ Photos TripAdvisor converties en Base64: {len(photos_base64)}")
            
        # TODO: Ajouter l'extraction d'autres données TripAdvisor (horaires, note, etc.) ici si nécessaire
        # Par exemple:
        # result['opening_hours'] = extract_opening_hours_tripadvisor(soup)
        # ... extraction de la note ...
            
        return result
        
    except Exception as e:
        print(f"{log_prefix}❌ Erreur lors de l'extraction des données TripAdvisor: {str(e)}")
        traceback.print_exc()
        return {}

@timing_decorator
def enrich_with_platforms(structured_data, name, address):
    """
    Enrichit les données du restaurant avec les informations des plateformes
    
    Args:
        structured_data: Données existantes du restaurant
        name: Nom du restaurant
        address: Adresse du restaurant
    
    Returns:
        Données enrichies avec les informations des plateformes
    """
    # Si BrightData n'est pas activé, on ne fait pas d'enrichissement
    if not BRIGHTDATA_ENABLED:
        print(f"ℹ️ Enrichissement des plateformes désactivé (pas de token BrightData)")
        return structured_data
    
    print(f"🔍 Recherche de liens pour {name}")
    
    # Rechercher des liens via Bing
    try:
        platform_links = search_links_bing(name, address)
        if not platform_links:
            print(f"⚠️ Aucun lien de plateforme trouvé pour {name}")
            return structured_data
            
        # Extraire les informations de TheFork/LaFourchette
        thefork_url = platform_links.get('thefork')
        thefork_data = {}
        if thefork_url and validate_platform_link(thefork_url, platform='thefork'):
            print(f"🍴 Extraction des données LaFourchette pour {name}")
            thefork_data = extract_thefork_data(thefork_url, restaurant_name=name)
        
        # Extraire les informations de TripAdvisor
        tripadvisor_url = platform_links.get('tripadvisor')
        tripadvisor_data = {}
        if tripadvisor_url and validate_platform_link(tripadvisor_url, platform='tripadvisor'):
            print(f"🌐 Extraction des données TripAdvisor pour {name}")
            tripadvisor_data = extract_tripadvisor_data(tripadvisor_url, restaurant_name=name)
        
        # Fusionner les données
        merged_data = structured_data.copy()
        
        # Mettre à jour les horaires (priorité: TheFork, puis TripAdvisor)
        if thefork_data.get('opening_hours'):
            merged_data['opening_hours'] = thefork_data['opening_hours']
            print(f"✅ Horaires extraits de LaFourchette: {len(thefork_data['opening_hours'])} entrées")
        elif tripadvisor_data.get('opening_hours'):
            merged_data['opening_hours'] = tripadvisor_data['opening_hours']
            print(f"✅ Horaires extraits de TripAdvisor: {len(tripadvisor_data['opening_hours'])} entrées")
        
        # Mettre à jour le téléphone
        if thefork_data.get('phone_number'):
            merged_data['phone_number'] = thefork_data['phone_number']
            merged_data['international_phone_number'] = thefork_data['phone_number']
            print(f"✅ Téléphone extrait de LaFourchette: {thefork_data['phone_number']}")
        elif tripadvisor_data.get('phone_number'):
            merged_data['phone_number'] = tripadvisor_data['phone_number']
            merged_data['international_phone_number'] = tripadvisor_data['phone_number']
            print(f"✅ Téléphone extrait de TripAdvisor: {tripadvisor_data['phone_number']}")
        
        # Mettre à jour le site web
        if thefork_data.get('website'):
            merged_data['website'] = thefork_data['website']
            print(f"✅ Site web extrait de LaFourchette: {thefork_data['website']}")
        elif tripadvisor_data.get('website'):
            merged_data['website'] = tripadvisor_data['website']
            print(f"✅ Site web extrait de TripAdvisor: {tripadvisor_data['website']}")
        
        # Mettre à jour la note si non définie
        if merged_data.get('rating', 0) == 0:
            if thefork_data.get('rating', 0) > 0:
                merged_data['rating'] = thefork_data['rating']
                print(f"✅ Note extraite de LaFourchette: {thefork_data['rating']}")
            elif tripadvisor_data.get('rating', 0) > 0:
                merged_data['rating'] = tripadvisor_data['rating']
                print(f"✅ Note extraite de TripAdvisor: {tripadvisor_data['rating']}")
        
        # Mettre à jour le niveau de prix si non défini
        if not merged_data.get('price_level') and thefork_data.get('price_level'):
            merged_data['price_level'] = thefork_data['price_level']
            print(f"✅ Niveau de prix extrait de LaFourchette: {thefork_data['price_level']}")
        
        # Mettre à jour la description si non définie
        if not merged_data.get('description') and thefork_data.get('description'):
            merged_data['description'] = thefork_data['description']
            print(f"✅ Description extraite de LaFourchette: {len(thefork_data['description'])} caractères")
        
        # Mettre à jour les photos (priorité aux photos de TheFork et TripAdvisor)
        photos = []
        
        # D'abord les photos de TheFork
        if thefork_data.get('photos'):
            photos.extend(thefork_data['photos'])
            print(f"✅ Photos extraites de LaFourchette: {len(thefork_data['photos'])}")
        
        # Ensuite les photos de TripAdvisor
        if tripadvisor_data.get('photos'):
            photos.extend(tripadvisor_data['photos'])
            print(f"✅ Photos extraites de TripAdvisor: {len(tripadvisor_data['photos'])}")
        
        # Supprimer les doublons et limiter le nombre total de photos
        photos = list(dict.fromkeys(photos))[:10]
        
        # Mise à jour des photos seulement si nous en avons trouvé
        if photos:
            # Si nous avons une photo principale mais pas de photos
            if merged_data.get('photo') and not merged_data.get('photos'):
                merged_data['photos'] = [merged_data['photo']] + photos
            else:
                merged_data['photos'] = photos
            
            # Utiliser la première photo comme photo principale si elle n'existe pas
            if not merged_data.get('photo') and photos:
                merged_data['photo'] = photos[0]
            
            print(f"✅ Total de photos après fusion: {len(merged_data.get('photos', []))}")
        
        # Traçage
        print(f"✅ Enrichissement terminé pour {name}")
        return merged_data
        
    except Exception as e:
        print(f"❌ Erreur lors de l'enrichissement avec des plateformes: {str(e)}")
        traceback.print_exc()
        return structured_data

@timing_decorator
def extract_with_brightdata(url, name=None, platform=None):
    """
    Extrait les données d'une plateforme via BrightData
    """
    if not url:
        return None
        
    print(f"[{name}] 🌐 Scraping {platform} via BrightData...")
    
    try:
        # Configuration BrightData
        brightdata_url = f"http://{BRIGHTDATA_TOKEN}:@brd.superproxy.io:22225"
        proxies = {
            "http": brightdata_url,
            "https": brightdata_url
        }
        
        # Requête avec BrightData
        response = requests.get(url, proxies=proxies, timeout=120)
        response.raise_for_status()
        
        # Parsing avec BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extraction selon la plateforme
        if platform == 'thefork' or 'lafourchette' in url:
            return extract_thefork_data(url=url, restaurant_name=name)
        elif platform == 'tripadvisor' or 'tripadvisor' in url:
            return extract_tripadvisor_data(tripadvisor_url=url, restaurant_name=name)
            
    except Exception as e:
        print(f"❌ [{name}] Erreur BrightData pour {platform}: {str(e)}")
        return None

@timing_decorator
def save_to_mongodb(restaurant_data):
    """
    Sauvegarde les données du restaurant dans MongoDB
    """
    try:
        # Normaliser les données
        normalized_data = normalize_restaurant_data(restaurant_data)
        
        # Connexion à MongoDB Atlas en utilisant la variable globale
        client = MongoClient(MONGODB_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        
        # Stocker l'ID et le retirer de l'ensemble de données pour l'update
        doc_id = normalized_data.get("_id")
        
        # Créer une copie pour l'update sans modifier le champ _id
        update_data = normalized_data.copy()
        if "_id" in update_data:
            del update_data["_id"]
        
        # Utiliser update_one avec upsert=True pour éviter les doublons
        # Place_id est utilisé comme clé si disponible, sinon utiliser le nom
        if doc_id:
            identifier = {"_id": doc_id}
        else:
            identifier = {"name": normalized_data["name"]}
        
        # Insérer ou mettre à jour le restaurant
        result = collection.update_one(
            identifier,
            {"$set": update_data},
            upsert=True
        )
        
        if result.acknowledged:
            if result.matched_count > 0:
                print(f"✅ {normalized_data['name']}: Restaurant mis à jour dans MongoDB")
            else:
                print(f"✅ {normalized_data['name']}: Restaurant ajouté dans MongoDB")
            return True
        else:
            print(f"❌ {normalized_data['name']}: Échec de la sauvegarde dans MongoDB")
            return False
            
    except Exception as e:
        print(f"❌ Erreur lors de la sauvegarde dans MongoDB: {str(e)}")
        traceback.print_exc()
        return False
    finally:
        if 'client' in locals():
            client.close()

# Fonction pour vérifier si un restaurant existe sur Google Maps
@timing_decorator
def verify_restaurant_on_maps(name, address, lat=None, lon=None, place_id=None):
    """
    Vérifie si le restaurant existe sur Google Maps et récupère ses données
    
    Args:
        name: Nom du restaurant
        address: Adresse du restaurant
        lat: Latitude (optionnelle)
        lon: Longitude (optionnelle)
        place_id: ID du lieu Google Maps (optionnel)
    
    Returns:
        Dictionnaire avec les données du restaurant trouvé
        ou None si non trouvé
    """
    try:
        # Si nous avons un place_id et des coordonnées, nous pouvons créer directement les données
        if place_id and lat is not None and lon is not None:
            return {
                "name": name,
                "address": address,
                "maps_url": f"https://www.google.com/maps/place/?q=place_id:{place_id}",
                "place_id": place_id,
                "latitude": lat,
                "longitude": lon
            }
            
        print(f"🔍 Recherche de {name} ({address}) sur Google Maps")
        
        with ChromeSessionManager() as session:
            # Utiliser le driver de la session et non la session elle-même
            driver = session.driver
            
            # Vérifier que le driver a été correctement initialisé
            if not driver:
                print(f"❌ Échec d'initialisation du driver Chrome pour {name}")
                return None
                
            # Si des coordonnées sont spécifiées, rechercher directement par coordonnées
            if lat is not None and lon is not None:
                maps_url = f"https://www.google.com/maps/search/{lat},{lon}"
            else:
                # Sinon recherche par nom et adresse
                maps_url = search_google_maps(driver, name, address)
                
            if not maps_url:
                print(f"❌ {name}: URL Google Maps non trouvée")
                return None
                
            # Extraire l'ID de lieu et les coordonnées
            extracted_place_id = extract_place_id(driver.current_url)
            coordinates = extract_coordinates(driver)
            
            if not extracted_place_id and not coordinates:
                print(f"❌ {name}: Impossible d'extraire l'ID de lieu ou les coordonnées")
                return None
                
            return {
                "name": name,
                "address": address,
                "maps_url": maps_url,
                "place_id": extracted_place_id,
                "latitude": coordinates.get("lat") if coordinates else None,
                "longitude": coordinates.get("lng") if coordinates else None
            }
            
    except Exception as e:
        print(f"❌ Erreur lors de la vérification sur Google Maps: {str(e)}")
        traceback.print_exc()
        return None

@timing_decorator
def process_restaurant(restaurant):
    """
    Traite un restaurant complet avec toutes les étapes
    """
    try:
        name = restaurant.get("name", "")
        address = restaurant.get("address", "")
        place_id = restaurant.get("place_id", "")
        lat = restaurant.get("lat")
        lon = restaurant.get("lon")
        rating = restaurant.get("rating", 0)
        
        print(f"\n{'='*50}")
        print(f"Traitement de: {name}, {address}")
        print(f"{'='*50}\n")
        
        # Vérifier si le restaurant existe déjà dans MongoDB
        if is_restaurant_in_mongodb(name, restaurant.get("maps_url"), place_id):
            print(f"⚠️ {name}: Déjà dans MongoDB, on passe au suivant")
            return True
            
        # Si nous avons déjà toutes les informations nécessaires depuis l'API Google Maps
        if place_id and lat is not None and lon is not None:
            print(f"[{name}] ✅ Utilisation des données Google Maps API existantes")
            # Créer directement les données à partir des informations de l'API
            maps_data = {
                "name": name,
                "address": address,
                "maps_url": f"https://www.google.com/maps/place/?q=place_id:{place_id}",
                "place_id": place_id,
                "latitude": lat,
                "longitude": lon,
                "rating": rating
            }
        else:
            # Sinon, vérifier sur Google Maps via le navigateur en transmettant les informations disponibles
            print(f"[{name}] 🔍 Vérification sur Google Maps...")
            maps_data = verify_restaurant_on_maps(
                name=name, 
                address=address,
                lat=lat,
                lon=lon,
                place_id=place_id
            )
            
            if not maps_data:
                # Si la recherche sur Maps a échoué mais qu'on a déjà les coordonnées, créer une fiche minimale
                if lat is not None and lon is not None:
                    print(f"[{name}] ⚠️ Création d'une fiche minimale avec les coordonnées disponibles")
                    maps_data = {
                        "name": name,
                        "address": address,
                        "maps_url": f"https://www.google.com/maps/search/{lat},{lon}",
                        "place_id": place_id if place_id else f"custom_{hashlib.md5(name.encode()).hexdigest()[:10]}_{name.replace(' ', '_')}",
                        "latitude": lat,
                        "longitude": lon,
                        "rating": rating
                    }
                else:
                    print(f"❌ {name}: Non trouvé sur Google Maps")
                    return False
            
        # Étape 2: Capture des screenshots et extraction des données additionnelles
        print(f"[{name}] 📸 Capture des données visuelles...")
        restaurant_data = process_restaurant_with_maps_screenshots(maps_data)
        if not restaurant_data:
            print(f"❌ {name}: Échec de l'extraction des données visuelles")
            return False
            
        # Étape 3: Enrichissement avec les plateformes externes
        if USE_BRIGHTDATA:
            print(f"[{name}] 🌐 Enrichissement avec les plateformes externes...")
            platform_links = search_links_bing(name, address)
            if platform_links:
                restaurant_data["platform_links"] = platform_links
        
        # Étape 4: Sauvegarde en MongoDB
        print(f"[{name}] 💾 Sauvegarde en MongoDB...")
        if save_to_mongodb(restaurant_data):
            print(f"✅ {name}: Traitement réussi")
            return True
        else:
            print(f"❌ {name}: Échec de la sauvegarde MongoDB")
            return False
            
    except Exception as e:
        print(f"❌ {name}: Erreur lors du traitement: {str(e)}")
        traceback.print_exc()
        return False
    finally:
        print(f"\n{'='*50}\n")

@timing_decorator
def process_restaurants_with_threadpool(restaurants, num_threads=NUM_THREADS, skip_existing=True):
    """
    Traite une liste de restaurants en parallèle avec un pool de threads
    
    Args:
        restaurants: Liste de restaurants à traiter
        num_threads: Nombre de threads à utiliser
        skip_existing: Si True, ignore les restaurants déjà en base
    
    Returns:
        Tuple (nb_success, nb_total)
    """
    if not restaurants:
        print("❌ Aucun restaurant à traiter")
        return 0, 0
    
    # Vérifier les restaurants déjà en base si demandé
    if skip_existing:
        print("🔍 Vérification des restaurants déjà en base...")
        restaurants_to_process = []
        
        for restaurant in restaurants:
            name = restaurant.get("name", "")
            maps_url = restaurant.get("maps_url", None)
            place_id = restaurant.get("place_id", None)
            
            if not is_restaurant_in_mongodb(name, maps_url, place_id):
                restaurants_to_process.append(restaurant)
        
        skipped = len(restaurants) - len(restaurants_to_process)
        print(f"📊 {skipped}/{len(restaurants)} restaurants déjà en base, ignorés")
        
        restaurants = restaurants_to_process
    
    if not restaurants:
        print("✅ Tous les restaurants sont déjà en base, rien à faire")
        return 0, 0
    
    # Créer un pool de threads et soumettre les tâches
    thread_pool = ThreadPoolExecutor(max_workers=num_threads)
    future_to_restaurant = {
        thread_pool.submit(process_restaurant, restaurant): restaurant
        for restaurant in restaurants
    }
    
    # Suivi du progrès et résultats
    completed = 0
    success = 0
    total = len(future_to_restaurant)
    
    print("\n" + "=" * 50)
    
    # Traiter les résultats au fur et à mesure
    for future in as_completed(future_to_restaurant):
        restaurant = future_to_restaurant[future]
        name = restaurant.get("name", "Inconnu")
        
        try:
            result = future.result()
            if result:
                success += 1
                print(f"✅ {name}: Traitement réussi")
            else:
                print(f"❌ {name}: Échec du traitement")
        except Exception as e:
            print(f"❌ {name}: Erreur lors du traitement: {str(e)}")
        
        completed += 1
        print(f"\r📊 Progrès: {completed}/{total} restaurants traités ({success} réussis)", end="")
    
    thread_pool.shutdown()
    
    print(f"\n\n🎉 Traitement terminé: {success}/{total} restaurants traités avec succès")
    
    return success, total

def print_timing_stats():
    """Affiche les statistiques de timing pour aider à identifier les goulots d'étranglement"""
    if not TIMING_STATS:
        return
    
    print("\n📊 STATISTIQUES DE PERFORMANCE:")
    print("=" * 80)
    print(f"{'FONCTION':<40} | {'APPELS':<6} | {'MOYENNE (s)':<12} | {'MIN (s)':<10} | {'MAX (s)':<10}")
    print("-" * 80)
    
    for func_name, times in sorted(TIMING_STATS.items(), key=lambda x: sum(x[1])/len(x[1]) if x[1] else 0, reverse=True):
        if not times:
            continue
        avg_time = sum(times) / len(times)
        min_time = min(times)
        max_time = max(times)
        print(f"{func_name:<40} | {len(times):<6} | {avg_time:<12.2f} | {min_time:<10.2f} | {max_time:<10.2f}")
    
    print("=" * 80)
    print("Les fonctions sont triées par temps moyen d'exécution (du plus lent au plus rapide)")
    print("Ces statistiques vous aideront à identifier les goulots d'étranglement du pipeline")

def parse_args():
    """
    Parse les arguments en ligne de commande
    """
    global DEBUG_MODE, NUM_THREADS, USE_BRIGHTDATA, BRIGHTDATA_ENABLED
    
    parser = argparse.ArgumentParser(description="Pipeline de collecte et traitement des restaurants")
    
    # Ajouter des options de ligne de commande
    parser.add_argument("--debug", action="store_true", help="Activer le mode debug avec logs détaillés")
    parser.add_argument("--threads", type=int, default=NUM_THREADS, help=f"Nombre de threads (défaut: {NUM_THREADS})")
    parser.add_argument("--max", type=int, default=None, help="Nombre maximum de restaurants à traiter")
    parser.add_argument("--start", type=int, default=0, help="Index de départ pour le traitement des restaurants")
    parser.add_argument("--brightdata", action="store_true", help="Utiliser BrightData pour contourner les mesures anti-bot")
    parser.add_argument("--small-area", action="store_true", help="Utiliser une zone de test plus petite")
    parser.add_argument("--skip-existing", action="store_true", help="Ignorer les restaurants déjà dans MongoDB")
    parser.add_argument("--restaurant", type=str, help="Traiter un restaurant spécifique (nom, adresse)")
    parser.add_argument("--test", action="store_true", help="Exécuter en mode test sur quelques restaurants")
    parser.add_argument("--load-from-file", type=str, help="Charger les restaurants depuis un fichier")
    # Nouvelles options
    parser.add_argument("--zones", type=int, default=None, help="Nombre de zones géographiques à traiter")
    parser.add_argument("--max-restaurants", type=int, default=None, help="Nombre maximum de restaurants à traiter")
    parser.add_argument("--test-area", action="store_true", help="Utiliser une petite zone de test")
    
    args = parser.parse_args()
    
    # Mettre à jour les variables globales selon les arguments
    DEBUG_MODE = args.debug
    NUM_THREADS = args.threads
    USE_BRIGHTDATA = args.brightdata
    BRIGHTDATA_ENABLED = args.brightdata
    
    return args

# =============================================
# FONCTION PRINCIPALE
# =============================================

def main():
    """
    Fonction principale exécutée quand le script est lancé directement
    """
    global USE_BRIGHTDATA, BRIGHTDATA_ENABLED
    
    # Créer les dossiers nécessaires
    os.makedirs(SCREENSHOT_DIR, exist_ok=True)
    
    # Enregistrer la fonction de nettoyage
    atexit.register(cleanup_temp_dirs)
    
    # Parser les arguments
    args = parse_args()
    
    # Configurer les options en fonction des arguments
    USE_BRIGHTDATA = args.brightdata
    BRIGHTDATA_ENABLED = args.brightdata
    DEBUG_MODE = args.debug
    
    # Vérifier le contenu de MongoDB avant de commencer
    check_mongodb_content()
    
    # Traitement spécial pour le mode test-area
    if args.test_area:
        print("\n📋 Mode zone de test activé")
        test_zone = get_small_test_area()
        restaurants = get_restaurants_in_zone(test_zone)
        
        # Limiter si nécessaire
        if args.max_restaurants:
            restaurants = restaurants[:args.max_restaurants]
            print(f"🔄 Nombre de restaurants limité à {args.max_restaurants}")
        
        print(f"✅ {len(restaurants)} restaurants trouvés dans la zone de test")
        process_restaurants_with_threadpool(restaurants, num_threads=args.threads, skip_existing=False)
        return
        
    # Traitement spécial pour le mode zones limité
    if args.zones:
        print(f"\n📋 Mode zones limité activé: {args.zones} zones")
        zones = generate_zones()
        limited_zones = zones[:args.zones]
        
        all_restaurants = []
        for i, zone in enumerate(limited_zones):
            print(f"📍 Traitement de la zone {i+1}/{len(limited_zones)}")
            zone_restaurants = get_restaurants_in_zone(zone)
            all_restaurants.extend(zone_restaurants)
            
            if args.max_restaurants and len(all_restaurants) >= args.max_restaurants:
                all_restaurants = all_restaurants[:args.max_restaurants]
                print(f"🔄 Nombre de restaurants limité à {args.max_restaurants} - arrêt du traitement de zones")
                break
                
        print(f"✅ Total de {len(all_restaurants)} restaurants récupérés dans {len(limited_zones)} zones")
        process_restaurants_with_threadpool(all_restaurants, num_threads=args.threads, skip_existing=args.skip_existing)
        return
    
    # Continuer avec le comportement normal pour les autres options...
    
    # Charger les données de restaurants
    print("\n📋 Chargement des données de restaurants...")
    
    # Utiliser un jeu de données plus petit pour les tests si demandé
    if args.small_area:
        restaurants = load_test_restaurants()
    else:
        restaurants = load_restaurants_from_file()
    
    if not restaurants:
        print("❌ Aucun restaurant trouvé dans les données")
        return
        
    total_restaurants = len(restaurants)
    print(f"✅ {total_restaurants} restaurants chargés")
    
    # Appliquer les limites
    start_idx = 0  # Initialiser à 0 par défaut
    if hasattr(args, 'start'):
        start_idx = min(args.start, total_restaurants - 1)
        
    if args.max:
        end_idx = min(start_idx + args.max, total_restaurants)
    else:
        end_idx = total_restaurants
        
    restaurants_to_process = restaurants[start_idx:end_idx]
    print(f"🔄 Traitement de {len(restaurants_to_process)} restaurants (#{start_idx} à #{end_idx-1})")
    
    # Traiter les restaurants
    start_time = time.time()
    
    if args.threads > 1:
        print(f"⚙️ Utilisation de {args.threads} threads parallèles")
        with ThreadPoolExecutor(max_workers=args.threads) as executor:
            results = list(executor.map(process_restaurant, restaurants_to_process))
        
        success_count = sum(1 for r in results if r)
    else:
        print("⚙️ Traitement séquentiel")
        success_count = 0
        for restaurant in restaurants_to_process:
            if process_restaurant(restaurant):
                success_count += 1
    
    # Afficher le résumé
    elapsed_time = time.time() - start_time
    print(f"\n{'='*50}")
    print(f"Résumé du traitement:")
    print(f"- {len(restaurants_to_process)} restaurants traités")
    print(f"- {success_count} restaurants traités avec succès")
    print(f"- {len(restaurants_to_process) - success_count} échecs")
    print(f"- Temps écoulé: {elapsed_time:.2f} secondes")
    print(f"{'='*50}\n")

def test_mongodb_results(restaurant_name):
    """
    Teste et affiche les résultats d'un restaurant dans MongoDB
    """
    try:
        client = get_mongo_client()
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        
        # Rechercher le restaurant
        restaurant = collection.find_one({"name": restaurant_name})
        
        if not restaurant:
            print(f"❌ Restaurant {restaurant_name} non trouvé dans MongoDB")
            return
            
        print(f"\n{'='*50}")
        print(f"Résultats MongoDB pour: {restaurant_name}")
        print(f"{'='*50}")
        
        # Afficher les informations principales
        print("\n📋 Informations principales:")
        print(f"  Nom: {restaurant.get('name', 'N/A')}")
        print(f"  Adresse: {restaurant.get('address', 'N/A')}")
        print(f"  Site web: {restaurant.get('website', 'N/A')}")
        print(f"  Prix: {restaurant.get('price_level', 'N/A')}")
        print(f"  Note: {restaurant.get('rating', 'N/A')}")
        print(f"  Nombre d'avis: {restaurant.get('user_ratings_total', 'N/A')}")
        
        # Afficher les liens des plateformes
        print("\n🔗 Liens des plateformes:")
        platform_links = restaurant.get('platform_links', {})
        for platform, url in platform_links.items():
            print(f"  {platform}: {url}")
            
        # Afficher les horaires
        print("\n⏰ Horaires d'ouverture:")
        opening_hours = restaurant.get('opening_hours', [])
        for hours in opening_hours:
            print(f"  {hours}")
            
        # Afficher les options de service
        print("\n🛍️ Options de service:")
        service_options = restaurant.get('service_options', {})
        for option, value in service_options.items():
            print(f"  {option}: {value}")
            
        print(f"\n{'='*50}\n")
        
    except Exception as e:
        print(f"❌ Erreur lors de la vérification MongoDB: {str(e)}")
    finally:
        client.close()

# Configuration globale
DEBUG_MODE = False  # Mode debug avec logs détaillés
USE_BRIGHTDATA = False  # Utilisation de BrightData pour contourner les mesures anti-bot
MAX_MAPS_API_REQUESTS = 500  # Limite quotidienne de requêtes Google Maps API
MAPS_API_REQUEST_COUNT = 0  # Compteur de requêtes Google Maps API
NUM_THREADS = 4  # Nombre de threads par défaut pour le traitement parallèle

# Timeout pour les opérations réseau
NETWORK_TIMEOUT = 30  # Timeout en secondes pour les requêtes réseau

# Dictionnaire global pour stocker les statistiques de timing
TIMING_STATS = defaultdict(list)

# Dossier pour les captures d'écran
SCREENSHOT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "screenshots")
os.makedirs(SCREENSHOT_DIR, exist_ok=True)

# Fonction pour nettoyer les répertoires temporaires
def cleanup_temp_directories():
    """Nettoie les répertoires temporaires créés par ChromeDriver"""
    try:
        temp_dir = tempfile.gettempdir()
        temp_chrome_dirs = glob.glob(os.path.join(temp_dir, "chrome_session_*"))
        for dir_path in temp_chrome_dirs:
            try:
                shutil.rmtree(dir_path, ignore_errors=True)
            except Exception as e:
                if DEBUG_MODE:
                    print(f"Échec nettoyage répertoire temp {dir_path}: {e}")
        if DEBUG_MODE:
            print(f"🧹 Nettoyage de {len(temp_chrome_dirs)} répertoires temporaires")
    except Exception as e:
        if DEBUG_MODE:
            print(f"❌ Erreur lors du nettoyage des répertoires temporaires: {e}")

# Enregistrer le nettoyage à effectuer à la fin du programme
atexit.register(cleanup_temp_directories)

def get_small_test_area():
    """
    Retourne une zone de test élargie dans Paris (Montmartre et environs)
    
    Returns:
        Dictionnaire définissant la zone de test
    """
    # Zone élargie de Montmartre (convertir en format pour get_restaurants_in_zone)
    return {
        "lat_min": 49.8800,  # Élargi vers le sud
        "lat_max": 48.8950,  # Élargi vers le nord
        "lng_min": 2.3250,   # Élargi vers l'ouest
        "lng_max": 2.3500    # Élargi vers l'est
    }

# =============================================
# ÉTAPE 2: CAPTURE D'ÉCRAN GOOGLE MAPS + OCR
# =============================================

# Variable globale pour stocker le chemin vers le ChromeDriver
CHROME_DRIVER_PATH = None

class ChromeSessionManager:
    """
    Gère une session Chrome pour les interactions avec les sites web
    """
    def __init__(self, headless=True):
        self.driver = None
        self.service = None
        self.temp_dir = None
        self.headless = headless
        self.max_retries = 3
    
    def __enter__(self):
        """Démarre une session Chrome"""
        # Initialiser la liste de répertoires temporaires si nécessaire
        if not hasattr(cleanup_temp_dirs, "temp_dirs"):
            cleanup_temp_dirs.temp_dirs = []
        
        for attempt in range(self.max_retries):
            try:
                # Créer un répertoire temporaire pour les données Chrome
                self.temp_dir = tempfile.mkdtemp(prefix="chrome_session_")
                cleanup_temp_dirs.temp_dirs.append(self.temp_dir)
                
                print(f"🔧 Chrome utilise le répertoire: {self.temp_dir}")
                
                # Options Chrome
                chrome_options = webdriver.ChromeOptions()
                
                if self.headless:
                    chrome_options.add_argument("--headless")
                    
                chrome_options.add_argument("--no-sandbox")
                chrome_options.add_argument("--disable-dev-shm-usage")
                chrome_options.add_argument("--disable-gpu")
                chrome_options.add_argument(f"--user-data-dir={self.temp_dir}")
                chrome_options.add_argument("--window-size=1920,1080")
                chrome_options.add_argument("--disable-notifications")
                chrome_options.add_argument("--disable-popup-blocking")
                chrome_options.add_argument("--disable-extensions")
                chrome_options.add_argument("--disable-infobars")
                
                # Utilisateur simulé
                chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36")
                
                # Service Chrome
                if CHROME_DRIVER_PATH:
                    self.service = Service(executable_path=CHROME_DRIVER_PATH)
                else:
                    self.service = Service()  # Laisser Selenium trouver le pilote
                
                # Création du driver
                self.driver = webdriver.Chrome(
                    service=self.service,
                    options=chrome_options
                )
                
                # Configurer les timeouts
                self.driver.set_page_load_timeout(30)
                self.driver.implicitly_wait(10)
                
                return self
            except Exception as e:
                print(f"⚠️ Tentative {attempt+1}/{self.max_retries} échouée: {str(e)}")
                self.cleanup()
                time.sleep(1)
        
        print("❌ Impossible de créer une session Chrome après plusieurs tentatives")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Nettoie la session Chrome"""
        self.cleanup()
        
    def cleanup(self):
        """Nettoie les ressources Chrome"""
        try:
            if self.driver:
                print("  ↳ Nettoyage des ressources Chrome")
                start_time = time.time()
                self.driver.quit()
                self.driver = None
                elapsed = time.time() - start_time
                print(f"  ↳ Nettoyage terminé en {elapsed:.2f} secondes")
        except Exception as e:
            print(f"⚠️ Erreur lors du nettoyage Chrome: {str(e)}")

def format_address(restaurant):
    """
    Formate l'adresse d'un restaurant pour la recherche avec gestion avancée des erreurs
    
    Args:
        restaurant: Dictionnaire contenant les infos du restaurant
    
    Returns:
        Adresse formatée comme string
    """
    # Vérification de base - si restaurant n'est pas un dict ou est None
    if not restaurant or not isinstance(restaurant, dict):
        return "Paris"  # Valeur par défaut sécurisée
    
    # Vérifier si l'adresse existe
    if "address" not in restaurant:
        # Utiliser les coordonnées si disponibles
        if "lat" in restaurant and "lon" in restaurant:
            lat = restaurant.get("lat", "")
            lon = restaurant.get("lon", "")
            if lat and lon:  # Vérifier que les coordonnées ne sont pas vides
                return f"{lat}, {lon}, Paris"
        return "Paris"  # Valeur par défaut
    
    addr = restaurant["address"]
    
    # Si l'adresse est déjà une chaîne, la retourner directement
    if isinstance(addr, str):
        return addr if addr.strip() else "Paris"  # Retourner Paris si la chaîne est vide
    
    # Si l'adresse est un dictionnaire, extraire les composants avec sécurité
    if isinstance(addr, dict):
        address_parts = []
        
        # Extraire les composants avec vérification de type
        for field in ["housenumber", "street", "postcode", "city"]:
            if field in addr and addr[field]:
                value = addr[field]
                # Convertir en string si ce n'est pas déjà le cas
                if not isinstance(value, str):
                    value = str(value)
                if value.strip():  # Ignorer les valeurs vides après nettoyage
                    address_parts.append(value)
        
        if address_parts:
            return ", ".join(address_parts)
    
    # Si pas d'adresse utilisable et coordonnées disponibles
    if "lat" in restaurant and "lon" in restaurant:
        lat = restaurant.get("lat", "")
        lon = restaurant.get("lon", "")
        if lat and lon:  # Vérifier que les coordonnées ne sont pas vides
            return f"{lat}, {lon}, Paris"
    
    return "Paris"  # Valeur par défaut si rien d'autre n'est disponible

@timing_decorator
def search_google_maps(driver, name, address):
    """
    Recherche un restaurant sur Google Maps
    
    Args:
        driver: WebDriver Selenium
        name: Nom du restaurant
        address: Adresse du restaurant
    
    Returns:
        URL de la page Google Maps
    """
    query = urllib.parse.quote(f"{name} {address}")
    url = f"https://www.google.com/maps/search/{query}"
    driver.get(url)
    
    # Gestion du consentement si nécessaire
    try:
        WebDriverWait(driver, 5).until(EC.element_to_be_clickable(
            (By.XPATH, "//button[contains(., 'Accepter') or contains(., 'Accept')]")
        )).click()
        print("✅ Consentement accepté")
        time.sleep(2)
    except:
        print("⚠️ Aucun consentement à gérer")
    
    # Attendre que le résultat soit chargé
    try:
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'DUwDvf')))
        time.sleep(2)
        return driver.current_url
    except:
        print("⚠️ Résultat non trouvé sur Google Maps")
        return None

@timing_decorator
def screenshot_photo(driver, prefix, max_retries=2):
    """
    Capture la photo principale du restaurant sur Google Maps
    
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

@timing_decorator
def screenshot_panel(driver, prefix, max_retries=2):
    """
    Capture le panneau d'informations latéral sur Google Maps avec gestion des erreurs
    
    Args:
        driver: WebDriver Selenium
        prefix: Préfixe pour le nom du fichier
        max_retries: Nombre maximum de tentatives
    
    Returns:
        Tuple (chemin de l'image, version base64, objet image)
    """
    last_error = None
    for attempt in range(max_retries):
        try:
            if attempt > 0 and DEBUG_MODE:
                print(f"  ↳ Tentative {attempt + 1}/{max_retries} de capture du panneau")
            
            # Attendre que le panneau soit chargé
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "DUwDvf"))
            )
            
            # Faire défiler pour voir toutes les informations
            scroll_to_bottom_info(driver)
            time.sleep(1)  # Attendre la fin du défilement
            
            # Prendre la capture d'écran
            screenshot = driver.get_screenshot_as_png()
            image = Image.open(BytesIO(screenshot))
            cropped = image.crop((0, 0, 600, 1700))
            
            # Vérifier que l'image n'est pas vide
            if cropped.size[0] < 100 or cropped.size[1] < 100:
                raise ValueError("Image du panneau trop petite")
            
            path = f"{prefix}_panel.png"
            cropped.save(path)
            
            if DEBUG_MODE and attempt > 0:
                print(f"  ↳ Capture du panneau réussie après {attempt + 1} tentative(s)")
            
            return path, encode_image_base64(cropped), cropped
            
        except Exception as e:
            last_error = e
            if DEBUG_MODE:
                print(f"⚠️ Échec de capture du panneau (tentative {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2)  # Pause avant nouvelle tentative
                continue
    
    raise RuntimeError(f"Impossible de capturer le panneau après {max_retries} tentatives") from last_error

@timing_decorator
def screenshot_opening_hours(driver, prefix, max_retries=3):
    """
    Capture les horaires d'ouverture directement depuis la fiche Google Maps sans faire défiler
    
    Args:
        driver: WebDriver Selenium
        prefix: Préfixe pour le nom du fichier
        max_retries: Nombre maximum de tentatives
    
    Returns:
        Tuple (chemin de l'image, version base64, objet image) ou (None, None, None)
    """
    last_error = None
    for attempt in range(max_retries):
        try:
            if attempt > 0 and DEBUG_MODE:
                print(f"  ↳ Tentative {attempt + 1}/{max_retries} de capture des horaires")
            
            # Rechercher le bouton des horaires
            horaires_btn = None
            horaire_xpaths = [
                "//button[.//span[contains(@aria-label, 'Ore') or contains(@aria-label, 'Heures') or contains(@aria-label, 'Hours')]]",
                "//button[contains(@aria-label, 'Horaires')]",
                "//div[contains(@role, 'button')][.//span[contains(text(), 'horaires')]]",
                "//div[contains(@aria-label, 'Informations')]//div[contains(text(), 'Fermé') or contains(text(), 'Ouvert')]"
            ]
            
            for xpath in horaire_xpaths:
                try:
                    horaires_btn = WebDriverWait(driver, 3).until(
                        EC.element_to_be_clickable((By.XPATH, xpath))
                    )
                    if horaires_btn:
                        break
                except:
                    continue
            
            if not horaires_btn:
                # Plan B : capturer la section des horaires directement depuis le panneau latéral
                print("  ↳ Capture directe des horaires depuis le panneau principal")
                
                # Capture d'écran complète
                screenshot = driver.get_screenshot_as_png()
                image = Image.open(BytesIO(screenshot))
                
                # Essayer de trouver la zone des horaires approximativement
                # Coordonnées typiques de la section des horaires
                horaires_crop = image.crop((600, 200, 1200, 800))
                
                path = f"{prefix}_horaires_direct.png"
                horaires_crop.save(path)
                
                if DEBUG_MODE:
                    print(f"  ↳ Capture directe des horaires effectuée")
                
                return path, encode_image_base64(horaires_crop), horaires_crop
            
            # Faire défiler jusqu'au bouton si nécessaire
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", horaires_btn)
            time.sleep(1)
            
            # Prendre une capture d'écran de la section avant de cliquer
            pre_click = driver.get_screenshot_as_png()
            pre_image = Image.open(BytesIO(pre_click))
            
            # Obtenir la position du bouton
            location = horaires_btn.location
            size = horaires_btn.size
            
            # Étendre la zone de capture pour inclure la liste des horaires 
            # qui apparaît souvent directement sous le bouton
            horaires_section = pre_image.crop((
                location['x'] - 50,  
                location['y'] - 20,
                location['x'] + size['width'] + 300,  # Capturer une zone plus large
                location['y'] + size['height'] + 300   # Capturer vers le bas pour les horaires
            ))
            
            # Sauvegarder cette première version
            path = f"{prefix}_horaires_section.png"
            horaires_section.save(path)
            
            # Maintenant cliquer pour voir s'il y a un popup
            try:
                horaires_btn.click()
                time.sleep(2)  # Attendre l'ouverture du popup
                
                # Vérifier si un popup est apparu
                dialog_present = False
                try:
                    dialog = WebDriverWait(driver, 3).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'div[role="dialog"]'))
                    )
                    dialog_present = True
                except:
                    dialog_present = False
                
                if dialog_present:
                    # Capturer le popup des horaires
                    screenshot = driver.get_screenshot_as_png()
                    image = Image.open(BytesIO(screenshot))
                    
                    # Coordonnées typiques du popup des horaires
                    horaires_popup = image.crop((500, 100, 1100, 800))
                    
                    path = f"{prefix}_horaires_popup.png"
                    horaires_popup.save(path)
                    
                    # Fermer le popup
                    try:
                        driver.find_element(By.CSS_SELECTOR, 'button[aria-label="Fermer"]').click()
                    except:
                        pass
                    
                    return path, encode_image_base64(horaires_popup), horaires_popup
            except:
                # Si le clic échoue, on utilise déjà la capture de la section
                pass
            
            # Si on arrive ici, on renvoie la section capturée initialement
            if DEBUG_MODE and attempt > 0:
                print(f"  ↳ Capture des horaires réussie après {attempt + 1} tentative(s)")
            
            return path, encode_image_base64(horaires_section), horaires_section
            
        except Exception as e:
            last_error = e
            if DEBUG_MODE:
                print(f"⚠️ Échec de capture des horaires (tentative {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2)  # Pause avant nouvelle tentative
                try:
                    # Fermer le popup s'il est resté ouvert
                    driver.find_element(By.CSS_SELECTOR, 'button[aria-label="Fermer"]').click()
                except:
                    pass
                continue
    
    if DEBUG_MODE:
        print(f"❌ Impossible de capturer les horaires après {max_retries} tentatives: {last_error}")
    return None, None, None

def encode_image_base64(image):
    """Convertit une image PIL en base64 pour stockage"""
    buffered = BytesIO()
    image.save(buffered, format="JPEG")
    img_base64 = base64.b64encode(buffered.getvalue()).decode("utf-8")
    # Ajouter le préfixe pour créer une URL data complète
    return f"data:image/jpeg;base64,{img_base64}"

@timing_decorator
def extract_text_from_image(image, max_retries=2):
    """
    Extrait le texte d'une image avec OCR amélioré et gestion des erreurs
    
    Args:
        image: Image PIL à traiter
        max_retries: Nombre maximum de tentatives
    
    Returns:
        Texte extrait ou chaîne vide en cas d'échec
    """
    last_error = None
    for attempt in range(max_retries):
        try:
            if attempt > 0 and DEBUG_MODE:
                print(f"  ↳ Tentative {attempt + 1}/{max_retries} d'extraction OCR")
            
            # Améliorer la qualité de l'image pour l'OCR
            enhanced = image.convert('L')  # Conversion en niveaux de gris
            enhanced = enhanced.point(lambda x: 0 if x < 128 else 255, '1')  # Binarisation
            
            # Configuration OCR optimisée
            custom_config = r'--oem 3 --psm 6 -l fra'
            text = pytesseract.image_to_string(enhanced, config=custom_config)
            text = text.strip()
            
            # Vérifier que le texte n'est pas vide ou trop court
            if not text or len(text) < 10:
                if attempt < max_retries - 1:
                    raise ValueError("Texte extrait trop court ou vide")
            
            if DEBUG_MODE and attempt > 0:
                print(f"  ↳ Extraction OCR réussie après {attempt + 1} tentative(s)")
            
            return text
            
        except Exception as e:
            last_error = e
            if DEBUG_MODE:
                print(f"⚠️ Échec d'extraction OCR (tentative {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(1)  # Pause avant nouvelle tentative
                continue
    
    print(f"❌ OCR erreur après {max_retries} tentatives: {last_error}")
    return ""

def parse_opening_hours_text(text):
    """Parse le texte des horaires extrait par OCR"""
    horaires = {}
    for line in text.splitlines():
        if re.match(r"^(luni|marți|miercuri|joi|vineri|sâmbătă|duminică|lundi|mardi|mercredi|jeudi|vendredi|samedi|dimanche)", line.strip().lower()):
            parts = line.split(" ")
            day = parts[0].lower()
            horaires[day] = " ".join(parts[1:])
    return horaires

@timing_decorator
def call_openai_structured_extraction(ocr_text):
    """
    Utilise OpenAI pour structurer les données extraites par OCR
    
    Args:
        ocr_text: Texte brut extrait par OCR
    
    Returns:
        Dictionnaire structuré des informations
    """
    prompt = f"""
Voici un texte brut issu d'un screenshot Google Maps en français :

{ocr_text}

Extrais les informations suivantes dans un dictionnaire JSON (valeurs vides si non disponibles) :
- address
- phone_number
- website
- price_level
- rating
- user_ratings_total
- service_options (ex: dine_in, takeaway, delivery)
- category (ex: 'Restaurant chinois')
    """
    
    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}]
        )
        content = response.choices[0].message.content.strip()
        return json.loads(content)
    except Exception as e:
        print(f"❌ Erreur LLM : {e}")
        return {}

def scroll_to_bottom_info(driver):
    """Fait défiler le panneau latéral pour voir toutes les informations"""
    try:
        driver.execute_script("""
            const scrollArea = document.querySelector('div[role="main"]');
            if (scrollArea) scrollArea.scrollTop = scrollArea.scrollHeight;
        """)
        time.sleep(2)
        print("📜 Scroll JS effectué")
    except Exception as e:
        print("⚠️ Scroll échoué :", e)

@timing_decorator
def process_restaurant_with_maps_screenshots(maps_data):
    """
    Traite les données d'un restaurant et capture des screenshots
    
    Args:
        maps_data: Données du restaurant depuis Google Maps
    
    Returns:
        Données du restaurant enrichies
    """
    if not maps_data:
        return None
        
    # Extraire les informations de base
    name = maps_data.get("name")
    address = maps_data.get("address")
    maps_url = maps_data.get("maps_url")
    place_id = maps_data.get("place_id")
    latitude = maps_data.get("latitude", 0)
    longitude = maps_data.get("longitude", 0)
    rating = maps_data.get("rating", 0)
    
    # Créer la structure de données initiale
    restaurant_data = {
        "name": name,
        "place_id": place_id,
        "address": address,
        "maps_url": maps_url,
        "location": {
            "type": "Point",
            "coordinates": [longitude, latitude]
        },
        "rating": rating,
        "image": "",
        "images": [],
        "phone": "",
        "website": "",
        "categories": [],
        "opening_hours": {},
        "price_level": 0,
        "created_at": datetime.now(),
        "updated_at": datetime.now(),
    }
    
    # Essayer de capturer les screenshots et extraire des données additionnelles
    try:
        print(f"📸 Capture des screenshots de Maps pour {name}")
        
        with ChromeSessionManager() as session:
            driver = session.driver
            
            if not driver:
                print(f"⚠️ Échec d'initialisation du driver Chrome pour {name}")
                return restaurant_data  # Retourner les données de base sans screenshots
                
            # Essayer de capturer l'état du restaurant
            try:
                driver.get(maps_url)
                time.sleep(3)
                
                # Capturer l'image principale du restaurant
                temp_name = f"temp_{hashlib.md5(name.encode()).hexdigest()[:8]}"
                try:
                    photo_path, photo_base64, _ = screenshot_photo(driver, temp_name)
                    restaurant_data["image"] = photo_base64
                    print(f"✅ Screenshot capturé pour {name}")
                except Exception as e:
                    print(f"⚠️ Impossible de capturer la photo de {name}: {str(e)}")
                
                # Extraire des informations additionnelles (site web, téléphone, horaires...)
                additional_info = extract_additional_info(driver, maps_url, restaurant_data)
                if additional_info:
                    # Mise à jour des données avec les infos extraites
                    for key, value in additional_info.items():
                        if value:  # Ne mettre à jour que si la valeur n'est pas vide
                            restaurant_data[key] = value
                
                # Si les horaires sont extraits mais pas dans le bon format, essayer de les parser
                if "opening_hours_text" in restaurant_data and not restaurant_data.get("opening_hours"):
                    hours_text = restaurant_data.get("opening_hours_text", "")
                    if hours_text:
                        try:
                            parsed_hours = parse_opening_hours_text(hours_text)
                            if parsed_hours:
                                restaurant_data["opening_hours"] = parsed_hours
                        except:
                            pass
                
                # S'assurer que certaines informations essentielles sont présentes, même si vides
                for key in ["phone", "website", "categories", "opening_hours", "price_level"]:
                    if key not in restaurant_data or restaurant_data[key] is None:
                        if key == "opening_hours":
                            restaurant_data[key] = {}
                        elif key == "categories":
                            restaurant_data[key] = []
                        elif key == "price_level":
                            restaurant_data[key] = 0
                        else:
                            restaurant_data[key] = ""
                
                # Afficher un résumé des données extraites
                print(f"📱 Téléphone: {restaurant_data.get('phone', 'Non trouvé')}")
                print(f"🌐 Site web: {restaurant_data.get('website', 'Non trouvé')}")
                print(f"⏰ Horaires: {len(restaurant_data.get('opening_hours', {}))} jours")
                print(f"⭐ Note: {restaurant_data.get('rating', 0)}")
                print(f"💰 Prix: {restaurant_data.get('price_level', 'Non trouvé')}")
                print(f"🏷️ Catégories: {restaurant_data.get('categories', [])}")
                
                return restaurant_data
                
            except Exception as e:
                print(f"❌ Erreur lors du traitement des données visuelles: {str(e)}")
                traceback.print_exc()
                # Même en cas d'erreur, retourner les données de base
                return restaurant_data
    
    except Exception as e:
        print(f"❌ Erreur lors du processus de capture d'écran: {str(e)}")
        traceback.print_exc()
        
    # Même en cas d'échec total, retourner les données de base
    return restaurant_data

def capture_maps_screenshot(driver, url):
    """
    Capture un screenshot de Google Maps
    
    Args:
        driver: Instance de webdriver (déjà initialisée)
        url: URL de Google Maps à capturer
    
    Returns:
        Base64 du screenshot
    """
    try:
        # Vérifier que le driver est bien initialisé
        if not driver:
            print("❌ Driver non initialisé pour la capture d'écran")
            return None
            
        # Charger l'URL si elle n'est pas déjà chargée
        current_url = driver.current_url
        if url != current_url:
            driver.get(url)
            # Attendre que la page se charge
            time.sleep(5)
            
            # Gérer le consentement aux cookies si nécessaire
            try:
                consent_buttons = driver.find_elements(By.XPATH, 
                    "//button[contains(., 'Accept') or contains(., 'Accepter') or contains(., 'Reject') or contains(., 'Refuser')]")
                if consent_buttons:
                    for button in consent_buttons:
                        if button.is_displayed():
                            button.click()
                            time.sleep(2)
                            break
            except:
                pass
        
        # Prendre le screenshot de la page
        screenshot = driver.get_screenshot_as_base64()
        return f"data:image/png;base64,{screenshot}"
        
    except Exception as e:
        print(f"❌ Erreur capture screenshot: {str(e)}")
        return None

def extract_additional_info(driver, maps_url, restaurant_data):
    """
    Extrait des informations supplémentaires sur un restaurant depuis Google Maps
    
    Args:
        driver: WebDriver Selenium (déjà initialisé)
        maps_url: URL Google Maps
        restaurant_data: Données existantes du restaurant
    
    Returns:
        Dictionnaire avec les informations extraites
    """
    # Vérifier que le driver est correctement initialisé
    if not driver:
        print("❌ Driver non initialisé pour l'extraction d'informations additionnelles")
        return {}
    
    # S'assurer que nous sommes sur la bonne URL
    if driver.current_url != maps_url:
        try:
            driver.get(maps_url)
            time.sleep(4)
        except Exception as e:
            print(f"❌ Erreur lors de la navigation vers {maps_url}: {str(e)}")
            return {}
    
    result = {}
    
    try:
        # Extraire les informations une par une
        
        # 1. Site web
        try:
            # Différents sélecteurs pour trouver le site web
            website_selectors = [
                "a[data-item-id='authority']",
                "a[aria-label*='site web']",
                "a[aria-label*='website']",
                "a[data-tooltip='Ouvrir le site Web']",
                "a[data-tooltip='Open website']",
                "div[aria-label*='Site web'] a",
                "button[aria-label*='site web'] ~ a",
                "button[data-item-id='authority']",
                "a[href^='http']:not([href*='google'])",
                "a.website",
                "a[href^='http'][data-item-id]"
            ]
            
            for selector in website_selectors:
                website_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for el in website_elements:
                    website_url = el.get_attribute("href")
                    if website_url and not website_url.startswith("https://www.google.com"):
                        result["website"] = website_url
                        break
                if "website" in result:
                    break
                    
        except Exception as e:
            if DEBUG_MODE:
                print(f"⚠️ Erreur lors de l'extraction du site web: {str(e)}")
        
        # 2. Numéro de téléphone
        try:
            # Différents sélecteurs pour trouver le numéro de téléphone
            phone_selectors = [
                "button[data-tooltip='Copier le numéro de téléphone']",
                "button[data-tooltip='Copy phone number']",
                "button[aria-label*='phone']",
                "button[aria-label*='téléphone']",
                "div[aria-label*='téléphone'] button",
                "button[data-item-id='phone:tel']",
                "button.phone",
                "a[href^='tel:']",
                "span[aria-label*='phone']"
            ]
            
            for selector in phone_selectors:
                phone_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for el in phone_elements:
                    # Essayer plusieurs attributs
                    phone_text = None
                    for attr in ["aria-label", "data-item-id", "data-tooltip", "title"]:
                        phone_text = el.get_attribute(attr)
                        if phone_text and (re.search(r'\d', phone_text) or "phone" in phone_text.lower() or "téléphone" in phone_text.lower()):
                            # Extraire seulement les chiffres et les caractères de formatage
                            phone_match = re.search(r'(?:\+\d{1,3}[-.\s]?)?(?:\(\d{1,4}\)[-.\s]?)?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,9}', phone_text)
                            if phone_match:
                                result["phone"] = phone_match.group(0).strip()
                                break
                                
                    # Si on n'a pas trouvé, essayer le texte de l'élément
                    if not result.get("phone"):
                        phone_text = el.text
                        if phone_text and re.search(r'\d', phone_text):
                            # Extraire seulement les chiffres et les caractères de formatage
                            phone_match = re.search(r'(?:\+\d{1,3}[-.\s]?)?(?:\(\d{1,4}\)[-.\s]?)?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,9}', phone_text)
                            if phone_match:
                                result["phone"] = phone_match.group(0).strip()
                                break
                
                if "phone" in result:
                    break
                    
            # Si on n'a toujours pas trouvé, chercher dans la page entière
            if not result.get("phone"):
                page_source = driver.page_source
                phone_matches = re.findall(r'(?:\+\d{1,3}[-.\s]?)?(?:\(\d{1,4}\)[-.\s]?)?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,9}', page_source)
                if phone_matches:
                    for match in phone_matches:
                        # Vérifier que c'est bien un numéro de téléphone (au moins 8 chiffres)
                        digit_count = sum(c.isdigit() for c in match)
                        if digit_count >= 8 and digit_count <= 15:
                            result["phone"] = match
                            break
            
        except Exception as e:
            if DEBUG_MODE:
                print(f"⚠️ Erreur lors de l'extraction du numéro de téléphone: {str(e)}")
        
        # 3. Catégories
        try:
            category_elements = driver.find_elements(By.CSS_SELECTOR, 
                "button[jsaction*='category'], span.widget-pane-link, span[jsan*='category'], button[aria-label*='restaurant'], button[jsaction*='restaurant'], span.category")
            
            categories = []
            for el in category_elements:
                category_text = el.text.strip()
                if category_text and len(category_text) > 2 and not re.search(r'^\d', category_text):
                    # Éviter les valeurs non pertinentes
                    if not category_text.startswith(("http", "www", "+", "Ouvrir", "Fermer", "Ouvert", "Fermé")):
                        categories.append(category_text)
            
            # Si pas de catégorie trouvée, essayer avec une autre approche
            if not categories:
                # Chercher directement dans le titre/sous-titre du restaurant
                title_elements = driver.find_elements(By.CSS_SELECTOR, ".section-hero-header-title-description")
                for el in title_elements:
                    subtitle = el.find_elements(By.CSS_SELECTOR, "div:not(.section-hero-header-title)")
                    if subtitle:
                        subtitle_text = subtitle[0].text.strip()
                        if subtitle_text and "·" in subtitle_text:
                            # Les catégories sont souvent séparées par des points médians
                            parts = subtitle_text.split("·")
                            for part in parts:
                                clean_part = part.strip()
                                if clean_part and len(clean_part) > 2:
                                    categories.append(clean_part)
            
            # Conversion pour compatibilité avec le reste du code
            if categories:
                # Filtrer les doublons et les valeurs vides
                categories = list(set(filter(None, categories)))
                if not restaurant_data.get("categories"):
                    # Si aucune catégorie n'existe, utiliser celles trouvées
                    result["categories"] = categories
                else:
                    # Sinon, fusionner avec les catégories existantes
                    existing_categories = restaurant_data.get("categories", [])
                    result["categories"] = list(set(existing_categories + categories))
            
        except Exception as e:
            if DEBUG_MODE:
                print(f"⚠️ Erreur lors de l'extraction des catégories: {str(e)}")
        
        # 4. Horaires d'ouverture
        try:
            # Différentes approches pour extraire les horaires
            
            # Approche 1: Chercher le bouton des horaires et cliquer dessus
            hours_button = None
            hours_selectors = [
                "button[data-item-id='oh']",
                "button[aria-label*='horaires']",
                "button[aria-label*='hours']",
                "div[aria-label*='horaires']",
                "button[jsaction*='open'][jsaction*='hours']",
                "div.section-info-hour-text"
            ]
            
            for selector in hours_selectors:
                hour_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if hour_elements:
                    hours_button = hour_elements[0]
                    break
            
            opening_hours = {}
            
            if hours_button:
                # Essayer de cliquer pour ouvrir les horaires détaillés
                try:
                    hours_button.click()
                    time.sleep(1)
                    
                    # Chercher les horaires dans la popup ou dans le panel
                    hours_rows = driver.find_elements(By.CSS_SELECTOR, "table tr, div.section-info-hour-row")
                    
                    if hours_rows:
                        for row in hours_rows:
                            row_text = row.text.strip()
                            # Analyser le texte pour extraire le jour et les heures
                            day_match = re.search(r'(lundi|mardi|mercredi|jeudi|vendredi|samedi|dimanche|monday|tuesday|wednesday|thursday|friday|saturday|sunday)', row_text.lower())
                            if day_match:
                                day = day_match.group(1)
                                # Normaliser le jour en français
                                day_map = {
                                    "monday": "lundi", "tuesday": "mardi", "wednesday": "mercredi", 
                                    "thursday": "jeudi", "friday": "vendredi", "saturday": "samedi", "sunday": "dimanche"
                                }
                                day = day_map.get(day, day)
                                
                                # Extraire les heures (format: 9:00–22:00 ou 9h00-22h00)
                                hours_match = re.search(r'(\d{1,2})[h:](\d{2})\s*[-–]\s*(\d{1,2})[h:](\d{2})', row_text)
                                if hours_match:
                                    open_hour = int(hours_match.group(1))
                                    open_minute = int(hours_match.group(2))
                                    close_hour = int(hours_match.group(3))
                                    close_minute = int(hours_match.group(4))
                                    
                                    opening_hours[day] = {
                                        "open": f"{open_hour:02d}:{open_minute:02d}",
                                        "close": f"{close_hour:02d}:{close_minute:02d}"
                                    }
                except Exception as e:
                    if DEBUG_MODE:
                        print(f"⚠️ Erreur lors du clic sur le bouton des horaires: {str(e)}")
            
            # Si pas d'horaires trouvés, essayer d'extraire directement du texte
            if not opening_hours:
                page_source = driver.page_source
                # Rechercher des patterns d'horaires dans la source de la page
                hours_pattern = r'(lundi|mardi|mercredi|jeudi|vendredi|samedi|dimanche|monday|tuesday|wednesday|thursday|friday|saturday|sunday)[^\n]*?(\d{1,2})[h:](\d{2})\s*[-–]\s*(\d{1,2})[h:](\d{2})'
                hours_matches = re.findall(hours_pattern, page_source, re.IGNORECASE)
                
                if hours_matches:
                    for match in hours_matches:
                        day = match[0].lower()
                        # Normaliser le jour en français
                        day_map = {
                            "monday": "lundi", "tuesday": "mardi", "wednesday": "mercredi", 
                            "thursday": "jeudi", "friday": "vendredi", "saturday": "samedi", "sunday": "dimanche"
                        }
                        day = day_map.get(day, day)
                        
                        open_hour = int(match[1])
                        open_minute = int(match[2])
                        close_hour = int(match[3])
                        close_minute = int(match[4])
                        
                        opening_hours[day] = {
                            "open": f"{open_hour:02d}:{open_minute:02d}",
                            "close": f"{close_hour:02d}:{close_minute:02d}"
                        }
            
            if opening_hours:
                result["opening_hours"] = opening_hours
            
        except Exception as e:
            if DEBUG_MODE:
                print(f"⚠️ Erreur lors de l'extraction des horaires: {str(e)}")
        
        # 5. Prix (€, €€, €€€)
        try:
            price_selectors = [
                "span[aria-label*='Prix'], span[aria-label*='Price']", 
                "span.price-level", 
                "span.section-rating-term span"
            ]
            
            for selector in price_selectors:
                price_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for el in price_elements:
                    price_text = el.text.strip()
                    # Compter le nombre de symboles € ou $
                    if '€' in price_text:
                        result["price_level"] = price_text.count('€')
                        break
                    elif '$' in price_text:
                        result["price_level"] = price_text.count('$')
                        break
                if "price_level" in result:
                    break
            
            # Si prix non trouvé, chercher dans les attributs et la page source
            if "price_level" not in result:
                price_hints = {
                    "peu coûteux": 1, "bon marché": 1, "abordable": 1, "économique": 1, 
                    "modéré": 2, "moyen": 2, "mid-range": 2, 
                    "haut de gamme": 3, "cher": 3, "coûteux": 3, "luxe": 4
                }
                
                # Chercher des indices de prix dans la page
                page_text = driver.page_source.lower()
                for hint, level in price_hints.items():
                    if hint in page_text:
                        result["price_level"] = level
                        break
                        
        except Exception as e:
            if DEBUG_MODE:
                print(f"⚠️ Erreur lors de l'extraction du niveau de prix: {str(e)}")
                
        # 6. Note/Rating (vérification/mise à jour)
        try:
            if not restaurant_data.get("rating") or restaurant_data.get("rating") == 0:
                rating_selectors = [
                    "span.section-star-display", 
                    "span.rating", 
                    "span[aria-label*='étoile']", 
                    "span[aria-label*='star']",
                    "div.gm2-display-2"
                ]
                
                for selector in rating_selectors:
                    rating_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    for el in rating_elements:
                        rating_text = el.text.strip()
                        if rating_text:
                            # Chercher un chiffre avec une décimale potentielle
                            rating_match = re.search(r'([\d.]+)', rating_text)
                            if rating_match:
                                try:
                                    rating_value = float(rating_match.group(1))
                                    if 0 < rating_value <= 5:
                                        result["rating"] = rating_value
                                        break
                                except:
                                    pass
                    if "rating" in result:
                        break
        except Exception as e:
            if DEBUG_MODE:
                print(f"⚠️ Erreur lors de l'extraction de la note: {str(e)}")
        
        return result
        
    except Exception as e:
        print(f"❌ Erreur lors de l'extraction des informations additionnelles: {str(e)}")
        traceback.print_exc()
        return {}

def search_links_bing(name, address):
    """
    Recherche les liens des plateformes via Bing avec mise en cache
    
    Args:
        name: Nom du restaurant
        address: Adresse du restaurant
    
    Returns:
        Dictionnaire avec les liens trouvés (clés: nom de plateforme, valeurs: URLs)
    """
    # Utiliser une clé de cache basée sur le nom et l'adresse
    cache_key = f"{name}_{address}"
    
    # Vérifier si les résultats sont déjà en cache
    if cache_key in BING_SEARCH_CACHE:
        print(f"✅ Résultats Bing récupérés depuis le cache pour {name}")
        return BING_SEARCH_CACHE[cache_key]
    
    query = f"{name} {address} restaurant tripadvisor lafourchette"
    print(f"🔍 Recherche sur Bing: {query}")
    
    # Construire l'URL Bing avec le paramètre cc=FR
    encoded_query = urllib.parse.quote_plus(query)
    bing_url = f"https://www.bing.com/search?q={encoded_query}&cc=FR"
    print(f"🌐 URL Bing: {bing_url}")
    
    # Utiliser BrightData pour récupérer le HTML
    html = fetch_html_with_brightdata(bing_url, name, "bing_search")
    if not html:
        print(f"❌ Impossible de récupérer le HTML de Bing pour {name}")
        return {}
        
    # Parser le HTML
    soup = BeautifulSoup(html, 'html.parser')
    
    # Dictionnaire pour stocker les liens trouvés
    platform_links = {}
    
    # Liste des plateformes à rechercher avec leurs patterns regex
    platforms = [
        ("tripadvisor", r'tripadvisor\.(?:fr|com)/Restaurant_Review'),
        ("lafourchette", r'lafourchette\.fr/restaurant'),
        ("facebook", r'facebook\.com'),
        ("instagram", r'instagram\.com')
    ]
    
    # Rechercher les liens pour chaque plateforme
    for platform_name, pattern in platforms:
        links = soup.find_all('a', href=re.compile(pattern))
        for link in links:
            url = link.get('href')
            if validate_platform_link(url, platform_name):
                platform_links[platform_name] = url
                print(f"  ✅ Trouvé lien {platform_name}: {url}")
                break
    
    print(f"✅ Trouvé {len(platform_links)} liens de plateformes")
    
    # Enregistrer dans le cache
    BING_SEARCH_CACHE[cache_key] = platform_links
    
    return platform_links

def load_test_restaurants():
    """
    Charge un jeu de données de test pour les restaurants de Montmartre
    """
    print("🔍 Utilisation d'une petite zone de test (Montmartre)")
    
    test_restaurants = [
        {"name": "Boulangerie Chaptal", "address": "2 Rue Chaptal, Paris"},
        {"name": "Les Apôtres de Pigalle", "address": "2 Rue Germain Pilon, Paris"},
        {"name": "Le Chaptal", "address": "50 Rue Jean-Baptiste Pigalle, Paris"},
        {"name": "Puce", "address": "1 Rue Chaptal, Paris"},
        {"name": "Le Pantruche", "address": "3 Rue Victor Massé, Paris"},
        {"name": "Bouillon Pigalle", "address": "22 Boulevard de Clichy, Paris"},
        {"name": "Hôtel Amour", "address": "8 Rue de Navarin, Paris"}
    ]
    
    print(f"✅ {len(test_restaurants)} restaurants chargés dans la zone de test")
    return test_restaurants

def load_restaurants_from_file():
    """
    Charge les restaurants depuis un fichier JSON
    """
    try:
        restaurant_file = os.path.join(CURRENT_DIR, "data", "restaurants.json")
        if os.path.exists(restaurant_file):
            with open(restaurant_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        else:
            print(f"❌ Fichier de restaurants introuvable: {restaurant_file}")
            return load_test_restaurants()  # Fallback sur les données de test
    except Exception as e:
        print(f"❌ Erreur lors du chargement des restaurants: {str(e)}")
        return load_test_restaurants()  # Fallback sur les données de test

def cleanup_temp_dirs():
    """
    Nettoie les répertoires temporaires à la fin de l'exécution
    """
    # Utiliser une variable statique dans la fonction
    if not hasattr(cleanup_temp_dirs, "temp_dirs"):
        cleanup_temp_dirs.temp_dirs = []
    
    if not cleanup_temp_dirs.temp_dirs:
        print("✅ Aucun répertoire temporaire à nettoyer")
        return
        
    print(f"🧹 Nettoyage de {len(cleanup_temp_dirs.temp_dirs)} répertoires temporaires...")
    for temp_dir in cleanup_temp_dirs.temp_dirs:
        try:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
        except Exception as e:
            print(f"⚠️ Erreur lors du nettoyage de {temp_dir}: {str(e)}")
            
    cleanup_temp_dirs.temp_dirs = []

def check_mongodb_content():
    """
    Vérifie le contenu de la collection MongoDB
    """
    try:
        client = get_mongo_client()
        if not client:
            print("❌ Impossible de se connecter à MongoDB")
            return
            
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        
        print(f"\n🔍 Vérification du contenu MongoDB:")
        print(f"  Base de données: {DB_NAME}")
        print(f"  Collection: {COLLECTION_NAME}")
        
        # Compter le nombre total de documents
        total_docs = collection.count_documents({})
        print(f"  Nombre total de documents: {total_docs}")
        
        # Afficher les 5 premiers documents
        print("\n📋 5 premiers documents:")
        for doc in collection.find().limit(5):
            print(f"  - {doc.get('name', 'Sans nom')} (ID: {doc.get('_id')})")
            
        client.close()
        
    except Exception as e:
        print(f"❌ Erreur lors de la vérification MongoDB: {str(e)}")
        traceback.print_exc()

def validate_platform_link(url, platform):
    """
    Valide un lien de plateforme
    """
    if not url:
        return False
        
    # Vérifier le domaine
    try:
        domain = urlparse(url).netloc.lower()
        if platform == "tripadvisor":
            return "tripadvisor" in domain and ".fr" in domain
        elif platform == "lafourchette":
            return "lafourchette" in domain and ".fr" in domain
        elif platform == "facebook":
            return "facebook.com" in domain
        return False
    except:
        return False

def enrich_with_platforms(restaurant_data):
    """
    Enrichit les données d'un restaurant avec les liens des plateformes externes
    """
    try:
        print(f"[{restaurant_data['name']}] 🌐 Enrichissement avec les plateformes externes...")
        
        # Vérifier si BrightData est activé
        if not BRIGHTDATA_ENABLED:
            print("❌ BrightData n'est pas activé, impossible de rechercher les liens")
            return restaurant_data
            
        # Vérifier le token BrightData
        if not BRIGHTDATA_TOKEN:
            print("❌ Token BrightData manquant")
            return restaurant_data
            
        # Rechercher les liens sur Bing
        query = f"{restaurant_data['name']} {restaurant_data['address']} restaurant tripadvisor lafourchette"
        print(f"🔍 Recherche sur Bing: {query}")
        
        # Construire l'URL Bing avec le paramètre cc=FR
        encoded_query = urllib.parse.quote_plus(query)
        bing_url = f"https://www.bing.com/search?q={encoded_query}&cc=FR"
        print(f"🌐 URL Bing: {bing_url}")
        
        # Utiliser BrightData pour récupérer le HTML
        html = fetch_html_with_brightdata(bing_url)
        if not html:
            print("❌ Impossible de récupérer le HTML de Bing")
            return restaurant_data
            
        # Parser le HTML
        soup = BeautifulSoup(html, 'html.parser')
        
        # Dictionnaire pour stocker les liens trouvés
        platform_links = {}
        
        # Rechercher les liens TripAdvisor
        tripadvisor_links = soup.find_all('a', href=re.compile(r'tripadvisor\.fr/Restaurant_Review'))
        for link in tripadvisor_links:
            url = link.get('href')
            if validate_platform_link(url, "tripadvisor"):
                platform_links["tripadvisor"] = url
                print(f"✅ Trouvé lien tripadvisor: {url}")
                break
                
        # Rechercher les liens LaFourchette
        lafourchette_links = soup.find_all('a', href=re.compile(r'lafourchette\.fr/restaurant'))
        for link in lafourchette_links:
            url = link.get('href')
            if validate_platform_link(url, "lafourchette"):
                platform_links["lafourchette"] = url
                print(f"✅ Trouvé lien lafourchette: {url}")
                break
                
        # Rechercher les liens Facebook
        facebook_links = soup.find_all('a', href=re.compile(r'facebook\.com'))
        for link in facebook_links:
            url = link.get('href')
            if validate_platform_link(url, "facebook"):
                platform_links["facebook"] = url
                print(f"✅ Trouvé lien facebook: {url}")
                break
                
        print(f"✅ Trouvé {len(platform_links)} liens de plateformes")
        
        # Mettre à jour les données du restaurant
        restaurant_data.update(platform_links)
        return restaurant_data
        
    except Exception as e:
        print(f"❌ Erreur lors de l'enrichissement: {str(e)}")
        return restaurant_data

def normalize_restaurant_data(restaurant_data):
    """
    Normalise les données du restaurant pour MongoDB
    - S'assure que le place_id est utilisé comme _id
    - Vérifie que toutes les données sont au bon format
    - Formate les coordonnées GPS en GeoJSON
    
    Args:
        restaurant_data: Dictionnaire des données du restaurant
    
    Returns:
        Dictionnaire normalisé compatible avec MongoDB
    """
    # Vérifier si le place_id existe et n'est pas vide
    place_id = restaurant_data.get('place_id')
    if not place_id or place_id == "":
        # Générer un ID unique basé sur le nom et l'adresse
        custom_id = f"{restaurant_data.get('name', 'unknown')}_{restaurant_data.get('address', 'unknown')}"
        # Ajouter un UUID pour éviter les doublons
        import uuid
        place_id = f"custom_{uuid.uuid4().hex[:10]}_{custom_id.replace(' ', '_')[:30]}"
        print(f"⚠️ place_id manquant, généré: {place_id}")
    
    # Récupérer les coordonnées GPS
    lat = restaurant_data.get('latitude', 0)
    lng = restaurant_data.get('longitude', 0)
    
    # S'assurer que les coordonnées sont des nombres flottants (pas des chaînes)
    try:
        lat = float(lat) if lat else 0
        lng = float(lng) if lng else 0
    except (ValueError, TypeError):
        lat, lng = 0, 0
    
    # Créer un objet GeoJSON pour les coordonnées (format attendu par MongoDB)
    gps_coordinates = {
        "type": "Point",
        "coordinates": [lng, lat]  # Important: MongoDB utilise [longitude, latitude]
    }
    
    # Normaliser les données
    normalized_data = {
        "_id": place_id,
        "place_id": place_id,
        "name": restaurant_data.get('name', ''),
        "verified": restaurant_data.get('verified', False),
        "photo": restaurant_data.get('photo', ''),
        "description": restaurant_data.get('description', ''),
        "address": restaurant_data.get('address', ''),
        "gps_coordinates": gps_coordinates,
        "category": restaurant_data.get('category', []),
        "opening_hours": restaurant_data.get('opening_hours', []),
        "phone_number": restaurant_data.get('phone_number', ''),
        "website": restaurant_data.get('website', ''),
        "photos": restaurant_data.get('photos', []),
        "business_status": restaurant_data.get('business_status', 'OPERATIONAL'),
        "international_phone_number": restaurant_data.get('international_phone_number', ''),
        "maps_url": restaurant_data.get('maps_url', ''),
        "price_level": restaurant_data.get('price_level', ''),
        "rating": restaurant_data.get('rating', 0),
        "created_at": datetime.now(),
        # Réintégration des champs utiles pour d'autres scripts :
        "reviews": restaurant_data.get('reviews', []),
        "images": restaurant_data.get('images', []),
        "notes_globales": restaurant_data.get('notes_globales', {}),
        "popular_times": restaurant_data.get('popular_times', {}),
        # Ajoute ici d'autres champs à conserver si besoin
    }
    # Si tu veux normaliser/transformer ces champs, fais-le ici avant le return
    # (ex: transformer reviews en liste de dicts, images en URLs, etc.)
    return normalized_data

@timing_decorator
def fetch_html_with_brightdata(url, name=None, platform=None, max_retries=3):
    """
    Récupère le HTML d'une URL en utilisant BrightData avec mise en cache
    """
    log_prefix = f"[{name}] " if name else ""
    
    # Vérifier d'abord dans le cache
    cache_key = f"{url}_{platform}"
    if cache_key in HTML_CACHE:
        print(f"{log_prefix}✅ HTML récupéré depuis le cache")
        return HTML_CACHE[cache_key]
    
    # Configuration BrightData
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {BRIGHTDATA_TOKEN}"
    }
    
    payload = {
        "url": url,
        "zone": "web_unlocker1",
        "render": True,
        "format": "raw"
    }
    
    # S'assurer que max_retries est un entier
    max_retries = int(max_retries)
    
    for attempt in range(max_retries):
        try:
            print(f"{log_prefix}🔑 Utilisation du token BrightData: {BRIGHTDATA_TOKEN[:8]}...")
            print(f"{log_prefix}🌐 Envoi requête BrightData pour: {platform or url}")
            
            response = requests.post(
                "https://api.brightdata.com/request",
                headers=headers,
                data=json.dumps(payload),
                timeout=120
            )
            
            if response.status_code == 200:
                print(f"{log_prefix}✅ Réponse BrightData reçue")
                # Stocker dans le cache
                HTML_CACHE[cache_key] = response.text
                return response.text
            else:
                print(f"{log_prefix}❌ Erreur BrightData {response.status_code}: {response.text}")
                if attempt < max_retries - 1:
                    # Attente exponentielle plus courte si ce n'est pas une erreur 429 (rate limiting)
                    wait_time = 2 ** attempt if response.status_code == 429 else 1
                    print(f"{log_prefix}⚠️ Tentative {attempt + 2}/{max_retries} dans {wait_time}s")
                    time.sleep(wait_time)
                    
        except Exception as e:
            print(f"{log_prefix}❌ Erreur lors de la requête BrightData: {str(e)}")
            if attempt < max_retries - 1:
                print(f"{log_prefix}⚠️ Tentative {attempt + 2}/{max_retries}")
                time.sleep(1)  # Attente plus courte en cas d'erreur de connexion
                
    return None

# Fonctions utilitaires pour l'extraction de données Google Maps

def extract_place_id(url):
    """
    Extrait le place_id d'une URL Google Maps
    
    Args:
        url: URL Google Maps
        
    Returns:
        place_id extrait ou None
    """
    if not url:
        return None
        
    # Recherche dans les paramètres d'URL
    place_id_match = re.search(r'place_id=([^&]+)', url)
    if place_id_match:
        return place_id_match.group(1)
        
    # Si ce n'est pas trouvé, il peut être sous d'autres formats
    cid_match = re.search(r'cid=(\d+)', url)
    if cid_match:
        return f"ChIJ_{cid_match.group(1)}"
        
    # Autre format possible
    data_pid_match = re.search(r'data-pid="([^"]+)"', url)
    if data_pid_match:
        return data_pid_match.group(1)
        
    return None

def extract_coordinates(driver):
    """
    Extrait les coordonnées géographiques depuis Google Maps
    
    Args:
        driver: Instance de WebDriver (déjà sur la page Google Maps)
        
    Returns:
        Dictionnaire avec lat et lng ou None
    """
    if not driver:
        return None
        
    # Méthode 1: Extraction depuis l'URL
    url = driver.current_url
    coords_match = re.search(r'@([-\d.]+),([-\d.]+)', url)
    if coords_match:
        return {
            "lat": float(coords_match.group(1)),
            "lng": float(coords_match.group(2))
        }
    
    # Méthode 2: Extraction depuis la source de la page
    try:
        page_source = driver.page_source
        
        # Format standard JSON dans la page
        location_match = re.search(r'location":\s*{\s*"latitude":\s*([-\d.]+),\s*"longitude":\s*([-\d.]+)', page_source)
        if location_match:
            return {
                "lat": float(location_match.group(1)),
                "lng": float(location_match.group(2))
            }
            
        # Format alternatif
        coords_match = re.search(r'center=([^,]+),([^&]+)', page_source)
        if coords_match:
            return {
                "lat": float(coords_match.group(1)),
                "lng": float(coords_match.group(2))
            }
            
        # Format spécifique à l'API Google Maps
        latlng_match = re.search(r'LatLng\(([-\d.]+), ([-\d.]+)\)', page_source)
        if latlng_match:
            return {
                "lat": float(latlng_match.group(1)),
                "lng": float(latlng_match.group(2))
            }
    except Exception as e:
        if DEBUG_MODE:
            print(f"⚠️ Erreur lors de l'extraction des coordonnées: {str(e)}")
    
    return None

def url_to_base64(url, max_size=(800, 600), quality=90):
    """Télécharge une image depuis une URL, la redimensionne et la convertit en URL data Base64."""
    if not url or not url.startswith(('http://', 'https://')):
        print(f"⚠️ URL d'image invalide: {url}")
        return None
    
    try:
        response = requests.get(url, stream=True, timeout=15) # Timeout court
        response.raise_for_status()
        
        # Vérifier le type de contenu
        content_type = response.headers.get('content-type')
        if not content_type or not content_type.startswith('image/'):
            print(f"⚠️ L'URL ne pointe pas vers une image valide: {url} (type: {content_type})")
            return None
            
        # Lire le contenu
        image_bytes = response.content
        img = Image.open(BytesIO(image_bytes))
        
        # Redimensionner
        img.thumbnail(max_size, Image.Resampling.LANCZOS)
        
        # Convertir en RGB si nécessaire
        if img.mode in ('RGBA', 'LA', 'P'): # 'P' for palette mode
            # Créer un fond blanc pour les images avec transparence ou palette
            alpha = img.convert('RGBA').split()[-1]
            background = Image.new("RGB", img.size, (255, 255, 255))
            background.paste(img, mask=alpha)
            img = background
        elif img.mode != 'RGB':
             img = img.convert('RGB')
             
        # Utiliser la fonction existante pour encoder (qui ajoute déjà data:image/jpeg;base64,)
        base64_string = encode_image_base64(img)
        return base64_string
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Erreur réseau lors du téléchargement de l'image {url}: {e}")
        return None
    except Exception as e:
        print(f"❌ Erreur lors du traitement de l'image {url}: {e}")
        return None

if __name__ == "__main__":
    main()