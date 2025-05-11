#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Générateur de posts pour Choice App utilisant OpenAI GPT-3.5-turbo
Ce script génère des posts réalistes pour les restaurants, événements et lieux de beauté
"""

# Importer les bibliothèques nécessaires
import os
import sys
import json
import time
import random
import logging
import requests
import hashlib
import argparse
import re
import math
from datetime import datetime, timedelta
from pymongo import MongoClient
from bson.objectid import ObjectId

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("openai_post_generator.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration OpenAI
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"
OPENAI_MODEL = "gpt-3.5-turbo"

# Remplacer la classe MistralClient par OpenAIClient
class OpenAIClient:
    """Client pour l'API OpenAI pour générer du texte avec GPT-3.5-turbo"""
    
    def __init__(self):
        self.api_key = OPENAI_API_KEY
        self.api_url = OPENAI_API_URL
        if not self.api_key:
            raise ValueError("Clé API OpenAI manquante. Définir la variable d'environnement OPENAI_API_KEY.")
    
    def generate_text(self, prompt, max_tokens=500, temperature=0.7):
        """
        Génère du texte avec l'API OpenAI
        
        Args:
            prompt: Le texte d'entrée pour guider la génération
            max_tokens: Nombre maximum de tokens à générer
            temperature: Contrôle la créativité (0.0 à 1.0)
            
        Returns:
            Le texte généré
        """
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": OPENAI_MODEL,
            "messages": [
                {"role": "system", "content": "Tu es un assistant expert en génération de contenu pour une application mobile sociale basée sur les expériences locales (restaurants, événements, beauté). Réponds toujours en français."},
                {"role": "user", "content": prompt}
            ],
            "max_tokens": max_tokens,
            "temperature": temperature,
            "n": 1,
            "stop": None
        }

        try:
            response = requests.post(self.api_url, headers=headers, json=payload, timeout=90)
            response.raise_for_status()

            result = response.json()

            if "choices" in result and len(result["choices"]) > 0:
                generated_text = result["choices"][0]["message"]["content"].strip()
                return generated_text
            else:
                logger.warning(f"Réponse inattendue d'OpenAI: {result}")
                return None

        except requests.exceptions.RequestException as e:
            logger.error(f"Erreur de requête OpenAI: {e}")
            if e.response is not None:
                 logger.error(f"Réponse d'erreur OpenAI ({e.response.status_code}): {e.response.text}")
            return None
        except Exception as e:
             logger.error(f"Erreur inattendue lors de l'appel OpenAI: {e}")
             return None
    
    def generate_text_with_retry(self, prompt, max_retries=2):
        """Version avec retry de la génération de texte"""
        for attempt in range(max_retries + 1):
            try:
                # Essayer de générer le texte
                response_text = self.generate_text(prompt)

                # Si la réponse est valide, la retourner
                if response_text:
                    return response_text

                # Si la réponse est vide ou None, logger et réessayer (sauf dernier essai)
                logger.warning(f"Tentative {attempt + 1}/{max_retries + 1}: Réponse vide reçue d'OpenAI.")

            except Exception as e:
                # Logger l'erreur et réessayer (sauf dernier essai)
                logger.error(f"Tentative {attempt + 1}/{max_retries + 1}: Erreur lors de la génération - {e}")

            # Attendre avant de réessayer (sauf après le dernier essai)
            if attempt < max_retries:
                wait_time = 2 ** attempt # Backoff exponentiel simple
                logger.info(f"Attente de {wait_time}s avant la prochaine tentative...")
                time.sleep(wait_time)

        # Si toutes les tentatives échouent
        logger.error(f"Échec de la génération de texte après {max_retries + 1} tentatives.")
        return None

# --- Configuration MongoDB ---
# URI par défaut
DEFAULT_MONGO_URI = "mongodb+srv://remibarbier:Calvi8Pierc2@lieuxrestauration.szq31.mongodb.net/?retryWrites=true&w=majority&appName=lieuxrestauration"
# URI pour les tests
TEST_MONGO_URI = "mongodb+srv://remibarbier:Calvi8Pierc2@lieuxrestauration.szq31.mongodb.net/mongo_connection_test?retryWrites=true&w=majority&appName=lieuxrestauration"

# URI active (peut être modifiée par argument)
MONGO_URI = os.environ.get("MONGO_URI", DEFAULT_MONGO_URI)

# Option pour les tests
USE_TEST_CONNECTION = os.environ.get("USE_TEST_CONNECTION", "false").lower() == "true"
if USE_TEST_CONNECTION:
    logger.info("🔄 Utilisation de la connexion de test (mongo_connection_test)")
    MONGO_URI = TEST_MONGO_URI

# Option pour ignorer les filtres (date pour événements, active pour restaurants)
BYPASS_FILTERS = os.environ.get("BYPASS_FILTERS", "false").lower() == "true"
if BYPASS_FILTERS:
    logger.info("⚠️ Mode bypass: les filtres de date et d'active sont désactivés")

# Noms des bases de données et collections
DB_CHOICE = "choice_app"
DB_RESTAURATION = "Restauration_Officielle"
DB_LOISIR = "Loisir&Culture"
DB_BEAUTY = "Beauty_Wellness"

COLL_POSTS = "Posts"
COLL_USERS = "Users"
COLL_PRODUCERS_CHOICE = "producers"
COLL_PRODUCERS_RESTAURATION = "producers"
COLL_VENUES_LOISIR = "Loisir_Paris_Producers"
COLL_EVENTS_LOISIR = "Loisir_Paris_Evenements"
COLL_BEAUTY_PLACES = "BeautyPlaces"
COLL_WELLNESS_PLACES = "BeautyPlaces"

# --- Configuration du générateur ---
DEFAULT_CONFIG = {
    # Configuration générale
    "sleep_time": 300,                 # Temps d'attente entre les lots (secondes)
    "ai_enabled": True,                # Activer l'IA (Mistral-7B)
    "active_hours_start": 3,           # Heure de début (3 AM)
    "active_hours_end": 7,             # Heure de fin (7 AM)
    
    # Configuration des posts de producteurs (événements)
    "event_posts_count": 3,            # Nombre de posts d'événements à générer
    "min_days_between_event_posts": 3, # Jours minimum entre deux posts pour le même lieu culturel
    
    # Configuration des posts de producteurs (restaurants)
    "restaurant_posts_count": 2,       # Nombre de posts de restaurants à générer
    "min_days_between_resto_posts": 3, # Jours minimum entre deux posts pour le même restaurant
    
    # Configuration des posts de producteurs (beauté/bien-être)
    "beauty_posts_count": 2,           # Nombre de posts de beauté/bien-être à générer
    "min_days_between_beauty_posts": 3, # Jours minimum entre deux posts pour le même lieu de beauté
    
    # Configuration des posts d'utilisateurs
    "user_posts_count": 5,             # Nombre de posts d'utilisateurs à générer
    "min_location_duration": 30,       # Durée minimale en minutes à un lieu pour considérer comme visite valide
    "max_post_age_days": 14,           # Nombre de jours max depuis la visite pour générer un post
    "good_review_probability": 0.7,    # Probabilité d'un avis positif
    "max_users": 50,                   # Nombre maximum d'utilisateurs à traiter (None = tous)
    "verify_location": True,           # Vérifier que l'utilisateur a bien visité le lieu
    "location_radius_meters": 30,      # Rayon en mètres autour du lieu pour valider la présence
}

# --- Répertoires pour les checkpoints ---
WORKSPACE_DIR = os.environ.get("WORKSPACE_DIR", "/workspace")
CHECKPOINTS_DIR = os.path.join(WORKSPACE_DIR, "checkpoints")
API_CACHE_DIR = os.path.join(WORKSPACE_DIR, "api_cache")

# Créer les répertoires nécessaires
os.makedirs(CHECKPOINTS_DIR, exist_ok=True)
os.makedirs(API_CACHE_DIR, exist_ok=True)

# --- Catégories de lieux et leurs aspects à évaluer pour les posts utilisateurs ---
VENUE_CATEGORIES = {
    "restaurant": {
        "aspects": ["service", "qualité des plats", "ambiance", "rapport qualité-prix", "présentation"],
        "emotions_positive": ["satisfait", "ravi", "impressionné", "émerveillé", "comblé", "surpris"],
        "emotions_negative": ["déçu", "frustré", "mécontent", "insatisfait", "contrarié", "agacé"],
        "emojis_positive": ["😍", "👌", "🤤", "😋", "🍽️", "🥂", "🍷", "✨", "👩‍🍳", "👨‍🍳", "🔥"],
        "emojis_negative": ["😕", "😞", "😒", "👎", "🤔", "💸", "⏱️"]
    },
    "café": {
        "aspects": ["qualité du café", "pâtisseries", "ambiance", "service", "confort"],
        "emotions_positive": ["satisfait", "ravi", "réconforté", "détendu", "charmé"],
        "emotions_negative": ["déçu", "frustré", "mécontent", "insatisfait", "agacé"],
        "emojis_positive": ["☕", "🍰", "😌", "✨", "📚", "🥐", "🥖", "🍮"],
        "emojis_negative": ["😕", "😞", "😒", "👎", "🤔", "💸", "⏱️"]
    },
    "bar": {
        "aspects": ["ambiance", "service", "qualité des boissons", "musique", "public"],
        "emotions_positive": ["satisfait", "ravi", "euphorique", "festif", "enthousiaste"],
        "emotions_negative": ["déçu", "frustré", "mécontent", "insatisfait", "contrarié"],
        "emojis_positive": ["🍻", "🍹", "🥂", "🍸", "🍷", "🎵", "🎶", "💃", "🕺"],
        "emojis_negative": ["😕", "😞", "😒", "👎", "🤔", "💸", "⏱️"]
    },
    "musique": {
        "aspects": ["acoustique", "performance", "ambiance", "public", "organisation"],
        "emotions_positive": ["ému", "transporté", "euphorique", "conquis", "enthousiaste"],
        "emotions_negative": ["déçu", "frustré", "mécontent", "insatisfait", "agacé"],
        "emojis_positive": ["🎵", "🎶", "🎸", "🎹", "🎤", "🎷", "🥁", "🎻", "😍", "🤩"],
        "emojis_negative": ["😕", "😞", "😒", "👎", "🤔", "💸", "⏱️"]
    },
    "théâtre": {
        "aspects": ["jeu d'acteurs", "mise en scène", "texte", "décors", "audience"],
        "emotions_positive": ["ému", "transporté", "captivé", "impressionné", "ravi"],
        "emotions_negative": ["déçu", "frustré", "mécontent", "insatisfait", "ennuyé"],
        "emojis_positive": ["🎭", "👏", "🎬", "😍", "🤩", "✨", "💯"],
        "emojis_negative": ["😕", "😞", "😒", "👎", "🤔", "💸", "⏱️"]
    },
    "exposition": {
        "aspects": ["œuvres", "scénographie", "information", "accessibilité", "originalité"],
        "emotions_positive": ["inspiré", "émerveillé", "captivé", "enrichi", "impressionné"],
        "emotions_negative": ["déçu", "frustré", "mécontent", "insatisfait", "indifférent"],
        "emojis_positive": ["🎨", "🖼️", "📷", "🏛️", "✨", "😍", "🤩", "👁️"],
        "emojis_negative": ["😕", "😞", "😒", "👎", "🤔", "💸", "⏱️"]
    },
    "cinéma": {
        "aspects": ["film", "acteurs", "réalisation", "atmosphère", "confort"],
        "emotions_positive": ["captivé", "ému", "diverti", "impressionné", "satisfait"],
        "emotions_negative": ["déçu", "frustré", "mécontent", "insatisfait", "ennuyé"],
        "emojis_positive": ["🎬", "🎥", "🍿", "🎞️", "😍", "🤩", "👏"],
        "emojis_negative": ["😕", "😞", "😒", "👎", "🤔", "💸", "⏱️"]
    },
    "festival": {
        "aspects": ["programmation", "organisation", "ambiance", "installations", "expérience globale"],
        "emotions_positive": ["euphorique", "enchanté", "transporté", "émerveillé", "exalté"],
        "emotions_negative": ["déçu", "frustré", "mécontent", "insatisfait", "contrarié"],
        "emojis_positive": ["🎪", "🎡", "🎵", "🎶", "🎉", "✨", "🎭", "🎬", "🎨"],
        "emojis_negative": ["😕", "😞", "😒", "👎", "🤔", "💸", "⏱️"]
    },
    "spa": {
        "aspects": ["soins", "ambiance", "service", "propreté", "rapport qualité-prix"],
        "emotions_positive": ["détendu", "relaxé", "ressourcé", "apaisé", "revitalisé"],
        "emotions_negative": ["déçu", "stressé", "mécontent", "insatisfait", "tendu"],
        "emojis_positive": ["💆", "🧖", "✨", "🌿", "💦", "🔆", "🌸", "🧘", "💫"],
        "emojis_negative": ["😕", "😞", "😒", "👎", "🤔", "💸", "⏱️"]
    },
    "institut_beaute": {
        "aspects": ["soins", "expertise", "accueil", "hygiène", "résultat"],
        "emotions_positive": ["embelli", "satisfait", "rayonnant", "confiant", "charmé"],
        "emotions_negative": ["déçu", "frustré", "mécontent", "insatisfait", "mal à l'aise"],
        "emojis_positive": ["✨", "💅", "💆", "👄", "🌿", "🧴", "💄", "💯", "🌸"],
        "emojis_negative": ["😕", "😞", "😒", "👎", "🤔", "💸", "⏱️"]
    },
    "salon_coiffure": {
        "aspects": ["coupe", "conseil", "technique", "accueil", "résultat"],
        "emotions_positive": ["satisfait", "transformé", "embelli", "confiant", "ravi"],
        "emotions_negative": ["déçu", "insatisfait", "frustré", "mécontent", "contrarié"],
        "emojis_positive": ["💇", "✂️", "💁", "✨", "🔝", "🤩", "👌", "💯"],
        "emojis_negative": ["😕", "😞", "😒", "👎", "🤔", "💸", "⏱️"]
    },
    "default": {
        "aspects": ["qualité générale", "service", "ambiance", "rapport qualité-prix", "expérience"],
        "emotions_positive": ["satisfait", "ravi", "impressionné", "content", "comblé"],
        "emotions_negative": ["déçu", "frustré", "mécontent", "insatisfait", "contrarié"],
        "emojis_positive": ["👍", "✨", "🙂", "😊", "👏", "💯"],
        "emojis_negative": ["😕", "😞", "😒", "👎", "🤔", "💸", "⏱️"]
    }
}

# Mappage des catégories de restaurant vers les catégories d'évaluation
RESTAURANT_CATEGORY_MAPPING = {
    "italien": "restaurant",
    "japonais": "restaurant",
    "français": "restaurant",
    "vietnamien": "restaurant",
    "chinois": "restaurant",
    "thai": "restaurant",
    "indien": "restaurant",
    "mexicain": "restaurant",
    "libanais": "restaurant",
    "café": "café",
    "boulangerie": "café",
    "pâtisserie": "café",
    "bar": "bar",
    "pub": "bar",
    "wine bar": "bar",
    "cocktail bar": "bar",
    "brasserie": "restaurant",
    "crêperie": "restaurant",
    "sushi": "restaurant",
    "burger": "restaurant",
    "pizza": "restaurant",
    "sandwich": "restaurant",
    "fastfood": "restaurant",
    "fast food": "restaurant",
    "végétarien": "restaurant",
    "vegan": "restaurant",
    "brunch": "café",
    # Par défaut, tout autre est considéré comme "restaurant"
}

# Mappage des catégories d'événements vers les catégories d'évaluation
EVENT_CATEGORY_MAPPING = {
    "concert": "musique",
    "musique": "musique",
    "électronique": "musique",
    "techno": "musique",
    "rock": "musique",
    "jazz": "musique",
    "classique": "musique",
    "opéra": "musique",
    "théâtre": "théâtre",
    "comédie": "théâtre",
    "danse": "théâtre",
}

# Mappage des catégories de lieux de beauté vers les catégories d'évaluation
BEAUTY_CATEGORY_MAPPING = {
    "spa": "spa",
    "hammam": "spa",
    "sauna": "spa",
    "massage": "spa",
    "soins du corps": "spa",
    "bien-être": "spa",
    "relaxation": "spa",
    "institut": "institut_beaute",
    "institut de beauté": "institut_beaute",
    "soin visage": "institut_beaute",
    "soin du visage": "institut_beaute",
    "épilation": "institut_beaute",
    "esthétique": "institut_beaute",
    "manucure": "institut_beaute",
    "pédicure": "institut_beaute",
    "onglerie": "institut_beaute",
    "coiffeur": "salon_coiffure",
    "coiffure": "salon_coiffure",
    "salon de coiffure": "salon_coiffure",
    "barbier": "salon_coiffure",
    "salon": "salon_coiffure",
    "beauté": "institut_beaute"
}

# --- Fonctions utilitaires ---

def save_checkpoint(name, data):
    """Sauvegarde un checkpoint"""
    checkpoint_path = os.path.join(CHECKPOINTS_DIR, f"post_generator_{name}.json")
    with open(checkpoint_path, 'w') as f:
        json.dump(data, f)
    logger.info(f"Checkpoint sauvegardé: {name}")

def load_checkpoint(name):
    """Charge un checkpoint"""
    checkpoint_path = os.path.join(CHECKPOINTS_DIR, f"post_generator_{name}.json")
    if os.path.exists(checkpoint_path):
        with open(checkpoint_path, 'r') as f:
            return json.load(f)
    return None

def cache_api_response(prompt, response):
    """Cache la réponse générée"""
    prompt_hash = hashlib.md5(prompt.encode()).hexdigest()
    cache_path = os.path.join(API_CACHE_DIR, f"{prompt_hash}.json")
    with open(cache_path, 'w') as f:
        json.dump({
            "prompt": prompt,
            "response": response,
            "timestamp": datetime.now().isoformat()
        }, f)

def get_cached_response(prompt):
    """Récupère une réponse en cache"""
    prompt_hash = hashlib.md5(prompt.encode()).hexdigest()
    cache_path = os.path.join(API_CACHE_DIR, f"{prompt_hash}.json")
    if os.path.exists(cache_path):
        with open(cache_path, 'r') as f:
            cached = json.load(f)
            # N'utiliser le cache que s'il a moins de 24 heures
            cache_time = datetime.fromisoformat(cached["timestamp"])
            if datetime.now() - cache_time < timedelta(hours=24):
                return cached["response"]
    return None

def parse_date_fr(date_str):
    """Parse une date au format français (DD/MM/YYYY)"""
    if not date_str or date_str == "Dates non disponibles":
        return None
    
    # Format standard: DD/MM/YYYY
    if re.match(r'\d{2}/\d{2}/\d{4}', date_str):
        day, month, year = map(int, date_str.split('/'))
        return datetime(year, month, day)
    
    # Format "sam 15 févr." (pour shotgun)
    months_fr = {
        'janv': 1, 'févr': 2, 'mars': 3, 'avr': 4, 'mai': 5, 'juin': 6,
        'juil': 7, 'août': 8, 'sept': 9, 'oct': 10, 'nov': 11, 'déc': 12
    }
    
    # Regex pour extraire le jour et le mois
    match = re.search(r'(\d{1,2})\s+(\w+)', date_str)
    if match:
        day = int(match.group(1))
        month_str = match.group(2).lower()[:4]  # Premiers caractères du mois
        
        # Trouver le mois correspondant
        month = None
        for m_name, m_num in months_fr.items():
            if month_str in m_name:
                month = m_num
                break
        
        if month:
            # Année en cours par défaut, ou année suivante si date déjà passée
            year = datetime.now().year
            date_obj = datetime(year, month, day)
            
            # Si la date est déjà passée, on suppose que c'est l'année prochaine
            if date_obj < datetime.now() and month < datetime.now().month:
                date_obj = datetime(year + 1, month, day)
            
            return date_obj
    
    return None

def is_event_ended(event):
    """Vérifie si un événement est terminé en fonction des dates"""
    # Si le mode bypass est activé, considérer tous les événements comme valides
    if BYPASS_FILTERS:
        return False
        
    today = datetime.now().date()
    
    # Vérifier la date de fin si elle existe
    if "date_fin" in event and event["date_fin"]:
        end_date = parse_date_fr(event["date_fin"])
        if end_date and end_date.date() < today:
            return True
    
    # Vérifier les prochaines dates
    if "prochaines_dates" in event and event["prochaines_dates"]:
        # Si les dates ne sont pas disponibles
        if event["prochaines_dates"] == "Dates non disponibles":
            # On considère que l'événement est actif pour les événements récents (moins de 2 semaines)
            if "dateAjout" in event:
                added_date = parse_date_fr(event["dateAjout"])
                if added_date and (today - added_date.date()).days > 14:
                    return True
            return False
        
        # Analyser la chaîne de prochaines dates
        date_obj = parse_date_fr(event["prochaines_dates"])
        if date_obj and date_obj.date() < today:
            return True
    
    # Par défaut, on considère que l'événement est toujours actif
    return False

def get_db_connections(uri=None):
    """Établit des connexions aux bases de données MongoDB"""
    try:
        # Utiliser l'URI fourni ou l'URI par défaut
        connection_uri = uri or MONGO_URI
        logger.info(f"Connexion à MongoDB: {connection_uri.split('@')[1].split('/')[0]}...")
        
        client = MongoClient(connection_uri)
        
        # Accéder aux bases de données
        db_choice = client[DB_CHOICE]
        db_restauration = client[DB_RESTAURATION]
        db_loisir = client[DB_LOISIR]
        db_beauty = client[DB_BEAUTY]
        
        # Vérifier si les collections existent
        collections_choice = db_choice.list_collection_names()
        collections_restauration = db_restauration.list_collection_names()
        collections_loisir = db_loisir.list_collection_names()
        collections_beauty = db_beauty.list_collection_names()
        
        logger.info(f"Collections disponibles:")
        logger.info(f"  • Choice: {', '.join(collections_choice)}")
        logger.info(f"  • Restauration: {', '.join(collections_restauration)}")
        logger.info(f"  • Loisir: {', '.join(collections_loisir)}")
        logger.info(f"  • Beauty: {', '.join(collections_beauty)}")
        
        # Vérifier l'accès aux collections spécifiques
        posts_count = db_choice[COLL_POSTS].count_documents({}) if COLL_POSTS in collections_choice else 0
        venues_count = db_loisir[COLL_VENUES_LOISIR].count_documents({}) if COLL_VENUES_LOISIR in collections_loisir else 0
        events_count = db_loisir[COLL_EVENTS_LOISIR].count_documents({}) if COLL_EVENTS_LOISIR in collections_loisir else 0
        restaurants_count = db_restauration[COLL_PRODUCERS_RESTAURATION].count_documents({}) if COLL_PRODUCERS_RESTAURATION in collections_restauration else 0
        beauty_count = db_beauty[COLL_BEAUTY_PLACES].count_documents({}) if COLL_BEAUTY_PLACES in collections_beauty else 0
        
        logger.info(f"Connexion établie aux bases de données MongoDB")
        logger.info(f"Posts: {posts_count}, Lieux: {venues_count}, Événements: {events_count}, Restaurants: {restaurants_count}, Lieux de beauté: {beauty_count}")
        
        return {
            "choice": db_choice,
            "restauration": db_restauration,
            "loisir": db_loisir,
            "beauty": db_beauty
        }
        
    except Exception as e:
        logger.error(f"Erreur de connexion MongoDB: {e}")
        logger.error(f"Détails: {str(e)}")
        raise

def generate_post_with_openai(prompt, openai_client):
    """Génère le contenu d'un post en utilisant le client OpenAI."""
    # Utiliser le cache si disponible
    cached = get_cached_response(prompt)
    if cached:
        logger.info("Utilisation de la réponse OpenAI mise en cache.")
        return cached

    # Générer le texte avec le client OpenAI
    generated_text = openai_client.generate_text_with_retry(prompt)

    # Mettre en cache la réponse si elle est valide
    if generated_text:
        cache_api_response(prompt, generated_text)

    return generated_text

# --- Fonctions pour les Posts des lieux culturels (événements) ---

def create_post_for_event(db_connections, event, leisure_venue, openai_client):
    """Crée un post producteur pour un événement en utilisant OpenAI."""
    try:
        # Vérifier que l'événement et le lieu existent
        if not event or not leisure_venue:
            logger.warning("❌ Événement ou lieu manquant")
            return None
        
        # Récupérer les données de l'événement (avec différents noms de champs possibles)
        # Titre de l'événement
        event_title = None
        for field in ["intitulé", "titre", "name", "title"]:
            if field in event and event[field]:
                event_title = event[field]
                break
        
        if not event_title:
            event_title = "Événement"  # Valeur par défaut
            logger.warning("⚠️ Titre d'événement non trouvé, utilisation de la valeur par défaut")
        
        # Détails de l'événement
        event_details = ""
        for field in ["détail", "description", "content", "details"]:
            if field in event and event[field]:
                event_details = event[field]
                break
        
        # Catégorie de l'événement
        event_category = ""
        for field in ["catégorie", "category", "type"]:
            if field in event and event[field]:
                event_category = event[field]
                break
        
        # Image de l'événement
        event_image = ""
        for field in ["image", "photo", "cover", "thumbnail"]:
            if field in event and event[field]:
                event_image = event[field]
                break
        
        # Si pas d'image principale, vérifier dans le lineup
        if not event_image and "lineup" in event and isinstance(event["lineup"], list) and len(event["lineup"]) > 0:
            for artist in event["lineup"]:
                if isinstance(artist, dict) and "image" in artist and artist["image"]:
                    event_image = artist["image"]
                    break
        
        # Dates de l'événement
        event_dates = ""
        for field in ["prochaines_dates", "dates", "date", "dateEvenement", "event_date"]:
            if field in event and event[field]:
                event_dates = event[field]
                break
        
        # Location et adresse
        event_location = {}
        if "location" in event and isinstance(event["location"], dict):
            # Structure MongoDB observée
            if "type" in event["location"] and event["location"]["type"] == "Point" and "coordinates" in event["location"]:
                event_location = {
                    "type": event["location"]["type"],
                    "coordinates": event["location"]["coordinates"]
                }
            elif "adresse" in event["location"]:
                event_location = {
                    "address": event["location"]["adresse"],
                    "coords": event["location"].get("coords", {})
                }
        elif "lieu" in event:
            event_location = {"address": event["lieu"]}
            
        # Lineup / artistes
        artists = []
        if "lineup" in event and isinstance(event["lineup"], list):
            for artist in event["lineup"]:
                if isinstance(artist, dict) and "nom" in artist:
                    artists.append(artist["nom"])
        
        # Informations du lieu - Assurer qu'on a toujours un nom valide
        venue_name = None
        for field in ["name", "nom", "lieu", "title"]:
            if field in leisure_venue and leisure_venue[field]:
                venue_name = leisure_venue[field]
                break
                
        if not venue_name or venue_name.strip() == "":
            venue_name = "Lieu Culturel"  # Fallback si nom vide
            logger.warning("⚠️ Nom de lieu non trouvé, utilisation de la valeur par défaut")
        
        venue_id = str(leisure_venue.get("_id", ""))
        
        # Chercher l'avatar dans différents champs possibles
        venue_avatar = None
        avatar_fields = ["profile_image", "avatar", "photo", "image", "logo"]
        for field in avatar_fields:
            if field in leisure_venue and leisure_venue[field] and isinstance(leisure_venue[field], str):
                venue_avatar = leisure_venue[field]
                break
        
        # Assurer un avatar par défaut correct
        if not venue_avatar or venue_avatar.strip() == "":
            venue_avatar = "/images/default_venue_avatar.png"
            logger.info(f"⚠️ Avatar manquant pour {venue_name}, utilisation de l'avatar par défaut")
        
        # Vérifier si un post similaire existe déjà récemment (éviter les doublons)
        try:
            # Utiliser count_documents au lieu de count() sur le curseur
            min_days = DEFAULT_CONFIG["min_days_between_event_posts"]
            recent_posts_count = db_connections["choice"][COLL_POSTS].count_documents({
                "producer_id": venue_id,
                "referenced_event_id": str(event["_id"]),
                "time_posted": {"$gt": (datetime.now() - timedelta(days=min_days)).isoformat()}
            })
            
            if recent_posts_count > 0:
                logger.info(f"Post similaire trouvé récemment pour {venue_name} -> {event_title}, ignoré.")
                return None
        except Exception as e:
            logger.warning(f"⚠️ Erreur lors de la vérification des posts récents: {e}")
        
        # Construire le prompt pour OpenAI GPT-3.5-turbo
        post_prompt = f"""
        Rédige un post engageant pour les réseaux sociaux où le lieu culturel "{venue_name}" présente l'événement "{event_title}".

        Informations sur l'événement:
        - Titre: {event_title}
        - Catégorie: {event_category}
        - Détails: {event_details}
        - Dates: {event_dates}
        {f"- Artistes: {', '.join(artists)}" if artists else ""}
        
        Le post doit:
        1. Être écrit du point de vue du lieu culturel qui accueille/présente l'événement
        2. Être captivant et inciter les utilisateurs à découvrir l'événement
        3. Inclure des émojis pertinents (comme ✨🎭🎵 selon le thème)
        4. Faire entre 300-400 caractères maximum
        5. Se terminer par une question ou une incitation à l'action
        
        N'invente aucune information qui n'est pas fournie. Utilise uniquement les faits donnés.
        Supprime toute instruction et ne réponds qu'avec le texte final du post.
        """
        
        # Générer le contenu du post
        content = None
        if DEFAULT_CONFIG["ai_enabled"] and openai_client:
            content = generate_post_with_openai(post_prompt, openai_client)
        
        # Si OpenAI échoue ou est désactivé, générer un contenu de secours
        if not content:
            logger.warning(f"⚠️ Génération AI (OpenAI) échouée, utilisation d'un contenu de secours")
            
            # Créer une description d'événement plus engageante
            emoji_map = {
                "concert": "🎵🎸",
                "musique": "🎧🎹",
                "théâtre": "🎭🎬",
                "exposition": "🖼️🎨",
                "house": "🔊💿",
                "deep": "🎧🔊",
                "techno": "🎛️🎚️",
                "dj": "🎧🎚️",
                "art": "🎨🖌️",
                "comédie": "😂🎭",
                "danse": "💃🕺",
                "festival": "🎉🎊"
            }
            
            # Choisir des émojis appropriés basés sur la catégorie
            emojis = "✨🎟️"  # Émojis par défaut
            category_lower = event_category.lower() if event_category else ""
            
            for key, value in emoji_map.items():
                if key in category_lower:
                    emojis = value
                    break
            
            # Construire des phrases d'accroche variées
            hooks = [
                f"{emojis} Ne manquez pas",
                f"{emojis} Rendez-vous pour découvrir",
                f"{emojis} Nous sommes ravis de vous présenter",
                f"{emojis} Un événement unique vous attend :",
                f"{emojis} Rejoignez-nous pour"
            ]
            
            # Construire des appels à l'action variés
            cta = [
                "Réservez vite votre place ! 🎟️",
                "Venez vivre cette expérience unique ! 🤩",
                "On vous attend nombreux ! 👥",
                "Êtes-vous prêts à vivre ce moment exceptionnel ? 💫",
                "Ne tardez pas, les places sont limitées ! 🎯"
            ]
            
            # Créer le contenu avec une structure améliorée
            hook = random.choice(hooks)
            action = random.choice(cta)
            
            # Limite de caractères pour la description
            desc_limit = 100
            details = event_details[:desc_limit] + ("..." if len(event_details) > desc_limit else "") if event_details else ""
            
            date_info = f" | {event_dates}" if event_dates else ""
            
            # Assembler le contenu final
            content = f"{hook} \"{event_title}\" {date_info}\n\n{details}\n\n{action}"
        
        # Créer le post avec structure conforme au MongoDB
        post = {
            "content": content,
            "time_posted": datetime.now().isoformat(),
            "author": {
                "id": venue_id,
                "name": venue_name,
                "avatar": venue_avatar
            },
            "producer_id": venue_id,
            "referenced_event_id": str(event["_id"]),
            "isProducerPost": True,
            "isLeisureProducer": True,
            "isBeautyProducer": False,
            "isRestaurationProducer": False,
            "is_automated": True,
            "is_event_post": True,  # Ajout d'un indicateur clair pour le comptage
            "likes": 0,              # Pour compatibilité frontend
            "likes_count": 0,
            "comments": 0,           # Pour compatibilité frontend
            "comments_count": 0,
            "interested": False,     # Indique si l'utilisateur actuel est intéressé
            "interested_count": 0,
            "choice": False,         # Indique si l'utilisateur actuel a choisi
            "choice_count": 0,
            "location": event_location,  # Coordonnées pour affichage carte
            "media": []             # Initialiser le tableau media (sera rempli ensuite)
        }

        # Ajouter l'image principale si disponible
        if event_image:
            post["media"].append({
                "type": "image",
                "url": event_image,
                "width": 800,
                "height": 600
            })
        
        # Ajouter le lineup si présent
        if "lineup" in event and isinstance(event["lineup"], list) and len(event["lineup"]) > 0:
            lineup_media = []
            for artist in event["lineup"]:
                if isinstance(artist, dict) and "image" in artist and artist["image"]:
                    lineup_media.append({
                        "type": "image",
                        "url": artist["image"],
                        "width": 400,
                        "height": 400
                    })
            
            # Ajouter les images du lineup au post
            if lineup_media:
                post["media"].extend(lineup_media[:2])  # Limiter à 2 images supplémentaires
        
        # Insérer le post dans la base de données
        post_id = db_connections["choice"][COLL_POSTS].insert_one(post).inserted_id
        logger.info(f"✅ Post créé avec succès dans la base de données. ID: {post_id}")
        
        # Mettre à jour l'événement avec le post référencé
        try:
            db_connections["loisir"][COLL_EVENTS_LOISIR].update_one(
                {"_id": event["_id"]},
                {"$push": {"posts": post_id}}
            )
            logger.info(f"✅ Événement {event['_id']} mis à jour avec le nouveau post référencé")
        except Exception as e:
            logger.warning(f"⚠️ Erreur lors de la mise à jour de l'événement: {e}")
        
        # Mettre à jour le producteur avec le post
        try:
            db_connections["loisir"][COLL_VENUES_LOISIR].update_one(
                {"_id": ObjectId(venue_id)},
                {"$push": {"posts": post_id}}
            )
            logger.info(f"✅ Producteur {venue_id} mis à jour avec le nouveau post")
        except Exception as e:
            logger.warning(f"⚠️ Erreur lors de la mise à jour du producteur: {e}")
        
        return {
            "post_id": str(post_id),
            "event_title": event_title,
            "venue_name": venue_name
        }
        
    except Exception as e:
        logger.error(f"Erreur lors de la création du post: {e}")
        return None

# --- Fonctions pour les Posts de restaurants ---

def create_post_for_restaurant(db_connections, restaurant, openai_client):
    """Crée un post producteur pour un restaurant en utilisant OpenAI."""
    try:
        # Extraction des données du restaurant - Assurer qu'on a toujours un nom valide
        restaurant_name = restaurant.get("name", "")
        if not restaurant_name or restaurant_name.strip() == "":
            restaurant_name = "Restaurant"  # Fallback si nom vide
            
        restaurant_id = str(restaurant.get("_id", ""))
        
        # Extraire le type de cuisine (plusieurs façons possibles selon le format)
        restaurant_cuisine = ""
        if "cuisine_type" in restaurant:
            restaurant_cuisine = restaurant["cuisine_type"]
        elif "category" in restaurant and isinstance(restaurant["category"], list):
            restaurant_cuisine = ", ".join(restaurant["category"])
        
        # Chercher des plats signature
        restaurant_signature = []
        
        # Vérifier d'abord les "dishes" directement
        if "dishes" in restaurant and isinstance(restaurant["dishes"], list):
            restaurant_signature = restaurant["dishes"][:3]
            
        # Si aucun plat signature dans "dishes", chercher dans structured_data
        elif "structured_data" in restaurant and restaurant["structured_data"]:
            struct_data = restaurant["structured_data"]
            
            # Chercher dans les Menus Globaux
            if "Menus Globaux" in struct_data and isinstance(struct_data["Menus Globaux"], list):
                for menu in struct_data["Menus Globaux"]:
                    if "inclus" in menu and isinstance(menu["inclus"], list):
                        for section in menu["inclus"]:
                            if "items" in section and isinstance(section["items"], list):
                                for item in section["items"]:
                                    if "nom" in item and item["nom"]:
                                        restaurant_signature.append(item["nom"])
                                        if len(restaurant_signature) >= 3:
                                            break
                                if len(restaurant_signature) >= 3:
                                    break
                        if len(restaurant_signature) >= 3:
                            break
            
            # Chercher dans Items Indépendants si on n'a pas encore 3 plats
            if len(restaurant_signature) < 3 and "Items Indépendants" in struct_data and isinstance(struct_data["Items Indépendants"], list):
                for item in struct_data["Items Indépendants"]:
                    if "nom" in item and item["nom"]:
                        restaurant_signature.append(item["nom"])
                        if len(restaurant_signature) >= 3:
                            break
        
        # Si toujours pas de plats, chercher dans le menu standard
        if not restaurant_signature and "menu" in restaurant and isinstance(restaurant["menu"], list):
            for section in restaurant["menu"]:
                if "items" in section and isinstance(section["items"], list):
                    for item in section["items"]:
                        if "name" in item:
                            restaurant_signature.append(item["name"])
                            if len(restaurant_signature) >= 3:
                                break
                    if len(restaurant_signature) >= 3:
                        break
        
        # Trouver l'image du restaurant
        restaurant_image = ""
        image_fields = ["main_image", "photo", "image"]
        
        for field in image_fields:
            if field in restaurant and restaurant[field]:
                restaurant_image = restaurant[field]
                break
                
        # Si pas d'image principale, chercher dans photos
        if not restaurant_image and "photos" in restaurant and isinstance(restaurant["photos"], list) and len(restaurant["photos"]) > 0:
            restaurant_image = restaurant["photos"][0]
        
        # Assurer un avatar par défaut correct
        restaurant_avatar = ""
        avatar_fields = ["profile_image", "avatar", "photo"]
        
        for field in avatar_fields:
            if field in restaurant and restaurant[field]:
                restaurant_avatar = restaurant[field]
                break
                
        if not restaurant_avatar or restaurant_avatar.strip() == "":
            restaurant_avatar = "/images/default_restaurant_avatar.png"
        
        # Récupérer les coordonnées de localisation
        restaurant_location = {}
        if "gps_coordinates" in restaurant and restaurant["gps_coordinates"]:
            restaurant_location = restaurant["gps_coordinates"]
        elif "location" in restaurant and restaurant["location"]:
            restaurant_location = restaurant["location"]
        
        # Vérifier si un post similaire existe déjà récemment (éviter les doublons)
        try:
            # Utiliser count_documents au lieu de count() sur le curseur
            min_days = DEFAULT_CONFIG["min_days_between_resto_posts"]
            recent_posts_count = db_connections["choice"][COLL_POSTS].count_documents({
                "producer_id": restaurant_id,
                "time_posted": {"$gt": (datetime.now() - timedelta(days=min_days)).isoformat()}
            })
            
            if recent_posts_count > 0:
                logger.info(f"Post similaire trouvé récemment pour {restaurant_name}, ignoré.")
                return None
        except Exception as e:
            logger.warning(f"⚠️ Erreur lors de la vérification des posts récents: {e}")
        
        # Construire le prompt pour OpenAI GPT-3.5-turbo
        post_prompt = f"""
        Rédige un post engageant pour les réseaux sociaux pour le restaurant "{restaurant_name}".

        Informations sur le restaurant:
        - Nom: {restaurant_name}
        - Type de cuisine: {restaurant_cuisine}
        - Plats signature: {', '.join(restaurant_signature) if restaurant_signature else 'Non spécifié'}
        
        Le post doit:
        1. Être écrit à la première personne du pluriel (nous) du point de vue du restaurant
        2. Être captivant et donner envie de découvrir le restaurant
        3. Inclure des émojis pertinents pour la nourriture (🍽️, 🥘, 🍷, etc.)
        4. Faire entre 300-400 caractères maximum
        5. Se terminer par une question ou une incitation à l'action
        
        N'invente aucune information qui n'est pas fournie. Utilise uniquement les faits donnés.
        Supprime toute instruction et ne réponds qu'avec le texte final du post.
        """
        
        # Générer le contenu du post
        content = None
        if DEFAULT_CONFIG["ai_enabled"] and openai_client:
            content = generate_post_with_openai(post_prompt, openai_client)
        
        # Si OpenAI échoue, générer un contenu de secours
        if not content:
            logger.warning(f"⚠️ Génération AI (OpenAI) échouée, utilisation d'un contenu de secours")
            signature_text = f"Nos spécialités: {', '.join(restaurant_signature[:2])}" if restaurant_signature else ""
            content = f"🍽️ {restaurant_name} vous accueille pour découvrir notre cuisine {restaurant_cuisine}. {signature_text} Venez nous rendre visite! #food #restaurant"
        
        # Créer le post avec structure conforme à MongoDB
        post = {
            "content": content,
            "time_posted": datetime.now().isoformat(),
            "author": {
                "id": restaurant_id,
                "name": restaurant_name,
                "avatar": restaurant_avatar
            },
            "producer_id": restaurant_id,
            "isProducerPost": True,
            "isLeisureProducer": False,
            "isBeautyProducer": False,
            "isRestaurationProducer": True,  # Nouveau champ pour identifier clairement les restaurants
            "is_automated": True,
            "is_restaurant_post": True,  # Ajout d'un indicateur clair pour le comptage
            "likes": 0,              # Pour compatibilité frontend
            "likes_count": 0,
            "comments": 0,           # Pour compatibilité frontend
            "comments_count": 0,
            "interested": False,     # Indique si l'utilisateur actuel est intéressé
            "interested_count": 0, 
            "choice": False,         # Indique si l'utilisateur actuel a choisi
            "choice_count": 0,
            "location": restaurant_location,  # Coordonnées pour affichage carte
            "media": []              # Initialiser le tableau media (sera rempli ensuite)
        }
        
        # Ajouter l'image principale si disponible
        if restaurant_image:
            post["media"].append({
                "type": "image",
                "url": restaurant_image,
                "width": 800,
                "height": 600
            })
        
        # Ajouter des images de plats si disponibles
        if "dish_images" in restaurant and isinstance(restaurant["dish_images"], list):
            dish_media = []
            for image_url in restaurant["dish_images"]:
                if image_url:
                    dish_media.append({
                        "type": "image",
                        "url": image_url,
                        "width": 400,
                        "height": 400
                    })
            
            # Ajouter les images de plats au post
            if dish_media:
                post["media"].extend(dish_media[:2])  # Limiter à 2 images supplémentaires
        # Si pas d'images de plats spécifiques, essayer les photos générales
        elif "photos" in restaurant and isinstance(restaurant["photos"], list) and len(restaurant["photos"]) > 1:
            # Ajouter jusqu'à 2 photos supplémentaires (en sautant la première qui est l'image principale)
            for i in range(1, min(3, len(restaurant["photos"]))):
                post["media"].append({
                    "type": "image",
                    "url": restaurant["photos"][i],
                    "width": 400,
                    "height": 400
                })
        
        # Insérer le post dans la base de données
        post_id = db_connections["choice"][COLL_POSTS].insert_one(post).inserted_id
        logger.info(f"✅ Post créé avec succès dans la base de données. ID: {post_id}")
        
        # Mettre à jour le restaurant avec le post
        try:
            db_connections["restauration"][COLL_PRODUCERS_RESTAURATION].update_one(
                {"_id": ObjectId(restaurant_id)},
                {"$push": {"posts": post_id}}
            )
            logger.info(f"✅ Restaurant {restaurant_id} mis à jour avec le nouveau post")
        except Exception as e:
            logger.warning(f"⚠️ Erreur lors de la mise à jour du restaurant: {e}")
        
        return {
            "post_id": str(post_id),
            "restaurant_name": restaurant_name
        }
        
    except Exception as e:
        logger.error(f"Erreur lors de la création du post pour restaurant: {e}")
        return None

# --- Fonctions pour les Posts utilisateurs ---

def is_within_distance(user_loc, venue_loc, max_distance_meters=30):
    """
    Vérifie si deux coordonnées GPS sont à proximité l'une de l'autre.
    Utilise une approximation simple (1 degré latitude ~ 111km, 1 degré longitude ~ 111km*cos(lat))
    
    Parameters:
    user_loc (dict): Localisation de l'utilisateur avec latitude et longitude
    venue_loc (dict or list): Localisation du lieu (structure variable)
    max_distance_meters (int): Distance maximale en mètres
    
    Returns:
    bool: True si les coordonnées sont proches, False sinon
    """
    try:
        # Gérer différents formats de coordonnées possibles
        user_lat = user_loc["latitude"] if "latitude" in user_loc else user_loc[1]
        user_lng = user_loc["longitude"] if "longitude" in user_loc else user_loc[0]
        
        venue_lat = None
        venue_lng = None
        
        # Format: {"type": "Point", "coordinates": [lng, lat]}
        if isinstance(venue_loc, dict) and "type" in venue_loc and venue_loc["type"] == "Point":
            venue_lng, venue_lat = venue_loc["coordinates"]
        
        # Format: {"coordinates": [lng, lat]}
        elif isinstance(venue_loc, dict) and "coordinates" in venue_loc:
            if isinstance(venue_loc["coordinates"], list) and len(venue_loc["coordinates"]) >= 2:
                venue_lng, venue_lat = venue_loc["coordinates"]
        
        # Format: {"lat": lat, "lng": lng}
        elif isinstance(venue_loc, dict) and "lat" in venue_loc and "lng" in venue_loc:
            venue_lat = venue_loc["lat"]
            venue_lng = venue_loc["lng"]
        
        # Format: [lng, lat]
        elif isinstance(venue_loc, list) and len(venue_loc) >= 2:
            venue_lng, venue_lat = venue_loc[0], venue_loc[1]
        
        if venue_lat is None or venue_lng is None:
            logger.warning(f"❌ Format de coordonnées non reconnu: {venue_loc}")
            return False
        
        # Conversion simple des degrés en mètres
        meters_per_degree_lat = 111000  # 111 km par degré de latitude
        meters_per_degree_lng = 111000 * abs(math.cos(math.radians(user_lat)))  # Varie selon la latitude
        
        # Calcul des distances en mètres
        distance_lat = abs(user_lat - venue_lat) * meters_per_degree_lat
        distance_lng = abs(user_lng - venue_lng) * meters_per_degree_lng
        
        # Distance euclidienne approximative en mètres
        distance = math.sqrt(distance_lat**2 + distance_lng**2)
        
        return distance <= max_distance_meters
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de la vérification de distance: {e}")
        # En cas d'erreur, on suppose que les lieux ne sont pas proches
        return False

def has_visited_venue(user, venue_id, config):
    """
    Vérifie si l'utilisateur a visité un lieu dans un passé récent et y est resté assez longtemps
    
    Parameters:
    user (dict): Profil utilisateur avec frequent_locations
    venue_id (str): ID du lieu à vérifier
    config (dict): Configuration avec min_location_duration et max_post_age_days
    
    Returns:
    tuple: (bool, datetime) Visité oui/non, et date de la dernière visite
    """
    if not "frequent_locations" in user or not user["frequent_locations"]:
        return False, None
    
    min_duration = config["min_location_duration"]
    max_age_days = config["max_post_age_days"]
    now = datetime.now()
    
    for location in user["frequent_locations"]:
        # Vérifier si c'est le bon lieu
        if location["id"] == venue_id:
            # Parcourir les visites
            if "visits" in location and location["visits"]:
                # Trier les visites par date (la plus récente en premier)
                sorted_visits = sorted(
                    location["visits"], 
                    key=lambda v: datetime.fromisoformat(v["date"]), 
                    reverse=True
                )
                
                for visit in sorted_visits:
                    visit_date = datetime.fromisoformat(visit["date"])
                    visit_duration = visit.get("duration_minutes", 0)
                    
                    # Vérifier l'âge de la visite
                    age_days = (now - visit_date).days
                    
                    if age_days <= max_age_days and visit_duration >= min_duration:
                        return True, visit_date
    
    return False, None

def determine_venue_category(venue, is_event=False, is_beauty=False):
    """
    Détermine la catégorie d'évaluation pour un lieu ou un événement
    
    Parameters:
    venue (dict): Document du lieu ou de l'événement
    is_event (bool): Si True, c'est un événement
    is_beauty (bool): Si True, c'est un lieu de beauté
    
    Returns:
    str: Catégorie d'évaluation
    """
    if is_beauty:
        # Recherche dans la catégorie de lieu de beauté
        beauty_category = ""
        
        if "sous_categorie" in venue:
            beauty_category = venue["sous_categorie"].lower() if venue["sous_categorie"] else ""
        elif "category" in venue:
            beauty_category = venue["category"].lower() if venue["category"] else ""
        
        # Parcourir le mappage des lieux de beauté
        for key, value in BEAUTY_CATEGORY_MAPPING.items():
            if key.lower() in beauty_category:
                return value
        
        # Par défaut pour les lieux de beauté
        return "institut_beaute"
    
    elif is_event:
        # Recherche dans la catégorie d'événement
        if "catégorie" in venue:
            category = venue["catégorie"].lower() if venue["catégorie"] else ""
        elif "category" in venue:
            category = venue["category"].lower() if venue["category"] else ""
        else:
            category = ""
        
        # Parcourir le mappage des événements
        for key, value in EVENT_CATEGORY_MAPPING.items():
            if key.lower() in category:
                return value
        
        # Si catégorie principale est disponible
        if "catégorie_principale" in venue and venue["catégorie_principale"]:
            main_category = venue["catégorie_principale"].lower()
            if main_category in EVENT_CATEGORY_MAPPING:
                return EVENT_CATEGORY_MAPPING[main_category]
        
        return "default"
    else:
        # Recherche dans la catégorie de restaurant
        if "category" in venue and isinstance(venue["category"], list) and venue["category"]:
            for cat in venue["category"]:
                cat_lower = cat.lower()
                if cat_lower in RESTAURANT_CATEGORY_MAPPING:
                    return RESTAURANT_CATEGORY_MAPPING[cat_lower]
        
        # Si cuisine_type est disponible
        if "cuisine_type" in venue and venue["cuisine_type"]:
            cuisine_type = venue["cuisine_type"].lower()
            if cuisine_type in RESTAURANT_CATEGORY_MAPPING:
                return RESTAURANT_CATEGORY_MAPPING[cuisine_type]
        
        return "restaurant"  # Par défaut, c'est un restaurant

def generate_rating_data(venue_category, is_positive=True):
    """
    Génère des données d'évaluation (aspects, émotions, emojis) en fonction de la catégorie
    
    Parameters:
    venue_category (str): Catégorie d'évaluation
    is_positive (bool): Si l'avis est positif ou négatif
    
    Returns:
    dict: Données d'évaluation
    """
    # Obtenir la catégorie ou utiliser "default" si non trouvée
    category_data = VENUE_CATEGORIES.get(venue_category, VENUE_CATEGORIES["default"])
    
    # Nombre d'aspects à évaluer (entre 2 et 4)
    num_aspects = random.randint(2, min(4, len(category_data["aspects"])))
    
    # Sélection aléatoire des aspects
    aspects = random.sample(category_data["aspects"], num_aspects)
    
    # Générer des scores
    aspect_scores = {}
    for aspect in aspects:
        if is_positive:
            # Scores positifs : 4 ou 5 sur 5
            score = random.randint(4, 5)
        else:
            # Scores négatifs : 1 à 3 sur 5
            score = random.randint(1, 3)
        aspect_scores[aspect] = score
    
    # Calcul du score global (moyenne arrondie à 1 décimale)
    overall_score = round(sum(aspect_scores.values()) / len(aspect_scores), 1)
    
    # Sélection des émotions (1 à 2)
    emotions_key = "emotions_positive" if is_positive else "emotions_negative"
    emotions = random.sample(
        category_data[emotions_key], 
        min(2, len(category_data[emotions_key]))
    )
    
    # Sélection des emojis (2 à 4)
    emojis_key = "emojis_positive" if is_positive else "emojis_negative"
    num_emojis = random.randint(2, min(4, len(category_data[emojis_key])))
    emojis = random.sample(category_data[emojis_key], num_emojis)
    
    return {
        "aspect_scores": aspect_scores,
        "overall_score": overall_score,
        "emotions": emotions,
        "emojis": emojis
    }

def get_aspects(category, venue_type="restaurant"):
    """
    Récupère les aspects pour une catégorie donnée
    
    Parameters:
    category (str): Catégorie (restaurant, spa, etc.)
    venue_type (str): Type de lieu (restaurant, event, beauty)
    
    Returns:
    list: Liste des aspects
    """
    if venue_type == "beauty":
        # Vérifier si la catégorie existe
        if category in VENUE_CATEGORIES:
            return VENUE_CATEGORIES[category]["aspects"]
        else:
            return VENUE_CATEGORIES["default"]["aspects"]
    else:
        # Recherche dans les catégories générales
        if category in VENUE_CATEGORIES:
            return VENUE_CATEGORIES[category]["aspects"]
        else:
            return VENUE_CATEGORIES["default"]["aspects"]

def get_positive_emotions(category, venue_type="restaurant"):
    """
    Récupère les émotions positives pour une catégorie donnée
    
    Parameters:
    category (str): Catégorie (restaurant, spa, etc.)
    venue_type (str): Type de lieu (restaurant, event, beauty)
    
    Returns:
    list: Liste des émotions positives
    """
    if venue_type == "beauty":
        # Vérifier si la catégorie existe
        if category in VENUE_CATEGORIES:
            return VENUE_CATEGORIES[category]["emotions_positive"]
        else:
            return VENUE_CATEGORIES["default"]["emotions_positive"]
    else:
        # Recherche dans les catégories générales
        if category in VENUE_CATEGORIES:
            return VENUE_CATEGORIES[category]["emotions_positive"]
        else:
            return VENUE_CATEGORIES["default"]["emotions_positive"]

def get_negative_emotions(category, venue_type="restaurant"):
    """
    Récupère les émotions négatives pour une catégorie donnée
    
    Parameters:
    category (str): Catégorie (restaurant, spa, etc.)
    venue_type (str): Type de lieu (restaurant, event, beauty)
    
    Returns:
    list: Liste des émotions négatives
    """
    if venue_type == "beauty":
        # Vérifier si la catégorie existe
        if category in VENUE_CATEGORIES:
            return VENUE_CATEGORIES[category]["emotions_negative"]
        else:
            return VENUE_CATEGORIES["default"]["emotions_negative"]
    else:
        # Recherche dans les catégories générales
        if category in VENUE_CATEGORIES:
            return VENUE_CATEGORIES[category]["emotions_negative"]
        else:
            return VENUE_CATEGORIES["default"]["emotions_negative"]

def get_emoji(category, venue_type="restaurant"):
    """
    Récupère un emoji aléatoire pour une catégorie donnée
    
    Parameters:
    category (str): Catégorie (restaurant, spa, etc.)
    venue_type (str): Type de lieu (restaurant, event, beauty)
    is_positive (bool): Si c'est un avis positif ou négatif
    
    Returns:
    str: Un emoji aléatoire
    """
    if venue_type == "beauty":
        # Pour les lieux de beauté
        if category in VENUE_CATEGORIES:
            emojis = VENUE_CATEGORIES[category]["emojis_positive"]
            return random.choice(emojis) if emojis else "✨"
        else:
            return random.choice(VENUE_CATEGORIES["default"]["emojis_positive"])
    else:
        # Pour les restaurants et événements
        if category in VENUE_CATEGORIES:
            emojis = VENUE_CATEGORIES[category]["emojis_positive"]
            return random.choice(emojis) if emojis else "👍"
        else:
            return random.choice(VENUE_CATEGORIES["default"]["emojis_positive"])

def get_reviews_for_venue(venue, is_event=False, is_beauty=False):
    """
    Extrait jusqu'à 3 reviews pertinentes selon le type de lieu.
    """
    reviews = []
    if is_beauty and "comments" in venue:
        reviews = [c.get("text", "") for c in venue["comments"] if c.get("text")]
    elif is_event and "commentaires" in venue:
        reviews = [c.get("contenu", "") for c in venue["commentaires"] if c.get("contenu")]
    # Restauration : pas de reviews
    if reviews:
        return random.sample(reviews, min(3, len(reviews)))
    return []

def generate_post_prompt(user, venue, visit_date, rating_data, is_event=False, is_beauty=False):
    """
    Génère un prompt pour OpenAI GPT-3.5-turbo pour créer un post utilisateur réaliste.
    """
    # Récupération des noms des lieux/événements
    if is_event:
        venue_name = venue.get('intitulé', venue.get('titre', venue.get('name', 'Événement')))
        venue_type = venue.get('catégorie', venue.get('category', 'événement'))
        venue_detail = venue.get('détail', venue.get('description', ''))
        venue_location = venue.get('lieu', '')
    elif is_beauty:
        venue_name = venue.get('name', 'Institut de Beauté')
        venue_type = venue.get('sous_categorie', venue.get('category', 'lieu de beauté'))
        venue_detail = venue.get('description', '')
        venue_location = venue.get('address', '')
    else:
        venue_name = venue.get('name', 'Restaurant')
        venue_type = ', '.join(venue.get('category', [])) if isinstance(venue.get('category', []), list) else venue.get('category', 'restaurant')
        venue_detail = venue.get('description', '')
        venue_location = venue.get('address', '')
    
    # Formater les scores pour le prompt
    aspect_scores_text = ""
    for aspect, score in rating_data["aspect_scores"].items():
        aspect_scores_text += f"- {aspect}: {score}/5\n"
    
    # Formater les émotions pour le prompt
    emotions_text = ", ".join(rating_data["emotions"])
    
    # Formater la date de visite
    visit_date_str = visit_date.strftime("%d/%m/%Y")
    
    # Déterminer le type de contenu
    if is_event:
        prompt_type = "un événement"
        venue_desc = f"l'événement {venue_type}"
    elif is_beauty:
        prompt_type = "un lieu de beauté et bien-être"
        venue_desc = f"{venue_type}"
    else:
        prompt_type = "un restaurant"
        venue_desc = f"le restaurant {venue_type}"
    
    # Créer le prompt
    prompt = f"""
    Rédige un post authentique sur {prompt_type} pour un utilisateur de l'application mobile "Choice".

    Informations sur l'utilisateur:
    - Nom: {user.get('name', 'Utilisateur')}
    - Genre: {user.get('gender', 'Non spécifié')}
    
    Informations sur {prompt_type}:
    - Nom: {venue_name}
    - Type: {venue_type}
    - Lieu: {venue_location}
    - Description: {venue_detail[:100] + '...' if len(venue_detail) > 100 else venue_detail}
    
    Évaluation de l'utilisateur:
    {aspect_scores_text}
    - Note globale: {rating_data["overall_score"]}/5
    - Ressenti: {emotions_text}
    
    Date de la visite: {visit_date_str}
    
    Directives pour le post:
    1. Écris à la première personne (je)
    2. Fais référence à des éléments spécifiques de {venue_desc}
    3. Mentionne au moins deux aspects évalués ci-dessus
    4. Exprime clairement ton ressenti et ta note globale
    5. Inclus quelques-uns de ces émojis pertinents: {' '.join(rating_data["emojis"])}
    6. Termine par une recommandation ou non, selon ton évaluation
    7. Longueur: environ 250-400 caractères (concis mais détaillé)
    """
    
    # Ajouter des instructions spécifiques selon le type
    if is_beauty:
        prompt += """
    Vocabulaire spécifique à inclure dans le post sur le lieu de beauté:
    - Parle de "soins", "relaxation", "détente", "bien-être"
    - Utilise des termes comme "ressourcer", "apaiser", "revitaliser"
    - Mentionne l'atmosphère, l'ambiance ou le cadre
    - Si positif, évoque la sensation après les soins
    - Si négatif, évoque précisément ce qui n'a pas fonctionné
    """
    
    prompt += """
    Le post doit être authentique, comme si l'utilisateur l'écrivait réellement sur l'application.
    """
    
    # Ajouter les reviews si disponibles
    reviews = get_reviews_for_venue(venue, is_event, is_beauty)
    if reviews:
        prompt += "\nAvis clients :\n" + "\n".join(f"- {r}" for r in reviews)
    
    return prompt

# Ajout d'une liste de vidéos de stock pour les posts
STOCK_VIDEO_URLS = [
    "https://samplelib.com/mp4/sample-720p.mp4",
    "https://www.w3schools.com/html/mov_bbb.mp4",
    "https://filesamples.com/samples/video/mp4/sample_640x360.mp4"
]

def create_media_from_venue(venue, is_event=False, is_beauty=False, with_video=False):
    """
    Extrait les médias (images/vidéos) à partir d'un document de lieu ou événement
    """
    media = []
    # Images (logique existante)
    if is_event:
        if "image" in venue and venue["image"]:
            media.append({"type": "image", "url": venue["image"], "width": 800, "height": 600})
        if "lineup" in venue and isinstance(venue["lineup"], list):
            for artist in venue["lineup"]:
                if isinstance(artist, dict) and "image" in artist and artist["image"]:
                    media.append({"type": "image", "url": artist["image"], "width": 400, "height": 400})
                    if len(media) >= 3:
                        break
    else:
        if "photos" in venue and isinstance(venue["photos"], list) and venue["photos"]:
            for i, photo in enumerate(venue["photos"]):
                if i >= 3:
                    break
                media.append({"type": "image", "url": photo, "width": 800, "height": 600})
        elif "photo" in venue and venue["photo"]:
            media.append({"type": "image", "url": venue["photo"], "width": 800, "height": 600})
        elif "main_image" in venue and venue["main_image"]:
            media.append({"type": "image", "url": venue["main_image"], "width": 800, "height": 600})
    # Vidéo (optionnelle)
    if with_video:
        # 30% de chance d'ajouter une vidéo, ou systématique si forçage
        if random.random() < 0.3 or with_video is True:
            video_url = random.choice(STOCK_VIDEO_URLS)
            media.append({
                "type": "video",
                "url": video_url,
                "width": 1280,
                "height": 720,
                "duration": 30
            })
    return media

def create_post_for_venue(db_connections, user, venue, is_event=False, is_beauty=False, openai_client=None, config=None, with_video=False):
    """
    Crée un post utilisateur pour un lieu, événement ou lieu de beauté.
    Utilise OpenAI pour générer le contenu si un client est fourni.
    """
    if config is None:
        config = DEFAULT_CONFIG.copy()
    
    try:
        # Vérifier si l'utilisateur a visité le lieu
        venue_id = str(venue["_id"])
        has_visited, visit_date = has_visited_venue(user, venue_id, config)
        
        if config["verify_location"] and not has_visited:
            logger.info(f"⚠️ L'utilisateur {user['name']} n'a pas visité {venue.get('name', venue.get('intitulé', 'Lieu'))} récemment")
            return None
        
        # Si pas de date de visite (si la vérification est désactivée), utiliser une date récente
        if not visit_date:
            days_ago = random.randint(1, config["max_post_age_days"])
            visit_date = datetime.now() - timedelta(days=days_ago)
        
        # Déterminer la catégorie du lieu
        venue_category = determine_venue_category(venue, is_event, is_beauty)
        
        # Déterminer si l'avis sera positif ou négatif
        # Pour les lieux de beauté, augmenter la probabilité d'avis positifs
        good_review_probability = config["good_review_probability"]
        if is_beauty:
            good_review_probability = 0.85  # 85% de chance d'avis positifs pour les lieux de beauté
        
        is_positive = random.random() < good_review_probability
        
        # Générer les données d'évaluation
        rating_data = generate_rating_data(venue_category, is_positive)
        
        # Générer le prompt pour OpenAI GPT-3.5-turbo
        prompt = generate_post_prompt(user, venue, visit_date, rating_data, is_event, is_beauty)
        
        # Générer le contenu du post
        content = None
        if config.get("ai_enabled", True) and openai_client:
            content = openai_client.generate_text_with_retry(prompt)
        
        # Si OpenAI échoue ou est désactivé, générer un contenu de secours
        if not content:
            logger.warning(f"⚠️ Génération AI (OpenAI) échouée pour {venue_name}, utilisation d'un contenu de secours")
            
            # Extraire le nom du lieu
            venue_name = venue.get('name', venue.get('intitulé', 'Lieu'))
            
            # Générer un début positif ou négatif
            if is_positive:
                if is_beauty:
                    starts = [
                        f"{random.choice(rating_data['emojis'])} Moment de détente chez {venue_name}!",
                        f"{random.choice(rating_data['emojis'])} Expérience bien-être chez {venue_name}!",
                        f"{random.choice(rating_data['emojis'])} Ressourcée après ma visite à {venue_name}."
                    ]
                else:
                    starts = [
                        f"{random.choice(rating_data['emojis'])} Super expérience à {venue_name}!",
                        f"{random.choice(rating_data['emojis'])} Je recommande vivement {venue_name}!",
                        f"{random.choice(rating_data['emojis'])} Excellente découverte: {venue_name}."
                    ]
            else:
                if is_beauty:
                    starts = [
                        f"{random.choice(rating_data['emojis'])} Déçu(e) par {venue_name}...",
                        f"{random.choice(rating_data['emojis'])} Expérience mitigée chez {venue_name}.",
                        f"{random.choice(rating_data['emojis'])} Pas convaincu(e) par {venue_name}."
                    ]
                else:
                    starts = [
                        f"{random.choice(rating_data['emojis'])} Déçu(e) par {venue_name}...",
                        f"{random.choice(rating_data['emojis'])} Expérience mitigée à {venue_name}.",
                        f"{random.choice(rating_data['emojis'])} Pas convaincu(e) par {venue_name}."
                    ]
            
            # Générer un corps incluant un aspect
            aspect, score = random.choice(list(rating_data["aspect_scores"].items()))
            if is_positive:
                if is_beauty:
                    bodies = [
                        f"J'ai particulièrement apprécié {aspect}, tellement relaxant.",
                        f"Le {aspect} était vraiment professionnel et efficace.",
                        f"Mention spéciale pour {aspect}, un vrai moment de bien-être."
                    ]
                else:
                    bodies = [
                        f"J'ai particulièrement apprécié {aspect}.",
                        f"Le {aspect} était vraiment top!",
                        f"Mention spéciale pour {aspect}."
                    ]
            else:
                bodies = [
                    f"Le {aspect} laisse à désirer.",
                    f"Déception sur {aspect}.",
                    f"Le {aspect} n'était pas à la hauteur."
                ]
            
            # Générer une conclusion
            if is_positive:
                if is_beauty:
                    ends = [
                        f"Je reviendrai pour un autre soin! {random.choice(rating_data['emojis'])}",
                        f"À essayer pour un moment de détente. {random.choice(rating_data['emojis'])}",
                        f"Un vrai havre de bien-être! {random.choice(rating_data['emojis'])}"
                    ]
                else:
                    ends = [
                        f"Je reviendrai! {random.choice(rating_data['emojis'])}",
                        f"À essayer absolument. {random.choice(rating_data['emojis'])}",
                        f"Une valeur sûre! {random.choice(rating_data['emojis'])}"
                    ]
            else:
                ends = [
                    f"Je ne reviendrai pas. {random.choice(rating_data['emojis'])}",
                    f"À éviter selon moi. {random.choice(rating_data['emojis'])}",
                    f"Il y a mieux ailleurs. {random.choice(rating_data['emojis'])}"
                ]
            
            # Assembler le contenu
            content = f"{random.choice(starts)} {random.choice(bodies)} {random.choice(ends)}"
        
        # Extraire les médias (images/vidéos)
        media = create_media_from_venue(venue, is_event, is_beauty, with_video=with_video)
        
        # Créer l'objet post
        user_id = str(user["_id"])
        target_id = venue_id
        
        # Déterminer le type de cible
        if is_event:
            target_type = "event"
        elif is_beauty:
            target_type = "beauty"
        else:
            target_type = "producer"
        
        # Déterminer si l'utilisateur a fait un choice
        # Les utilisateurs qui ont des avis positifs ont plus de chances de faire un choice
        made_choice = is_positive and random.random() < 0.8
        
        # Préparer la structure du post
        post = {
            "user_id": user_id,
            "target_id": target_id,
            "target_type": target_type,
            "content": content,
            "media": media,
            "posted_at": datetime.now().isoformat(),
            "location": {
                "name": venue.get('name', venue.get('intitulé', venue.get('lieu', 'Lieu'))),
                "coordinates": [],
                "address": venue.get('address', venue.get('adresse', ''))
            },
            "likes": [],
            "likes_count": 0,
            "comments": [],
            "comments_count": 0,
            "author": {
                "id": user_id,
                "name": user["name"],
                "avatar": user.get("photo_url", "")
            },
            "rating": rating_data["overall_score"],
            "post_type": target_type
        }
        
        # Ajouter les métadonnées spécifiques au type de post
        if is_beauty:
            post["isBeautyPlace"] = True
            post["beauty_id"] = target_id
            post["beauty_name"] = venue.get('name', 'Lieu de beauté')
            post["beauty_category"] = venue.get('category', '')
            post["beauty_subcategory"] = venue.get('sous_categorie', '')
        elif is_event:
            post["isEvent"] = True
            post["event_id"] = target_id
            post["event_title"] = venue.get('intitulé', venue.get('titre', venue.get('name', 'Événement')))
        else:
            post["isRestaurant"] = True
            post["restaurant_id"] = target_id
            post["restaurant_name"] = venue.get('name', 'Restaurant')
        
        # Ajouter les coordonnées selon leur format
        if is_event and "location" in venue and "coordinates" in venue["location"]:
            post["location"]["coordinates"] = venue["location"]["coordinates"]
        elif "gps_coordinates" in venue:
            if "coordinates" in venue["gps_coordinates"]:
                post["location"]["coordinates"] = venue["gps_coordinates"]["coordinates"]
            elif "lat" in venue["gps_coordinates"] and "lng" in venue["gps_coordinates"]:
                post["location"]["coordinates"] = [venue["gps_coordinates"]["lng"], venue["gps_coordinates"]["lat"]]
        
        # Insérer le post dans la base de données
        post_id = db_connections["choice"][COLL_POSTS].insert_one(post).inserted_id
        
        logger.info(f"✅ Post créé avec succès pour {user['name']} sur {post['location']['name']} (ID: {post_id})")
        
        # Mettre à jour la liste des posts de l'utilisateur
        try:
            db_connections["choice"][COLL_USERS].update_one(
                {"_id": ObjectId(user_id)},
                {"$push": {"posts": str(post_id)}}
            )
        except Exception as e:
            logger.warning(f"⚠️ Erreur lors de la mise à jour de l'utilisateur: {e}")
        
        # Si l'utilisateur a fait un choice, mettre à jour les documents correspondants
        if made_choice:
            try:
                if is_event:
                    # Ajouter le choice à l'événement
                    db_connections["loisir"][COLL_EVENTS_LOISIR].update_one(
                        {"_id": ObjectId(target_id)},
                        {"$push": {"choices": user_id}}
                    )
                elif is_beauty:
                    # Ajouter le choice au lieu de beauté
                    db_connections["beauty"][COLL_BEAUTY_PLACES].update_one(
                        {"_id": ObjectId(target_id)},
                        {"$inc": {"choice_count": 1}}
                    )
                else:
                    # Ajouter le choice au restaurant
                    db_connections["restauration"][COLL_PRODUCERS_RESTAURATION].update_one(
                        {"_id": ObjectId(target_id)},
                        {"$inc": {"choice": 1}}
                    )
                
                # Ajouter également le choice à l'utilisateur
                db_connections["choice"][COLL_USERS].update_one(
                    {"_id": ObjectId(user_id)},
                    {"$push": {"choices": target_id}}
                )
                
                logger.info(f"✅ Choice ajouté pour {user['name']} sur {post['location']['name']}")
            except Exception as e:
                logger.warning(f"⚠️ Erreur lors de l'ajout du choice: {e}")
        
        return {
            "post_id": str(post_id),
            "user_name": user["name"],
            "venue_name": post["location"]["name"],
            "is_choice": made_choice,
            "post_type": target_type
        }
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de la création du post: {e}")
        return None

def generate_user_posts(db_connections, count, openai_client=None, config=None):
    """Génère des posts pour les utilisateurs en fonction de leurs localisations fréquentes"""
    if config is None:
        config = DEFAULT_CONFIG.copy()
    
    try:
        # Récupérer les utilisateurs qui ont des localisations fréquentes
        user_query = {
            "frequent_locations": {"$exists": True, "$ne": []}
        }
        
        # Limiter le nombre d'utilisateurs si spécifié
        user_limit = config.get("max_users")
        
        users = list(db_connections["choice"][COLL_USERS].find(user_query).limit(user_limit if user_limit else 0))
        
        if not users:
            logger.warning("❌ Aucun utilisateur avec des localisations fréquentes trouvé")
            return []
        
        logger.info(f"🔍 {len(users)} utilisateurs avec des localisations fréquentes trouvés")
        
        # Vérifier que le nombre de posts demandé est réalisable
        posts_count = min(count, len(users))
        if posts_count < count:
            logger.warning(f"⚠️ Nombre de posts demandé ({count}) supérieur au nombre d'utilisateurs disponibles ({len(users)})")
            logger.warning(f"⚠️ Génération limitée à {posts_count} posts")
        
        # Sélectionner aléatoirement des utilisateurs
        selected_users = random.sample(users, posts_count)
        
        # Liste pour stocker les posts créés
        generated_posts = []
        
        # Pour chaque utilisateur, créer un post
        for user in selected_users:
            logger.info(f"🔄 Génération de post pour {user['name']}")
            
            # Décider si on crée un post pour un restaurant, un événement ou un lieu de beauté
            # Vérifier si l'utilisateur a un intérêt pour le bien-être
            has_wellness_interest = any(tag in ["spa", "massage", "soins", "beauté", "bien-être"] for tag in user.get("liked_tags", []))
            
            # Augmenter les chances de posts wellness si l'utilisateur s'y intéresse
            if has_wellness_interest:
                post_type_weights = {"event": 0.3, "beauty": 0.4, "restaurant": 0.3}
            else:
                post_type_weights = {"event": 0.4, "beauty": 0.2, "restaurant": 0.4}
            
            post_type = random.choices(
                list(post_type_weights.keys()),
                weights=list(post_type_weights.values()),
                k=1
            )[0]
            
            # Selon le type de post choisi
            if post_type == "event":
                # Récupérer les lieux fréquents de type loisir
                frequent_locations = user.get("frequent_locations", [])
                leisure_locations = [loc for loc in frequent_locations if loc.get("type") == "leisure"]
                
                if not leisure_locations:
                    logger.info(f"⚠️ Aucun lieu de loisir fréquenté par {user['name']}, essai de fallback")
                    # Fallback sur un autre type de post
                    if has_wellness_interest:
                        post_type = "beauty"
                    else:
                        post_type = "restaurant"
                else:
                    # Sélectionner une localisation aléatoire
                    location = random.choice(leisure_locations)
                    location_id = location.get("id")
                    
                    # Récupérer l'événement correspondant
                    event = db_connections["loisir"][COLL_EVENTS_LOISIR].find_one({"_id": ObjectId(location_id)})
                    
                    if event:
                        # Créer le post
                        post_result = create_post_for_venue(
                            db_connections, user, event, is_event=True, is_beauty=False, openai_client=openai_client, config=config
                        )
                        
                        if post_result:
                            generated_posts.append(post_result)
                            continue
                    
                    # Si on arrive ici, c'est qu'on n'a pas pu créer de post d'événement
                    # On va essayer avec un autre type de post
                    if has_wellness_interest:
                        post_type = "beauty"
                    else:
                        post_type = "restaurant"
            
            # Si post_type est beauty à ce stade (soit directement, soit par fallback)
            if post_type == "beauty":
                # Récupérer les lieux fréquents de type beauté
                frequent_locations = user.get("frequent_locations", [])
                beauty_locations = [loc for loc in frequent_locations if loc.get("type") == "beauty"]
                
                if not beauty_locations:
                    logger.info(f"⚠️ Aucun lieu de beauté fréquenté par {user['name']}, fallback sur restaurant")
                    post_type = "restaurant"
                else:
                    # Sélectionner une localisation aléatoire
                    location = random.choice(beauty_locations)
                    location_id = location.get("id")
                    
                    # Récupérer le lieu de beauté correspondant
                    beauty_place = db_connections["beauty"][COLL_BEAUTY_PLACES].find_one({"_id": ObjectId(location_id)})
                    
                    if beauty_place:
                        # Créer le post
                        post_result = create_post_for_venue(
                            db_connections, user, beauty_place, is_event=False, is_beauty=True, openai_client=openai_client, config=config
                        )
                        
                        if post_result:
                            generated_posts.append(post_result)
                            continue
                    
                    # Si on arrive ici, fallback sur restaurant
                    post_type = "restaurant"
            
            # Si post_type est restaurant à ce stade (soit directement, soit par fallback)
            if post_type == "restaurant":
                # Récupérer les lieux fréquents de type restaurant
                frequent_locations = user.get("frequent_locations", [])
                restaurant_locations = [loc for loc in frequent_locations if loc.get("type") == "restaurant"]
                
                if not restaurant_locations:
                    logger.info(f"⚠️ Aucun restaurant fréquenté par {user['name']}, impossible de créer un post")
                    continue
                
                # Sélectionner une localisation aléatoire
                location = random.choice(restaurant_locations)
                location_id = location.get("id")
                
                # Récupérer le restaurant correspondant
                restaurant = db_connections["restauration"][COLL_PRODUCERS_RESTAURATION].find_one({"_id": ObjectId(location_id)})
                
                if restaurant:
                    # Créer le post
                    post_result = create_post_for_venue(
                        db_connections, user, restaurant, is_event=False, is_beauty=False, openai_client=openai_client, config=config
                    )
                    
                    if post_result:
                        generated_posts.append(post_result)
        
        return generated_posts
    
    except Exception as e:
        logger.error(f"❌ Erreur lors de la génération des posts: {e}")
        return []

# --- Fonctions générales pour l'exécution ---

def is_within_active_hours():
    """Vérifie si l'heure actuelle est dans la plage des heures actives."""
    now = datetime.now()
    start_hour = 3  # 3h du matin
    end_hour = 7    # 7h du matin
    
    return start_hour <= now.hour < end_hour

def generate_event_posts(db_connections, count, openai_client=None):
    """Génère des posts avec référence à des événements à venir"""
    logger.info(f"🗓️ Génération de {count} posts avec référence à des événements à venir...")
    
    try:
        # Vérifier que les collections existent
        if COLL_EVENTS_LOISIR not in db_connections["loisir"].list_collection_names():
            logger.warning(f"❌ Collection {COLL_EVENTS_LOISIR} non trouvée dans la base de données loisir")
            if BYPASS_FILTERS:
                logger.info("⚠️ Mode bypass activé, création d'événements fictifs pour test")
                # Créer des événements fictifs pour les tests
                mock_events = [
                    {
                        "_id": ObjectId(),
                        "intitulé": "Concert de Jazz",
                        "détail": "Une soirée jazz exceptionnelle avec les meilleurs artistes",
                        "catégorie": "concert",
                        "prochaines_dates": "ven 20 mars",
                        "lieu": "Le Blue Note",
                        "location": {"adresse": "15 rue de la Musique, Paris"}
                    },
                    {
                        "_id": ObjectId(),
                        "intitulé": "Festival de Théâtre",
                        "détail": "Découvrez les nouvelles pièces de théâtre contemporain",
                        "catégorie": "théâtre",
                        "prochaines_dates": "sam 15 avr.",
                        "lieu": "Théâtre du Marais",
                        "location": {"adresse": "25 rue du Temple, Paris"}
                    }
                ]
                logger.info(f"✅ {len(mock_events)} événements fictifs créés pour test")
                
                # Récupérer ou créer des lieux fictifs
                leisure_venues = []
                try:
                    if COLL_VENUES_LOISIR in db_connections["loisir"].list_collection_names():
                        leisure_venues = list(db_connections["loisir"][COLL_VENUES_LOISIR].find().limit(3))
                except Exception as e:
                    logger.warning(f"⚠️ Erreur lors de la récupération des lieux: {e}")
                
                if not leisure_venues:
                    leisure_venues = [
                        {
                            "_id": ObjectId(),
                            "name": "Le Club de Jazz",
                            "photo": "/images/default_venue_avatar.png"
                        },
                        {
                            "_id": ObjectId(),
                            "name": "Théâtre Municipal",
                            "photo": "/images/default_venue_avatar.png"
                        }
                    ]
                
                # Générer des posts avec les données fictives
                posts_created = []
                for _ in range(min(count, len(mock_events))):
                    # Sélectionner un événement et un lieu aléatoirement
                    event = random.choice(mock_events)
                    venue = random.choice(leisure_venues)
                    
                    logger.info(f"🤖 Génération de post pour test avec événement fictif: {event['intitulé']}")
                    
                    # Créer un post fictif 
                    post = {
                        "post_id": f"mock_{datetime.now().timestamp()}",
                        "event_title": event["intitulé"],
                        "venue_name": venue.get("name", "Lieu Culturel Test")
                    }
                    posts_created.append(post)
                
                return posts_created
            else:
                return []
        
        events_collection = db_connections["loisir"][COLL_EVENTS_LOISIR]
        
        # Récupérer les événements avec debug info
        logger.info(f"🔍 Recherche d'événements dans la collection {COLL_EVENTS_LOISIR}...")
        
        try:
            all_events = []
            cursor = events_collection.find({})
            for event in cursor:
                all_events.append(event)
                # Afficher quelques détails du premier événement
                if len(all_events) == 1:
                    logger.info(f"📝 Exemple d'événement trouvé: {event.get('_id')}")
                    # Afficher les champs clés disponibles
                    for field in ["intitulé", "titre", "name", "détail", "prochaines_dates", "date_fin"]:
                        if field in event:
                            logger.info(f"  • {field}: {event.get(field, '(vide)')[:50]}...")
            
            logger.info(f"🔍 {len(all_events)} événements récupérés pour analyse")
        except Exception as e:
            logger.error(f"❌ Erreur lors de la récupération des événements: {str(e)}")
            if BYPASS_FILTERS:
                logger.info("⚠️ Mode bypass activé, création d'événements fictifs après erreur")
                # Créer des événements fictifs en cas d'erreur
                all_events = [
                    {
                        "_id": ObjectId(),
                        "intitulé": "Concert de Jazz (fictif)",
                        "détail": "événement fictif pour test suite à une erreur",
                        "catégorie": "concert",
                        "prochaines_dates": "ven 20 mars"
                    }
                ]
            else:
                return []
            
        if not all_events:
            logger.warning("❌ Aucun événement trouvé dans la base de données")
            if BYPASS_FILTERS:
                logger.info("⚠️ Mode bypass activé, création d'événements fictifs car aucun trouvé")
                # Créer des événements fictifs pour les tests
                all_events = [
                    {
                        "_id": ObjectId(),
                        "intitulé": "Exposition d'Art (fictif)",
                        "détail": "événement fictif pour test car aucun trouvé",
                        "catégorie": "exposition",
                        "prochaines_dates": "mar 10 mai"
                    }
                ]
            else:
                return []
        
        # Si BYPASS_FILTERS est activé, utiliser tous les événements
        if BYPASS_FILTERS:
            logger.info("⚠️ Mode bypass activé, utilisation de tous les événements sans filtrage")
            # Prenons un échantillon aléatoire pour la diversité
            if len(all_events) > count * 3:
                valid_events = random.sample(all_events, count * 3)
            else:
                valid_events = all_events
            logger.info(f"✅ {len(valid_events)} événements sélectionnés en mode bypass")
        else:
            # Filtrer les événements qui ne sont pas terminés
            valid_events = [event for event in all_events if not is_event_ended(event)]
            logger.info(f"✅ {len(valid_events)} événements valides après filtrage par date")
            
            # Si aucun événement valide, utiliser les plus récents
            if not valid_events and all_events:
                logger.warning("⚠️ Aucun événement valide après filtrage - utilisation des événements récents")
                # Trier par date d'ajout si disponible (chercher parmi différents noms de champs possibles)
                date_fields = ["dateAjout", "created_at", "date_creation", "date_ajout"]
                sort_field = None
                
                for field in date_fields:
                    if any(field in event for event in all_events):
                        sort_field = field
                        break
                
                if sort_field:
                    all_events.sort(key=lambda x: x.get(sort_field, ""), reverse=True)
                
                valid_events = all_events[:min(10, len(all_events))]
                logger.info(f"✅ Utilisation de {len(valid_events)} événements récents")
        
        # Récupérer des lieux de loisir
        leisure_venues = list(db_connections["loisir"][COLL_VENUES_LOISIR].find({}))
        
        if not leisure_venues:
            logger.warning("❌ Aucun lieu de loisir trouvé, génération de posts impossible.")
            return []
        
        # Générer des posts
        posts_created = []
        for _ in range(min(count, len(valid_events))):
            # Sélectionner un événement et un lieu aléatoirement
            event = random.choice(valid_events)
            venue = random.choice(leisure_venues)
            
            logger.info(f"🤖 Génération de post pour un leisure avec événement référencé...")
            
            # Créer le post
            result = create_post_for_event(db_connections, event, venue, openai_client)
            if result:
                posts_created.append(result)
                # Afficher le nom de l'événement avec différents champs possibles
                event_title = None
                for field in ["intitulé", "titre", "name", "title"]:
                    if field in event and event[field]:
                        event_title = event[field]
                        break
                logger.info(f"✅ Post généré pour l'événement: {event_title or 'Sans titre'} via {venue.get('name', 'Lieu inconnu')}")
        
        return posts_created
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de la génération des posts pour événements: {e}")
        return []

def generate_restaurant_posts(db_connections, count, openai_client=None):
    """Génère des posts pour des restaurants populaires"""
    logger.info(f"🍽️ Génération de {count} posts pour des restaurants populaires...")
    
    try:
        # Vérifier que les collections existent
        if COLL_PRODUCERS_RESTAURATION not in db_connections["restauration"].list_collection_names():
            logger.warning(f"❌ Collection {COLL_PRODUCERS_RESTAURATION} non trouvée dans la base restauration")
            if BYPASS_FILTERS:
                logger.info("⚠️ Mode bypass activé, création de restaurants fictifs pour test")
                # Créer des restaurants fictifs pour les tests
                mock_restaurants = [
                    {
                        "_id": ObjectId(),
                        "name": "Le Bistrot Parisien",
                        "cuisine_type": "Française",
                        "photo": "/images/default_restaurant_avatar.png"
                    },
                    {
                        "_id": ObjectId(),
                        "name": "Sushi Sakura",
                        "cuisine_type": "Japonaise",
                        "photo": "/images/default_restaurant_avatar.png"
                    }
                ]
                logger.info(f"✅ {len(mock_restaurants)} restaurants fictifs créés pour test")
                
                # Générer des posts avec les données fictives
                posts_created = []
                for _ in range(min(count, len(mock_restaurants))):
                    restaurant = random.choice(mock_restaurants)
                    
                    logger.info(f"🤖 Génération de post pour test avec restaurant fictif: {restaurant['name']}")
                    
                    # Créer un post fictif
                    post = {
                        "post_id": f"mock_{datetime.now().timestamp()}",
                        "restaurant_name": restaurant["name"]
                    }
                    posts_created.append(post)
                
                return posts_created
            else:
                return []
        
        restaurant_collection = db_connections["restauration"][COLL_PRODUCERS_RESTAURATION]
        
        # Compter le nombre total de restaurants
        try:
            total_restaurants = restaurant_collection.count_documents({})
            logger.info(f"📊 Nombre total de restaurants: {total_restaurants}")
        except Exception as e:
            logger.error(f"❌ Erreur lors du comptage des restaurants: {e}")
            total_restaurants = 0
        
        if total_restaurants == 0:
            logger.warning("❌ Aucun restaurant trouvé dans la collection")
            if BYPASS_FILTERS:
                logger.info("⚠️ Mode bypass activé, création de restaurants fictifs car aucun trouvé")
                # Créer des restaurants fictifs pour les tests
                mock_restaurants = [
                    {
                        "_id": ObjectId(),
                        "name": "Le Bistrot Parisien (fictif)",
                        "cuisine_type": "Française"
                    }
                ]
                # Générer des posts avec les données fictives
                posts_created = []
                for restaurant in mock_restaurants:
                    post = {
                        "post_id": f"mock_{datetime.now().timestamp()}",
                        "restaurant_name": restaurant["name"]
                    }
                    posts_created.append(post)
                return posts_created
            else:
                return []
        
        # Debug: Afficher les premiers restaurants pour analyse
        sample_restaurants = list(restaurant_collection.find().limit(2))
        if sample_restaurants:
            logger.info(f"📝 Exemples de restaurants trouvés:")
            for i, r in enumerate(sample_restaurants):
                logger.info(f"  • Restaurant {i+1}: {r.get('name', '(sans nom)')} (ID: {r.get('_id')})")
                for field in ["active", "photo", "profile_image", "cuisine_type"]:
                    if field in r:
                        logger.info(f"    - {field}: {r.get(field)}")
        
        # Si BYPASS_FILTERS est activé, ne pas utiliser de filtre
        if BYPASS_FILTERS:
            logger.info(f"⚠️ Mode bypass activé pour les restaurants")
            try:
                # Récupérer des restaurants sans filtre - ACCÈS DIRECT SANS AGGREGATION
                # Utilisation de $sample via aggregate pour la sélection aléatoire, même en mode bypass
                restaurants = list(restaurant_collection.aggregate([
                    {"$sample": {"size": max(1, count * 3)}}
                ], allowDiskUse=True))
                logger.info(f"✅ {len(restaurants)} restaurants récupérés sans filtre (méthode directe avec sample)")
            except Exception as e:
                logger.error(f"❌ Erreur lors de la récupération directe des restaurants (avec sample): {str(e)}")
                restaurants = []
        else:
            # Vérifier si le champ active existe dans au moins un document
            try:
                has_active_field = restaurant_collection.find_one({"active": {"$exists": True}})
            except Exception as e:
                logger.error(f"❌ Erreur lors de la vérification du champ 'active': {e}")
                has_active_field = None
            
            # Adapter la requête en fonction de la présence du champ active
            if has_active_field:
                logger.info("📋 Utilisation du filtre 'active: true' pour les restaurants")
                try:
                    restaurants = list(restaurant_collection.aggregate([
                        {"$match": {"active": True}},
                        {"$sample": {"size": max(1, count * 3)}}
                    ], allowDiskUse=True))
                except Exception as e:
                    logger.error(f"❌ Erreur lors de la récupération des restaurants actifs: {e}")
                    restaurants = []
            else:
                logger.warning("⚠️ Le champ 'active' n'existe pas - récupération sans filtre")
                try:
                    restaurants = list(restaurant_collection.aggregate([
                        {"$sample": {"size": max(1, count * 3)}}
                    ], allowDiskUse=True))
                except Exception as e:
                    logger.error(f"❌ Erreur lors de la récupération des restaurants: {e}")
                    restaurants = []
        
        if not restaurants:
            logger.warning("❌ Aucun restaurant trouvé avec les critères actuels")
            # Dernière tentative: récupérer n'importe quel document
            try:
                restaurants = list(restaurant_collection.find().limit(count))
                if restaurants:
                    logger.info(f"✅ {len(restaurants)} restaurants récupérés en dernier recours")
                else:
                    logger.error("❌ Impossible de trouver des restaurants même sans filtre")
                    return []
            except Exception as e:
                logger.error(f"❌ Erreur lors de la dernière tentative de récupération: {e}")
                return []
        
        # Générer des posts
        posts_created = []
        for i in range(min(count, len(restaurants))):
            restaurant = restaurants[i]
            
            logger.info(f"🤖 Génération de post pour un restaurant...")
            
            # Créer le post
            result = create_post_for_restaurant(db_connections, restaurant, openai_client)
            if result:
                posts_created.append(result)
                logger.info(f"✅ Post généré pour le restaurant: {restaurant.get('name', 'Sans nom')}")
        
        return posts_created
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de la génération des posts pour restaurants: {e}")
        return []

def generate_beauty_posts(db_connections, count, openai_client=None):
    """Génère des posts pour des lieux de beauté et bien-être"""
    logger.info(f"✨ Génération de {count} posts pour des lieux de beauté et bien-être...")
    
    try:
        # Vérifier que les collections existent
        if COLL_BEAUTY_PLACES not in db_connections["beauty"].list_collection_names():
            logger.warning(f"❌ Collection {COLL_BEAUTY_PLACES} non trouvée dans la base Beauty_Wellness")
            if BYPASS_FILTERS:
                logger.info("⚠️ Mode bypass activé, création de lieux de beauté fictifs pour test")
                # Créer des lieux fictifs pour les tests
                mock_places = [
                    {
                        "_id": ObjectId(),
                        "name": "Spa Zen",
                        "category": "Soins esthétiques et bien-être",
                        "sous_categorie": "Spa",
                        "profile_photo": "/images/default_beauty_avatar.png"
                    },
                    {
                        "_id": ObjectId(),
                        "name": "Institut Beauté Parfaite",
                        "category": "Soins esthétiques et bien-être",
                        "sous_categorie": "Institut de beauté",
                        "profile_photo": "/images/default_beauty_avatar.png"
                    }
                ]
                logger.info(f"✅ {len(mock_places)} lieux de beauté fictifs créés pour test")
                
                # Générer des posts avec les données fictives
                posts_created = []
                for _ in range(min(count, len(mock_places))):
                    place = random.choice(mock_places)
                    
                    logger.info(f"🤖 Génération de post pour test avec lieu de beauté fictif: {place['name']}")
                    
                    # Créer un post fictif
                    post = {
                        "post_id": f"mock_{datetime.now().timestamp()}",
                        "beauty_name": place["name"]
                    }
                    posts_created.append(post)
                
                return posts_created
            else:
                return []
        
        beauty_collection = db_connections["beauty"][COLL_BEAUTY_PLACES]
        
        # Compter le nombre total de lieux de beauté
        try:
            total_places = beauty_collection.count_documents({})
            logger.info(f"📊 Nombre total de lieux de beauté: {total_places}")
        except Exception as e:
            logger.error(f"❌ Erreur lors du comptage des lieux de beauté: {e}")
            total_places = 0
        
        if total_places == 0:
            logger.warning("❌ Aucun lieu de beauté trouvé dans la collection")
            if BYPASS_FILTERS:
                logger.info("⚠️ Mode bypass activé, création de lieux de beauté fictifs car aucun trouvé")
                # Créer des lieux fictifs pour les tests
                mock_places = [
                    {
                        "_id": ObjectId(),
                        "name": "Salon Beauté (fictif)",
                        "category": "Soins esthétiques et bien-être"
                    }
                ]
                # Générer des posts avec les données fictives
                posts_created = []
                for place in mock_places:
                    post = {
                        "post_id": f"mock_{datetime.now().timestamp()}",
                        "beauty_name": place["name"]
                    }
                    posts_created.append(post)
                return posts_created
            else:
                return []
        
        # Debug: Afficher les premiers lieux pour analyse
        sample_places = list(beauty_collection.find().limit(2))
        if sample_places:
            logger.info(f"📝 Exemples de lieux de beauté trouvés:")
            for i, p in enumerate(sample_places):
                logger.info(f"  • Lieu {i+1}: {p.get('name', '(sans nom)')} (ID: {p.get('_id')})")
                for field in ["category", "sous_categorie", "profile_photo", "average_score"]:
                    if field in p:
                        logger.info(f"    - {field}: {p.get(field)}")
        
        # Filtrer les lieux par note si BYPASS_FILTERS n'est pas activé
        if BYPASS_FILTERS:
            logger.info(f"⚠️ Mode bypass activé pour les lieux de beauté")
            try:
                # Récupérer des lieux sans filtre
                beauty_places = list(beauty_collection.find().limit(count * 3))
                logger.info(f"✅ {len(beauty_places)} lieux de beauté récupérés sans filtre")
            except Exception as e:
                logger.error(f"❌ Erreur lors de la récupération des lieux de beauté: {str(e)}")
                return []
        else:
            # Récupérer tous les lieux de beauté sans filtre sur la note
            try:
                beauty_places = list(beauty_collection.find().limit(count * 3))
                logger.info(f"✅ {len(beauty_places)} lieux de beauté récupérés")
            except Exception as e:
                logger.error(f"❌ Erreur lors de la récupération des lieux de beauté: {e}")
                return []
        
        if not beauty_places:
            logger.warning("❌ Aucun lieu de beauté trouvé avec les critères actuels")
            return []
        
        # Générer des posts
        posts_created = []
        for i in range(min(count, len(beauty_places))):
            place = beauty_places[i]
            
            logger.info(f"🤖 Génération de post pour un lieu de beauté...")
            
            # Créer le post
            result = create_post_for_beauty_place(db_connections, place, openai_client)
            if result:
                posts_created.append(result)
                logger.info(f"✅ Post généré pour le lieu de beauté: {place.get('name', 'Sans nom')}")
        
        return posts_created
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de la génération des posts pour lieux de beauté: {e}")
        return []

def generate_all_posts(db_connections, config, openai_client=None):
    """
    Génère tous les types de posts (utilisateurs, producteurs, beauté, etc.) selon la config
    """
    all_posts = []
    # Génération des posts utilisateurs
    user_posts = generate_user_posts(db_connections, config.get("user_posts_count", 5), openai_client, config)
    all_posts.extend(user_posts)
    # Génération des posts producteurs (événements)
    event_posts = generate_event_posts(db_connections, config.get("event_posts_count", 3), openai_client)
    all_posts.extend(event_posts)
    # Génération des posts producteurs (restaurants)
    restaurant_posts = generate_restaurant_posts(db_connections, config.get("restaurant_posts_count", 2), openai_client)
    all_posts.extend(restaurant_posts)
    # Génération des posts beauté/wellness
    beauty_posts = generate_beauty_posts(db_connections, config.get("beauty_posts_count", 2), openai_client)
    all_posts.extend(beauty_posts)
    return all_posts

def run_generator(config=None, openai_client=None):
    """Exécute le générateur de posts en continu"""
    if config is None:
        config = DEFAULT_CONFIG.copy()
    
    # Mettre à jour la configuration globale
    for key, value in config.items():
        if key in DEFAULT_CONFIG:
            DEFAULT_CONFIG[key] = value
    
    # Établir les connexions MongoDB
    db_connections = get_db_connections(config.get("mongo_uri"))
    
    # Initialiser OpenAI GPT-3.5-turbo si ce n'est pas déjà fait
    if openai_client is None and DEFAULT_CONFIG["ai_enabled"]:
        try:
            openai_client = OpenAIClient()
            logger.info("✅ Client OpenAI initialisé.")
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'initialisation du client OpenAI: {e}")
            logger.warning("⚠️ Désactivation de la génération AI.")
            config["ai_enabled"] = False # Forcer la désactivation si l'init échoue
    
    # Récupérer le dernier checkpoint
    checkpoint = load_checkpoint("last_run")
    last_run = checkpoint.get("timestamp") if checkpoint else None
    
    logger.info("🚀 Démarrage du générateur de posts automatiques avec OpenAI GPT-3.5-turbo")
    logger.info(f"⏰ Heures actives: {DEFAULT_CONFIG['active_hours_start']}h-{DEFAULT_CONFIG['active_hours_end']}h")
    
    if last_run:
        logger.info(f"📝 Dernière exécution: {last_run}")
    
    try:
        while True:
            # Vérifier si nous sommes dans les heures actives
            if not is_within_active_hours():
                logger.info(f"💤 Heure actuelle ({datetime.now().hour}h) hors plage active ({DEFAULT_CONFIG['active_hours_start']}h-{DEFAULT_CONFIG['active_hours_end']}h). Attente...")
                time.sleep(600)  # Attendre 10 minutes
                continue
            
            # Générer les posts
            posts = generate_all_posts(db_connections, DEFAULT_CONFIG, openai_client)
            
            # Sauvegarder le checkpoint
            save_checkpoint("last_run", {
                "timestamp": datetime.now().isoformat(),
                "posts_generated": len(posts),
                "posts": posts
            })
            
            # Pause entre les générations
            sleep_time = DEFAULT_CONFIG["sleep_time"] + random.randint(-60, 60)  # Ajouter un peu d'aléatoire
            logger.info(f"⏱️ Planification de la prochaine génération dans {sleep_time // 60} minutes")
            
            next_run = datetime.now() + timedelta(seconds=sleep_time)
            logger.info(f"🔄 Exécution planifiée à {next_run.strftime('%H:%M:%S')}: Génération de posts aléatoires")
            
            time.sleep(sleep_time)
            
    except KeyboardInterrupt:
        logger.info("\n⛔ Interruption utilisateur. Sauvegarde de l'état...")
        save_checkpoint("interrupted", {
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"❌ Erreur critique: {e}")
        save_checkpoint("error", {
            "timestamp": datetime.now().isoformat(),
            "error": str(e)
        })
        raise

def run_test_generation(config):
    """Exécute une génération de test pour vérifier le bon fonctionnement"""
    logger.info("🧪 Lancement de la génération de test...")
    
    try:
        # Établir les connexions MongoDB
        db_connections = get_db_connections(config.get("mongo_uri"))
        
        # Initialiser OpenAI GPT-3.5-turbo pour la génération de texte
        openai_client = None
        if config["ai_enabled"]:
            try:
                openai_client = OpenAIClient()
                logger.info("✅ Client OpenAI initialisé pour le test.")
            except Exception as e:
                logger.error(f"❌ Erreur lors de l'initialisation du client OpenAI: {e}")
                logger.warning("⚠️ Désactivation de la génération AI.")
                config["ai_enabled"] = False
        
        # Afficher la configuration de test
        logger.info("Configuration du test:")
        logger.info(f"- Posts d'événements: {config['event_posts_count']}")
        logger.info(f"- Posts de restaurants: {config['restaurant_posts_count']}")
        logger.info(f"- Posts d'utilisateurs: {config['user_posts_count']}")
        
        # Générer les posts
        start_time = time.time()
        posts = generate_all_posts(db_connections, config, openai_client)
        end_time = time.time()
        
        # Afficher les résultats
        logger.info(f"✅ Test terminé en {end_time - start_time:.2f} secondes")
        logger.info(f"📊 {len(posts)} posts générés")
        
        # Afficher les détails des posts générés
        for i, post in enumerate(posts):
            type_info = ""
            if "event_title" in post:
                type_info = f"Événement: {post['event_title']}"
            elif "restaurant_name" in post:
                type_info = f"Restaurant: {post['restaurant_name']}"
            elif "user_name" in post:
                type_info = f"Utilisateur: {post['user_name']} -> {post['venue_name']}"
            
            logger.info(f"📝 Post {i+1}: {type_info}")
        
        return posts
    
    except Exception as e:
        logger.error(f"❌ Erreur lors du test: {e}")
        return []

def main():
    """Point d'entrée du script"""
    parser = argparse.ArgumentParser(description="Générateur de posts pour Choice App")
    parser.add_argument("--test", action="store_true", help="Mode test - génère quelques posts et les affiche")
    parser.add_argument("--mongo-uri", type=str, help="URI MongoDB alternative")
    parser.add_argument("--users-count", type=int, dest="user_posts_count", default=DEFAULT_CONFIG["user_posts_count"], help="Nombre de posts utilisateurs à générer")
    parser.add_argument("--events-count", type=int, dest="event_posts_count", default=DEFAULT_CONFIG["event_posts_count"], help="Nombre de posts d'événements à générer")
    parser.add_argument("--restaurants-count", type=int, dest="restaurant_posts_count", default=DEFAULT_CONFIG["restaurant_posts_count"], help="Nombre de posts de restaurants à générer")
    parser.add_argument("--beauty-count", type=int, dest="beauty_posts_count", default=DEFAULT_CONFIG["beauty_posts_count"], help="Nombre de posts de lieux beauté à générer")
    parser.add_argument("--one-shot", action="store_true", help="Exécution unique puis arrêt")
    parser.add_argument("--ai-disabled", action="store_true", help="Désactive les appels à l'IA")
    parser.add_argument("--run-now", action="store_true", help="Exécuter immédiatement, sans vérifier les heures actives")
    parser.add_argument('--use-test-db', action='store_true', help='Utiliser la base de données de test')
    parser.add_argument('--bypass-filters', action='store_true', help='Ignorer les filtres de dates et d\'active')
    parser.add_argument('--bypass-location-check', action='store_true', help='Ignorer la vérification de localisation pour les posts utilisateurs')
    parser.add_argument('--sleep-time', type=int, help='Temps d\'attente entre les générations (en secondes)', default=None)
    parser.add_argument('--mega-generation', action='store_true', help='Génère 1500 posts (répartis entre les différents types)')
    parser.add_argument("--force-ai", action="store_true", help="Force l'utilisation de l'IA et ignore le cache")
    parser.add_argument("--clear-cache", action="store_true", help="Vide le cache de l'IA avant génération")
    parser.add_argument('--with-video', action='store_true', help='Inclure des vidéos dans les posts générés')
    parser.add_argument("--max-users", type=int, help="Nombre maximum d'utilisateurs à traiter")
    parser.add_argument("--location-radius", type=int, help="Rayon en mètres pour la vérification de localisation", default=DEFAULT_CONFIG["location_radius_meters"])
    
    args = parser.parse_args()
    
    # Configuration personalisée
    config = DEFAULT_CONFIG.copy()
    
    # Vérifier si mode mega-generation est activé
    if args.mega_generation:
        # Répartition pour 1500 posts au total
        config.update({
            "event_posts_count": 300,
            "restaurant_posts_count": 500,
            "beauty_posts_count": 300,
            "user_posts_count": 400
        })
        logger.info("🚀 Mode mega-generation activé: génération de 1500 posts")
        args.one_shot = True  # Forcer le mode one-shot
        args.ai_disabled = True  # Désactiver l'IA pour accélérer
    
    # Si l'utilisateur demande de vider le cache
    if args.clear_cache:
        try:
            cache_files = os.listdir(API_CACHE_DIR)
            for file in cache_files:
                os.remove(os.path.join(API_CACHE_DIR, file))
            logger.info(f"✅ Cache vidé, {len(cache_files)} fichiers supprimés")
        except Exception as e:
            logger.error(f"❌ Erreur lors de la suppression du cache: {e}")
            
    # Force l'utilisation de l'IA
    if args.force_ai:
        config["ai_enabled"] = True
        # Modifier la fonction get_cached_response pour toujours retourner None
        global get_cached_response
        original_get_cached_response = get_cached_response
        def force_no_cache(prompt):
            return None
        get_cached_response = force_no_cache
        logger.info("⚙️ Mode force-AI activé: cache désactivé et IA forcée")
    
    # Appliquer les paramètres de ligne de commande
    if args.mongo_uri:
        global MONGO_URI
        MONGO_URI = args.mongo_uri
    
    if args.use_test_db:
        global USE_TEST_CONNECTION
        USE_TEST_CONNECTION = True
        logger.info("🔄 Utilisation de la base de données de test activée")
    
    if args.bypass_filters:
        global BYPASS_FILTERS
        BYPASS_FILTERS = True
        logger.info("⚠️ Mode bypass filters activé")
    
    if args.bypass_location_check:
        config["verify_location"] = False
        logger.info("⚠️ Vérification de localisation désactivée")
    
    if args.sleep_time is not None:
        config["sleep_time"] = args.sleep_time
    
    if args.ai_disabled:
        config["ai_enabled"] = False
        logger.info("⚠️ IA désactivée, mode de secours activé")
    
    if args.user_posts_count is not None:
        config["user_posts_count"] = args.user_posts_count
    if args.event_posts_count is not None:
        config["event_posts_count"] = args.event_posts_count
    if args.restaurant_posts_count is not None:
        config["restaurant_posts_count"] = args.restaurant_posts_count
    if args.beauty_posts_count is not None:
        config["beauty_posts_count"] = args.beauty_posts_count
    
    if args.location_radius is not None:
        config["location_radius_meters"] = args.location_radius
    
    if args.max_users is not None:
        config["max_users"] = args.max_users
    
    # Initialiser OpenAI GPT-3.5-turbo si l'IA est activée
    openai_client = None
    if config["ai_enabled"]:
        try:
            openai_client = OpenAIClient()
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'initialisation du client OpenAI: {e}")
            logger.warning("⚠️ Désactivation de la génération AI.")
            config["ai_enabled"] = False
    
    # Passer l'option vidéo dans la config
    config["with_video"] = args.with_video
    
    # Vérification des heures actives
    if not args.run_now and not args.test and not args.one_shot:
        if not is_within_active_hours():
            current_hour = datetime.now().hour
            logger.info(f"💤 Heure actuelle ({current_hour}h) hors plage active (3h-7h). Attente...")
            # Attendre jusqu'à 3h du matin
            time.sleep(60 * (3 - current_hour))
    
    if args.test:
        # Exécuter le test
        run_test_generation(config)
    elif args.one_shot or args.run_now:
        # Mode one-shot: génération unique
        mode_desc = "one-shot" if args.one_shot else "run-now"
        logger.info(f"🎯 Mode {mode_desc} activé. Génération unique en cours...")
        db_connections = get_db_connections()
        
        # Générer tous les posts
        all_posts = generate_all_posts(db_connections, config, openai_client)
        
        # Afficher un résumé
        event_posts = [p for p in all_posts if p.get("is_event_post", False)]
        restaurant_posts = [p for p in all_posts if p.get("is_restaurant_post", False)]
        beauty_posts = [p for p in all_posts if p.get("is_beauty_post", False)]
        user_posts = [p for p in all_posts if not p.get("is_event_post", False) and not p.get("is_restaurant_post", False) and not p.get("is_beauty_post", False)]

        logger.info(f"📊 Résumé de la génération {mode_desc}:")
        logger.info(f"  • {len(event_posts)} posts d'événements")
        logger.info(f"  • {len(restaurant_posts)} posts de restaurants")
        logger.info(f"  • {len(beauty_posts)} posts de lieux de beauté")
        logger.info(f"  • {len(user_posts)} posts d'utilisateurs")
        logger.info(f"  • {len(all_posts)} posts au total")
        logger.info("✅ Génération terminée. Arrêt du script.")
    else:
        # Mode normal (exécution continue)
        logger.info("🚀 Démarrage du générateur de posts automatiques...")
        run_generator(config, openai_client)

def create_post_for_beauty_place(db_connections, beauty_place, openai_client=None):
    """Crée un post producteur pour un lieu de beauté/bien-être en utilisant OpenAI."""
    logger.info(f"🧖‍♀️ Création d'un post pour le lieu de beauté: {beauty_place.get('name', 'Sans nom')}")
    
    try:
        # Préparer les données pour le post
        place_name = beauty_place.get('name', 'Sans nom')
        place_id = str(beauty_place.get('_id', ''))
        place_category = beauty_place.get('category', '')
        place_sous_categorie = beauty_place.get('sous_categorie', '')
        
        # Déterminer la catégorie précise pour ce lieu de beauté
        beauty_category = determine_venue_category(beauty_place, is_beauty=True)
        
        # Si la catégorie n'est pas déterminée, prendre une catégorie par défaut
        if not beauty_category:
            logger.warning(f"⚠️ Catégorie non déterminée pour {place_name}, utilisation de 'spa' par défaut")
            beauty_category = "spa"
        
        # Récupérer les émojis pour cette catégorie
        emojis = VENUE_CATEGORIES.get(beauty_category, VENUE_CATEGORIES["default"])["emojis_positive"]
        emoji = random.choice(emojis) if emojis else "✨"
            
        # Générer des données pour ce lieu
        photos = beauty_place.get('photos', [])
        profile_photo = beauty_place.get('profile_photo', '')
        
        # Si pas de photo de profil spécifique, chercher dans d'autres champs
        if not profile_photo:
            for field in ['main_image', 'image', 'photo', 'thumbnail', 'logo', 'avatar']:
                if field in beauty_place and beauty_place[field]:
                    profile_photo = beauty_place[field]
                    break
        
        # Si aucune photo trouvée, utiliser la première du tableau photos
        if not profile_photo and photos and len(photos) > 0:
            profile_photo = photos[0]
            
        # Définir un avatar par défaut si aucune photo n'est trouvée
        if not profile_photo:
            profile_photo = "/images/default_beauty_avatar.png"
            
        address = beauty_place.get('address', 'Adresse non disponible')
        description = beauty_place.get('description', 'Description non disponible')
        average_score = beauty_place.get('average_score', 4.0)  # Note par défaut de 4.0
        
        # Extraire les coordonnées selon leur format
        coordinates = []
        if "location" in beauty_place and "coordinates" in beauty_place["location"]:
            coordinates = beauty_place["location"]["coordinates"]
        elif "gps_coordinates" in beauty_place:
            if "coordinates" in beauty_place["gps_coordinates"]:
                coordinates = beauty_place["gps_coordinates"]["coordinates"]
            elif "lat" in beauty_place["gps_coordinates"] and "lng" in beauty_place["gps_coordinates"]:
                coordinates = [beauty_place["gps_coordinates"]["lng"], beauty_place["gps_coordinates"]["lat"]]
        
        # Choisir un template aléatoire entre positif et négatif selon un ratio 80/20 biaisé vers le positif
        is_positive = random.choices([True, False], weights=[80, 20], k=1)[0]

        # Si note < 3.5, on force un post négatif
        if average_score < 3.5:
            is_positive = False
            
        # Si note > 4.5, on force un post positif
        if average_score > 4.5:
            is_positive = True
            
        # Construire le prompt pour OpenAI GPT-3.5-turbo
        if is_positive:
            instructions = f"""
[INST] IMPORTANT - RÉPONDS UNIQUEMENT EN FRANÇAIS.

Génère un post court pour {place_name}, un lieu de beauté et bien-être.

Ce post doit:
1. Être écrit en FRANÇAIS UNIQUEMENT
2. Parler UNIQUEMENT des services proposés dans ce lieu précis
3. NE PAS MENTIONNER DE PARTENARIAT avec d'autres établissements
4. Utiliser l'emoji {emoji}
5. Évoquer le bien-être, la détente ou les soins
6. Être à la première personne du pluriel (nous)
7. Faire moins de 250 caractères

INTERDIT:
- Ne parle pas de partenariat
- N'utilise aucun mot anglais
- Ne mentionne pas d'autres établissements
- Ne propose pas de réduction ni de promotion précise

Exemple correct: "{emoji} Chez [nom], nous vous proposons des soins relaxants dans une ambiance apaisante. Notre équipe de professionnels sera ravie de vous accueillir pour un moment de détente. Venez découvrir notre espace bien-être!"
[/INST]
"""
        else:
            instructions = f"""
[INST] IMPORTANT - RÉPONDS UNIQUEMENT EN FRANÇAIS.

Génère un post court pour {place_name}, un lieu de beauté et bien-être.

Ce post doit:
1. Être écrit en FRANÇAIS UNIQUEMENT
2. Parler UNIQUEMENT des services proposés dans ce lieu précis
3. NE PAS MENTIONNER DE PARTENARIAT avec d'autres établissements
4. Utiliser l'emoji {emoji}
5. Évoquer le bien-être, la détente ou les soins
6. Être à la première personne du pluriel (nous)
7. Faire moins de 250 caractères

INTERDIT:
- Ne parle pas de partenariat
- N'utilise aucun mot anglais
- Ne mentionne pas d'autres établissements
- Ne propose pas de réduction ni de promotion précise

Exemple correct: "{emoji} Chez [nom], nous vous proposons des soins relaxants dans une ambiance apaisante. Notre équipe de professionnels sera ravie de vous accueillir pour un moment de détente. Venez découvrir notre espace bien-être!"
[/INST]
"""
    
        # Générer le contenu avec OpenAI
        post_content = None
        if openai_client:
            try:
                post_content = openai_client.generate_text(instructions)
                if not post_content or len(post_content) < 20:
                    raise Exception("Contenu généré trop court ou vide")
            except Exception as e:
                logger.warning(f"⚠️ Erreur lors de la génération avec OpenAI: {e}")
                post_content = ""
        
        # Si pas de contenu généré par OpenAI ou contenu trop court, utiliser un texte par défaut
        if not post_content or len(post_content) < 20:
            logger.info(f"⚠️ Utilisation d'un texte par défaut pour le lieu de beauté {place_name}")
            if is_positive:
                post_content = f"{emoji} Chez {place_name}, nous vous accueillons dans une ambiance relaxante pour prendre soin de vous. Nos experts sont dédiés à votre bien-être et proposent des soins personnalisés de qualité. Prenez rendez-vous et offrez-vous un moment de détente! #beauté #bienêtre"
            else:
                post_content = f"{emoji} Cette semaine chez {place_name}, nous vous proposons une offre spéciale sur nos soins signatures. Venez découvrir notre équipe de professionnels dans un cadre apaisant. Réservez votre moment détente! #beauté #bienêtre"
        
        # Créer l'objet post
        post = {
            "content": post_content,
            "time_posted": datetime.now().isoformat(),
            "author": {
                "id": place_id,
                "name": place_name,
                "avatar": profile_photo
            },
            "producer_id": place_id,
            "isProducerPost": True,
            "isLeisureProducer": False,
            "isBeautyProducer": True,  # Flag spécifique pour identifier les posts de beauté
            "beauty_producer": True,    # Flag alternatif pour compatibilité
            "post_type": "beauty",      # Champ explicite pour le type
            "is_automated": True,
            "is_beauty_post": True,  # Ajout d'un indicateur clair pour le comptage
            "likes": 0,              # Pour compatibilité frontend
            "likes_count": 0,
            "comments": 0,
            "comments_count": 0,
            "interested": False,
            "interested_count": 0,
            "choice": False,
            "choice_count": 0,
            "beauty_id": place_id,      # Identifiant du lieu de beauté
            "beauty_name": place_name,  # Nom du lieu explicite
            "beauty_category": beauty_category,
            "beauty_subcategory": place_sous_categorie
        }
        
        # Ajouter la localisation si disponible
        if coordinates:
            post["location"] = {
                "type": "Point",
                "coordinates": coordinates
            }
        
        # Ajouter des médias au post
        media = []
        
        # Ajouter l'image principale
        if profile_photo:
            media.append({
                "type": "image",
                "url": profile_photo,
                "width": 800,
                "height": 600
            })
        
        # Ajouter d'autres photos si disponibles (max 2 photos supplémentaires)
        if photos and len(photos) > 0:
            for photo in photos[:2]:
                # Éviter d'ajouter la même photo que le profil
                if photo != profile_photo:
                    media.append({
                        "type": "image",
                        "url": photo,
                        "width": 600,
                        "height": 400
                    })
        
        # Ajouter les médias au post
        post["media"] = media
        
        # Insérer le post dans la base de données
        post_id = db_connections["choice"][COLL_POSTS].insert_one(post).inserted_id
        logger.info(f"✅ Post créé avec succès dans la base de données. ID: {post_id}")
        
        # Mettre à jour le lieu de beauté avec le post
        try:
            db_connections["beauty"][COLL_BEAUTY_PLACES].update_one(
                {"_id": ObjectId(place_id)},
                {"$push": {"posts": post_id}}
            )
            logger.info(f"✅ Lieu de beauté {place_id} mis à jour avec le nouveau post")
        except Exception as e:
            logger.warning(f"⚠️ Erreur lors de la mise à jour du lieu de beauté: {e}")
        
        return {
            "post_id": str(post_id),
            "beauty_name": place_name,
            "post_type": "beauty"
        }
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de la création du post pour lieu de beauté: {e}")
        return None

if __name__ == "__main__":
    main()