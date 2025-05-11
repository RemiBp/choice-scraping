#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Générateur automatique d'utilisateurs factices pour Choice App
Ce script génère des profils d'utilisateurs réalistes pour l'application Choice,
qui pourront ensuite être utilisés pour générer du contenu de posts simulé.
"""

import os
import json
import time
import random
import logging
import argparse
import string
import bcrypt
from datetime import datetime, timedelta
from pymongo import MongoClient
from bson.objectid import ObjectId
import uuid
import requests

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("openai_fake_user_generator.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

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

# Noms des bases de données et collections
DB_CHOICE = "choice_app"
DB_RESTAURATION = "Restauration_Officielle"
DB_LOISIR = "Loisir&Culture"
DB_BEAUTY = "Beauty_Wellness"

COLL_USERS = "Users"
COLL_POSTS = "Posts"
COLL_PRODUCERS_RESTAURATION = "producers"
COLL_VENUES_LOISIR = "Loisir_Paris_Producers"
COLL_EVENTS_LOISIR = "Loisir_Paris_Evenements"
COLL_WELLNESS_PLACES = "BeautyPlaces"

# --- Configuration du générateur ---
DEFAULT_CONFIG = {
    "users_count": 20,        # Nombre d'utilisateurs à générer
    "seed": None,             # Graine aléatoire pour la reproductibilité
    "real_locations": True,   # Utiliser des localisations réelles de Paris
    "verify_existing": True,  # Vérifier si les emails existent déjà avant création
}

# Données pour la génération de profils réalistes
FRENCH_FIRST_NAMES = [
    # Prénoms masculins
    "Thomas", "Nicolas", "Julien", "Quentin", "Antoine", "Maxime", "Alexandre", "Lucas", "Pierre", "Louis",
    "Hugo", "Mathieu", "Clément", "Alexis", "Arthur", "Paul", "Théo", "Romain", "Baptiste", "Kevin",
    "Vincent", "Simon", "Benjamin", "François", "Adrien", "Guillaume", "Valentin", "Jérémy", "Arnaud", "Florian",
    "Martin", "Samuel", "Raphaël", "Thibault", "David", "Jules", "Etienne", "Gabriel", "Nathan", "Sébastien",
    # Prénoms féminins
    "Léa", "Emma", "Manon", "Chloé", "Camille", "Sarah", "Marine", "Julie", "Pauline", "Laura",
    "Mathilde", "Justine", "Alice", "Louise", "Juliette", "Charlotte", "Clara", "Lucie", "Marie", "Inès",
    "Anaïs", "Océane", "Elisa", "Sophie", "Audrey", "Marion", "Mélanie", "Amandine", "Lisa", "Clémence",
    "Emilie", "Elodie", "Julia", "Zoé", "Noémie", "Eva", "Margaux", "Célia", "Morgane", "Valentine"
]

FRENCH_LAST_NAMES = [
    "Martin", "Bernard", "Dubois", "Thomas", "Robert", "Richard", "Petit", "Durand", "Leroy", "Moreau",
    "Simon", "Laurent", "Lefebvre", "Michel", "Garcia", "David", "Bertrand", "Roux", "Vincent", "Fournier",
    "Morel", "Girard", "André", "Lefevre", "Mercier", "Dupont", "Lambert", "Bonnet", "Francois", "Martinez",
    "Legrand", "Garnier", "Faure", "Rousseau", "Blanc", "Guerin", "Muller", "Henry", "Roussel", "Nicolas",
    "Perrin", "Morin", "Mathieu", "Clement", "Gauthier", "Dumont", "Lopez", "Fontaine", "Chevalier", "Robin"
]

PARIS_LOCATIONS = [
    {"latitude": 48.8566, "longitude": 2.3522},  # Centre de Paris
    {"latitude": 48.8738, "longitude": 2.2950},  # 16ème arr.
    {"latitude": 48.8848, "longitude": 2.3231},  # 17ème arr.
    {"latitude": 48.8829, "longitude": 2.3320},  # 18ème arr.
    {"latitude": 48.8844, "longitude": 2.3964},  # 19ème arr.
    {"latitude": 48.8614, "longitude": 2.3935},  # 20ème arr.
    {"latitude": 48.8350, "longitude": 2.3892},  # 13ème arr.
    {"latitude": 48.8304, "longitude": 2.3376},  # 14ème arr.
    {"latitude": 48.8402, "longitude": 2.2872},  # 15ème arr.
    {"latitude": 48.8690, "longitude": 2.3484},  # 9ème arr.
    {"latitude": 48.8792, "longitude": 2.3508},  # 10ème arr.
    {"latitude": 48.8673, "longitude": 2.3629},  # 11ème arr.
    {"latitude": 48.8362, "longitude": 2.3730},  # 12ème arr.
    {"latitude": 48.8649, "longitude": 2.3305},  # 1er arr.
    {"latitude": 48.8598, "longitude": 2.3408},  # 2ème arr.
    {"latitude": 48.8620, "longitude": 2.3614},  # 3ème arr.
    {"latitude": 48.8566, "longitude": 2.3585},  # 4ème arr.
    {"latitude": 48.8436, "longitude": 2.3522},  # 5ème arr.
    {"latitude": 48.8494, "longitude": 2.3373},  # 6ème arr.
    {"latitude": 48.8582, "longitude": 2.3220},  # 7ème arr.
    {"latitude": 48.8742, "longitude": 2.3136},  # 8ème arr.
]

FOOD_CATEGORIES = [
    "japonais", "italien", "français", "indien", "mexicain", "libanais", "végétarien", "fruits de mer", 
    "fast-food", "gastronomique", "bistro", "pizzeria", "sushi", "fusion", "coréen", "vegan", 
    "cuisine du monde", "brunch", "crêperie", "burger", "végétalien", "sans gluten", "pâtisserie", 
    "vietnamien", "chinois", "thaïlandais", "sandwich", "tapas"
]

CULTURE_CATEGORIES = [
    "cinéma", "théâtre", "opéra", "musée", "exposition", "concert", "danse", "littérature", "photographie", 
    "art contemporain", "festival", "spectacle musical", "ballet", "cirque", "art urbain", "performance", 
    "musique classique", "jazz", "electro", "hip-hop", "rock", "humour", "stand-up", "lecture", "poésie"
]

BEAUTY_WELLNESS_CATEGORIES = [
    "spa", "massage", "soins du visage", "manucure", "pédicure", "coiffure", "épilation", 
    "salon de beauté", "institut", "bien-être", "relaxation", "thérapie", "méditation", "yoga",
    "pilates", "soins corporels", "esthétique", "cosmétique", "soin holistique", "détente"
]

INTEREST_TAGS = FOOD_CATEGORIES + CULTURE_CATEGORIES + BEAUTY_WELLNESS_CATEGORIES + [
    "rooftop", "terrasse", "vue", "ambiance", "romantique", "familial", "entre amis", "afterwork", 
    "architecture", "histoire", "visite guidée", "nature", "parc", "écologie", "bien-être", "sport"
]

UNSPLASH_ACCESS_KEY = os.environ.get("UNSPLASH_ACCESS_KEY")

def generate_user_location(config):
    """Génère une localisation utilisateur réaliste"""
    if config["real_locations"]:
        # Utiliser une localisation parisienne
        base_location = random.choice(PARIS_LOCATIONS)
        
        # Ajouter une petite variation (+/- 0.002 degré ~ 220m max)
        lat_variation = random.uniform(-0.002, 0.002)
        lng_variation = random.uniform(-0.002, 0.002)
        
        return {
            "latitude": base_location["latitude"] + lat_variation,
            "longitude": base_location["longitude"] + lng_variation
        }
    else:
        # Localisation aléatoire dans Paris
        return {
            "latitude": 48.8566 + random.uniform(-0.05, 0.05),
            "longitude": 2.3522 + random.uniform(-0.05, 0.05)
        }

def download_avatar(photo_path, gender, seed=None):
    """Télécharge un avatar stylisé DiceBear selon le genre."""
    if seed is None:
        seed = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    # Choix du style DiceBear selon le genre
    if gender == "male":
        style = "adventurer"
    elif gender == "female":
        style = "avataaars"
    else:
        style = "adventurer-neutral"
    url = f"https://api.dicebear.com/7.x/{style}/png?seed={seed}"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            with open(photo_path, "wb") as f:
                f.write(response.content)
            return True
        else:
            logger.warning(f"⚠️ Avatar DiceBear non téléchargé (status {response.status_code}) pour {photo_path}")
            return False
    except Exception as e:
        logger.warning(f"⚠️ Avatar DiceBear non téléchargé : {e}")
        return False

def download_realistic_avatar(photo_path, gender):
    """Télécharge une photo réaliste d'humain depuis Unsplash selon le genre."""
    query = "portrait"
    if gender == "male":
        query += " man"
    elif gender == "female":
        query += " woman"
    
    url = f"https://api.unsplash.com/photos/random?query={query}&client_id={UNSPLASH_ACCESS_KEY}"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            image_url = data["urls"]["regular"]
            image_response = requests.get(image_url, timeout=10)
            if image_response.status_code == 200:
                with open(photo_path, "wb") as f:
                    f.write(image_response.content)
                # Attendre entre 2 et 3 secondes pour éviter les limitations
                time.sleep(random.uniform(2, 3))
                return True
            else:
                logger.warning(f"⚠️ Image Unsplash non téléchargée (status {image_response.status_code}) pour {photo_path}")
                return False
        else:
            logger.warning(f"⚠️ Requête Unsplash échouée (status {response.status_code})")
            return False
    except Exception as e:
        logger.warning(f"⚠️ Erreur lors du téléchargement de l'image Unsplash : {e}")
        return False

def generate_unique_email(existing_emails, first_name, last_name):
    """Génère un email unique qui n'est pas déjà dans existing_emails."""
    base = f"{first_name.lower()}.{last_name.lower()}"
    domain = "@choiceapp.com"
    i = 1
    email = base + domain
    while email in existing_emails:
        email = f"{base}{i}{domain}"
        i += 1
    return email

def generate_password():
    """Génère et hash un mot de passe aléatoire"""
    # Générer un mot de passe de 10-14 caractères
    length = random.randint(10, 14)
    chars = string.ascii_letters + string.digits + "!@#$%^&*"
    password = ''.join(random.choice(chars) for _ in range(length))
    
    # Hash du mot de passe (format attendu par le système)
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password.encode(), salt)
    
    return hashed.decode()

def generate_user_profile(config, existing_emails):
    """Génère un profil utilisateur complet"""
    # Déterminer le genre
    gender = random.choice(["male", "female", "Non spécifié"])
    
    # Sélectionner prénom/nom en fonction du genre
    if gender == "male":
        first_name = random.choice(FRENCH_FIRST_NAMES[:40])  # Prénoms masculins en premier
    elif gender == "female":
        first_name = random.choice(FRENCH_FIRST_NAMES[40:])  # Prénoms féminins ensuite
    else:
        first_name = random.choice(FRENCH_FIRST_NAMES)
    
    last_name = random.choice(FRENCH_LAST_NAMES)
    full_name = f"{first_name} {last_name}"
    
    # Générer un email unique
    email = generate_unique_email(existing_emails, first_name, last_name)
    existing_emails.add(email)
    
    # Générer l'avatar réaliste Unsplash
    photo_path = f"profile_photos/{first_name}_{last_name}_{gender}.jpg"
    success = download_realistic_avatar(photo_path, gender)
    if success:
        photo_url = photo_path
        logger.info(f"ℹ️ Avatar réaliste Unsplash généré pour {full_name} ({gender})")
    else:
        photo_url = "/images/default_avatar.png"
        logger.warning(f"❌ Avatar par défaut utilisé pour {full_name} ({gender})")
    
    # Générer la localisation
    location = generate_user_location(config)
    
    # Générer les préférences de contenu
    content_prefs = {
        "text": round(random.uniform(0.3, 0.9), 2),
        "image": round(random.uniform(0.3, 0.9), 2),
        "video": round(random.uniform(0.3, 0.9), 2)
    }
    
    # Sélectionner des tags d'intérêt (5-15 tags)
    num_tags = random.randint(5, 15)
    liked_tags = random.sample(INTEREST_TAGS, num_tags)
    
    # Générer des informations sur les secteurs d'intérêt
    food_preferences = {
        "avg_spending": random.randint(15, 80),
        "vegan": random.random() < 0.3,
        "carbon_aware": random.random() < 0.4
    }
    
    # Styles culturels préférés
    culture_styles = ["sculpture", "classical_art", "modern_art", "street_art", "photography", 
                    "impressionism", "abstract", "realism", "pop_art", "performance_art"]
    event_types = ["exhibition", "concert", "theater", "opera", "stand_up", "cinema", "dance", "festival"]
    
    num_culture_styles = random.randint(1, 5)
    num_event_types = random.randint(1, 4)
    
    culture_preferences = {
        "preferred_styles": random.sample(culture_styles, num_culture_styles),
        "event_types": random.sample(event_types, num_event_types)
    }
    
    # Préférences wellness
    wellness_services = ["massage", "facial", "hair_styling", "nails", "spa", "therapy", "skincare", "body_treatment"]
    atmosphere_prefs = ["quiet", "luxurious", "modern", "natural", "relaxing", "energizing", "holistic"]
    
    num_wellness_services = random.randint(1, 4)
    num_atmosphere_prefs = random.randint(1, 3)
    
    wellness_preferences = {
        "services": random.sample(wellness_services, num_wellness_services),
        "atmosphere": random.sample(atmosphere_prefs, num_atmosphere_prefs),
        "price_range": random.randint(40, 200),
        "eco_friendly": random.random() < 0.5
    }
    
    # Générer des métriques d'interaction
    total_interactions = random.randint(10, 500)
    
    # Proportions approximatives pour chaque type d'interaction
    comments_ratio = random.uniform(0.2, 0.4)
    choices_ratio = random.uniform(0.1, 0.3)
    shares_ratio = random.uniform(0.05, 0.15)
    
    interaction_metrics = {
        "total_interactions": total_interactions,
        "comments_given": int(total_interactions * comments_ratio),
        "choices_given": int(total_interactions * choices_ratio),
        "shares_given": int(total_interactions * shares_ratio)
    }
    
    # Comportement de consommation
    consumption_behavior = {
        "varies_preferences": random.random() < 0.6,
        "tries_new_content": random.random() < 0.7
    }
    
    # Définir si l'utilisateur est "star" (utilisateur influent)
    is_star = random.random() < 0.1  # 10% de chances
    
    # Générer un nombre de followers
    followers_base = 5 if not is_star else 50
    followers_variance = 20 if not is_star else 300
    followers_count = max(0, random.randint(followers_base, followers_base + followers_variance))
    
    # Score d'influence
    influence_score = min(100, max(1, int(30 + (followers_count / 5) + random.randint(-10, 10))))
    
    # Créer le profil complet
    user_profile = {
        "name": full_name,
        "username": "",  # à remplir si besoin
        "email": email,
        "password": "$2a$10$5OifLZb9qAYC3vNE4RBnKuqeQAiIJAKfBXHUxNDleA6nWsXcQqmLu",  # Hash bcrypt de "123456"
        "gender": gender,
        "age": random.randint(18, 60) if random.random() < 0.8 else None,
        "photo_url": photo_url,
        "coverPhoto": "",
        "bio": "",
        "phone": "",
        "dateOfBirth": None,
        "website": "",
        "socialLinks": {
            "facebook": "",
            "twitter": "",
            "instagram": "",
            "linkedin": "",
            "github": ""
        },
        "badges": [],
        "status": {
            "text": "",
            "emoji": "",
            "expiresAt": None,
            "clearAfterExpiry": True,
            "visibility": "everyone",
            "updatedAt": datetime.now().isoformat()
        },
        "presence": {
            "isOnline": False,
            "lastSeen": datetime.now().isoformat(),
            "device": "web"
        },
        "connections": {
            "friends": [],
            "followers": [],
            "following": [],
            "blocked": []
        },
        "favorites": {
            "conversations": [],
            "users": [],
            "messages": []
        },
        "settings": {
            "notifications": {},
            "privacy": {},
            "appearance": {},
            "language": "en",
            "timezone": "UTC",
            "autoDownload": {
                "images": True,
                "videos": False,
                "documents": True,
                "audio": True
            },
            "twoFactorAuth": {
                "enabled": False,
                "method": "email",
                "verified": False
            }
        },
        "deviceTokens": [],
        "isVerified": False,
        "isActive": True,
        "createdAt": datetime.now().isoformat(),
        "updatedAt": datetime.now().isoformat(),
        "lastActiveAt": datetime.now().isoformat(),
        "metadata": {},
        "location": location,
        "preferred_content_format": content_prefs,
        "liked_tags": liked_tags,
        "trusted_circle": [],  # Sera rempli après création des utilisateurs
        "sector_preferences": {
            "food": food_preferences,
            "culture": culture_preferences,
            "wellness": wellness_preferences
        },
        "interaction_metrics": interaction_metrics,
        "consumption_behavior": consumption_behavior,
        "frequent_locations": [],  # Sera rempli plus tard
        "affinity_producers": [],  # Sera rempli plus tard
        "search_keywords": [],  # Laissé vide délibérément
        "is_star": is_star,
        "followers_count": followers_count,
        "influence_score": influence_score,
        "posts": [],  # Sera rempli lors de la génération de posts
        "following": [],  # Sera rempli lors de l'établissement des connexions sociales
        "followers": [],  # Sera initialisé vide et rempli par interactions
        "interests": [],  # Sera rempli plus tard
        "choices": []  # Sera rempli lors de la génération de posts
    }
    
    return user_profile

def establish_social_connections(users, db_connections):
    """Établit des connexions sociales entre les utilisateurs"""
    logger.info("🔄 Établissement des connexions sociales entre utilisateurs...")
    user_collection = db_connections["choice"][COLL_USERS]
    
    for i, user in enumerate(users):
        # Déterminer le nombre de personnes à suivre (1 à 10)
        num_following = random.randint(1, min(10, len(users) - 1))
        
        # Sélectionner des utilisateurs aléatoires à suivre (excluant soi-même)
        potential_follows = [u for u in users if u["_id"] != user["_id"]]
        follows = random.sample(potential_follows, min(num_following, len(potential_follows)))
        
        # Les utilisateurs "stars" ont plus de chances d'être suivis
        for u in users:
            if u["is_star"] and u["_id"] != user["_id"] and u not in follows:
                if random.random() < 0.7:  # 70% de chances
                    follows.append(u)
        
        # Mettre à jour le cercle de confiance de l'utilisateur
        trusted_circle = [str(follow["_id"]) for follow in follows]
        
        try:
            # Ajout du champ "following" pour les relations sociales
            user_collection.update_one(
                {"_id": user["_id"]},
                {"$set": {
                    "trusted_circle": trusted_circle,
                    "following": trusted_circle
                }}
            )
            logger.info(f"✅ Utilisateur {i+1}/{len(users)}: {len(trusted_circle)} connexions établies")
        except Exception as e:
            logger.error(f"❌ Erreur lors de la mise à jour des connexions sociales: {e}")

def establish_producer_affinities(users, db_connections):
    """Établit des affinités avec des producteurs pour les utilisateurs"""
    logger.info("🔄 Établissement des affinités avec des producteurs...")
    
    # Récupérer des producteurs de restaurants
    restaurant_producers = list(db_connections["restauration"][COLL_PRODUCERS_RESTAURATION].find(
        {"rating": {"$gte": 3.5}},  # Uniquement les restaurants bien notés
        {"_id": 1, "name": 1, "category": 1}
    ).limit(50))
    
    # Récupérer des producteurs de loisirs
    leisure_producers = list(db_connections["loisir"][COLL_VENUES_LOISIR].find(
        {},
        {"_id": 1, "lieu": 1, "nombre_evenements": 1}
    ).limit(50))
    
    user_collection = db_connections["choice"][COLL_USERS]
    
    for i, user in enumerate(users):
        # Nombre d'affinités à créer
        num_affinities = random.randint(3, 12)
        
        # Distribution entre restaurants et loisirs
        num_restaurants = random.randint(1, num_affinities - 1)
        num_leisure = num_affinities - num_restaurants
        
        # Sélectionner les producteurs
        selected_restaurants = random.sample(restaurant_producers, min(num_restaurants, len(restaurant_producers)))
        selected_leisure = random.sample(leisure_producers, min(num_leisure, len(leisure_producers)))
        
        # Créer la liste d'affinités
        affinity_producers = []
        
        for restaurant in selected_restaurants:
            affinity_producers.append({
                "id": str(restaurant["_id"]),
                "name": restaurant.get("name", "Restaurant"),
                "type": "restaurant",
                "affinity_score": round(random.uniform(0.6, 0.95), 2)
            })
        
        for leisure in selected_leisure:
            affinity_producers.append({
                "id": str(leisure["_id"]),
                "name": leisure.get("lieu", "Lieu Culturel"),
                "type": "leisure",
                "affinity_score": round(random.uniform(0.6, 0.95), 2)
            })
        
        try:
            # Mettre à jour l'utilisateur
            user_collection.update_one(
                {"_id": user["_id"]},
                {"$set": {
                    "affinity_producers": affinity_producers,
                    # Création de listes pour les "interests" et "followingProducers"
                    "interests": [p["id"] for p in affinity_producers if p["affinity_score"] > 0.8],
                    "followingProducers": [p["id"] for p in affinity_producers if p["affinity_score"] > 0.7]
                }}
            )
            logger.info(f"✅ Utilisateur {i+1}/{len(users)}: {len(affinity_producers)} affinités établies")
        except Exception as e:
            logger.error(f"❌ Erreur lors de la mise à jour des affinités: {e}")

def generate_frequent_locations(users, db_connections):
    """Génère des localisations fréquentes pour les utilisateurs (utilisation position historique)"""
    logger.info("🔄 Génération des localisations fréquentes...")
    
    # Récupérer des lieux de restaurants
    restaurant_locations = list(db_connections["restauration"][COLL_PRODUCERS_RESTAURATION].find(
        {"gps_coordinates": {"$exists": True}},
        {"_id": 1, "name": 1, "gps_coordinates": 1, "address": 1}
    ).limit(100))
    
    # Récupérer des lieux de loisirs
    leisure_locations = list(db_connections["loisir"][COLL_VENUES_LOISIR].find(
        {"location.coordinates": {"$exists": True}},
        {"_id": 1, "lieu": 1, "location": 1, "adresse": 1}
    ).limit(100))
    
    # Récupérer des lieux de beauté/bien-être
    wellness_locations = list(db_connections["beauty"][COLL_WELLNESS_PLACES].find(
        {"location.coordinates": {"$exists": True}},
        {"_id": 1, "name": 1, "location": 1, "address": 1}
    ).limit(100))
    
    user_collection = db_connections["choice"][COLL_USERS]
    
    for i, user in enumerate(users):
        # Nombre de lieux fréquents
        num_locations = random.randint(3, 15)
        
        # Distribution entre restaurants, loisirs et beauté/bien-être
        # Vérifier si l'utilisateur a un intérêt pour le bien-être
        has_wellness_interest = any(tag in BEAUTY_WELLNESS_CATEGORIES for tag in user.get("liked_tags", []))
        
        if has_wellness_interest:
            # Plus de lieux de beauté si l'utilisateur s'y intéresse
            num_wellness = random.randint(1, min(5, num_locations - 2))
            num_restaurants = random.randint(1, num_locations - num_wellness - 1)
            num_leisure = num_locations - num_restaurants - num_wellness
        else:
            # Moins de lieux de beauté si l'utilisateur ne s'y intéresse pas
            num_wellness = random.randint(0, min(2, num_locations - 2))
            num_restaurants = random.randint(1, num_locations - num_wellness - 1)
            num_leisure = num_locations - num_restaurants - num_wellness
        
        # Sélectionner les lieux
        selected_restaurants = random.sample(restaurant_locations, min(num_restaurants, len(restaurant_locations)))
        selected_leisure = random.sample(leisure_locations, min(num_leisure, len(leisure_locations)))
        selected_wellness = random.sample(wellness_locations, min(num_wellness, len(wellness_locations)))
        
        # Créer la liste des lieux fréquents avec historique
        frequent_locations = []
        
        # Ajouter les restaurants
        for restaurant in selected_restaurants:
            # Formater les coordonnées selon leur structure
            coordinates = None
            if "gps_coordinates" in restaurant:
                if "coordinates" in restaurant["gps_coordinates"]:
                    coordinates = restaurant["gps_coordinates"]["coordinates"]
                elif "lat" in restaurant["gps_coordinates"] and "lng" in restaurant["gps_coordinates"]:
                    coordinates = [restaurant["gps_coordinates"]["lng"], restaurant["gps_coordinates"]["lat"]]
            
            if coordinates:
                # Générer entre 1 et 5 visites sur les 30 derniers jours
                visits = []
                num_visits = random.randint(1, 5)
                
                for _ in range(num_visits):
                    # Date aléatoire entre aujourd'hui et il y a 30 jours
                    days_ago = random.randint(0, 30)
                    visit_date = datetime.now() - timedelta(days=days_ago)
                    
                    # Durée aléatoire entre 30 minutes et 3 heures
                    duration_minutes = random.randint(30, 180)
                    
                    visits.append({
                        "date": visit_date.isoformat(),
                        "duration_minutes": duration_minutes
                    })
                
                frequent_locations.append({
                    "id": str(restaurant["_id"]),
                    "name": restaurant.get("name", "Restaurant"),
                    "type": "restaurant",
                    "coordinates": coordinates,
                    "address": restaurant.get("address", ""),
                    "visits": visits
                })
        
        # Ajouter les lieux de loisirs
        for leisure in selected_leisure:
            # Formater les coordonnées selon leur structure
            coordinates = None
            if "location" in leisure and "coordinates" in leisure["location"]:
                coordinates = leisure["location"]["coordinates"]
            
            if coordinates:
                # Générer entre 1 et 3 visites sur les 30 derniers jours
                visits = []
                num_visits = random.randint(1, 3)
                
                for _ in range(num_visits):
                    # Date aléatoire entre aujourd'hui et il y a 30 jours
                    days_ago = random.randint(0, 30)
                    visit_date = datetime.now() - timedelta(days=days_ago)
                    
                    # Durée aléatoire entre 1 heure et 4 heures
                    duration_minutes = random.randint(60, 240)
                    
                    visits.append({
                        "date": visit_date.isoformat(),
                        "duration_minutes": duration_minutes
                    })
                
                frequent_locations.append({
                    "id": str(leisure["_id"]),
                    "name": leisure.get("lieu", "Lieu Culturel"),
                    "type": "leisure",
                    "coordinates": coordinates,
                    "address": leisure.get("adresse", ""),
                    "visits": visits
                })
        
        # Ajouter les lieux de beauté/bien-être
        for wellness in selected_wellness:
            # Formater les coordonnées selon leur structure
            coordinates = None
            if "location" in wellness and "coordinates" in wellness["location"]:
                coordinates = wellness["location"]["coordinates"]
            
            if coordinates:
                # Générer entre 1 et 2 visites sur les 30 derniers jours (moins fréquent que les restaurants)
                visits = []
                num_visits = random.randint(1, 2)
                
                for _ in range(num_visits):
                    # Date aléatoire entre aujourd'hui et il y a 30 jours
                    days_ago = random.randint(0, 30)
                    visit_date = datetime.now() - timedelta(days=days_ago)
                    
                    # Durée aléatoire entre 1 heure et 3 heures
                    duration_minutes = random.randint(60, 180)
                    
                    visits.append({
                        "date": visit_date.isoformat(),
                        "duration_minutes": duration_minutes
                    })
                
                frequent_locations.append({
                    "id": str(wellness["_id"]),
                    "name": wellness.get("name", "Lieu de Beauté"),
                    "type": "wellness",
                    "coordinates": coordinates,
                    "address": wellness.get("address", ""),
                    "visits": visits
                })
        
        try:
            # Mettre à jour l'utilisateur
            user_collection.update_one(
                {"_id": user["_id"]},
                {"$set": {"frequent_locations": frequent_locations}}
            )
            logger.info(f"✅ Utilisateur {i+1}/{len(users)}: {len(frequent_locations)} lieux fréquents générés")
        except Exception as e:
            logger.error(f"❌ Erreur lors de la mise à jour des lieux fréquents: {e}")

def establish_wellness_affinities(user, wellness_places, db_connections=None):
    """Établit des affinités avec des lieux de beauté pour l'utilisateur"""
    
    # Augmenter fortement la probabilité d'avoir des intérêts beauté
    if random.random() < 0.85:  # 85% de chance d'avoir des intérêts beauté (augmenté de 50% à 85%)
        # Augmenter le nombre max de lieux appréciés
        max_wellness_places = min(len(wellness_places), random.randint(8, 15))  # Augmenté de 3-7 à 8-15
        
        # Sélectionner des lieux de beauté aléatoires
        selected_places = random.sample(wellness_places, max_wellness_places)
        
        # Pour chaque lieu sélectionné, établir une affinité
        for place in selected_places:
            place_id = str(place["_id"])
            place_name = place.get("name", "Institut de beauté")
            
            # Score d'affinité (entre 0.3 et 1.0) - plus de chance d'avoir un score élevé
            affinity_score = round(random.uniform(0.5, 1.0), 2)  # Augmenté de 0.3-1.0 à 0.5-1.0
            
            # Ajouter aux producteurs d'affinité, avec type = "beauty"
            if "affinity_producers" not in user:
                user["affinity_producers"] = []
            
            user["affinity_producers"].append({
                "producer_id": place_id,
                "producer_name": place_name,
                "score": affinity_score,
                "type": "beauty",
                "date_added": datetime.now().isoformat()
            })
            
            # Si score très élevé (>0.75), ajouter aux intérêts
            if affinity_score > 0.75:  # Seuil baissé de 0.8 à 0.75
                if "interests" not in user:
                    user["interests"] = []
                
                if place_id not in user["interests"]:
                    user["interests"].append(place_id)
            
            # Si score élevé (>0.6), ajouter aux lieux suivis
            if affinity_score > 0.6:  # Seuil baissé de 0.7 à 0.6
                if "followingProducers" not in user:
                    user["followingProducers"] = []
                
                if place_id not in user["followingProducers"]:
                    user["followingProducers"].append(place_id)
                    
                    # Ajouter l'utilisateur aux followers du lieu de beauté
                    if db_connections is not None:
                        try:
                            db_connections["beauty"][COLL_WELLNESS_PLACES].update_one(
                                {"_id": ObjectId(place_id)},
                                {"$addToSet": {"interestedUsers": str(user["_id"])}},
                                upsert=False
                            )
                        except Exception as e:
                            logger.warning(f"⚠️ Erreur lors de l'ajout du follower au lieu de beauté: {e}")
            
            # 40% de chance de l'avoir déjà visité (augmenté)
            if random.random() < 0.40:
                # Ajouter aux lieux fréquents avec localisation
                if "frequent_locations" not in user:
                    user["frequent_locations"] = []
                
                # Créer une visite récente
                recent_days = random.randint(1, 30)
                visit_datetime = datetime.now() - timedelta(days=recent_days)
                
                # Extraire les coordonnées si disponibles
                coordinates = []
                if "gps_coordinates" in place and "coordinates" in place["gps_coordinates"]:
                    coordinates = place["gps_coordinates"]["coordinates"]
                
                # Déterminer la durée de visite (entre 30 et 180 minutes)
                visit_duration = random.randint(30, 180)
                
                user["frequent_locations"].append({
                    "id": place_id,
                    "name": place_name,
                    "type": "wellness",
                    "coordinates": coordinates,
                    "visits": [
                        {
                            "date": visit_datetime.isoformat(),
                            "duration_minutes": visit_duration
                        }
                    ],
                    "total_visits": 1,
                    "last_visit": visit_datetime.isoformat()
                })
    
    return user

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
        
        # Compter le nombre d'utilisateurs existants
        users_count = db_choice[COLL_USERS].count_documents({}) if COLL_USERS in collections_choice else 0
        
        logger.info(f"Connexion établie aux bases de données MongoDB")
        logger.info(f"Utilisateurs actuels: {users_count}")
        
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

def create_users(config):
    """Crée des utilisateurs dans la base de données"""
    # Établir les connexions MongoDB
    db_connections = get_db_connections()
    
    # Définir une graine aléatoire pour la reproductibilité si spécifiée
    if config["seed"] is not None:
        random.seed(config["seed"])
    
    # Collection des utilisateurs
    user_collection = db_connections["choice"][COLL_USERS]
    
    # Liste pour stocker les utilisateurs créés
    created_users = []
    duplicates = 0
    
    logger.info(f"🚀 Génération de {config['users_count']} utilisateurs factices...")
    
    # Préparer la liste des emails existants
    existing_emails = set(u["email"] for u in user_collection.find({}, {"email": 1}))
    
    for i in range(config['users_count']):
        # Générer un profil
        user_profile = generate_user_profile(config, existing_emails)
        
        # Vérifier si l'email existe déjà
        if config["verify_existing"]:
            existing_user = user_collection.find_one({"email": user_profile["email"]})
            
            if existing_user:
                logger.info(f"⚠️ Utilisateur {i+1}/{config['users_count']}: Email déjà existant, génération d'un nouvel utilisateur")
                duplicates += 1
                # Réessayer avec un nouvel utilisateur
                i -= 1
                continue
        
        try:
            # Insérer l'utilisateur dans la base de données
            result = user_collection.insert_one(user_profile)
            user_id = result.inserted_id
            
            # Mettre à jour l'ID de l'utilisateur dans le profil
            user_profile["_id"] = user_id
            created_users.append(user_profile)
            
            logger.info(f"✅ Utilisateur {i+1}/{config['users_count']}: {user_profile['name']} créé avec succès (ID: {user_id})")
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la création de l'utilisateur {i+1}: {e}")
    
    logger.info(f"✅ Création d'utilisateurs terminée: {len(created_users)} créés, {duplicates} doublons évités")
    
    # Établir des connexions sociales entre les utilisateurs
    if len(created_users) > 5:
        establish_social_connections(created_users, db_connections)
    
    # Établir des affinités avec des producteurs
    establish_producer_affinities(created_users, db_connections)
    
    # Établir des affinités avec des lieux de beauté
    for user in created_users:
        wellness_places = list(db_connections["beauty"][COLL_WELLNESS_PLACES].find(
            {"average_score": {"$gte": 3.0}},  # Uniquement les lieux bien notés
            {"_id": 1, "name": 1, "category": 1, "sous_categorie": 1}
        ).limit(50))
        user = establish_wellness_affinities(user, wellness_places, db_connections)
    
    # Générer des localisations fréquentes
    generate_frequent_locations(created_users, db_connections)
    
    return created_users

def main():
    """Point d'entrée du script"""
    parser = argparse.ArgumentParser(description="Générateur d'utilisateurs fictifs pour Choice App")
    parser.add_argument("--count", type=int, default=100, help="Nombre d'utilisateurs à générer")
    parser.add_argument("--test", action="store_true", help="Mode test - génère un utilisateur et l'affiche")
    parser.add_argument("--mongo-uri", type=str, help="URI MongoDB alternative")
    parser.add_argument("--batch-size", type=int, default=20, help="Taille des lots pour l'insertion")
    parser.add_argument("--max-connections", type=int, default=50, help="Nombre maximum de connexions sociales par utilisateur")
    parser.add_argument("--min-locations", type=int, default=2, help="Nombre minimum de localisations fréquentes")
    parser.add_argument("--max-locations", type=int, default=10, help="Nombre maximum de localisations fréquentes")
    parser.add_argument("--location-radius", type=int, default=5000, help="Rayon en mètres autour de Paris pour la génération")
    parser.add_argument("--seed", type=int, help="Graine aléatoire pour la reproductibilité", default=None)
    parser.add_argument("--fake-locations", action="store_true", help="Utiliser des localisations fictives")
    
    args = parser.parse_args()
    
    # Configurer l'URI MongoDB
    if args.mongo_uri:
        global MONGO_URI
        MONGO_URI = args.mongo_uri
    
    # Configuration personnalisée
    config = {
        "establish_social_connections": True,
        "save_to_db": not args.test,
        "generate_locations": True,
        "batch_size": args.batch_size,
        "max_social_connections": args.max_connections,
        "min_locations": args.min_locations,
        "max_locations": args.max_locations,
        "location_radius": args.location_radius,
        "seed": args.seed,
        "users_count": args.count,
        "verify_existing": True,
        "real_locations": not args.fake_locations
    }
    
    start_time = time.time()
    
    if args.test:
        logger.info("Mode test: Génération d'un utilisateur fictif")
        db_connections = get_db_connections() if not args.test else None
        user = generate_fake_user(db_connections, config, 1)
        logger.info(f"Utilisateur généré: {json.dumps(user, default=str, indent=2)}")
    else:
        # Mode normal
        user_count = args.count
        if user_count >= 3000:
            logger.warning(f"⚠️ Génération d'un grand nombre d'utilisateurs ({user_count}) peut prendre du temps")
        
        logger.info(f"🚀 Génération de {user_count} utilisateurs fictifs")
        
        # Établir les connexions MongoDB
        db_connections = get_db_connections()
        
        # Générer et insérer les utilisateurs
        config["users_count"] = user_count
        create_users(config)
    
    elapsed_time = time.time() - start_time
    logger.info(f"✅ Exécution terminée en {elapsed_time:.2f} secondes")

if __name__ == "__main__":
    main()