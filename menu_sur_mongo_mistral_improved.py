#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Extracteur et analyseur de menus de restaurants utilisant GPT
Version optimisée avec une approche multi-phase et un chunking intelligent.

Cette version apporte plusieurs améliorations critiques:
1. Approche multi-phase: identification des sections → extraction des plats → structuration finale
2. Chunks plus petits (800 caractères) pour améliorer la performance
3. Prompts adaptés à chaque étape pour maximiser les performances
4. Système de détection d'erreurs intelligent
5. Support amélioré pour Google Drive et autres types de fichiers
"""

import os
import re
import time
import json
import hashlib
import requests
import tempfile
from bs4 import BeautifulSoup
import fitz  # PyMuPDF
from bson.objectid import ObjectId
from urllib.parse import urljoin
from PIL import Image
import io
import logging
from dotenv import load_dotenv
from pymongo import MongoClient
import openai
import pickle

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("menu_processor_gpt_enhanced.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Ajouter le chemin scripts/Restauration/ au PYTHONPATH
import sys
import os
restauration_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'Restauration')
if restauration_path not in sys.path:
    sys.path.append(restauration_path)

# ---- Configuration des répertoires ----
TMP_DIR = "tmp_files"
TMP_PDF_DIR = os.path.join(TMP_DIR, "pdf")
TMP_IMG_DIR = os.path.join(TMP_DIR, "img")
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "api_cache")
CHECKPOINTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "checkpoints")

# Création de tous les répertoires nécessaires
for directory in [TMP_DIR, TMP_PDF_DIR, TMP_IMG_DIR, CACHE_DIR, CHECKPOINTS_DIR]:
    os.makedirs(directory, exist_ok=True)

# ---- Fonctions utilitaires pour cache et checkpoints ----
def sanitize_filename(filename):
    """
    Nettoie un nom de fichier pour qu'il soit valide dans le système de fichiers
    - Remplace les caractères spéciaux par '_'
    - Tronque les noms trop longs avec un hash
    """
    # Remplacer les caractères non autorisés dans les noms de fichiers
    import re
    safe_name = re.sub(r'[\\/*?:"<>|]', '_', str(filename))
    safe_name = safe_name.replace('http://', 'http_').replace('https://', 'https_')
    safe_name = safe_name.replace('/', '_').replace('\\', '_')
    
    # Limiter la longueur du nom de fichier
    if len(safe_name) > 100:
        # Générer un hash pour la partie tronquée
        hash_suffix = hashlib.md5(filename.encode()).hexdigest()[:10]
        safe_name = f"{safe_name[:50]}_{hash_suffix}"
    
    return safe_name

def get_from_cache(key, max_age_hours=24, prefix=""):
    """Récupère une valeur depuis le cache si elle existe et n'est pas expirée"""
    # Ajouter le préfixe au nom du fichier et le nettoyer
    raw_filename = f"{prefix}_{key}" if prefix else str(key)
    safe_filename = sanitize_filename(raw_filename)
    cache_file = os.path.join(CACHE_DIR, f"{safe_filename}.json")
    
    if os.path.exists(cache_file):
        file_age_hours = (time.time() - os.path.getmtime(cache_file)) / 3600
        
        # Vérifier si le cache est expiré
        if file_age_hours <= max_age_hours:
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Erreur lors de la lecture du cache: {e}")
                
    return None

def save_to_cache(key, value, prefix=""):
    """Sauvegarde une valeur dans le cache"""
    # Ajouter le préfixe au nom du fichier et le nettoyer
    raw_filename = f"{prefix}_{key}" if prefix else str(key)
    safe_filename = sanitize_filename(raw_filename)
    cache_file = os.path.join(CACHE_DIR, f"{safe_filename}.json")
    
    try:
        # S'assurer que le répertoire parent existe
        os.makedirs(os.path.dirname(cache_file), exist_ok=True)
        
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(value, f, ensure_ascii=False)
        return True
    except Exception as e:
        logger.warning(f"Erreur lors de l'écriture dans le cache: {e}")
        return False

def save_checkpoint(checkpoint_data, checkpoint_name="gpt_progress"):
    """Sauvegarde l'état d'avancement pour pouvoir reprendre après déconnexion"""
    checkpoint_file = os.path.join(CHECKPOINTS_DIR, f"{checkpoint_name}.pkl")
    
    try:
        with open(checkpoint_file, 'wb') as f:
            pickle.dump(checkpoint_data, f)
        logger.info(f"Checkpoint sauvegardé dans {checkpoint_file}")
        return True
    except Exception as e:
        logger.warning(f"Erreur lors de la sauvegarde du checkpoint: {e}")
        return False

def load_checkpoint(checkpoint_name="gpt_progress"):
    """Charge le dernier état d'avancement sauvegardé"""
    checkpoint_file = os.path.join(CHECKPOINTS_DIR, f"{checkpoint_name}.pkl")
    
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, 'rb') as f:
                checkpoint_data = pickle.load(f)
            logger.info(f"Checkpoint chargé depuis {checkpoint_file}")
            return checkpoint_data
        except Exception as e:
            logger.warning(f"Erreur lors du chargement du checkpoint: {e}")
            
    logger.info("Aucun checkpoint trouvé, démarrage depuis le début")
    return None

# Charger les variables d'environnement
load_dotenv()

# Configuration OpenAI
# Clé API OpenAI en dur pour le test
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# En production, il faudrait plutôt utiliser:
# OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

# Déterminer la version du client OpenAI et configurer en conséquence
client = None
OPENAI_API_VERSION = "unknown"

try:
    # Tenter d'initialiser avec la nouvelle API (v1.0+)
    if hasattr(openai, 'OpenAI'):
        client = openai.OpenAI(api_key=OPENAI_API_KEY)
        OPENAI_API_VERSION = "v1"
        logger.info("Utilisation de l'API OpenAI v1.0+")
    else:
        # Fallback vers l'ancienne API (v0.28.x)
        openai.api_key = OPENAI_API_KEY
        OPENAI_API_VERSION = "legacy"
        logger.info("Utilisation de l'API OpenAI legacy (v0.28.x)")
except Exception as e:
    logger.warning(f"Erreur lors de l'initialisation d'OpenAI: {e}. Utilisation du mode legacy.")
    try:
        # Dernière tentative avec méthode legacy
        openai.api_key = OPENAI_API_KEY
        OPENAI_API_VERSION = "legacy"
        logger.info("Fallback: utilisation de l'API OpenAI legacy")
    except Exception as e2:
        logger.error(f"Échec de l'initialisation d'OpenAI dans tous les modes: {e2}")

# --- Configuration MongoDB ---
# URI MongoDB en dur pour le test
MONGO_URI = "mongodb+srv://remibarbier:Calvi8Pierc2@lieuxrestauration.szq31.mongodb.net/?retryWrites=true&w=majority&appName=lieuxrestauration"
DB_NAME = "Restauration_Officielle"
COLLECTION_NAME = "producers"

# Clé API Google Cloud Vision en dur pour le test
GOOGLE_VISION_API_KEY = os.getenv("GOOGLE_VISION_API_KEY")

# --- Fonctions de base de données ---
def get_db_connection():
    """Établit une connexion à MongoDB"""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    logger.info(f"Connexion établie à MongoDB: {DB_NAME}.{COLLECTION_NAME}")
    return db, collection

# Établir la connexion à MongoDB
db, collection = get_db_connection()

# --- Fonction OpenAI pour génération de texte ---
def generate_ai_response_gpt(prompt, max_tokens=1024, temperature=0.7, model="gpt-3.5-turbo", retry_limit=3):
    """
    Génère une réponse en utilisant l'API OpenAI (GPT).
    
    Args:
        prompt (str): Le prompt à envoyer au modèle
        max_tokens (int): Nombre maximum de tokens à générer
        temperature (float): Température pour le sampling (0.0 = déterministe, > 0.0 = plus créatif)
        model (str): Modèle GPT à utiliser (par défaut: gpt-3.5-turbo)
        retry_limit (int): Nombre maximum de tentatives en cas d'erreur
        
    Returns:
        str: La réponse générée ou une chaîne vide en cas d'échec
    """
    # Calculer un hash du prompt pour le caching
    prompt_hash = hashlib.md5(prompt.encode()).hexdigest()
    cache_key = f"gpt_{prompt_hash}_{max_tokens}_{temperature}_{model}"
    
    # Vérifier dans le cache d'abord
    cached_response = get_from_cache(cache_key, max_age_hours=72, prefix="ai_responses")
    if cached_response:
        logger.info("Utilisation d'une réponse mise en cache pour économiser du calcul")
        return cached_response
    
    attempt = 0
    while attempt < retry_limit:
        try:
            # Utiliser l'API appropriée selon la version
            if OPENAI_API_VERSION == "v1":
                # Nouvelle API (v1.0+)
                response = client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": "Vous êtes un assistant expert en analyse de menus de restaurants."},
                        {"role": "user", "content": prompt}
                    ],
                    max_tokens=max_tokens,
                    temperature=temperature
                )
                # Extraire la réponse
                result = response.choices[0].message.content.strip()
            else:
                # Ancienne API (v0.28.x)
                response = openai.ChatCompletion.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": "Vous êtes un assistant expert en analyse de menus de restaurants."},
                        {"role": "user", "content": prompt}
                    ],
                    max_tokens=max_tokens,
                    temperature=temperature
                )
                # Extraire la réponse
                result = response.choices[0].message['content'].strip()
            
            # Sauvegarder dans le cache
            save_to_cache(cache_key, result, prefix="ai_responses")
            
            return result
        
        except Exception as e:
            attempt += 1
            error_msg = str(e)
            
            # Log l'erreur et attendre avant de réessayer
            logger.warning(f"Erreur lors de l'appel à l'API GPT (tentative {attempt}/{retry_limit}): {error_msg}")
            
            # Attendre de plus en plus longtemps entre les tentatives
            if attempt < retry_limit:
                sleep_time = 2 ** attempt  # Backoff exponentiel: 2, 4, 8, 16...
                logger.info(f"Nouvelle tentative dans {sleep_time} secondes...")
                time.sleep(sleep_time)
    
    logger.error(f"Échec de l'appel à GPT après {retry_limit} tentatives")
    return ""

# --- Fonctions pour les requêtes API ---
def make_api_request(url, params=None, method="GET", timeout=10, retries=3, backoff_factor=2):
    """
    Effectue une requête API avec gestion des erreurs et retry.
    
    Args:
        url (str): URL de l'API
        params (dict): Paramètres de la requête
        method (str): Méthode HTTP ("GET" ou "POST")
        timeout (int): Délai d'attente en secondes
        retries (int): Nombre de tentatives en cas d'échec
        backoff_factor (int): Facteur d'attente entre les tentatives
    
    Returns:
        dict/None: Réponse JSON ou None en cas d'échec
    """
    attempt = 0
    while attempt < retries:
        try:
            if method.upper() == "GET":
                response = requests.get(url, params=params, timeout=timeout)
            else:  # POST
                response = requests.post(url, json=params, timeout=timeout)
            
            response.raise_for_status()
            return response.json()
        
        except requests.exceptions.Timeout:
            attempt += 1
            logger.warning(f"Timeout lors de la tentative {attempt} pour {url}. Réessayer...")
            time.sleep(backoff_factor * attempt)
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Erreur lors de la connexion à {url}: {e}")
            break
    
    logger.error(f"Échec de la requête API après {retries} tentatives pour {url}")
    return None

# --- Fonctions Utilitaires ---
def is_valid_url(url):
    """Vérifie si une URL est valide et bien formée."""
    if not url:
        return False
    
    # Vérifier le format de base de l'URL
    if not isinstance(url, str):
        return False
    
    # Vérifier que l'URL commence par http:// ou https://
    if not url.startswith(('http://', 'https://')):
        return False
    
    # Vérifier la syntaxe de l'URL avec une expression régulière basique
    url_pattern = re.compile(
        r'^(?:http|https)://'  # http:// ou https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domaine
        r'localhost|'  # localhost
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ou adresse IP
        r'(?::\d+)?'  # port optionnel
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    
    return url_pattern.match(url) is not None

def fetch_restaurant_websites(limit=3, processed_only=True):
    """
    Récupère les restaurants ayant un site web non vide et valide dans MongoDB.
    Avec l'option processed_only=False, récupère seulement les restaurants
    sans menus_structures.
    """
    try:
        # Requête pour récupérer les restaurants avec un site web non vide et non null
        query = {
            "website": {
                "$exists": True,
                "$nin": ["", None]
            }
        }
        if not processed_only:
            query["menus_structures"] = {"$exists": False}
        
        all_restaurants = list(collection.find(query, {"_id": 1, "name": 1, "website": 1, "rating": 1}).limit(limit))
        
        # Filtrer pour garder uniquement les restaurants avec des URLs valides
        restaurants = []
        for r in all_restaurants:
            if is_valid_url(r.get("website")):
                restaurants.append(r)
            else:
                logger.warning(f"Restaurant ignoré - URL invalide: {r.get('name')} - URL: {r.get('website')}")
        
        logger.info(f"Trouvé {len(restaurants)} restaurants avec un site web valide")
        
        # Afficher les 3 premiers sites web pour debug
        for i, r in enumerate(restaurants[:3]):
            logger.info(f"Restaurant {i+1}: {r.get('name')} - Site web: {r.get('website')}")
        
        return restaurants
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des restaurants : {e}")
        return []

def extract_links_from_website(url, retries=3, backoff_factor=2):
    """
    Extrait tous les liens d'un site web (PDFs, plateformes externes, images) avec gestion de cache et des retries.
    """
    if not is_valid_url(url):
        logger.error(f"URL invalide, impossible d'extraire les liens: {url}")
        return []

    cache_key = f"links_{url}"
    cached_links = get_from_cache(cache_key, max_age_hours=168, prefix="websites")
    if cached_links:
        return cached_links

    external_platforms = [
        'drive.google.com', 'dropbox.com', 'docdroid.net',
        'calameo.com', 'issuu.com', 'yumpu.com'
    ]

    attempt = 0
    while attempt < retries:
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
            }
            response = requests.get(url, headers=headers, timeout=20)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")

            links = []

            # Liens classiques <a href=...>
            for link in soup.find_all("a", href=True):
                href = link["href"]
                text = link.get_text(strip=True)

                if href.startswith("/"):
                    href = urljoin(url, href)

                if href.lower().endswith(".pdf") or any(platform in href.lower() for platform in external_platforms):
                    logger.info(f"🔗 Lien vers menu détecté : {href}")
                    links.append({"href": href, "text": text})

            # Images <img src=...>
            for img in soup.find_all("img", src=True):
                img_src = img["src"]
                if not img_src.startswith(("http", "https", "data:")):
                    img_src = urljoin(url, img_src)
                img_alt = img.get("alt", "")
                links.append({"href": img_src, "text": img_alt})

            # Iframes <iframe src=...> (souvent menus embedded type Issuu/Calameo)
            for iframe in soup.find_all("iframe", src=True):
                src = iframe["src"]
                if any(platform in src.lower() for platform in external_platforms):
                    full_url = urljoin(url, src)
                    logger.info(f"🖼️ Iframe vers menu détectée : {full_url}")
                    links.append({"href": full_url, "text": "iframe"})

            save_to_cache(cache_key, links, prefix="websites")
            return links

        except requests.exceptions.Timeout:
            attempt += 1
            logger.warning(f"Timeout lors de la tentative {attempt} pour {url}. Réessayer...")
            time.sleep(backoff_factor * attempt)
        except requests.exceptions.RequestException as e:
            logger.error(f"Erreur lors de la connexion à {url}: {e}")
            break

    logger.error(f"Échec de l'extraction des liens après {retries} tentatives pour {url}")
    return []

def filter_menu_links(all_links, base_url):
    """
    Filtre les liens pour ne conserver que ceux liés aux menus et complète les liens relatifs.
    Détecte aussi les liens vers des images qui pourraient être des menus.
    Version améliorée avec une meilleure détection des formats pdf et images, et exclusion des SVG.
    """
    menu_links = []
    seen_links = set()
    
    # Mots-clés pour le menu - élargi pour meilleure détection
    menu_keywords = [
        "menu", "carte", "plats", "boissons", "pdf", 
        "dejeuner", "diner", "déjeuner", "dîner", "formule",
        "nos plats", "nos spécialités", "à la carte", "notre cuisine",
        "entrées", "plats principaux", "desserts", "boissons",
        "tarifs", "prix", "emporter", "livraison", "voir le menu", "notre carte", # Nouveaux mots-clés
        "menu du jour", "suggestions"
    ]
    
    # Extensions d'images potentiellement utiles
    image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.webp']
    
    # Extensions de documents
    document_extensions = ['.pdf', '.doc', '.docx']
    
    for link in all_links:
        href = link.get("href", "") # Utiliser .get avec défaut
        text = link.get("text", "").lower() # Utiliser .get avec défaut
        
        # Ignorer les liens vides ou ancres
        if not href or href.startswith('#'):
            continue
            
        # Ignorer explicitement les data URLs SVG
        if href.startswith("data:image/svg+xml"):
            continue
            
        # Convertir en URL absolue si c'est un lien relatif
        if not href.startswith(("http", "https", "data:")): # Garder data: pour les images base64 potentielles
            try:
                href = urljoin(base_url, href)
            except ValueError:
                 logger.warning(f"Impossible de joindre base_url '{base_url}' et href '{link.get('href')}'")
                 continue # Ignorer si l'URL ne peut être formée
        
        # Éviter les doublons après normalisation de l'URL
        if href in seen_links:
            continue
            
        is_relevant = False
        href_lower = href.lower()
        
        # 1. Vérifier les mots-clés de menu dans le texte du lien ou l'URL
        if any(keyword in text or keyword in href_lower for keyword in menu_keywords):
            is_relevant = True
        
        # 2. Vérifier si c'est une image qui pourrait être un menu (basé sur contexte)
        # Ne pas considérer les data:svg comme image ici
        if any(href_lower.endswith(ext) for ext in image_extensions) or href.startswith("data:image/"):
            # Si le texte (alt) évoque un menu ou la carte
            if any(keyword in text for keyword in menu_keywords):
                is_relevant = True
            # Ou si le nom de fichier (href) a "menu", "carte", etc.
            elif any(keyword in href_lower for keyword in ["menu", "carte", "tarif"]):
                is_relevant = True
                
        # 3. Vérifier si c'est un document (PDF, DOC, etc.)
        if any(href_lower.endswith(ext) for ext in document_extensions):
             # Les documents sont souvent pertinents s'ils contiennent les mots-clés
            if any(keyword in href_lower or keyword in text for keyword in menu_keywords):
                is_relevant = True
        
        # 4. Cas spécial: Google Drive et autres plateformes de partage
        if any(platform in href_lower for platform in ['drive.google.com', 'dropbox.com', 'docdroid.net']):
            # Pertinent si le lien ou le texte contient des mots-clés
            if any(keyword in href_lower or keyword in text for keyword in menu_keywords):
                is_relevant = True
        
        # Ajouter le lien s'il est pertinent
        if is_relevant:
            seen_links.add(href)
            menu_links.append({"href": href, "text": link["text"]})
    
    return menu_links

def extract_text_from_google_drive(url):
    """
    Extrait le texte d'un document Google Drive.
    Convertit l'URL de partage en lien direct de téléchargement.
    Gère les autorisations et les accès restreints.
    
    Args:
        url (str): L'URL Google Drive (format standard de partage)
        
    Returns:
        str: Le texte extrait du document
    """
    try:
        # Vérifier si c'est une URL Google Drive
        if not ('drive.google.com' in url.lower() or 'docs.google.com' in url.lower()):
            logger.warning(f"L'URL fournie n'est pas une URL Google Drive: {url}")
            return ""
        
        # Extraire l'ID du document
        file_id = None
        
        # Format: https://drive.google.com/file/d/{FILE_ID}/view
        file_match = re.search(r"/file/d/([a-zA-Z0-9_-]+)", url)
        if file_match:
            file_id = file_match.group(1)
        
        # Format: https://drive.google.com/open?id={FILE_ID}
        elif "open?id=" in url:
            file_id = url.split("open?id=")[1].split("&")[0]
        
        # Format: https://docs.google.com/document/d/{FILE_ID}/edit
        elif "/document/d/" in url:
            file_id = url.split("/document/d/")[1].split("/")[0]
        
        # Format: https://drive.google.com/uc?id={FILE_ID}
        elif "uc?id=" in url or "uc?export=download&id=" in url:
            if "id=" in url:
                file_id = url.split("id=")[1].split("&")[0]
        
        if not file_id:
            logger.error(f"Impossible d'extraire l'ID du document Google Drive: {url}")
            return ""
        
        # Construire l'URL de téléchargement direct
        download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
        
        logger.info(f"Téléchargement du document Google Drive: {download_url}")
        
        # Télécharger le fichier avec un User-Agent
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Première requête pour vérifier le type et la taille
        response = requests.get(download_url, headers=headers, stream=True, timeout=30)
        response.raise_for_status()
        
        # Vérifier si c'est une page de confirmation (fichier volumineux)
        if "Content-Disposition" not in response.headers and "confirm=" in response.text:
            # Extraire le code de confirmation
            confirm_match = re.search(r'confirm=([0-9A-Za-z]+)', response.text)
            if confirm_match:
                confirm_code = confirm_match.group(1)
                download_url = f"{download_url}&confirm={confirm_code}"
                
                # Nouvelle requête avec le code de confirmation
                response = requests.get(download_url, headers=headers, timeout=30)
                response.raise_for_status()
        
        # Déterminer le type de contenu
        content_type = response.headers.get('content-type', '').lower()
        
        # Traiter selon le type de contenu
        if 'pdf' in content_type or url.lower().endswith('.pdf'):
            # Sauvegarder le PDF temporairement
            temp_path = os.path.join(TMP_PDF_DIR, f"gdrive_{file_id}.pdf")
            with open(temp_path, 'wb') as f:
                f.write(response.content)
            
            # Extraire le texte du PDF
            try:
                pdf = fitz.open(temp_path)
                text = "".join(page.get_text() for page in pdf)
                pdf.close()
                
                # Si peu de texte, essayer OCR
                if len(text.strip()) < 200:
                    text = extract_text_from_pdf(f"file://{temp_path}")
                
                return text
            except Exception as e:
                logger.error(f"Erreur lors de l'extraction PDF Google Drive: {e}")
                # Tentative d'OCR direct sur le PDF
                return extract_text_from_pdf(f"file://{temp_path}")
            
        elif 'image' in content_type or any(url.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif']):
            # Sauvegarder l'image temporairement
            temp_path = os.path.join(TMP_IMG_DIR, f"gdrive_{file_id}.jpg")
            with open(temp_path, 'wb') as f:
                f.write(response.content)
                
            # OCR sur l'image
            return extract_text_from_image(f"file://{temp_path}")
            
        elif 'text/plain' in content_type:
            return response.text
            
        elif 'html' in content_type:
            # Utiliser BeautifulSoup pour extraire le texte
            soup = BeautifulSoup(response.content, 'html.parser')
            return soup.get_text(separator="\n", strip=True)
            
        elif 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' in content_type or url.lower().endswith('.docx'):
            # C'est un document Word
            try:
                # Sauvegarder temporairement
                temp_path = os.path.join(TMP_DIR, f"gdrive_{file_id}.docx")
                with open(temp_path, 'wb') as f:
                    f.write(response.content)
                
                # Utiliser docx2txt si disponible
                try:
                    import docx2txt
                    text = docx2txt.process(temp_path)
                    return text
                except ImportError:
                    # Alternative: essayer d'extraire du texte via OCR
                    logger.warning("Module docx2txt non disponible, tentative OCR")
                    return extract_text_from_image(f"file://{temp_path}")
            except Exception as e:
                logger.error(f"Erreur lors de l'extraction DOCX: {e}")
                return ""
        else:
            # Type inconnu, essayer d'extraire comme données binaires
            temp_path = os.path.join(TMP_DIR, f"gdrive_{file_id}.bin")
            with open(temp_path, 'wb') as f:
                f.write(response.content)
            
            # Essayer les différentes méthodes d'extraction
            try:
                # Essayer PDF
                pdf = fitz.open(temp_path)
                text = "".join(page.get_text() for page in pdf)
                pdf.close()
                return text
            except Exception:
                try:
                    # Essayer OCR
                    return extract_text_from_image(f"file://{temp_path}")
                except Exception as e:
                    logger.error(f"Échec de l'extraction du document Google Drive: {e}")
                    return ""
    
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction du document Google Drive: {e}")
        return ""

def extract_text_from_link(url):
    """
    Extrait le texte d'un lien, qu'il soit PDF, HTML ou image.
    Détecte automatiquement le type de contenu.
    Version améliorée avec meilleure gestion des erreurs et des types de contenu.
    """
    # Ignorer directement les SVG Data URLs qui ne sont pas supportées
    if url.startswith("data:image/svg+xml"):
        logger.warning(f"Type de data URL non supporté (SVG) ignoré : {url[:60]}...")
        return ""
        
    if not is_valid_url(url) and not url.startswith("file://") and not url.startswith("data:"):
        logger.error(f"URL invalide ou non supportée, impossible d'extraire le texte: {url}")
        return ""
    
    # Vérifier le cache
    cache_key = f"text_{url}"
    cached_text = get_from_cache(cache_key, max_age_hours=168, prefix="menu_text")
    if cached_text:
        return cached_text
    
    # Nombre maximal de tentatives en cas d'échec
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            # Cas spécial: Google Drive
            if 'drive.google.com' in url.lower() or 'docs.google.com' in url.lower():
                logger.info(f"Extraction d'un document Google Drive: {url}")
                text = extract_text_from_google_drive(url)
                if text:
                    # Nettoyer et sauvegarder dans le cache
                    text = re.sub(r'\n{3,}', '\n\n', text)
                    save_to_cache(cache_key, text, prefix="menu_text")
                    return text
            
            if url.startswith("file://"):
                # Cas d'un fichier local
                local_path = url[7:]  # Enlever le préfixe "file://"
                
                if local_path.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.webp')):
                    text = extract_text_from_image(url)
                elif local_path.lower().endswith('.pdf'):
                    pdf = fitz.open(local_path)
                    text = "".join(page.get_text() for page in pdf)
                    pdf.close()
                else:
                    with open(local_path, 'r', encoding='utf-8', errors='ignore') as f:
                        text = f.read()
            
            else:
                # Cas d'une URL distante
                # Obtenir les en-têtes pour vérifier le type de contenu
                try:
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                    }
                    response = requests.head(url, headers=headers, timeout=15)
                    content_type = response.headers.get('content-type', '').lower()
                except Exception:
                    # Si on ne peut pas obtenir le content-type, on devine à partir de l'extension
                    content_type = ''
                
                # Déterminer le type de contenu et extraire le texte en conséquence
                if url.lower().endswith(".pdf") or "application/pdf" in content_type:
                    text = extract_text_from_pdf(url)
                elif url.startswith("data:image"):
                    # Cas d'une image encodée en base64
                    text = extract_text_from_data_url(url)
                elif (url.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.webp')) or 
                    any(img_type in content_type for img_type in ['image/jpeg', 'image/png', 'image/gif', 'image/webp'])):
                    text = extract_text_from_image(url)
                else:
                    text = extract_text_from_html(url)
            
            if text:
                # Nettoyer le texte (supprimer les lignes vides multiples, etc.)
                text = re.sub(r'\n{3,}', '\n\n', text)
                
                # Sauvegarder dans le cache
                save_to_cache(cache_key, text, prefix="menu_text")
                
                return text
            else:
                # Si le texte est vide, essayer une autre méthode
                logger.warning(f"Extraction sans résultat pour {url}, tentative {attempt+1}/{max_retries}")
                
                # Si c'est la dernière tentative, essayer OCR en dernier recours
                if attempt == max_retries - 1 and not url.startswith("file://"):
                    try:
                        # Télécharger le contenu et essayer OCR comme dernier recours
                        headers = {
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                        }
                        response = requests.get(url, headers=headers, timeout=30)
                        response.raise_for_status()
                        
                        # Sauvegarder temporairement
                        with tempfile.NamedTemporaryFile(delete=False, suffix=".bin") as temp_file:
                            temp_file.write(response.content)
                            temp_path = temp_file.name
                        
                        # Essayer OCR
                        text = extract_text_from_image(f"file://{temp_path}")
                        
                        # Nettoyer
                        os.unlink(temp_path)
                        
                        if text:
                            save_to_cache(cache_key, text, prefix="menu_text")
                            return text
                    except Exception as e:
                        logger.error(f"Échec de la tentative OCR finale pour {url}: {e}")
                
                # Attendre un peu avant de réessayer
                time.sleep(1)
        
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Erreur lors de l'extraction du texte de {url} (tentative {attempt+1}/{max_retries}): {e}")
                time.sleep(2)  # Pause avant de réessayer
            else:
                logger.error(f"Échec final de l'extraction du texte de {url}: {e}")
    
    return ""

def extract_text_from_data_url(data_url):
    """Extrait le texte d'une URL de données (data URL) contenant une image."""
    try:
        # Extraire le type MIME et les données encodées
        header, encoded = data_url.split(",", 1)
        import base64
        
        # Décoder les données base64
        if ";base64" in header:
            decoded = base64.b64decode(encoded)
        else:
            import urllib.parse
            decoded = urllib.parse.unquote_to_bytes(encoded)
        
        # Créer un fichier temporaire pour l'image
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_file:
            temp_file.write(decoded)
            temp_path = temp_file.name
        
        # Utiliser extract_text_from_image avec le chemin du fichier temporaire
        text = extract_text_from_image(f"file://{temp_path}")
        
        # Supprimer le fichier temporaire
        os.unlink(temp_path)
        
        return text
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction du texte depuis data URL: {e}")
        return ""

def extract_text_from_html(url):
    """
    Extrait le texte brut d'une page HTML avec focus sur les sections pertinentes pour les menus.
    Version améliorée avec extraction ciblée des sections de menu.
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7'
        }
        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()
        
        # Détecter l'encodage
        encoding = response.encoding
        
        soup = BeautifulSoup(response.content, "html.parser")
        
        # Supprimer les éléments non pertinents
        for element in soup.find_all(['script', 'style', 'nav', 'footer', 'header', 'aside']):
            element.decompose()
        
        # Stratégie 1: Recherche d'éléments avec des classes/IDs spécifiques aux menus
        menu_content = ""
        
        # Priorité 1: Éléments explicitement identifiés comme des menus
        menu_elements = soup.find_all(id=lambda x: x and any(keyword in x.lower() 
                                                  for keyword in ['menu', 'carte', 'food', 'dish', 'plat']))
        menu_elements.extend(soup.find_all(class_=lambda x: x and any(keyword in x.lower() 
                                                         for keyword in ['menu', 'carte', 'food', 'dish', 'plat'])))
        
        if menu_elements:
            for element in menu_elements:
                menu_content += element.get_text(separator="\n", strip=True) + "\n\n"
        
        # Priorité 2: Sections avec titres liés aux menus
        if not menu_content:
            headers = soup.find_all(['h1', 'h2', 'h3'], string=lambda x: x and any(keyword in x.lower() 
                                                                    for keyword in ['menu', 'carte', 'nos plats', 'formule']))
            for header in headers:
                # Extraire la section qui suit le titre
                section_content = ""
                for sibling in header.next_siblings:
                    if sibling.name in ['h1', 'h2', 'h3']:
                        break
                    if sibling.name:
                        section_content += sibling.get_text(separator="\n", strip=True) + "\n"
                
                if section_content:
                    menu_content += header.get_text(strip=True) + "\n" + section_content + "\n\n"
        
        # Priorité 3: Éléments de liste qui ressemblent à des menus (avec prix)
        if not menu_content:
            price_pattern = re.compile(r'\d+[.,]?\d*\s*(?:€|\$|EUR|euros?)')
            
            for list_element in soup.find_all(['ul', 'ol', 'dl', 'table']):
                list_text = list_element.get_text(separator="\n", strip=True)
                # Si le texte contient des prix, probablement un menu
                if price_pattern.search(list_text):
                    menu_content += list_text + "\n\n"
        
        # Fallback: Utiliser le contenu entier si aucun contenu de menu n'a été trouvé
        if not menu_content:
            # Diviser en paragraphes et filtrer ceux qui pourraient contenir des informations de menu
            potential_menu_content = []
            price_pattern = re.compile(r'\d+[.,]?\d*\s*(?:€|\$|EUR|euros?)')
            
            # Parcourir tous les paragraphes et rechercher des patterns de menu
            for p in soup.find_all('p'):
                p_text = p.get_text(strip=True)
                if price_pattern.search(p_text) or re.search(r'\b(?:menu|entrée|plat|dessert|boisson)\b', p_text.lower()):
                    potential_menu_content.append(p_text)
            
            if potential_menu_content:
                menu_content = '\n\n'.join(potential_menu_content)
            else:
                # Dernier recours: tout le contenu de la page
                menu_content = soup.get_text(separator="\n", strip=True)
        
        return menu_content
    except Exception as e:
        logger.error(f"[ERREUR] Problème lors de l'extraction HTML ({url}) : {e}")
        return ""

def extract_text_from_image(image_url):
    """
    Extrait le texte d'une image de menu en utilisant l'OCR.
    Version améliorée avec prétraitement d'image et multiples méthodes d'OCR.
    """
    # Vérifier si la fonctionnalité IA est activée
    if not AI_ENABLED:
        logger.info("La fonctionnalité IA est désactivée. OCR non disponible pour l'image.")
        return ""
        
    try:
        # Créer un nom de fichier unique basé sur l'URL
        img_filename = hashlib.md5(image_url.encode()).hexdigest() + ".jpg"
        img_path = os.path.join(TMP_IMG_DIR, img_filename)
        
        # Télécharger l'image si non présente
        if not os.path.exists(img_path):
            if image_url.startswith("file://"):
                local_path = image_url[7:]  # Enlever le préfixe "file://"
                if os.path.exists(local_path):
                    # Copier le fichier au lieu de le télécharger
                    from shutil import copyfile
                    copyfile(local_path, img_path)
                else:
                    logger.error(f"Fichier local non trouvé: {local_path}")
                    return ""
            else:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
                response = requests.get(image_url, headers=headers, timeout=20)
                response.raise_for_status()
                with open(img_path, "wb") as f:
                    f.write(response.content)
        
        # Méthode 1: Google Cloud Vision API (si clé disponible)
        if GOOGLE_VISION_API_KEY:
            try:
                from google.cloud import vision
                
                client = vision.ImageAnnotatorClient()
                
                with open(img_path, "rb") as image_file:
                    content = image_file.read()
                
                image = vision.Image(content=content)
                response = client.text_detection(image=image)
                texts = response.text_annotations
                
                if texts:
                    # Le premier élément contient tout le texte
                    extracted_text = texts[0].description
                    logger.info(f"Texte extrait avec Google Vision: {len(extracted_text)} caractères")
                    return extracted_text
            
            except Exception as e:
                logger.error(f"Erreur lors de l'extraction avec Google Vision: {e}")
        
        # Méthode 2: Utiliser pytesseract (OCR local) avec prétraitement avancé
        try:
            import pytesseract
            from PIL import Image, ImageEnhance, ImageFilter
            
            img = Image.open(img_path)
            
            # Prétraitement de l'image pour améliorer l'OCR
            # 1. Redimensionner l'image si trop grande pour améliorer la précision
            if max(img.width, img.height) > 3000:
                ratio = 3000 / max(img.width, img.height)
                new_width = int(img.width * ratio)
                new_height = int(img.height * ratio)
                img = img.resize((new_width, new_height), Image.LANCZOS)
            
            # 2. Conversion en niveaux de gris
            img_gray = img.convert('L')
            
            # 3. Augmentation du contraste
            enhancer = ImageEnhance.Contrast(img_gray)
            img_contrast = enhancer.enhance(2.0)
            
            # 4. Netteté améliorée
            img_sharp = img_contrast.filter(ImageFilter.SHARPEN)
            
            # 5. Binarisation pour aider à la détection du texte
            threshold = 150
            img_bin = img_sharp.point(lambda p: 255 if p > threshold else 0)
            
            # Effectuer l'OCR avec les différentes versions prétraitées
            # commençant par la plus sophistiquée
            ocr_results = []
            
            # Version 1: Image avec netteté et binarisation
            ocr_results.append(pytesseract.image_to_string(img_bin, lang='fra+eng'))
            
            # Version 2: Image avec contraste amélioré
            ocr_results.append(pytesseract.image_to_string(img_contrast, lang='fra+eng'))
            
            # Version 3: Image en niveaux de gris simple
            ocr_results.append(pytesseract.image_to_string(img_gray, lang='fra+eng'))
            
            # Version 4: Image originale
            ocr_results.append(pytesseract.image_to_string(img, lang='fra+eng'))
            
            # Trouver la meilleure version (celle avec le plus de texte)
            best_text = max(ocr_results, key=lambda x: len(x.strip()) if x else 0)
            
            if best_text and len(best_text.strip()) > 50:
                logger.info(f"Texte extrait avec Tesseract (meilleure version): {len(best_text)} caractères")
                return best_text
        
        except Exception as e:
            logger.error(f"Erreur lors de l'extraction avec Tesseract: {e}")
        
        logger.warning(f"Aucune méthode d'OCR n'a pu extraire du texte de {image_url}")
        return ""
    
    except Exception as e:
        logger.error(f"Erreur générale lors de l'extraction d'image ({image_url}): {e}")
        return ""

def extract_text_from_pdf(pdf_url):
    """
    Télécharge et extrait le texte brut d'un PDF, avec OCR pour les PDF scannés.
    Version améliorée avec meilleure détection des PDFs scannés.
    """
    try:
        # Créer un nom de fichier unique basé sur l'URL
        pdf_filename = hashlib.md5(pdf_url.encode()).hexdigest() + ".pdf"
        pdf_path = os.path.join(TMP_PDF_DIR, pdf_filename)
        
        # Télécharger le PDF si non présent
        if not os.path.exists(pdf_path):
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(pdf_url, headers=headers, timeout=20)
            response.raise_for_status()
            with open(pdf_path, "wb") as f:
                f.write(response.content)
        
        # Extraire le texte normalement
        pdf = fitz.open(pdf_path)
        text = ""
        
        # Vérifier chaque page
        for page_num in range(len(pdf)):
            page = pdf[page_num]
            page_text = page.get_text()
            
            # Si la page a peu de texte, elle est probablement scannée ou contient une image
            if len(page_text.strip()) < 100:
                logger.info(f"Page {page_num+1} du PDF avec peu de texte, tentative d'OCR...")
                
                # Extraire l'image de la page
                pix = page.get_pixmap(dpi=300)
                img_path = os.path.join(TMP_IMG_DIR, f"{pdf_filename}_page{page_num}.png")
                pix.save(img_path)
                
                # OCR sur l'image
                page_text = extract_text_from_image(f"file://{img_path}")
            
            text += page_text + "\n\n"
        
        pdf.close()
        
        # Si le texte total est trop court, tenter OCR sur tout le document
        if len(text.strip()) < 200:
            logger.info(f"PDF entier avec peu de texte, nouvelle tentative OCR complète...")
            full_text = []
            
            # Réessayer avec une résolution plus élevée
            for page_num in range(len(pdf)):
                page = pdf[page_num]
                pix = page.get_pixmap(dpi=600)  # Résolution plus élevée
                img_path = os.path.join(TMP_IMG_DIR, f"{pdf_filename}_hires_page{page_num}.png")
                pix.save(img_path)
                
                # OCR avec options améliorées
                page_text = extract_text_from_image(f"file://{img_path}")
                full_text.append(page_text)
            
            # Si l'OCR a donné de meilleurs résultats, l'utiliser
            new_text = "\n\n".join(full_text)
            if len(new_text.strip()) > len(text.strip()):
                logger.info(f"OCR haute résolution réussi: {len(new_text)} caractères")
                text = new_text
        
        return text.strip()
    
    except Exception as e:
        logger.error(f"[ERREUR] Problème lors de l'extraction PDF ({pdf_url}) : {e}")
        return ""

def preprocess_text_for_llm(text):
    """
    Prétraite le texte avant de l'envoyer au modèle pour améliorer les résultats.
    Version améliorée avec nettoyage plus complet et normalisation.
    
    Args:
        text (str): Le texte brut à prétraiter
        
    Returns:
        str: Le texte prétraité
    """
    if not text:
        return ""
    
    # 1. Supprimer les caractères spéciaux qui pourraient perturber le modèle
    text = re.sub(r'[^\x00-\x7F]+', ' ', text)  # Garder uniquement les caractères ASCII
    
    # 2. Nettoyer les balises HTML
    text = re.sub(r'<[^>]*>', ' ', text)
    
    # 3. Normaliser les prix (s'assurer que les euros sont correctement formatés)
    text = re.sub(r'(\d+)[.,](\d+)\s*€', r'\1,\2 €', text)
    text = re.sub(r'(\d+)[.,](\d+)\s*euros', r'\1,\2 €', text)
    
    # 4. Remplacer les caractères spéciaux par leurs équivalents simples
    text = text.replace('œ', 'oe').replace('Œ', 'OE')
    text = text.replace('æ', 'ae').replace('Æ', 'AE')
    text = text.replace('ß', 'ss')
    text = text.replace('«', '"').replace('»', '"')
    text = text.replace('…', '...')
    
    # 5. Supprimer les caractères de contrôle
    text = re.sub(r'[\x00-\x1F\x7F]', '', text)
    
    # 6. Normaliser les espaces et sauts de ligne pour une meilleure lisibilité
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    
    # 7. Accentuer les délimiteurs de sections de menu pour améliorer le chunking
    text = re.sub(r'(?i)(menu|carte|entrées?|plats?|desserts?|boissons?|vins?)\s*:', r'\n\n\1:\n', text)
    
    # 8. Limiter la longueur totale
    if len(text) > 6000:
        text = text[:6000]
    
    return text.strip()

def detect_non_french_response(text):
    """
    Détecte si une réponse est en anglais plutôt qu'en français.
    Permet de repérer les hallucinations plus efficacement que la simple détection cyrillique.
    
    Args:
        text (str): Le texte à analyser
        
    Returns:
        bool: True si le texte semble être non-français, False sinon
    """
    if not text:
        return False
        
    # Plages Unicode des caractères cyrilliques (détection originale)
    cyrillic_pattern = re.compile('[\u0400-\u04FF\u0500-\u052F\u2DE0-\u2DFF\uA640-\uA69F]')
    if cyrillic_pattern.search(text):
        return True
    
    # Liste d'indices de langue anglaise (mots communs anglais)
    english_indicators = [
        " the ", " is ", " are ", " was ", " were ", " be ", " been ", " has ", " have ", " had ",
        " and ", " or ", " but ", " not ", " with ", " from ", " by ", " for ", " at ", " on ", " to ",
        " name ", " price ", " description ", " category ",
        " Appetizer", " Entree", " Seafood", " Dessert", " Salad", "Snack"
    ]
    
    # Liste d'indices de langue française (mots communs français)
    french_indicators = [
        " le ", " la ", " les ", " un ", " une ", " des ", " du ", " de ", " est ", " sont ", " a ", " ont ",
        " et ", " ou ", " mais ", " pas ", " avec ", " depuis ", " par ", " pour ", " à ", " sur ", " au ",
        " nom ", " prix ", " description ", " catégorie ",
        " Entrée", " Plat", " Dessert", " Boisson"
    ]
    
    # Compter les indicateurs de langue
    text_lower = " " + text.lower() + " "  # Ajouter espaces pour éviter faux positifs
    
    english_count = sum(text_lower.count(indicator) for indicator in english_indicators)
    french_count = sum(text_lower.count(indicator) for indicator in french_indicators)
    
    # Présence de JSON en anglais
    json_english_indicators = ["name", "price", "description", "category"]
    json_english_count = sum(text.count(indicator) for indicator in json_english_indicators)
    
    # Si le texte contient des indicateurs JSON anglais mais peu de français
    if json_english_count > 1 and french_count < 5:
        logger.warning(f"Détection de structure JSON en anglais ({json_english_count} indicateurs)")
        return True
    
    # Si le ratio anglais/français est élevé
    if english_count > 0 and french_count > 0:
        ratio = english_count / french_count
        if ratio > 1.5:  # Si indicateurs anglais > 1.5 * indicateurs français
            logger.warning(f"Ratio anglais/français élevé: {ratio:.2f} ({english_count}/{french_count})")
            return True
    
    # Si beaucoup d'anglais et peu ou pas de français
    if english_count > 10 and french_count < 3:
        logger.warning(f"Beaucoup d'anglais ({english_count}) et peu de français ({french_count})")
        return True
    
    return False

def contains_cyrillic(text):
    """
    Détecte si un texte contient des caractères cyrilliques.
    Maintenue pour compatibilité avec le code existant.
    
    Args:
        text (str): Le texte à vérifier
        
    Returns:
        bool: True si le texte contient des caractères cyrilliques, False sinon
    """
    if not text:
        return False
        
    # Plages Unicode des caractères cyrilliques
    cyrillic_pattern = re.compile('[\u0400-\u04FF\u0500-\u052F\u2DE0-\u2DFF\uA640-\uA69F]')
    return bool(cyrillic_pattern.search(text))

def chunk_text(text, max_chunk_size=800, overlap=150):
    """
    Divise un texte en chunks de taille maximale spécifiée avec un chevauchement intelligent.
    Version optimisée avec meilleure gestion des frontières naturelles du texte.
    """
    if not text or len(text) <= max_chunk_size:
        return [text]
        
    chunks = []
    
    # Liste des délimiteurs par ordre de priorité pour une meilleure segmentation
    delimiters = [
        ('\n\n\n', 3),  # Sections principales (triple saut de ligne)
        ('\n\n', 2),    # Paragraphes (double saut de ligne)
        ('\n', 1),      # Lignes (saut de ligne simple)
        ('. ', 2),      # Phrases (point + espace)
        ('! ', 2),      # Phrases exclamatives
        ('? ', 2),      # Phrases interrogatives
        (', ', 2),      # Virgules (moins idéal mais acceptable)
        (' ', 1)        # Dernier recours: couper aux espaces
    ]
    
    start = 0
    while start < len(text):
        # Position de fin maximale pour ce chunk
        max_end = min(start + max_chunk_size, len(text))
        
        # Position où on va effectivement couper (par défaut, la fin maximale)
        end = max_end
        
        # Si on n'est pas à la fin du texte, chercher un point de coupure naturel
        if max_end < len(text):
            # Essayer chaque délimiteur par ordre de priorité
            for delimiter, extra_chars in delimiters:
                # Rechercher le délimiteur en partant de la fin du chunk potentiel
                break_pos = text.rfind(delimiter, start, max_end)
                
                # Si trouvé et suffisamment loin du début (au moins 40% de la taille maximale)
                # pour éviter des chunks trop petits
                min_acceptable = start + int(max_chunk_size * 0.4)
                if break_pos > min_acceptable:
                    end = break_pos + extra_chars
                    break
        
        # Ajouter le chunk au résultat
        chunks.append(text[start:end])
        
        # Calculer le début du prochain chunk avec chevauchement intelligent
        # Le chevauchement devrait être plus grand pour les sections importantes (menus)
        # et plus petit pour le texte général
        if '\n\n' in text[max(start, end - overlap):end]:
            # Si le chevauchement contient un paragraphe, réduire le chevauchement
            # pour éviter de dupliquer des sections entières
            overlap_adjusted = min(overlap, 100)
        else:
            # Sinon, utiliser le chevauchement standard
            overlap_adjusted = overlap
            
        start = max(start + 1, end - overlap_adjusted)
    
    return chunks

def extract_json_from_text(text):
    """
    Extrait et valide une structure JSON à partir d'un texte.
    Utilise plusieurs techniques pour trouver et réparer le JSON.
    
    Returns:
        dict/None: Le dictionnaire JSON extrait ou None si impossible à extraire
    """
    if not text:
        return None
    
    # 1. Tenter d'extraire le JSON en utilisant différents patterns
    json_patterns = [
        # Pattern 1: Recherche un JSON entouré par des accolades, en tenant compte des espaces/newlines
        r'({[\s\S]*?})(?:\s*$|\n)',
        # Pattern 2: Recherche un JSON complet qui commence par { et se termine par }
        r'({[\s\S]*})(?:\s*$)',
        # Pattern 3: Plus strict, recherche "{ ... }"
        r'({[^{]*?})(?:\s*$|\n)'
    ]
    
    json_text = None
    for pattern in json_patterns:
        matches = re.findall(pattern, text)
        if matches:
            for match in matches:
                try:
                    # Essayer de parser le JSON
                    data = json.loads(match)
                    # Si on arrive ici, c'est que le JSON est valide
                    json_text = match
                    break
                except json.JSONDecodeError:
                    # Continuer avec le prochain match
                    continue
            
            if json_text:
                break
    
    # Si on n'a pas trouvé de JSON valide, essayer des corrections
    if not json_text:
        # Essayer de corriger les problèmes de JSON courants
        # 1. Problème: Guillemets simples au lieu de doubles
        corrected_text = text.replace("'", '"')
        
        # 2. Problème: Points-virgules à la fin des lignes
        corrected_text = re.sub(r';\s*\n', ',\n', corrected_text)
        
        # 3. Problème: Propriétés sans guillemets
        corrected_text = re.sub(r'(\s)([a-zA-Z_][a-zA-Z0-9_]*)(\s*:)', r'\1"\2"\3', corrected_text)
        
        # 4. Problème: Virgules finales dans les objets ou tableaux
        corrected_text = re.sub(r',(\s*[}\]])', r'\1', corrected_text)
        
        # 5. Problème: Noms de variables avant le JSON (ex: result = { ... })
        corrected_text = re.sub(r'^.*?=\s*({.*}).*$', r'\1', corrected_text, flags=re.DOTALL)
        
        # Chercher à nouveau des structures JSON
        for pattern in json_patterns:
            matches = re.findall(pattern, corrected_text)
            if matches:
                for match in matches:
                    try:
                        # Essayer de parser le JSON corrigé
                        data = json.loads(match)
                        # Si on arrive ici, c'est que le JSON est valide
                        json_text = match
                        break
                    except json.JSONDecodeError:
                        # Continuer avec le prochain match
                        continue
                
                if json_text:
                    break
    
    # Si nous avons une correspondance JSON valide, la charger
    if json_text:
        try:
            return json.loads(json_text)
        except json.JSONDecodeError:
            # Dernière tentative: essayer d'identifier et de corriger les problèmes restants
            try:
                # 1. Remplacer les caractères non ASCII problématiques
                clean_json = re.sub(r'[^\x00-\x7F]+', ' ', json_text)
                # 2. Échapper les guillemets dans les chaînes
                clean_json = re.sub(r'(?<!\\)"(?=.*?:)', r'\"', clean_json)
                
                return json.loads(clean_json)
            except json.JSONDecodeError:
                # Encore une tentative: parser le JSON ligne par ligne
                try:
                    # Transformer en une seule ligne et nettoyer
                    json_oneline = json_text.replace('\n', ' ').replace('\r', '')
                    json_oneline = re.sub(r'\s+', ' ', json_oneline)
                    
                    # Corriger manuellement certaines structures
                    json_oneline = re.sub(r',\s*}', '}', json_oneline)
                    json_oneline = re.sub(r',\s*]', ']', json_oneline)
                    
                    # Réessayer de parser
                    return json.loads(json_oneline)
                except json.JSONDecodeError:
                    logger.error("Impossible de corriger le JSON après plusieurs tentatives")
                    return None
    
    logger.error("Aucune structure JSON valide trouvée dans le texte")
    return None

def is_valid_menu_result(result):
    """
    Vérifie si un résultat d'extraction de menu est valide et non vide.
    Version améliorée avec validation plus stricte.
    """
    if not result or not isinstance(result, dict):
        return False
    
    # Vérifier que les clés obligatoires sont présentes
    required_keys = ["Menus Globaux", "Plats Indépendants"]
    if not all(key in result for key in required_keys):
        return False
    
    # Vérifier que les valeurs sont des listes
    if not (isinstance(result["Menus Globaux"], list) and isinstance(result["Plats Indépendants"], list)):
        return False
    
    # Vérifier qu'au moins une des listes contient quelque chose
    if not (len(result["Menus Globaux"]) > 0 or len(result["Plats Indépendants"]) > 0):
        return False
    
    # Vérifier la structure interne des menus
    for menu in result.get("Menus Globaux", []):
        if not isinstance(menu, dict) or "nom" not in menu:
            return False
    
    # Vérifier la structure interne des plats
    for plat in result.get("Plats Indépendants", []):
        if not isinstance(plat, dict) or "nom" not in plat:
            return False
    
    return True

def is_rich_enough_result(result):
    """
    Vérifie si un résultat est suffisamment riche en informations.
    """
    if not result or not isinstance(result, dict):
        return False
    
    # Compter les menus globaux
    menu_count = len(result.get("Menus Globaux", []))
    
    # Compter les plats indépendants
    dish_count = len(result.get("Plats Indépendants", []))
    
    # Considéré comme riche si au moins 5 éléments au total
    return (menu_count + dish_count) >= 5

def is_better_result(result1, result2):
    """
    Compare deux résultats et détermine lequel est meilleur.
    """
    if not is_valid_menu_result(result1):
        return False
    if not is_valid_menu_result(result2):
        return True
    
    # Compter les éléments dans chaque résultat
    count1 = len(result1.get("Menus Globaux", [])) + len(result1.get("Plats Indépendants", []))
    count2 = len(result2.get("Menus Globaux", [])) + len(result2.get("Plats Indépendants", []))
    
    # Le résultat avec plus d'éléments est considéré meilleur
    if count1 > count2:
        return True
    elif count1 < count2:
        return False
    
    # Si même nombre d'éléments, vérifier la richesse des détails
    # (existence de descriptions, catégories, etc.)
    details1 = sum(1 for plat in result1.get("Plats Indépendants", []) 
                  if plat.get("description", "") or plat.get("catégorie", ""))
    details2 = sum(1 for plat in result2.get("Plats Indépendants", []) 
                  if plat.get("description", "") or plat.get("catégorie", ""))
    
    return details1 >= details2

def generate_menu_structure_with_openai(text, restaurant_name, is_chunk=False, chunk_num=0, total_chunks=1):
    """
    Génère une structure de menu à partir d'un texte en utilisant OpenAI comme fallback
    quand Mistral échoue. Gère la compatibilité avec les API OpenAI v1.0+ et <=0.28.
    
    Args:
        text (str): Le texte brut du menu
        restaurant_name (str): Nom du restaurant
        is_chunk (bool): Indique si le texte est un chunk d'un document plus grand
        chunk_num (int): Numéro du chunk actuel (si is_chunk=True)
        total_chunks (int): Nombre total de chunks (si is_chunk=True)
        
    Returns:
        dict: Structure du menu ou None si erreur
    """
    if not ENABLE_OPENAI_FALLBACK:
        return None
        
    try:
        # Vérifier le cache
        chunk_suffix = f"_chunk{chunk_num}" if is_chunk else ""
        cache_key = f"openai_menu_{hashlib.md5((restaurant_name + text[:100]).encode()).hexdigest()}{chunk_suffix}"
        cached_result = get_from_cache(cache_key, max_age_hours=720, prefix="openai_menus")
        if cached_result:
            logger.info(f"Utilisation du cache OpenAI pour le menu de {restaurant_name}{' (chunk '+str(chunk_num)+')' if is_chunk else ''}")
            return cached_result
            
        logger.info(f"Génération de structure de menu avec OpenAI pour {restaurant_name}{' (chunk '+str(chunk_num)+')' if is_chunk else ''} (fallback)")
        
        # Vérifier si l'API OpenAI est correctement configurée
        if not OPENAI_API_KEY or (OPENAI_API_VERSION == "unknown"):
            logger.error("API OpenAI non configurée correctement, fallback impossible")
            return None
        
        # Ajuster le contenu pour les chunks
        chunk_info = f" (chunk {chunk_num}/{total_chunks})" if is_chunk else ""
        chunk_note = "\nNOTE: Ce texte n'est qu'une partie du menu complet. Extrait uniquement les plats et menus visibles dans ce fragment." if is_chunk else ""
        
        # Limiter la taille du texte pour éviter les dépassements de tokens
        text_to_use = text[:3500]
        
        # Créer un prompt adapté pour OpenAI
        prompt = f"""RÉPONDEZ EN FRANÇAIS UNIQUEMENT. N'UTILISEZ QUE L'ALPHABET LATIN.
Tu vas analyser un menu de restaurant et l'organiser en JSON structuré.

Restaurant: "{restaurant_name}"{chunk_info}

Menu:
```
{text_to_use}
```{chunk_note}

Renvoie uniquement une structure JSON valide avec ce format exact:
{{
  "Menus Globaux": [
    {{
      "nom": "Nom du menu (ex: Menu du jour, Formule midi)",
      "prix": "Prix tel qu'indiqué",
      "inclus": [
        {{ "nom": "Plat inclus 1", "description": "Description si présente" }},
        {{ "nom": "Plat inclus 2", "description": "Description si présente" }}
      ]
    }}
  ],
  "Plats Indépendants": [
    {{
      "nom": "Nom du plat",
      "catégorie": "Entrée/Plat/Dessert/Boisson",
      "prix": "Prix tel qu'indiqué",
      "description": "Description complète si présente"
    }}
  ]
}}

IMPORTANT:
1. Ta réponse doit UNIQUEMENT contenir le JSON, sans aucun texte avant ou après
2. Ne JAMAIS utiliser de caractères cyrilliques ou non-latin
3. Si tu ne trouves pas de menu, renvoie quand même la structure avec des tableaux vides
4. Assure-toi que ton JSON est parfaitement valide (accolades fermées, guillemets cohérents)
5. Sois fidèle au texte: préserve les noms et descriptions tels quels
6. Extrait uniquement les plats et menus que tu vois dans ce texte{" (c'est seulement une partie du menu complet)" if is_chunk else ""}
"""

        # Système de multi-tentatives et multiple versions API
        for attempt in range(3):  # 3 tentatives
            try:
                if OPENAI_API_VERSION == "v1" and (attempt == 0 or attempt == 1):
                    # Première tentative: API v1.0+
                    try:
                        logger.info(f"Tentative {attempt+1} avec OpenAI API v1.0+ pour {restaurant_name}")
                        
                        # Configurer les messages pour l'API v1.0+
                        messages = [
                            {"role": "system", "content": "Tu es un assistant spécialisé dans l'extraction de données de menus de restaurants. Tu réponds toujours uniquement en JSON valide, jamais en texte."},
                            {"role": "user", "content": prompt}
                        ]
                        
                        response = client.chat.completions.create(
                            model="gpt-3.5-turbo",
                            messages=messages,
                            temperature=0.3,
                            max_tokens=2000
                        )
                        
                        # Extraire le contenu de la réponse avec la nouvelle structure API
                        result_text = response.choices[0].message.content
                        break  # Sortir de la boucle si succès
                    except Exception as e:
                        logger.warning(f"Échec de la tentative {attempt+1} avec API v1.0+: {e}")
                        if attempt < 2:  # Ne pas lever d'exception si ce n'est pas la dernière tentative
                            continue
                        raise
                else:
                    # Dernière tentative: API legacy (v0.x)
                    try:
                        logger.info(f"Tentative {attempt+1} avec OpenAI API legacy pour {restaurant_name}")
                        
                        # Configuration pour l'API legacy
                        openai.api_key = OPENAI_API_KEY
                        
                        # Vérifier quelle méthode/classe est disponible
                        if hasattr(openai, 'ChatCompletion'):
                            # Format v0.28.x
                            response = openai.ChatCompletion.create(
                                model="gpt-3.5-turbo",
                                messages=[
                                    {"role": "system", "content": "Tu es un assistant spécialisé dans l'extraction de données de menus de restaurants. Tu réponds toujours uniquement en JSON valide, jamais en texte."},
                                    {"role": "user", "content": prompt}
                                ],
                                temperature=0.3,
                                max_tokens=2000
                            )
                            result_text = response.choices[0]["message"]["content"]
                        else:
                            # Encore plus ancien
                            response = openai.Completion.create(
                                engine="text-davinci-003",
                                prompt=f"Extrait le menu du restaurant sous forme JSON:\n\n{prompt}",
                                temperature=0.3,
                                max_tokens=2000
                            )
                            result_text = response.choices[0].text
                        
                        break  # Sortir de la boucle si succès
                    except Exception as e:
                        logger.error(f"Échec de toutes les tentatives OpenAI pour {restaurant_name}: {e}")
                        return None
            except Exception as e:
                logger.warning(f"Erreur lors de la tentative {attempt+1} avec OpenAI pour {restaurant_name}: {e}")
                if attempt == 2:  # Si c'est la dernière tentative
                    logger.error(f"Toutes les tentatives OpenAI ont échoué pour {restaurant_name}")
                    return None
        
        # Extraire et valider le JSON
        result = extract_json_from_text(result_text)
        
        if not result:
            logger.error(f"Extraction JSON OpenAI échouée pour {restaurant_name}")
            return None
            
        # Vérifier et corriger la structure
        if "Menus Globaux" not in result:
            result["Menus Globaux"] = []
        if "Plats Indépendants" not in result:
            result["Plats Indépendants"] = []
            
        # Sauvegarder dans le cache
        save_to_cache(cache_key, result, prefix="openai_menus")
        
        return result
        
    except Exception as e:
        logger.error(f"Erreur lors de la génération avec OpenAI pour {restaurant_name}: {e}")
        return None

def identify_menu_sections(text, restaurant_name, chunk_num=1, total_chunks=1):
    """
    Identifie les sections de menu dans un texte.
    
    Cette fonction utilise GPT pour trouver et segmenter les sections d'un menu.
    Si plusieurs chunks sont fournis, elle adapte son prompt pour indiquer au modèle
    qu'il travaille sur une partie du document.
    
    Args:
        text (str): Texte du menu à analyser
        restaurant_name (str): Nom du restaurant (pour la contextualisation)
        chunk_num (int): Numéro du chunk actuel (pour les gros documents)
        total_chunks (int): Nombre total de chunks (pour les gros documents)
        
    Returns:
        dict: Sections identifiées avec leurs types
    """
    # Prétraiter le texte pour le LLM
    processed_text = preprocess_text_for_llm(text)
    
    # Créer un hash pour le cache
    text_hash = hashlib.md5(processed_text.encode()).hexdigest()
    cache_key = f"sections_{text_hash}_{restaurant_name}"
    
    # Vérifier si on a déjà un résultat en cache
    cached_result = get_from_cache(cache_key, max_age_hours=720, prefix="gpt_sections")
    if cached_result:
        logger.info(f"Utilisation du résultat en cache pour l'identification des sections de {restaurant_name}")
        return cached_result
    
    # Construire un prompt pour identifier les sections
    prompt = f"""Voici le texte d'un menu du restaurant "{restaurant_name}". 
{'' if total_chunks == 1 else f'IMPORTANT: Ce texte est le chunk {chunk_num} sur {total_chunks}, donc il peut être incomplet ou fragmenté.'}

Ta tâche est d'identifier toutes les SECTIONS du menu (comme entrées, plats principaux, desserts, boissons, etc.).
N'extrais pas encore les plats individuels, seulement les catégories principales.

Pour chaque section que tu identifies, détermine son type parmi:
- STARTERS: entrées, apéritifs, hors d'œuvres
- MAIN_COURSES: plats principaux, spécialités, grillades
- SIDE_DISHES: accompagnements, garnitures
- DESSERTS: desserts, pâtisseries, glaces
- BEVERAGES: boissons, vins, cocktails
- BREAKFAST: petit-déjeuner, brunch matinal
- KIDS: menu enfant, plats pour enfants
- COMBO_MEALS: menus, formules, combinaisons
- SPECIALS: spécialités, suggestions du chef, plats du jour
- OTHER: toute autre section qui ne correspond pas aux catégories ci-dessus

Voici le texte du menu:
---
{processed_text}
---

Réponds au format JSON uniquement avec la structure suivante:
{{
  "sections": [
    {{
      "name": "Nom exact de la section comme dans le texte",
      "type": "CATÉGORIE_DÉTERMINÉE",
      "start_index": position de début approximative dans le texte,
      "end_index": position de fin approximative dans le texte
    }},
    // autres sections...
  ]
}}

Si aucune section n'est détectable, réponds avec:
{{ "sections": [] }}

NE FOURNIS PAS d'explications supplémentaires, juste le JSON.
"""
    
    # Premier essai avec paramètres standard
    result_text = generate_ai_response_gpt(
        prompt=prompt,
        max_tokens=800,
        temperature=0.1,
    )
    
    # Essayer d'extraire le JSON
    result = extract_json_from_text(result_text)
    
    # Vérifier si le résultat est valide
    if result and "sections" in result and isinstance(result["sections"], list):
        # Sauvegarder en cache
        save_to_cache(cache_key, result, prefix="gpt_sections")
        logger.info(f"Sections identifiées pour {restaurant_name}: {len(result['sections'])} sections trouvées")
        return result
    
    # Si l'extraction a échoué, essayer avec un prompt simplifié
    logger.warning(f"L'extraction des sections a échoué pour {restaurant_name}. Tentative simplifiée...")
    
    simplified_prompt = f"""Identifie les sections du menu du restaurant "{restaurant_name}" dans ce texte:
---
{processed_text[:1500]}
---

Réponds UNIQUEMENT en JSON:
{{
  "sections": [
    {{
      "name": "Nom de la section",
      "type": "STARTERS/MAIN_COURSES/SIDE_DISHES/DESSERTS/BEVERAGES/BREAKFAST/KIDS/COMBO_MEALS/SPECIALS/OTHER",
      "start_index": 0,
      "end_index": 0
    }}
  ]
}}"""
    
    logger.info(f"Second essai d'identification des sections pour {restaurant_name} (chunk {chunk_num}/{total_chunks})")
    
    result_text = generate_ai_response_gpt(
        prompt=simplified_prompt,
        max_tokens=600,
        temperature=0.1,
    )
    
    # Essayer d'extraire le JSON à nouveau
    sections_result = extract_json_from_text(result_text)
    
    # Vérifier si le résultat simplifié est valide
    if sections_result and "sections" in sections_result and isinstance(sections_result["sections"], list):
        # Sauvegarder en cache
        save_to_cache(cache_key, sections_result, prefix="gpt_sections")
        logger.info(f"Sections identifiées (essai simplifié) pour {restaurant_name}: {len(sections_result['sections'])} sections trouvées")
        return sections_result
    
    # Fallback: essayer OpenAI GPT
    logger.warning(f"L'extraction des sections a également échoué avec un prompt simplifié pour {restaurant_name}")
    
    # Créer un fallback manuel avec une structure vide
    fallback_result = {
        "sections": []
    }
    
    # Vérifier si le texte est assez long pour probablement contenir un menu
    if len(processed_text) > 200:
        # Ajouter une section générique
        fallback_result["sections"].append({
            "name": "Menu complet",
            "type": "OTHER",
            "start_index": 0,
            "end_index": len(processed_text)
        })
    
    # Sauvegarder ce fallback en cache pour éviter de réessayer
    save_to_cache(cache_key, fallback_result, prefix="gpt_sections")
    
    logger.warning(f"Utilisation d'un fallback pour les sections de {restaurant_name}")
    return fallback_result

def extract_dishes_by_section(text, restaurant_name, sections_info, chunk_num=1, total_chunks=1):
    """
    Extrait les plats par section à partir du texte et des informations de section.
    
    Cette fonction utilise GPT pour analyser chaque section précédemment identifiée
    et en extraire les plats, avec détails et prix.
    
    Args:
        text (str): Texte du menu complet
        restaurant_name (str): Nom du restaurant
        sections_info (dict): Informations sur les sections identifiées
        chunk_num (int): Numéro du chunk actuel
        total_chunks (int): Nombre total de chunks
        
    Returns:
        dict: Structure complète du menu avec sections et plats
    """
    # Prétraiter le texte pour le LLM
    processed_text = preprocess_text_for_llm(text)
    
    # Hash pour le cache
    input_hash = hashlib.md5((processed_text + str(sections_info)).encode()).hexdigest()
    cache_key = f"dishes_{input_hash}_{restaurant_name}"
    
    # Vérifier le cache
    cached_result = get_from_cache(cache_key, max_age_hours=720, prefix="gpt_dishes")
    if cached_result:
        logger.info(f"Utilisation du résultat en cache pour l'extraction des plats de {restaurant_name}")
        return cached_result
    
    # Construire un prompt pour extraire les plats, section par section
    all_sections = sections_info.get("sections", [])
    
    if not all_sections:
        logger.warning(f"Aucune section trouvée pour {restaurant_name}, impossible d'extraire les plats")
        return {"menu": []}
    
    # Limiter le nombre de sections à traiter pour éviter des prompts trop longs
    sections_to_process = all_sections[:5]  # Limiter à 5 sections maximum
    
    # Construire le prompt avec les sections détectées
    sections_text = ""
    for i, section in enumerate(sections_to_process):
        section_name = section.get("name", f"Section {i+1}")
        section_type = section.get("type", "OTHER")
        
        # Extraire le texte de cette section
        start_idx = max(0, section.get("start_index", 0))
        end_idx = min(len(processed_text), section.get("end_index", len(processed_text)))
        
        # Vérifier que les indices sont valides
        if start_idx >= end_idx or start_idx >= len(processed_text):
            continue
            
        section_content = processed_text[start_idx:end_idx]
        
        # Ajouter au texte des sections
        sections_text += f"\n--- SECTION: {section_name} (TYPE: {section_type}) ---\n{section_content}\n"
    
    # Prompt principal
    prompt = f"""Tu es un expert en analyse de menus de restaurants. Analyse ce menu du restaurant "{restaurant_name}".
{'' if total_chunks == 1 else f'IMPORTANT: Ce texte est le chunk {chunk_num} sur {total_chunks}, donc il peut être incomplet.'}

Je t'ai déjà identifié les sections principales. Pour chaque section, extrais tous les plats avec leurs détails.

{sections_text}

Pour chaque plat, identifie:
1. Le nom exact du plat
2. Sa description (si disponible)
3. Son prix (si disponible)
4. Ses options ou variations (si disponibles)

Réponds STRICTEMENT au format JSON suivant:
{{
  "menu": [
    {{
      "section_name": "Nom de la section comme fourni",
      "section_type": "TYPE_DE_SECTION comme fourni",
      "items": [
        {{
          "name": "Nom du plat",
          "description": "Description du plat ou null si non disponible",
          "price": "Prix au format texte (ex: '12,50 €') ou null",
          "options": ["Option 1", "Option 2"] ou null si pas d'options
        }},
        // autres plats...
      ]
    }},
    // autres sections...
  ]
}}

Si tu ne trouves aucun plat, réponds avec: {{ "menu": [] }}

IMPORTANT: 
- Pour les prix, conserve EXACTEMENT le format du texte original (symbole €, virgule/point, etc.)
- Ne réponds qu'avec le JSON, sans commentaire ni introduction
- Si une information est absente (description/prix/options), utilise null
"""
    
    # Premier essai avec paramètres standard
    result_text = generate_ai_response_gpt(
        prompt=prompt,
        max_tokens=1500,  # Plus de tokens car l'extraction est plus détaillée
        temperature=0.3,
    )
    
    # Essayer d'extraire le JSON
    result = extract_json_from_text(result_text)
    
    # Vérifier si le résultat est valide
    if is_valid_menu_result(result):
        # Sauvegarder en cache
        save_to_cache(cache_key, result, prefix="gpt_dishes")
        return result
    
    # Si l'extraction a échoué, essayer avec un prompt simplifié
    logger.warning(f"L'extraction des plats a échoué pour {restaurant_name}. Tentative simplifiée...")
    
    # Simplifier le prompt et se concentrer sur une seule section à la fois
    simplified_sections = []
    
    for i, section in enumerate(sections_to_process[:2]):  # Limiter à 2 sections pour simplifier
        section_name = section.get("name", f"Section {i+1}")
        section_type = section.get("type", "OTHER")
        
        # Extraire le texte de cette section
        start_idx = max(0, section.get("start_index", 0))
        end_idx = min(len(processed_text), section.get("end_index", len(processed_text)))
        
        # Vérifier que les indices sont valides
        if start_idx >= end_idx or start_idx >= len(processed_text):
            continue
            
        section_content = processed_text[start_idx:end_idx]
        
        simplified_sections.append({
            "name": section_name,
            "type": section_type,
            "content": section_content[:500]  # Limiter le contenu pour simplifier
        })
    
    # Si nous n'avons pas de sections valides, créer une "section complète"
    if not simplified_sections:
        simplified_sections.append({
            "name": "Menu complet",
            "type": "OTHER",
            "content": processed_text[:1000]  # Limiter à 1000 caractères
        })
    
    # Construire le prompt simplifié
    sections_details_str = "\n".join([f"--- {s['name']} ({s['type']}) ---\n{s['content']}\n" for s in simplified_sections])
    simplified_prompt = f"""Analyse ces sections du menu du restaurant "{restaurant_name}" et extrais les plats.

{sections_details_str}

Réponds en JSON:
{{
  "menu": [
    {{
      "section_name": "Nom de la section",
      "section_type": "TYPE_DE_SECTION",
      "items": [
        {{
          "name": "Nom du plat",
          "description": "Description ou null",
          "price": "Prix ou null",
          "options": ["Option 1"] ou null
        }}
      ]
    }}
  ]
}}"""
    
    logger.info(f"Second essai d'extraction des plats pour {restaurant_name} (chunk {chunk_num}/{total_chunks})")
    
    result_text = generate_ai_response_gpt(
        prompt=simplified_prompt,
        max_tokens=1200,
        temperature=0.1,
    )
    
    # Essayer d'extraire le JSON à nouveau
    standard_result = extract_json_from_text(result_text)
    
    # Vérifier si le résultat simplifié est valide
    if is_valid_menu_result(standard_result):
        # Sauvegarder en cache
        save_to_cache(cache_key, standard_result, prefix="gpt_dishes")
        logger.info(f"Plats extraits (essai simplifié) pour {restaurant_name}")
        return standard_result
    
    # Fallback: structurer manuellement un résultat vide
    logger.warning(f"L'extraction des plats a également échoué avec un prompt simplifié pour {restaurant_name}")
    
    # Créer un fallback avec une structure de menu de base
    fallback_result = {
        "menu": []
    }
    
    # Ajouter au moins une section vide pour chaque section identifiée
    for section in sections_to_process:
        fallback_result["menu"].append({
            "section_name": section.get("name", "Section sans nom"),
            "section_type": section.get("type", "OTHER"),
            "items": []
        })
    
    # S'il n'y a aucune section, ajouter une section générique
    if not fallback_result["menu"]:
        fallback_result["menu"].append({
            "section_name": "Menu complet",
            "section_type": "OTHER",
            "items": []
        })
    
    # Sauvegarder ce fallback en cache pour éviter de réessayer
    save_to_cache(cache_key, fallback_result, prefix="gpt_dishes")
    
    logger.warning(f"Utilisation d'un fallback pour les plats de {restaurant_name}")
    return fallback_result

def convert_to_standard_format(dishes_result):
    """
    Convertit le résultat des plats extraits en format standardisé pour le traitement du menu.
    """
    # Les lignes suivantes semblent orphelines et doivent être supprimées ou commentées
    # max_tokens=1000,
    # temperature=0.0,
    # do_sample=False
    #)
    
    # Vérifier et nettoyer la réponse
    if result_text and not contains_cyrillic(result_text):
        result = extract_json_from_text(result_text)
        if is_valid_menu_result(result):
            # Sauvegarder dans le cache
            save_to_cache(cache_key, result, prefix="mistral_direct")
            return result
    
    # Structure vide en dernier recours
    return {"Menus Globaux": [], "Plats Indépendants": []}

def extract_menu_direct_minimal(text, restaurant_name):
    """
    Extraction minimale pour les cas très difficiles.
    Utilise un prompt très simple et des paramètres conservatifs.
    """
    if not text:
        return {"Menus Globaux": [], "Plats Indépendants": []}
    
    # Limiter la taille du texte
    short_text = text[:1500]
    
    # Prompt minimaliste pour extraction basique
    prompt = (
        f"[INST] FRANÇAIS UNIQUEMENT. JSON UNIQUEMENT.\n"
        f"Extrait les plats et prix de ce menu du restaurant \"{restaurant_name}\":\n"
        f"\n{short_text}\n"
        f"\nFormat: {{ \"Plats\": [ {{ \"nom\": \"Nom\", \"prix\": \"Prix\" }} ] }}\n"
        f"\nRÈGLES: Que le JSON! Pas de texte avant/après! Pas de commentaires! [/INST]"
    )
    
    logger.info(f"Tentative d'extraction minimale pour {restaurant_name}")
    
    result_text = generate_ai_response_gpt(
        prompt=prompt,
        max_tokens=600,
        temperature=0.0,
        do_sample=False
    )
    
    # Vérifier et corriger le résultat
    if result_text and not contains_cyrillic(result_text):
        result = extract_json_from_text(result_text)
        
        if result and isinstance(result, dict) and "Plats" in result and isinstance(result["Plats"], list):
            # Convertir au format standard
            standard_result = {
                "Menus Globaux": [],
                "Plats Indépendants": []
            }
            
            for plat in result["Plats"]:
                if isinstance(plat, dict) and "nom" in plat:
                    standard_result["Plats Indépendants"].append({
                        "nom": plat.get("nom", ""),
                        "catégorie": "Plat",
                        "prix": plat.get("prix", ""),
                        "description": ""
                    })
            
            return standard_result
    
    # Structure vide en dernier recours
    return {"Menus Globaux": [], "Plats Indépendants": []}

def merge_chunk_results_enhanced(chunk_results):
    """
    Fusion intelligente des résultats de chunks avec gestion améliorée des doublons.
    Déduplique et enrichit les informations avec les détails les plus complets.
    """
    if not chunk_results:
        return {"Menus Globaux": [], "Plats Indépendants": []}
    
    # Si un seul chunk, le retourner directement
    if len(chunk_results) == 1:
        return chunk_results[0]
    
    merged_result = {"Menus Globaux": [], "Plats Indépendants": []}
    
    # Tables de suivi pour la déduplication intelligente
    seen_menus = {}  # nom+prix -> {menu complet}
    seen_dishes = {}  # nom+catégorie+prix -> {plat complet}
    
    # Fonction de normalisation pour comparaison
    def normalize_for_comparison(text):
        if not text:
            return ""
        # Convertir en minuscules, supprimer accents et ponctuation
        text = text.lower()
        text = re.sub(r'[àáâãäå]', 'a', text)
        text = re.sub(r'[èéêë]', 'e', text)
        text = re.sub(r'[ìíîï]', 'i', text)
        text = re.sub(r'[òóôõö]', 'o', text)
        text = re.sub(r'[ùúûü]', 'u', text)
        text = re.sub(r'[ç]', 'c', text)
        text = re.sub(r'[^\w\s]', '', text)
        return re.sub(r'\s+', ' ', text).strip()
    
    # Fonction pour fusionner deux objets en gardant les informations les plus complètes
    def merge_objects(obj1, obj2):
        result = obj1.copy()
        for key, value in obj2.items():
            # Si la valeur est vide dans obj1 ou celle de obj2 est plus détaillée
            if key not in obj1 or not obj1[key] or (isinstance(value, str) and len(value) > len(obj1[key])):
                result[key] = value
            # Pour les listes (comme "inclus"), les fusionner
            elif isinstance(value, list) and isinstance(obj1[key], list):
                # Fusion intelligente des éléments des listes (avec déduplication)
                if key == "inclus" and all(isinstance(item, dict) for item in obj1[key] + value):
                    # Pour les plats inclus, déduplication basée sur le nom
                    seen_included = {}
                    for item in obj1[key] + value:
                        if "nom" in item:
                            item_key = normalize_for_comparison(item["nom"])
                            if item_key not in seen_included or len(item.get("description", "")) > len(seen_included[item_key].get("description", "")):
                                seen_included[item_key] = item
                    result[key] = list(seen_included.values())
                else:
                    # Simple concaténation pour les autres types de listes
                    result[key] = obj1[key] + value
        return result
    
    # Parcourir tous les chunks et fusionner les résultats
    for chunk_result in chunk_results:
        if not isinstance(chunk_result, dict):
            continue
        
        # Traiter les menus globaux
        for menu in chunk_result.get("Menus Globaux", []):
            if not isinstance(menu, dict) or "nom" not in menu:
                continue
            
            # Créer une clé unique pour ce menu
            menu_key = (normalize_for_comparison(menu.get("nom", "")), 
                       normalize_for_comparison(menu.get("prix", "")))
            
            if menu_key in seen_menus:
                # Fusionner avec le menu existant
                seen_menus[menu_key] = merge_objects(seen_menus[menu_key], menu)
            else:
                seen_menus[menu_key] = menu
        
        # Traiter les plats indépendants
        for dish in chunk_result.get("Plats Indépendants", []):
            if not isinstance(dish, dict) or "nom" not in dish:
                continue
            
            # Créer une clé unique pour ce plat
            dish_key = (normalize_for_comparison(dish.get("nom", "")),
                       normalize_for_comparison(dish.get("catégorie", "")),
                       normalize_for_comparison(dish.get("prix", "")))
            
            if dish_key in seen_dishes:
                # Fusionner avec le plat existant
                seen_dishes[dish_key] = merge_objects(seen_dishes[dish_key], dish)
            else:
                seen_dishes[dish_key] = dish
    
    # Reconstruire le résultat final à partir des tables de déduplication
    merged_result["Menus Globaux"] = list(seen_menus.values())
    merged_result["Plats Indépendants"] = list(seen_dishes.values())
    
    return merged_result

def merge_document_results(main_result, new_result):
    """
    Fusionne les résultats de deux documents différents.
    Utilisé pour fusionner les résultats de plusieurs pages ou sources.
    """
    # Si le résultat principal est vide, utiliser le nouveau
    if not main_result or not isinstance(main_result, dict):
        return new_result
    
    # Si le nouveau résultat est vide, conserver le principal
    if not new_result or not isinstance(new_result, dict):
        return main_result
    
    # Utiliser la fusion intelligente déjà implémentée
    return merge_chunk_results_enhanced([main_result, new_result])

def post_process_menu_result(menu_result):
    """
    Post-traitement pour améliorer la qualité des résultats.
    Nettoie, normalise et enrichit les structures de menu.
    """
    if not menu_result or not isinstance(menu_result, dict):
        return {"Menus Globaux": [], "Plats Indépendants": []}
    
    # Vérifier et corriger la structure de base
    if "Menus Globaux" not in menu_result:
        menu_result["Menus Globaux"] = []
    if "Plats Indépendants" not in menu_result:
        menu_result["Plats Indépendants"] = []
    
    # Nettoyage et normalisation des menus globaux
    for i, menu in enumerate(menu_result["Menus Globaux"]):
        if not isinstance(menu, dict):
            menu_result["Menus Globaux"][i] = {"nom": str(menu), "prix": "", "inclus": []}
            continue
        
        # Nettoyer les champs
        if "nom" in menu:
            menu["nom"] = menu["nom"].strip()
        else:
            menu["nom"] = "Menu non spécifié"
            
        if "prix" in menu:
            # Standardiser le format des prix
            prix = menu["prix"]
            prix = re.sub(r'(\d+)[.,](\d+)\s*€', r'\1,\2 €', prix)
            prix = re.sub(r'(\d+)[.,](\d+)\s*euros?', r'\1,\2 €', prix)
            menu["prix"] = prix.strip()
        else:
            menu["prix"] = ""
            
        # Nettoyer les plats inclus
        if "inclus" not in menu or not isinstance(menu["inclus"], list):
            menu["inclus"] = []
        else:
            for j, plat in enumerate(menu["inclus"]):
                if not isinstance(plat, dict):
                    menu["inclus"][j] = {"nom": str(plat), "description": ""}
                    continue
                
                if "nom" in plat:
                    plat["nom"] = plat["nom"].strip()
                else:
                    plat["nom"] = "Plat non spécifié"
                    
                if "description" in plat:
                    plat["description"] = plat["description"].strip()
                else:
                    plat["description"] = ""
    
    # Nettoyage et normalisation des plats indépendants
    for i, plat in enumerate(menu_result["Plats Indépendants"]):
        if not isinstance(plat, dict):
            menu_result["Plats Indépendants"][i] = {
                "nom": str(plat), 
                "catégorie": "Autre", 
                "prix": "", 
                "description": ""
            }
            continue
        
        # Nettoyer les champs
        if "nom" in plat:
            plat["nom"] = plat["nom"].strip()
        else:
            plat["nom"] = "Plat non spécifié"
            
        if "catégorie" in plat:
            plat["catégorie"] = plat["catégorie"].strip()
        else:
            plat["catégorie"] = "Autre"
            
        if "prix" in plat:
            # Standardiser le format des prix
            prix = plat["prix"]
            prix = re.sub(r'(\d+)[.,](\d+)\s*€', r'\1,\2 €', prix)
            prix = re.sub(r'(\d+)[.,](\d+)\s*euros?', r'\1,\2 €', prix)
            plat["prix"] = prix.strip()
        else:
            plat["prix"] = ""
            
        if "description" in plat:
            plat["description"] = plat["description"].strip()
        else:
            plat["description"] = ""
    
    # Supprimer les plats avec des noms trop courts ou non significatifs
    menu_result["Plats Indépendants"] = [
        plat for plat in menu_result["Plats Indépendants"] 
        if len(plat.get("nom", "")) > 2 and 
           not plat.get("nom", "").lower() in ["le", "la", "les", "des", "un", "une"]
    ]
    
    # Normalisation des catégories de plats
    categorie_mapping = {
        # Entrées
        "entree": "Entrées", "entrée": "Entrées", "entrées": "Entrées", "entrees": "Entrées",
        "starter": "Entrées", "starters": "Entrées", "appetizer": "Entrées", "appetizers": "Entrées",
        # Plats
        "plat": "Plats", "plats": "Plats", "main": "Plats", "dish": "Plats",
        "main course": "Plats", "main courses": "Plats", "principal": "Plats",
        # Desserts
        "dessert": "Desserts", "desserts": "Desserts", "sucré": "Desserts", "sucre": "Desserts",
        "sweet": "Desserts", "sweets": "Desserts", "pâtisserie": "Desserts",
        # Boissons
        "boisson": "Boissons", "boissons": "Boissons", "drink": "Boissons", "drinks": "Boissons",
        "beverage": "Boissons", "beverages": "Boissons", "soft": "Boissons",
        # Vins
        "vin": "Vins", "vins": "Vins", "wine": "Vins", "wines": "Vins",
        # Autres catégories
        "fromage": "Fromages", "fromages": "Fromages", "cheese": "Fromages",
        "accompagnement": "Accompagnements", "side": "Accompagnements",
        "enfant": "Menu Enfant", "kids": "Menu Enfant", "children": "Menu Enfant"
    }
    
    for plat in menu_result["Plats Indépendants"]:
        if "catégorie" in plat:
            # Chercher des correspondances dans le mapping
            categorie_lower = plat["catégorie"].lower()
            for key, value in categorie_mapping.items():
                if key == categorie_lower or key in categorie_lower:
                    plat["catégorie"] = value
                    break
    
    return menu_result

def batch_structure_menus_with_gpt(raw_texts, restaurant_name, default_rating):
    """
    Analyse en lot plusieurs textes de menu avec GPT-3.5-turbo pour créer une structure cohérente.
    """
    # Prétraiter tous les textes bruts
    processed_texts = [preprocess_text_for_llm(text) for text in raw_texts if text and len(text.strip()) > 50]
    if not processed_texts:
        logger.warning(f"Aucun texte valide pour {restaurant_name} après prétraitement")
        return {"Menus Globaux": [], "Plats Indépendants": []}
    
    all_menus = {"Menus Globaux": [], "Plats Indépendants": []}
    for i, processed_text in enumerate(processed_texts):
        cache_key = f"gpt_menu_structure_{restaurant_name}_{i}_{hashlib.md5(processed_text[:100].encode()).hexdigest()}"
        cached_result = get_from_cache(cache_key, max_age_hours=720, prefix="gpt_menus")
        if cached_result:
            logger.info(f"Utilisation du cache pour le menu de {restaurant_name} (batch {i+1})")
            result = cached_result
        else:
            result = generate_menu_structure_with_openai(processed_text, restaurant_name)
            if result:
                save_to_cache(cache_key, result, prefix="gpt_menus")
        if result and isinstance(result, dict):
            all_menus["Menus Globaux"].extend(result.get("Menus Globaux", []))
            all_menus["Plats Indépendants"].extend(result.get("Plats Indépendants", []))
    return all_menus

def categorize_item(name, description):
    """
    Catégorise un plat en fonction de son nom et de sa description.
    Version améliorée avec meilleure détection des catégories.
    
    Args:
        name (str): Nom du plat
        description (str): Description du plat
        
    Returns:
        str: Catégorie du plat (Entrée, Plat, Dessert, Boisson, etc.)
    """
    name_lower = name.lower() if name else ""
    desc_lower = description.lower() if description else ""
    combined = name_lower + " " + desc_lower
    
    # Mots-clés pour les catégories - élargi pour une meilleure détection
    entree_keywords = [
        "entrée", "starter", "appetizer", "soupe", "salade", "tartare", "carpaccio", "assiette", 
        "foie gras", "velouté", "gaspacho", "terrine", "amuse-bouche", "ceviche", "gravlax",
        "hors d'oeuvre", "antipasti", "bruschetta", "charcuterie"
    ]
    
    plat_keywords = [
        "plat", "main", "principal", "burger", "steak", "viande", "poisson", "pâtes", "pizza", "risotto",
        "poulet", "boeuf", "volaille", "canard", "agneau", "veau", "porc", "filet", "côte", "entrecôte",
        "saumon", "thon", "cabillaud", "lotte", "rôti", "grillé", "braisé", "mijoté", "curry",
        "ravioli", "lasagne", "tagliatelle", "gnocchi", "paella", "couscous", "tajine"
    ]
    
    dessert_keywords = [
        "dessert", "sucré", "gâteau", "cake", "tarte", "fondant", "glace", "sorbet", "mousse", "crème",
        "chocolat", "tiramisu", "profiterole", "éclair", "mille-feuille", "panna cotta", "bavarois",
        "cheesecake", "macaron", "pâtisserie", "crumble", "brownie", "cookie", "financier", "biscuit",
        "crêpe sucrée", "brioche", "pudding", "yaourt", "fruit", "fraise", "vanille", "caramel"
    ]
    
    boisson_keywords = [
        "boisson", "drink", "vin", "wine", "bière", "beer", "soda", "eau", "jus", "café", "thé", 
        "cocktail", "cl", "bouteille", "verre", "carafe", "soft", "spiritueux", "digestif", 
        "champagne", "prosecco", "mocktail", "smoothie", "milkshake", "limonade", "infusion",
        "cappuccino", "espresso", "americano", "latte", "chocolat chaud"
    ]
    
    fromage_keywords = [
        "fromage", "cheese", "camembert", "brie", "comté", "roquefort", "chèvre", "goat cheese",
        "emmental", "gruyère", "parmesan", "bleu", "raclette", "reblochon", "tomme"
    ]
    
    # Vérifier les fromages
    if any(kw in combined for kw in fromage_keywords):
        return "Fromages"
    
    # Vérifier les boissons (priorité car souvent identifiable par la taille)
    if any(kw in combined for kw in boisson_keywords) or re.search(r"\d+\s*cl", combined):
        return "Boisson"
    
    # Vérifier les desserts
    if any(kw in combined for kw in dessert_keywords):
        return "Dessert"
    
    # Vérifier les entrées
    if any(kw in combined for kw in entree_keywords):
        return "Entrée"
    
    # Vérifier les plats principaux
    if any(kw in combined for kw in plat_keywords):
        return "Plat"
    
    # Analyse contextuelle avec expressions régulières
    if re.search(r"entr[ée]e\s+de", combined) or re.search(r"pour commencer", combined, re.IGNORECASE):
        return "Entrée"
    
    if re.search(r"plat\s+principal", combined) or re.search(r"nos plats", combined, re.IGNORECASE):
        return "Plat"
    
    if re.search(r"pour terminer", combined, re.IGNORECASE) or re.search(r"douceur", combined):
        return "Dessert"
    
    # Identifier à partir du prix - les entrées sont généralement moins chères
    price_match = re.search(r"(\d+)[,.](\d+)", combined)
    if price_match:
        price = float(price_match.group(1) + "." + price_match.group(2))
        if price < 10:
            return "Entrée"
        elif price > 20:
            return "Plat"
    
    # Par défaut, considérer comme un plat principal
    return "Plat"

def validate_and_enrich_items(items, default_rating):
    """
    Valide et enrichit les plats avec des valeurs par défaut.
    Version améliorée avec meilleure gestion des erreurs et enrichissement des informations.
    """
    validated_items = []
    
    # Protection contre None pour default_rating
    if default_rating is None or not isinstance(default_rating, (int, float)):
        default_rating = 7.0  # Valeur par défaut sécurisée (3.5 * 2)
    
    # Vérifier que items est bien une liste
    if not isinstance(items, list):
        logger.warning("Liste de plats invalide ou vide, retour d'une liste vide")
        return []
    
    for item in items:
        if not isinstance(item, dict):
            continue
            
        try:
            # Normalisation des champs avec protection contre None
            validated_item = {
                "nom": str(item.get("nom", "Nom non spécifié")).strip(),
                "description": str(item.get("description", "")).strip(),
                "prix": str(item.get("prix", "Non spécifié")).strip(),
                "note": str(item.get("note", f"{default_rating}/10")).strip(),
                "catégorie": str(item.get("catégorie", "Non spécifié")).strip()
            }
            
            # Enrichissement: catégorisation si manquante ou générique
            if validated_item["catégorie"] in ["Non spécifié", "", "non catégorisé", "Autre"]:
                validated_item["catégorie"] = categorize_item(validated_item["nom"], validated_item["description"])
            
            # Extraction et standardisation du prix
            if validated_item["prix"] != "Non spécifié":
                # Vérifier s'il y a un prix
                price_match = re.search(r"(\d+[,.]?\d*)\s*(?:€|EUR|euro|euros)?", validated_item["prix"])
                if price_match:
                    price = price_match.group(1)
                    # Normaliser le format (virgule pour les décimales, toujours avec symbole €)
                    price = price.replace('.', ',')
                    if not ',' in price:
                        price += ",00"
                    if not "€" in validated_item["prix"]:
                        price += " €"
                    validated_item["prix"] = price
            
            # Extractions de métadonnées utiles (taille, origine, allergènes)
            # Taille pour les boissons
            taille_match = re.search(r"(\d{1,4}\s*(?:cl|ml|L|litres?|g|kg))", validated_item["description"], re.IGNORECASE)
            if taille_match:
                validated_item["taille"] = taille_match.group(1)
            
            # Origine/provenance pour viandes et fromages
            origin_match = re.search(r"(?:de|du|d[e'])\s+([A-Z][a-zé]+(?:[- ][A-Z][a-zé]+)?)", validated_item["description"])
            if origin_match and validated_item["catégorie"] in ["Plat", "Fromages"]:
                validated_item["origine"] = origin_match.group(1)
                
            # Allergènes si mentionnés
            if re.search(r"(?:allerg[èé]ne|gluten|lactose|fruit.*coque|arachide)", validated_item["description"], re.IGNORECASE):
                allergen_info = []
                for allergen in ["gluten", "lactose", "fruit.*coque", "arachide", "oeuf", "soja", "poisson", "crustac[ée]"]:
                    if re.search(allergen, validated_item["description"], re.IGNORECASE):
                        match = re.search(f"({allergen}[^,.;]*)", validated_item["description"], re.IGNORECASE)
                        if match:
                            allergen_info.append(match.group(1).strip())
                if allergen_info:
                    validated_item["allergènes"] = allergen_info
            
            validated_items.append(validated_item)
        except Exception as e:
            logger.error(f"Erreur lors de la validation d'un plat: {e}")
            # Continuer avec le plat suivant
    
    return validated_items

def deduplicate_items(items):
    """
    Supprime les doublons dans une liste d'items.
    Version améliorée avec meilleure détection de similarité.
    """
    if not items:
        return []
    
    seen = set()
    unique_items = []
    
    # Fonction de normalisation pour comparaison
    def normalize_for_comparison(text):
        if not text:
            return ""
        # Convertir en minuscules, supprimer accents et ponctuation
        text = text.lower()
        text = re.sub(r'[àáâãäå]', 'a', text)
        text = re.sub(r'[èéêë]', 'e', text)
        text = re.sub(r'[ìíîï]', 'i', text)
        text = re.sub(r'[òóôõö]', 'o', text)
        text = re.sub(r'[ùúûü]', 'u', text)
        text = re.sub(r'[ç]', 'c', text)
        text = re.sub(r'[^\w\s]', '', text)
        return re.sub(r'\s+', ' ', text).strip()
    
    # Première passe: déduplication exacte et construction d'un index de similarité
    similar_items = {}  # nom normalisé -> [indices d'items similaires]
    
    for item in items:
        if not isinstance(item, dict):
            continue
            
        # Créer un identifiant unique (nom + prix)
        identifier = (
            str(item.get("nom", "")).strip().lower(),
            str(item.get("prix", "")).strip().lower()
        )
        
        # Nom normalisé pour recherche de similarité
        norm_name = normalize_for_comparison(item.get("nom", ""))
        
        if identifier not in seen:
            seen.add(identifier)
            unique_items.append(item)
            
            # Ajouter à l'index de similarité
            if norm_name not in similar_items:
                similar_items[norm_name] = [len(unique_items) - 1]
            else:
                similar_items[norm_name].append(len(unique_items) - 1)
    
    # Deuxième passe: fusion des items très similaires
    final_items = []
    processed_indices = set()
    
    for i, item in enumerate(unique_items):
        if i in processed_indices:
            continue
            
        norm_name = normalize_for_comparison(item.get("nom", ""))
        
        # Chercher des items similaires
        if norm_name in similar_items and len(similar_items[norm_name]) > 1:
            # Fusionner les items similaires
            merged_item = item.copy()
            similar_indices = similar_items[norm_name]
            
            for idx in similar_indices:
                if idx != i:  # Éviter de se fusionner avec soi-même
                    similar_item = unique_items[idx]
                    
                    # Conserver les meilleures informations
                    for key in ["description", "catégorie"]:
                        if key in similar_item and (key not in merged_item or len(similar_item[key]) > len(merged_item[key])):
                            merged_item[key] = similar_item[key]
                    
                    processed_indices.add(idx)
            
            final_items.append(merged_item)
            processed_indices.add(i)
        else:
            # Pas de similaire, ajouter tel quel
            final_items.append(item)
            processed_indices.add(i)
    
    return final_items

def process_restaurant_menus(limit=3):
    """
    Pipeline principal pour extraire et structurer les menus.
    Version améliorée avec validation URL et gestion des erreurs renforcée.
    """
    restaurants = fetch_restaurant_websites(limit=limit, processed_only=False)
    
    # Récupérer l'état de progression du checkpoint
    checkpoint = load_checkpoint(checkpoint_name="menu_extraction_enhanced")
    processed_ids = set()
    
    if checkpoint:
        processed_ids = set(checkpoint.get("processed_ids", []))
        logger.info(f"Reprise à partir d'un checkpoint avec {len(processed_ids)} restaurants déjà traités")
    
    for restaurant in restaurants:
        restaurant_id = str(restaurant["_id"])
        
        # Ignorer les restaurants déjà traités
        if restaurant_id in processed_ids:
            logger.info(f"Restaurant {restaurant['name']} déjà traité. Ignoré.")
            continue
        
        name = restaurant["name"]
        website = restaurant["website"]
        
        # Protection renforcée contre les None values dans rating
        raw_rating = restaurant.get("rating")
        rating = (float(raw_rating) if raw_rating is not None else 3.5) * 2  # Conversion en notation sur 10
        
        logger.info(f"\n=== Restaurant : {name} ===")
        
        # VÉRIFICATION CRITIQUE: ne jamais retraiter un restaurant déjà dans la base
        if collection.find_one({"_id": ObjectId(restaurant_id), "menus_structures": {"$exists": True}}):
            logger.info(f"Restaurant {name} déjà traité dans la base de données. Ignoré.")
            processed_ids.add(restaurant_id)
            continue
        
        # Vérifier que le website est valide
        if not is_valid_url(website):
            logger.warning(f"Site web invalide ({website}) pour {name}. Impossible d'extraire les menus.")
            processed_ids.add(restaurant_id)
            continue
            
        # Extraire les liens du site
        links = extract_links_from_website(website)
        # Log de débogage pour voir tous les liens extraits AVANT filtrage
        logger.debug(f"Liens extraits pour {name} AVANT filtrage: {links}") 
        menu_links = filter_menu_links(links, website)
        
        if not menu_links:
            logger.warning(f"Aucun lien de menu trouvé pour {name}. Tentative d'extraction depuis HTML principal.")
            # Fallback: Extraire le texte directement depuis la page HTML principale
            html_text = extract_text_from_html(website)
            if html_text and len(html_text.strip()) > 100: # Minimum de contenu
                logger.info(f"Texte extrait depuis HTML principal pour {name}. Ajout aux sources.")
                # On ne peut pas être sûr que c'est un menu, on le traite comme un texte brut
                raw_texts.append(html_text) 
            else:
                logger.warning(f"Échec de l'extraction depuis HTML principal ou contenu trop court pour {name}.")
                processed_ids.add(restaurant_id) # Marquer comme traité car aucune source trouvée
                continue # Passer au restaurant suivant
        else:
             # Extraire le texte des menus trouvés via les liens
            for link in menu_links:
                logger.info(f"Extraction du menu depuis: {link['href']}")
                text = extract_text_from_link(link["href"])
                if text and len(text.strip()) > 50:  # Ignorer les textes trop courts
                    raw_texts.append(text)
            
        if not raw_texts:
            logger.warning(f"Aucun texte de menu exploitable extrait pour {name} (même après fallback HTML si tenté).")
            processed_ids.add(restaurant_id)
            continue
        
        # Analyser les menus avec Mistral (approche multi-phase)
        logger.info(f"Analyse des menus pour {name}...")
        structured_menus = batch_structure_menus_with_gpt(raw_texts, name, rating)
        
        # Validation, enrichissement et déduplication
        structured_menus["Plats Indépendants"] = deduplicate_items(
            validate_and_enrich_items(structured_menus["Plats Indépendants"], rating)
        )
        
        # Sauvegarder dans MongoDB
        if structured_menus["Menus Globaux"] or structured_menus["Plats Indépendants"]:
            collection.update_one(
                {"_id": ObjectId(restaurant_id)},
                {"$set": {"menus_structures": structured_menus}}
            )
            logger.info(f"Menus sauvegardés pour {name}: {len(structured_menus['Menus Globaux'])} menus, {len(structured_menus['Plats Indépendants'])} plats")
        else:
            logger.warning(f"Aucun menu structuré obtenu pour {name}")
        
        # Marquer comme traité et sauvegarder le checkpoint
        processed_ids.add(restaurant_id)
        save_checkpoint({
            "processed_ids": list(processed_ids),
            "timestamp": time.time()
        }, checkpoint_name="menu_extraction_enhanced")

# --- CONFIGURATION GLOBALE ---
AI_ENABLED = True # Flag pour activer/désactiver les fonctionnalités IA (OCR, etc.)
ENABLE_OPENAI_FALLBACK = True # Flag pour activer/désactiver le fallback OpenAI

# ---- Configuration des répertoires ----
TMP_DIR = "tmp_files"
TMP_PDF_DIR = os.path.join(TMP_DIR, "pdf")
# ... (reste de la configuration) ...

# Lancer le processus
if __name__ == "__main__":
    import argparse
    
    # Définition du parser d'arguments
    parser = argparse.ArgumentParser(description='Extracteur et analyseur de menus de restaurants avec Mistral - Version optimisée multi-phase')
    parser.add_argument('--limit', type=int, default=100, help='Nombre de restaurants à traiter (défaut: 100)')
    parser.add_argument('--no-resume', action='store_true', help='Ne pas utiliser les checkpoints, démarrer depuis le début')
    parser.add_argument('--skip-ai', action='store_true', help="Désactiver l'utilisation de l'IA")
    parser.add_argument('--no-openai', action='store_true', help="Désactiver le fallback vers OpenAI")
    parser.add_argument('--chunk-size', type=int, default=800, help='Taille maximale des chunks (défaut: 800 caractères)')
    
    # Récupérer les arguments de ligne de commande
    args = parser.parse_args()
    
    # Configurer l'activation/désactivation de l'IA
    if args.skip_ai:
        AI_ENABLED = False # Correction: assigner à la variable globale
        logger.info("Fonctionnalité IA désactivée par option --skip-ai")
    else:
        AI_ENABLED = True # Correction: assigner à la variable globale
        logger.info("Fonctionnalité IA activée (utilisant le modèle Mistral)")
        
    # Configurer le fallback OpenAI
    if args.no_openai:
        ENABLE_OPENAI_FALLBACK = False
        logger.info("Fallback vers OpenAI désactivé par option --no-openai")
    
    # Configurer la taille des chunks
    if args.chunk_size != 800:
        logger.info(f"Taille des chunks définie à {args.chunk_size} caractères")
        # Cette variable sera utilisée comme valeur par défaut dans la fonction chunk_text
        
    # Vérifier si tous les modules requis sont installés
    try:
        import PIL
        import fitz
        logger.info("Tous les modules nécessaires sont installés.")
        
        # Vérification OCR optionnelle
        try:
            import pytesseract
            logger.info("Module OCR pytesseract disponible.")
        except ImportError:
            logger.warning("Module OCR pytesseract non disponible. L'OCR local ne sera pas utilisé.")
            logger.info("Installer avec: pip install pytesseract")
            logger.info("Et installez Tesseract OCR: https://github.com/UB-Mannheim/tesseract/wiki")
        
        try:
            # Procéder au traitement
            process_restaurant_menus(limit=args.limit)
        except Exception as e:
            logger.error(f"Erreur lors du traitement: {e}")
            import traceback
            logger.error(traceback.format_exc())  # Log complet de l'erreur pour debug
    
    except ImportError as e:
        logger.error(f"Module manquant: {e}")
        logger.info("Installez les dépendances avec: pip install pymongo requests beautifulsoup4 PyMuPDF python-dotenv pillow pytesseract openai")
