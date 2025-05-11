# Project Instance Scripts Overview

This document provides an overview of the Python scripts located in the `Instance` directory, detailing their purpose and how they interrelate.

## Scripts

### 1. `wellness.py` (Data Collection)

*   **Purpose:** This script is designed to collect and process data about beauty and wellness establishments.
*   **Functionality:**
    *   Scrapes data from Google Maps (places, details, reviews, photos) using the Google Places API. It also has capabilities to use BrightData for enhanced scraping.
    *   Caches scraped results in MongoDB (`Beauty_Wellness` database) to optimize API usage.
    *   Performs sentiment analysis on user reviews using `vaderSentiment`.
    *   Categorizes establishments (e.g., "Institut de beauté", "Spa", "Salon de massage").
    *   Extracts detailed information such as websites, phone numbers, and opening hours.
    *   Can utilize OpenAI GPT-3.5-turbo for advanced review analysis and Bing Search for finding related links (e.g., Tripadvisor).
    *   Includes features for taking screenshots of place pages.
    *   Saves all processed data to the MongoDB `BeautyPlaces` collection.
*   **Primary Data Source:** Google Maps API, BrightData (optional).
*   **Output Database:** MongoDB (`Beauty_Wellness` database, `BeautyPlaces` and cache collections).

### 2. `billetreduc_shotgun_mistral.py` (Data Collection)

*   **Purpose:** This script focuses on gathering and processing data for events and venues, primarily for leisure and cultural activities in Paris.
*   **Functionality:**
    *   Scrapes event and venue data from two main sources: `BilletReduc.com` and `Shotgun.live`.
    *   Utilizes a combination of `requests`, `BeautifulSoup`, `Playwright`, and `Selenium` for web scraping.
    *   Stores scraped data (events and producers/venues) in MongoDB (`Loisir&Culture` database, specifically `Loisir_Paris_Evenements` and `Loisir_Paris_Producers` collections).
    *   Performs geocoding of addresses, previously using Google Geocoding API, with some parts potentially refactored to use Selenium-based lookups.
    *   Standardizes event categories across different sources.
    *   Features AI-driven analysis of event comments/reviews using OpenAI GPT-3.5-turbo to identify aspects and emotions.
    *   Manages image uploads for venues, potentially using ImgBB.
    *   Includes checkpointing to allow for resumption of long scraping/processing tasks.
*   **Primary Data Sources:** BilletReduc.com, Shotgun.live.
*   **Output Database:** MongoDB (`Loisir&Culture` database, `Loisir_Paris_Evenements`, `Loisir_Paris_Producers` collections).

### 3. `pipeline_complet_fixed.py` (Data Collection & Enrichment)

*   **Purpose:** This script is a comprehensive pipeline dedicated to gathering detailed information about restaurants in Paris. It is designed to minimize reliance on costly Google Places API calls for full details.
*   **Functionality:**
    *   Initiates restaurant discovery using Google Maps Nearby Search API.
    *   For detailed data extraction:
        *   Captures screenshots of restaurant pages on Google Maps.
        *   Applies OCR (Pytesseract) to extract text (e.g., opening hours, address) from these screenshots.
        *   Uses OpenAI (GPT) for structured data extraction from the OCR output.
    *   Searches Bing for links to restaurant listings on major platforms like TheFork and TripAdvisor.
    *   Scrapes these platforms for rich information including reviews, menus (links to menus, not necessarily full parsing here), photos, and detailed opening hours.
    *   Supports BrightData for robust scraping.
    *   Saves all aggregated and processed restaurant data into the `producers` collection of the `Restauration_Officielle` MongoDB database.
    *   Includes features like parallel processing, data caching, and detailed logging.
*   **Primary Data Sources:** Google Maps Nearby Search API (for discovery), Google Maps (via Selenium for screenshots), Bing Search, TheFork, TripAdvisor. BrightData (optional).
*   **Output Database:** MongoDB (`Restauration_Officielle` database, `producers` collection).

### 4. `menu_sur_mongo_mistral_improved.py` (Data Extraction & Structuring)

*   **Purpose:** This script specializes in finding, extracting, and structuring detailed menu information for restaurants.
*   **Functionality:**
    *   Takes restaurant website URLs (often sourced from data collected by `pipeline_complet_fixed.py`) as input.
    *   Scans websites to find links to menus (PDFs, images, pages with menu text, Google Drive/Dropbox links, etc.).
    *   Extracts raw text content from these various menu formats (PDFs using PyMuPDF, images via OCR - potentially Google Vision or another engine, HTML).
    *   Employs a multi-phase AI approach (using OpenAI GPT, despite "Mistral" in the filename) to:
        1.  Identify menu sections (starters, main courses, desserts, drinks).
        2.  Extract individual dishes, descriptions, and prices within each section.
        3.  Structure this information into a standardized JSON format.
    *   Handles large menus by chunking text for the LLM.
    *   Updates the restaurant documents in the `producers` collection of the `Restauration_Officielle` MongoDB database with the structured menu data (likely in a field like `menus_structures`).
    *   Includes robust caching for API calls and downloaded content, plus checkpointing.
*   **Primary Data Sources:** Restaurant websites, menu files (PDF, JPG, PNG), Google Drive links. OpenAI GPT for text understanding and structuring.
*   **Output Database:** MongoDB (`Restauration_Officielle` database, updates `producers` collection with structured menu data).

### 5. `openai_fake_user_generator.py` (User Generation)

*   **Purpose:** This script generates synthetic, realistic-looking user profiles for a hypothetical application called "Choice App".
*   **Functionality:**
    *   Creates user profiles with names, emails, hashed passwords, gender, age, and profile photos (from DiceBear/Unsplash).
    *   Assigns users realistic Paris locations and diverse interests (food, culture, beauty/wellness).
    *   Generates detailed user preferences for different sectors.
    *   Simulates social graphs by establishing connections (following/followers) between these fake users.
    *   Creates affinities between users and "producers" (venues/businesses from the `Restauration_Officielle`, `Loisir&Culture`, and `Beauty_Wellness` databases).
    *   Saves the generated user profiles into the `Users` collection in the `choice_app` MongoDB database.
*   **Primary Data Sources:** Predefined lists (names), Unsplash/DiceBear (avatars), existing producer data from other MongoDB databases.
*   **Output Database:** MongoDB (`choice_app` database, `Users` collection).

### 6. `openai_post_generator.py` (Content Generation)

*   **Purpose:** This script leverages the fake users (from `openai_fake_user_generator.py`) and the venue/event data (from other scripts) to generate posts for the "Choice App".
*   **Functionality:**
    *   Uses OpenAI GPT-3.5-turbo to generate textual content for posts.
    *   Creates two main types of posts:
        *   **Producer Posts:** Promotional or informational content related to restaurants, events, or beauty/wellness places, as if posted by the businesses themselves.
        *   **User Posts:** Simulated user experiences, reviews, and check-ins at various venues, posted by the fake users.
    *   For user posts, it considers user profiles, their (simulated) visit history, venue categories, and generates relevant ratings, review text, and emotions.
    *   Incorporates media (photos/videos) likely sourced from the venue data collected by other scripts.
    *   Saves generated posts into the `Posts` collection in the `choice_app` MongoDB database.
    *   Features caching for OpenAI responses and checkpointing for generation tasks.
*   **Primary Data Sources:** User profiles from `choice_app.Users`, venue/event data from `Restauration_Officielle.producers`, `Loisir&Culture.Loisir_Paris_Evenements`, `Loisir&Culture.Loisir_Paris_Producers`, and `Beauty_Wellness.BeautyPlaces`. OpenAI GPT-3.5-turbo for text generation.
*   **Output Database:** MongoDB (`choice_app` database, `Posts` collection).

## Inter-Script Relationships & Data Flow

The scripts often work in a sequence or rely on data produced by others:

1.  **Data Collection Scripts (Sources of Truth):**
    *   `wellness.py`: Collects primary data for **beauty and wellness places**.
    *   `billetreduc_shotgun_mistral.py`: Collects primary data for **events and cultural venues**.
    *   `pipeline_complet_fixed.py`: Collects primary data for **restaurants**.

2.  **Specialized Data Extraction & Enrichment:**
    *   `menu_sur_mongo_mistral_improved.py`: Takes restaurant data (especially website URLs from `pipeline_complet_fixed.py`) and enriches it with detailed, structured **menu information**.

3.  **Synthetic User & Content Generation:**
    *   `openai_fake_user_generator.py`: Creates fake users for the "Choice App". This script may read from the producer collections to establish user affinities.
    *   `openai_post_generator.py`: This is a consumer of data from all previous stages. It uses the fake users and the collected/enriched business/event data to generate posts.

**Overall Workflow Idea:**

*   First, the data collection scripts (`wellness.py`, `billetreduc_shotgun_mistral.py`, `pipeline_complet_fixed.py`) populate MongoDB with information about real-world places and events.
*   `menu_sur_mongo_mistral_improved.py` then further processes the restaurant data to add detailed menus.
*   `openai_fake_user_generator.py` creates a population of simulated users who have preferences and connections related to the collected real-world data.
*   Finally, `openai_post_generator.py` uses all this information (real places, detailed menus, fake users) to generate dynamic content (posts and reviews) for the "Choice App", making it appear active and populated.

This ecosystem of scripts allows for the creation of a rich, simulated environment for the "Choice App", from sourcing real-world data to generating user interactions around it. 