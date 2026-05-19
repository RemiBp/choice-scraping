#!/bin/bash
# Run this on a fresh Ubuntu 22.04 instance (EC2 t3.medium or t3.large)
# Usage: bash setup_instance.sh

set -e
echo "=== Installing system dependencies ==="
sudo apt-get update -y
sudo apt-get install -y \
  python3-pip python3-venv python3-dev \
  chromium-browser chromium-driver \
  tesseract-ocr tesseract-ocr-fra \
  libpoppler-cpp-dev poppler-utils \
  xvfb libglib2.0-0 libnss3 libgconf-2-4 \
  libfontconfig1 libxrender1 libxtst6 \
  unzip curl git

echo "=== Setting up Python venv ==="
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

echo "=== Installing Playwright browsers ==="
playwright install chromium
playwright install-deps chromium

echo "=== Done! ==="
echo "Run scripts with: source .venv/bin/activate"
echo "  python wellness.py"
echo "  python billetreduc_shotgun_mistral.py"
echo "  python pipeline_complet_fixed.py --zones 5   # test with 5 zones first"
echo "  python pipeline_complet_fixed.py              # full Paris run"
echo "  python menu_sur_mongo_mistral_improved.py"
echo "  python openai_fake_user_generator.py"
echo "  python openai_post_generator.py"
