import os
import warnings
import sys
import logging
import urllib3

# Suppress various warnings and verbose logging
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

# Suppress Selenium and Chrome logging
os.environ['WDM_LOG_LEVEL'] = '0'
os.environ['WDM_PRINT_FIRST_LINE'] = 'False'
os.environ['WDM_LOCAL'] = '1'

# Suppress urllib3 warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Suppress TensorFlow logging if present
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
