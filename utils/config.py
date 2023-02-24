import os

from dotenv import load_dotenv, find_dotenv

root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
load_dotenv(find_dotenv(root_path + '/.env'))



# ------------------------------
# |       Logger Config        |
# ------------------------------
# Logger type   -> persist | show
# Logger Format -> string  | json
LOGGER_TYPE = os.getenv('LOGGER_TYPE')
LOGGER_LEVEL = os.getenv('LOGGER_LEVEL')
LOGGER_FORMAT = os.getenv('LOGGER_FORMAT')


