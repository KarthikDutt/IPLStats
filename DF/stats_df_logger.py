import logging
from logging.handlers import RotatingFileHandler
import configparser as cp

config = cp.ConfigParser()
config.read("config.ini")
logger = config['BASIC']['logfile']

print("Initiating Logger File")
formatter = logging.Formatter("[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s")
handler = RotatingFileHandler(logger, maxBytes=100000, backupCount=2)
handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)