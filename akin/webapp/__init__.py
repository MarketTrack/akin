from flask import Flask
from akin import Akin

UPLOAD_FOLDER = 'c:\\src\\data\\akin'
ALLOWED_EXTENSIONS = set(['txt', 'csv'])

webapp = Flask(__name__)
webapp.secret_key = 'its a secret to everyone'
webapp.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

akin = Akin('brand_settings.json')
akin.initialize()

from webapp import routes
