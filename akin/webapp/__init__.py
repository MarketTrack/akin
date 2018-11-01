from akin import __version__, Akin
from flask import Flask
from yaml import safe_load
from os import chdir, path

flask_app = Flask(__name__, instance_relative_config=True)
flask_app.secret_key = f'{__file__}:{__version__}'
with flask_app.open_instance_resource('service.yml') as configuration_file:
    flask_app.config.from_mapping(safe_load(configuration_file))

chdir(flask_app.instance_path)

akin = Akin(path.join(flask_app.instance_path, 'brand_settings.yml'))
akin.initialize()

from akin.webapp import routes
