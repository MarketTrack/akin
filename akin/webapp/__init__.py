from akin import __version__, Akin
from flask import Flask
from yaml import safe_load
from os import chdir, path


def create_app():
    app = Flask(__name__, instance_relative_config=True)

    log = app.logger
    try:
        with app.open_instance_resource('service-log.yml') as configuration_file:
            from logging.config import dictConfig
            try:
                dictConfig(safe_load(configuration_file))
                log.info('Log configuration loaded from: %s', configuration_file.name)
            except Exception as e:
                log.warning('Log configuration skipped; invalid configuration: %s', e)
    except FileNotFoundError as e:
        log.warning('Log configuration skipped; missing configuration file: %s', e.filename)

    app.secret_key = f'{__file__}:{__version__}'
    with app.open_instance_resource('service.yml') as configuration_file:
        app.config.from_mapping(safe_load(configuration_file))

    chdir(app.instance_path)

    akin = Akin(path.join(app.instance_path, 'brand_settings.yml'))
    akin.initialize()

    from akin.webapp.routes import blueprint as ui_blueprint
    app.register_blueprint(ui_blueprint, akin=akin)

    return app
