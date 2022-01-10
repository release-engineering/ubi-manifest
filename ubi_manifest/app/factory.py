from flask import Flask


def create_app(test_config=None):
    app = Flask(__name__)

    if test_config is None:  # pragma: no cover
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile("config.py", silent=True)
    else:
        # load the test config if passed in
        app.config.update(test_config)

    from ubi_manifest.app import api

    app.register_blueprint(api.bp)

    return app
