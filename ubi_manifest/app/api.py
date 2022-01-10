from flask import Blueprint, jsonify

bp = Blueprint("api", __name__, url_prefix="/api/v1")


@bp.route("/status")
def status():
    return jsonify(status="OK")
