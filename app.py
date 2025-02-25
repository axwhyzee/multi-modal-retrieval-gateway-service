import logging
from io import BytesIO

from event_core.adapters.services.exceptions import ObjectNotExists
from flask import Flask, request, send_file
from flask_cors import CORS

from handlers import handle_add, handle_object_get, handle_query_text

app = Flask(__name__)
CORS(app)


@app.route("/add", methods=["POST"])
def add():
    if "file" not in request.files:
        return "File required", 400
    if "user" not in request.form:
        return "`user` required", 400

    file = request.files["file"]
    user: str = request.form["user"]
    handle_add(file.read(), file.filename, user)
    return "Success", 200


@app.route("/query/text", methods=["GET"])
def query_text():
    user: str = request.args["user"]
    text: str = request.args["text"]
    return handle_query_text(user, text, n_cands=20, n_rank=4)


@app.route("/object/get/<path:obj_path>", methods=["GET"])
def object_get(obj_path: str):
    try:
        obj_data = handle_object_get(obj_path)
    except ObjectNotExists as e:
        return e, 404
    return send_file(
        BytesIO(obj_data), download_name=obj_path, as_attachment=True
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app.run(port=5004)
