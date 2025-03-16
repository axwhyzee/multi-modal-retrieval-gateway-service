import logging
from io import BytesIO

from flask import Flask, request, send_file
from flask_cors import CORS

from bootstrap import bootstrap
from handlers import handle_add, handle_get, handle_list, handle_query_text


def create_app():
    logging.basicConfig(level=logging.INFO)
    app = Flask(__name__)
    CORS(app)
    bootstrap()
    return app


app = create_app()


@app.route("/add", methods=["POST"])
def add():
    if "file" not in request.files:
        return "File required", 400
    if "user" not in request.form:
        return "`user` required", 400
    
    file = request.files["file"]
    user = request.form["user"]
    filename = request.form.get("filename", file.filename)
    handle_add(file.read(), filename, user)
    return "Success", 200


@app.route("/query/text", methods=["GET"])
def query_text():
    user = request.args["user"]
    text = request.args["text"]
    top_n = int(request.args.get("top_n", 5))
    exclude_elems = request.args.get("exclude_elems")
    return handle_query_text(user, text, top_n, exclude_elems)


@app.route("/get/<path:path>", methods=["GET"])
def get(path: str):
    try:
        data = handle_get(path)
    except KeyError as e:
        return str(e), 404
    return send_file(BytesIO(data), download_name=path, as_attachment=False)


@app.route("/list", methods=["GET"])
def list_objs():
    return handle_list()


if __name__ == "__main__":
    app.run(port=5004)
