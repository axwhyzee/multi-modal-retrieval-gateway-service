from flask import Flask, request
from flask_cors import CORS

from handlers import handle_add, handle_text_query

app = Flask(__name__)
CORS(app)


@app.route("/add", methods=["POST"])
def serve_add():
    if "file" not in request.files:
        return "File required", 400
    if "user" not in request.form:
        return "`user` required", 400

    file = request.files["file"]
    user: str = request.form["user"]
    handle_add(file.read(), file.name, user)
    return "Success", 200


@app.route("/query/text", methods=["GET"])
def serve_text_query():
    user: str = request.args["user"]
    text: str = request.args["text"]
    return handle_text_query(user, text)


if __name__ == "__main__":
    app.run(port=5004)
