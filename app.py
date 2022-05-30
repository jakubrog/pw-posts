import logging

from flask import Flask, jsonify, request
from flask_cors import CORS

from posts import Post, Posts

app = Flask(__name__)
CORS(app)
posts = Posts()
logging.basicConfig(level=logging.INFO, format='%(levelname)s %(asctime)s : %(message)s')


@app.route('/post/<post_id>', methods=['GET'])
def get_post(post_id):
    return jsonify(posts.get_post_by(post_id))


@app.route('/post', methods=['POST'])
def add_post():
    post = Post().load(request.get_json(force=True))
    if posts.add_post(post['title'], post['content'], post['author']):
        return {}, 201
    return {}, 500


@app.route('/post/<post_id>', methods=['DELETE'])
def delete_post(post_id):
    if posts.delete_post(post_id):
        return {}, 204
    return {}, 404


@app.route('/post/<post_id>', methods=['PUT'])
def update_post(post_id):
    post = Post().load(request.get_json(force=True))
    if posts.update_post(post_id, post['title'], post['content']):
        return get_post(post_id), 200
    return {}, 404


@app.route('/posts', methods=['GET'])
def get_posts():
    return jsonify(posts.get_all_posts())


@app.errorhandler(Exception)
def handle_bad_request(e):
    logging.error("An error occurred: ", e)
    return {}, 400


if __name__ == '__main__':
    app.run()
