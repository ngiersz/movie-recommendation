from flask import Flask, request, redirect, url_for, Response
import engine.api_logic as client

app = Flask(__name__)
client = client.RatingsClient()


@app.route('/')
def index():
    return 'hello world'


@app.route('/ratings', methods=['GET'])
def ratings():
    return client.get_ratings_json()


@app.route('/ratings', methods=['DELETE'])
def delete_rating():
    client.delete_rating()
    return '', 204


@app.route('/rating', methods=['POST'])
def add_rating():
    return Response(client.add_to_ratings(request.data), status=201, mimetype='application/json')


@app.route('/avg-genre-ratings/all-users', methods=['GET'])
def get_avg_genre_ratings():
    return client.avg_genre_ratings()
    # return Response(client.avg_genre_ratings(), status=201, mimetype='application/json')


# request format: /avg-genre-ratings?user=1
@app.route('/avg-genre-ratings', methods=['GET'])
def get_user_avg_genre_ratings():
    user = request.args.get('user', type=int)
    return Response(client.avg_genre_ratings_user(user), status=201, mimetype='application/json')


# request format: /profile?user=1
@app.route('/profile', methods=['GET'])
def get_user_profile():
    user = request.args.get('user', type=int)
    return Response(client.get_updated_profile(user), status=201, mimetype='application/json')


if __name__  == "__main__":
    app.run()
