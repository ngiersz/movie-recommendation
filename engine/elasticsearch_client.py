import pandas as pd
from elasticsearch import Elasticsearch, helpers
import numpy as np
import random


class ElasticClient:
    def __init__(self, address='localhost:10000'):
        self.es = Elasticsearch(address)

    def index_documents(self):
        df = pd.read_csv('data/user_ratedmovies.dat', delimiter='\t').loc[:, ['userID', 'movieID', 'rating']]
        means = df.groupby(['userID'], as_index=False, sort=False).mean().loc[:, ['userID', 'rating']].rename(columns={'rating': 'ratingMean'})
        df = pd.merge(df, means, on='userID', how="left", sort=False)
        df['ratingNormal'] = df['rating'] - df['ratingMean']
        ratings = df.loc[:, ['userID', 'movieID', 'ratingNormal']] \
            .rename(columns={'ratingNormal': 'rating'}) \
            .pivot_table(index='userID', columns='movieID', values='rating') \
            .fillna(0)
        print("Indexing users...")
        index_users = [{
            "_index": "users",
            "_type": "user",
            "_id": index,
            "_source": {
                'ratings': row[row > 0] \
                    .sort_values(ascending=False) \
                    .index.values.tolist()
            }
        } for index, row in ratings.iterrows()]
        helpers.bulk(self.es, index_users)
        print("Done")
        print("Indexing movies...")
        index_movies = [{
            "_index": "movies",
            "_type": "movie",
            "_id": column,
            "_source": {
                "whoRated": ratings[column][ratings[column] > 0] \
                    .sort_values(ascending=False) \
                    .index.values.tolist()
            }
        } for column in ratings]
        helpers.bulk(self.es, index_movies)
        print("Done")

    def get_movies_liked_by_user(self, user_id, index='users'):
        user_id = int(user_id)

        return self.es.get(index=index, doc_type="user", id=user_id)["_source"]

    def get_users_that_like_movie(self, movie_id, index='movies'):
        movie_id = int(movie_id)
        return self.es.get(index=index, doc_type="movie", id=movie_id)["_source"]

    def get_preselection_for_user(self, userID, index):
        user_movies_ids = self.get_movies_liked_by_user(userID, index)
        body = {
            "query": {

                "terms": {
                    "ratings": user_movies_ids.get('ratings')
                }

            }
        }
        similar_users = self.es.search(index='users', body=body)['hits']['hits']
        recommended_movies = []
        for user in similar_users:
            recommended_movies += (user['_source'].get('ratings'))
        recommended_movies = set(recommended_movies) - set(user_movies_ids.get('ratings'))
        recommended_movies = list(set(recommended_movies))
        # print(recommended_movies)
        return recommended_movies

    def get_preselection_for_movie(self, movieID, index):
        users_ids = self.get_users_that_liked_this_movie(movieID, index)
        # print(movies_ids)
        body = {
            "query": {

                "terms": {
                    "whoRated": users_ids.get('whoRated')
                }

            }
        }
        similar_movies = self.es.search(index='movies', body=body)['hits']['hits']
        recommended_users = []
        for user in similar_movies:
            recommended_users += (user['_source'].get('whoRated'))
        recommended_users = set(recommended_users) - set(users_ids.get('whoRated'))
        recommended_users = list(set(recommended_users))
        print(recommended_users)
        return recommended_users

    def get_users_that_liked_this_movie(self, movie_id, index):
        # movie_id = int(movie_id)
        return self.es.get(index=index, doc_type="movie", id=movie_id)["_source"]


# CRUD
    def add_movie_document(self, movie_id, users_who_like_movie, user_index, movie_index):
        index_movies = {
            "_index": "movies",
            "_type": "movie",
            "_id": movie_id,
            "_source": {
                "whoRated": users_who_like_movie
            }
        }
        self.es.index(index_movies)



#     def add_user_document(self, user_id, movies_liked_by_user, user_index, movie_index):
#         index_users = {
#             "_index": user_index,
#             "_type": "user",
#             "_id": user_id,
#             "_source": {
#                 'ratings': movies_liked_by_user
#             }
#         }
#         # helpers.bulk(self.es, index_users)
#         self.es.index(index_users)
#  add user
#  update movies that this user liked


if __name__ == "__main__":
    ec = ElasticClient()
    # ec.index_documents()

    # ------ Simple operations ------
    print()
    user_document = ec.get_movies_liked_by_user(75)
    movie_id = np.random.choice(user_document['ratings'])
    movie_document = ec.get_users_that_like_movie(movie_id)
    random_user_id = np.random.choice(movie_document['whoRated'])
    random_user_document = ec.get_movies_liked_by_user(random_user_id)
    print('User 75 likes following movies:')
    print(user_document)
    print('Movie {} is liked by following users:'.format(movie_id))
    print(movie_document)
    print('Is user 75 among users in movie {} document?'.format(movie_id))
    print(movie_document['whoRated'].index(75) != -1)

    some_test_movie_ID = 1
    print("Some test movie ID: ", some_test_movie_ID)
    list_of_users_who_liked_movie_of_given_ID = ec.get_users_that_like_movie(some_test_movie_ID)["whoRated"]
    print("List of users who liked the test movie: ", *list_of_users_who_liked_movie_of_given_ID)
    index_of_random_user_who_liked_movie_of_given_ID = random.randint(0, len(list_of_users_who_liked_movie_of_given_ID))
    print("Index of random user who liked the test movie: ", index_of_random_user_who_liked_movie_of_given_ID)
    some_test_user_ID = list_of_users_who_liked_movie_of_given_ID[index_of_random_user_who_liked_movie_of_given_ID]
    print("ID of random user who liked the test movie: ", some_test_user_ID)
    movies_liked_by_user_of_given_ID = ec.get_movies_liked_by_user(some_test_user_ID)["ratings"]
    print("IDs of movies liked by the random user who liked the test movie: ", *movies_liked_by_user_of_given_ID)
    if some_test_movie_ID in movies_liked_by_user_of_given_ID:
        print("As expected, the test movie ID is among the IDs of movies " + "liked by the random user who liked the test movie ;-)")

    # ec.get_preselected_list(75)
    ec.get_preselection_for_movie('3', 'movies')
    print(ec.get_users_that_liked_this_movie(1, 'movies'))
    # ec.add_movie_document(10000, [1, 2], None, None)