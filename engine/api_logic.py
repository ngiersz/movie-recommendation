import pandas as pd
import json
import engine.db_client as db


class RatingsClient:
    ratings = pd.DataFrame()
    db = db.DBClient()
    RATINGS_QUEUE = 'ratings'

    def __init__(self, fill=False):
        if fill:
            self.db.flush_queue(self.RATINGS_QUEUE)
            ratings = self.create_ratings()
            ratings = ratings.reset_index()
            # print(ratings)
            self.db.add_df(self.RATINGS_QUEUE, ratings)

    def create_ratings(self):
        return self.create_merged_ratings_and_genres().sum()

    def create_merged_ratings_and_genres(self):
        df_user_rated = pd.read_csv('data/user_ratedmovies.dat', sep='\t', nrows=100)
        df_movie_genres = pd.read_csv('data/movie_genres.dat', sep='\t')
        merged = df_user_rated.merge(df_movie_genres, on='movieID')
        ratings_multi_hot = pd.concat([merged, pd.get_dummies(merged['genre'], prefix='genre')], axis=1)
        ratings_multi_hot = ratings_multi_hot.drop(
            columns=['date_day', 'date_month', 'date_year', 'date_hour', 'date_minute', 'date_second'])
        ratings_multi_hot = ratings_multi_hot.groupby(['userID', 'movieID', 'rating'])
        return ratings_multi_hot

    def get_ratings(self):
        return self.db.get_all(self.RATINGS_QUEUE)

    def get_ratings_and_genres_column_names(self):
        ratings = self.get_ratings()
        genres_column_names = self.get_genres_column_names(ratings)
        return ratings, genres_column_names

    def get_genres_column_names(self, ratings):
        genres_column_names = []
        for col in ratings:
            if col.startswith('genre'):
                genres_column_names.append(col)
        return genres_column_names

    def delete_all_ratings(self):
        self.db.flush_queue(self.RATINGS_QUEUE)

    # profiles:
    def create_all_profiles(self):
        users = self.get_ratings().userID.unique()
        for user in users:
            user = int(user)
            print(user)
            self.db.add(user, self.get_user_profile_unbiased(user).to_json())

    def get_profile(self, user_id):
        profile = self.get_user_profile_unbiased(user_id).to_json()
        self.db.add(user_id, profile)
        return profile

    # avgerage ratings:
    def get_avg_ratings_for_genres(self):
        ratings = self.get_ratings()
        genres = self.get_genres_column_names(ratings)
        all_ratings = pd.DataFrame()
        for genre in genres:
            genre_ratings = ratings[ratings[genre] == 1]
            all_ratings = all_ratings.append(
                pd.Series(genre_ratings.rating.mean(), index=[genre]), ignore_index=True)
        # print('all users:')
        # print(all_ratings.sum())
        return all_ratings.sum()

    def get_avg_user_ratings_for_genres(self, user_id):
        ratings = self.get_ratings()
        genres = self.get_genres_column_names(ratings)
        user_ratings = ratings[ratings.userID == user_id]
        user_ratings_with_genres = pd.DataFrame()
        for genre in genres:
            genre_ratings = user_ratings[user_ratings[genre] == 1]
            user_ratings_with_genres = user_ratings_with_genres.append(
                pd.Series(genre_ratings.rating.mean(), index=[genre]), ignore_index=True)
        user_ratings_with_genres = user_ratings_with_genres.sum()
        # print('biased: ')
        # print(user_ratings_with_genres)
        return user_ratings_with_genres

    def get_user_profile_unbiased(self, user_id):
        return self.get_avg_ratings_for_genres() - self.get_avg_user_ratings_for_genres(user_id)

    # json
    def get_ratings_json(self):
        return self.get_ratings().to_json(orient='index')

    def add_json_to_ratings(self, rating):
        rating_series = pd.Series(json.loads(rating))
        self.db.add(self.RATINGS_QUEUE, json.loads(rating))
        print('added new rating userId=' + str(rating_series['userID']) + ' movieID=' + str(rating_series['movieID']) +
              ' rating=' + str(rating_series['rating']))

    def avg_genre_ratings(self):
        avg_ratings = pd.DataFrame()
        ratings = self.get_ratings()
        for col in ratings:
            if col.startswith('genre'):
                genre_avg = ratings.groupby(col).rating.mean()[1]
                avg_ratings = avg_ratings.append(pd.Series([genre_avg], index=[col]), ignore_index=True)
        return avg_ratings.sum().to_json()

    def avg_genre_ratings_user(self, user_id):
        avg_ratings = pd.DataFrame()
        user_ratings = self.get_ratings()[self.get_ratings().userID == user_id]
        for col in user_ratings:
            if col.startswith('genre'):
                try:
                    genre_avg = user_ratings.groupby(col).rating.mean()[1]
                    avg_ratings = avg_ratings.append(pd.Series([genre_avg], index=[col]), ignore_index=True)
                except KeyError:
                    avg_ratings = avg_ratings.append(pd.Series([0.0], index=[col]), ignore_index=True)
        avg_ratings = avg_ratings.sum()
        return avg_ratings.to_json()

    def get_user_profile_unbiased_json(self, user_id):
        return self.get_user_profile_unbiased(user_id).to_json()

    # dict
    def get_avg_ratings_for_genres_dict(self):
        return self.get_avg_ratings_for_genres().to_dict()


if __name__ == "__main__":
    # create_ratings_json()
    cl = RatingsClient(fill=False)
    # cl.create_all_profiles()
    # print(cl.get_profile(75))
    # cl.delete_all_ratings()
    # print(cl.get_ratings())

    #  zad 4
    # list_of_dict = cl.get_list_of_dict(cl.ratings)
    # print(l.__getitem__(1))
    # data_check = cl.get_df_from_list_of_dict(list_of_dict)
    # print(cl.ratings.shape)
    # print(data_check.shape)

    unbiased = cl.get_user_profile_unbiased(75)
    print('unbiased:')
    print(unbiased)

    # print(cl.get_avg_ratings_for_genres_dict())

