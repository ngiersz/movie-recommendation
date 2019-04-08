import pandas as pd
import json


class RatingsClient:
    ratings = pd.DataFrame()

    def __init__(self):
        self.ratings = self.create_ratings()
        self.ratings = self.ratings.reset_index()

        print(self.ratings.head())
        print('client init')

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

    def get_ratings_and_genres_column_names(self):
        ratings = self.create_ratings()
        genres_column_names = self.get_genres_column_names(self, ratings)
        return ratings, genres_column_names

    def get_genres_column_names(self, ratings):
        genres_column_names = []
        for col in ratings:
            if col.startswith('genre'):
                genres_column_names.append(col)
        return genres_column_names

    def get_ratings_json(self):
        return self.ratings.to_json(orient='index')

    def delete_rating(self):
        self.ratings = self.ratings.iloc[0:0]
        print('after del: ' + str(self.ratings.size))
        print(self.ratings.head())

    def add_json_to_ratings(self, rating):
        rating_series = pd.Series(json.loads(rating))
        self.ratings = self.ratings
        # print(self.ratings)
        # print('before add: ' + str(self.ratings.shape))
        self.ratings = self.ratings.append(rating_series, ignore_index=True)
        # self.ratings = self.ratings.append(rating_series[~['userID', 'movieID', 'rating']], ignore_index=True)
        # print('after add: ' + str(self.ratings.shape))
        # print(self.ratings)
        print('added new rating userId=' + str(rating_series['userID']) + ' movieID=' + str(rating_series['movieID']) +
              ' rating=' + str(rating_series['rating']))

    def avg_genre_ratings(self):
        avg_ratings = pd.DataFrame()
        for col in self.ratings:
            if col.startswith('genre'):
                genre_avg = self.ratings.groupby(col).rating.mean()[1]
                avg_ratings = avg_ratings.append(pd.Series([genre_avg], index=[col]), ignore_index=True)
        return avg_ratings.sum().to_json()

    def avg_genre_ratings_user(self, user_id):
        avg_ratings = pd.DataFrame()
        user_ratings = self.ratings[self.ratings.userID == user_id]
        for col in user_ratings:
            if col.startswith('genre'):
                try:
                    genre_avg = user_ratings.groupby(col).rating.mean()[1]
                    avg_ratings = avg_ratings.append(pd.Series([genre_avg], index=[col]), ignore_index=True)
                except KeyError:
                    avg_ratings = avg_ratings.append(pd.Series([0.0], index=[col]), ignore_index=True)
        avg_ratings = avg_ratings.sum()
        return avg_ratings.to_json()

    def get_avg_ratings_for_genres(self):
        genres = self.get_genres_column_names(self.ratings)
        all_ratings = pd.DataFrame()
        for genre in genres:
            genre_ratings = self.ratings[self.ratings[genre] == 1]
            all_ratings = all_ratings.append(
                pd.Series(genre_ratings.rating.mean(), index=[genre]), ignore_index=True)
        # print('all users:')
        # print(all_ratings.sum())
        return all_ratings.sum()

    def get_avg_user_ratings_for_genres(self, user_id):
        genres = self.get_genres_column_names(self.ratings)
        user_ratings = self.ratings[self.ratings.userID == user_id]
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


if __name__ == "__main__":
    # create_ratings_json()
    cl = RatingsClient()
    # print(cl.get_ratings_and_genres_column_names()[1])

    #  zad 4
    # list_of_dict = cl.get_list_of_dict(cl.ratings)
    # print(l.__getitem__(1))
    # data_check = cl.get_df_from_list_of_dict(list_of_dict)
    # print(cl.ratings.shape)
    # print(data_check.shape)

    # unbiased = cl.get_user_profile_unbiased(75)
    # print('unbiased:')
    # print(unbiased)

    print(cl.get_ratings_json())
