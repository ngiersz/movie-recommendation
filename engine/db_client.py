import engine.redis_client as redis
import pandas as pd
import json
from cassandra.cluster import Cluster
import engine.cass_client as cass
from cassandra.query import dict_factory


class DBClient:

    def __init__(self):
        self.cluster = Cluster(['127.0.0.1'], port=9042)
        self.session = self.cluster.connect()
        self.session.row_factory = dict_factory
        self.keyspace = "user_ratings"
        # self.keyspace = "ratings"

    def get_all(self, name):
        db_data = cass.get_data_table(self.session, self.keyspace, name)
        data = pd.DataFrame()

        for rating in db_data._current_rows:
            data = data.append(pd.Series(rating), ignore_index=True)
        return data

    def flush_queue(self, name):
        cass.clear_table(self.session, self.keyspace, name)

    def delete_table(self, name):
        cass.delete_table(self.session, self.keyspace, name)

    def add_df(self, name, df):
        cass.create_table_ratings(self.session, self.keyspace)
        for index, row in df.iterrows():
            cass.push_rating(self.session, self.keyspace, name, json.dumps(row.to_dict()))
            # redis.add_to_queue(name, json.dumps(row.to_dict()))

    def add(self, name, new_obj):
        cass.create_table_ratings(self.session, self.keyspace)
        cass.push_rating(self.session, self.keyspace, name, userId=1324, avgMovieRating=1.1)
        # redis.add_to_queue(name, json.dumps(new_obj))


if __name__ == "__main__":
    RATINGS_QUEUE = 'ratings'
    cl = DBClient()

    # cl.delete_table(RATINGS_QUEUE)
    data = pd.read_csv('data/user_ratedmovies.dat', delimiter='\t', nrows=1000)
    # cl.add_df(RATINGS_QUEUE, data)
    new = '{"userID": 75.0, "movieID": 3.0, "rating": 1.0, "date_day": 29.0, "date_month": 10.0, "date_year": 2006.0, "date_hour": 23.0, "date_minute": 17.0, "date_second": 16.0}'
    # cl.add(RATINGS_QUEUE, new)
    # cl.add(RATINGS_QUEUE, new)
    # print(cl.get_all(RATINGS_QUEUE).head())

    # cl.add('user_avg_rating', new)
    print(cl.get_all(RATINGS_QUEUE).head())
