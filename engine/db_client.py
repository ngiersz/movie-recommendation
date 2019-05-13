import engine.redis_client as redis
import pandas as pd
import json
from cassandra.cluster import Cluster
import engine.cass_client as cass
from cassandra.query import dict_factory
import numpy as np

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

    def add_df_ratings(self, df):
        # print(df.columns.values)
        # index_userID = np.argwhere(df.columns.values=='userID')
        # index_movieID = np.argwhere(df.columns.values=='movieID')
        # additional_columns = np.delete(df.columns.values, [index_userID, index_movieID])
        # print(additional_columns)
        # cass.create_table_ratings(self.session, self.keyspace, '"userID", "movieID"', additional_columns)
        cass.create_table_ratings(self.session, self.keyspace)
        for index, row in df.iterrows():
            cass.push_rating(self.session, self.keyspace, json.dumps(row.to_dict()))

    def add_profile(self, user_id, profile):
        cass.create_table_avg_ratings(self.session, self.keyspace)
        cass.push_avg_ratings(self.session, self.keyspace, user_id, profile)

    def add_rating(self, rating):
         cass.create_table_ratings(self.session, self.keyspace)
         cass.push_rating(self.session, self.keyspace, json.dumps(rating))



if __name__ == "__main__":
    RATINGS_QUEUE = 'ratings'
    cl = DBClient()

    cl.delete_table(RATINGS_QUEUE)
    data = pd.read_csv('data/user_ratedmovies.dat', delimiter='\t', nrows=1000)
    # cl.add_df(RATINGS_QUEUE, data)

    # new = '{"userID": 75.0, "movieID": 3.0, "rating": 1.0, "date_day": 29.0, "date_month": 10.0, "date_year": 2006.0, "date_hour": 23.0, "date_minute": 17.0, "date_second": 16.0}'
    # cl.add(RATINGS_QUEUE, new)


    # cl.add('user_avg_rating', new)
    print(cl.get_all(RATINGS_QUEUE))
