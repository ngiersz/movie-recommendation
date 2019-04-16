import engine.redis_client as redis
import pandas as pd
import json


class DBClient:

    @staticmethod
    def get_all(name):
        queue_data = redis.get_queue(name)
        data = pd.DataFrame()
        for rating in queue_data:
            rating_dict = json.loads(rating)
            data = data.append(pd.Series(rating_dict), ignore_index=True)
        return data

    @staticmethod
    def flush_queue(name):
        redis.flush_queue(name)

    @staticmethod
    def add_df(name, df):
        for index, row in df.iterrows():
            redis.add_to_queue(name, json.dumps(row.to_dict()))


if __name__ == "__main__":
    RATINGS_QUEUE = 'ratings'
    cl = DBClient()
    cl.flush_queue(RATINGS_QUEUE)

    data = pd.read_csv('data/user_ratedmovies.dat', delimiter='\t', nrows=1000)
    cl.add_df(RATINGS_QUEUE, data)
    print(cl.get_all(RATINGS_QUEUE).size)


