from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import json

def create_keyspace(session, keyspace):
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS """ + keyspace + """
    WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
    """)


def create_table_ratings(session, keyspace):
    session.execute("""
    CREATE TABLE IF NOT EXISTS """ + keyspace + """.""" + 'ratings' + """ (
    user_id int ,
    movieID int , 
    rating float , 
    date_day int , 
    date_month int , 
    date_year int , 
    date_hour int , 
    date_minute int , 
    date_second int ,
    PRIMARY KEY(user_id)
    )
    """)


def push_data_table(session, keyspace, table, userId, avgMovieRating):
    session.execute(
        """
        INSERT INTO """ + keyspace + """.""" + table + """ (user_id, avg_movie_rating)
    VALUES (%(user_id)s, %(avg_movie_rating)s)
    """,
        {
            'user_id': userId,
            'avg_movie_rating': avgMovieRating
        }
    )

def push_rating(session, keyspace, table, rating):
    # print(json.dumps(rating.to_dict()))
    rating = json.loads(rating)
    print(rating)
    print(rating.get('userID'))
    print(type(rating))
    session.execute(
        """
        INSERT INTO """ + keyspace + """.""" + table + """ (user_id, movieID, rating, date_day, date_month, date_year, date_hour, date_minute, date_second)
    VALUES (%(user_id)s, %(movieID)s, %(rating)s, %(date_day)s, %(date_month)s, %(date_year)s, %(date_hour)s, %(date_minute)s, %(date_second)s)
    """,
        {
            'user_id': int(rating.get('userID')),
            'movieID': int(rating.get('movieID')),
            'rating': rating.get('rating'),
            'date_day': int(rating.get('date_day')),
            'date_month': int(rating.get('date_month')),
            'date_year': int(rating.get('date_year')),
            'date_hour': int(rating.get('date_hour')),
            'date_minute': int(rating.get('date_minute')),
            'date_second': int(rating.get('date_second'))
        }
    )


def get_data_table(session, keyspace, table):
    return session.execute("SELECT * FROM " + keyspace + "." + table + ";")


def clear_table(session, keyspace, table):
    session.execute("TRUNCATE " + keyspace + "." + table + ";")


def delete_table(session, keyspace, table):
    session.execute("DROP TABLE " + keyspace + "." + table + ";")


if __name__ == "__main__":
    keyspace = "user_ratings"
    table = "user_avg_rating"

    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect()

    create_keyspace(session, keyspace)

    # ustawienie używanego keyspace w sesji
    session.set_keyspace(keyspace)

    # użycie dict_factory pozwala na zwracanie słowników
    # znanych z języka Python przy zapytaniach do bazy danych
    session.row_factory = dict_factory

    create_table(session, keyspace, table)

    push_data_table(session, keyspace, table, userId=1337, avgMovieRating=4.2)

    get_data_table(session, keyspace, table)

    # clear_table(session, keyspace, table)

    # delete_table(session, keyspace, table)
