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
    "userID" int ,
    "movieID" int , 
    rating float , 
    genre_Action int ,
    genre_Adventure int ,
    genre_Animation int ,
    genre_Children int ,
    genre_Comedy int ,
    genre_Crime int ,
    genre_Documentary int ,
    genre_Drama int ,
    genre_Fantasy int ,
    genre_FilmNoir int ,
    genre_Horror int ,
    genre_Musical int ,
    genre_Mystery int ,
    genre_Romance int ,
    genre_SciFi int ,
    genre_Thriller int ,
    genre_War int ,
    genre_Western int ,
    PRIMARY KEY("userID", "movieID")
    )
    """)

def push_rating(session, keyspace, table, rating):
    # print(json.dumps(rating.to_dict()))
    rating = json.loads(rating)
    session.execute(
        """
        INSERT INTO """ + keyspace + """.""" + table + """ ("userID", "movieID", rating, genre_Action, genre_Adventure, 
        genre_Animation, genre_Children , genre_Comedy, genre_Crime, genre_Documentary, genre_Drama, genre_Fantasy,
        genre_FilmNoir, genre_Horror, genre_Musical, genre_Mystery, genre_Romance, genre_SciFi, genre_Thriller,
        genre_War, genre_Western )
    VALUES (%(userID)s, %(movieID)s, %(rating)s, %(genre_Action)s, %(genre_Adventure)s, %(genre_Animation)s, 
    %(genre_Children)s, %(genre_Comedy)s, %(genre_Crime)s, %(genre_Documentary)s, %(genre_Drama)s, %(genre_Fantasy)s, 
    %(genre_FilmNoir)s, %(genre_Horror)s, %(genre_Musical)s, %(genre_Mystery)s, %(genre_Romance)s, %(genre_SciFi)s, 
    %(genre_Thriller)s, %(genre_War)s, %(genre_Western)s
    )
    """,
        {
            'userID': int(rating.get('userID')),
            'movieID': int(rating.get('movieID')),
            'rating': rating.get('rating'),
            'genre_Action': int(rating.get('genre_Action')),
            'genre_Adventure': int(rating.get('genre_Adventure')),
            'genre_Animation': int(rating.get('genre_Animation')),
            'genre_Children': int(rating.get('genre_Children')),
            'genre_Comedy': int(rating.get('genre_Comedy')),
            'genre_Crime': int(rating.get('genre_Crime')),
            'genre_Documentary': int(rating.get('genre_Documentary')),
            'genre_Drama': int(rating.get('genre_Drama')),
            'genre_Fantasy': int(rating.get('genre_Fantasy')),
            'genre_FilmNoir': int(rating.get('genre_Film-Noir')),
            'genre_Horror': int(rating.get('genre_Horror')),
            'genre_Musical': int(rating.get('genre_Musical')),
            'genre_Mystery': int(rating.get('genre_Mystery')),
            'genre_Romance': int(rating.get('genre_Romance')),
            'genre_SciFi': int(rating.get('genre_Sci-Fi')),
            'genre_Thriller': int(rating.get('genre_Thriller')),
            'genre_War': int(rating.get('genre_War')),
            'genre_Western': int(rating.get('genre_Western'))
        }
    )
    print('Added rating: userID=' + str(rating.get('userID')) + ' movieID=' + str(rating.get('movieID')))


def get_data_table(session, keyspace, table):
    return session.execute("SELECT * FROM " + keyspace + "." + table + ";")


def clear_table(session, keyspace, table):
    session.execute("TRUNCATE " + keyspace + "." + table + ";")


def delete_table(session, keyspace, table):
    session.execute("DROP TABLE IF EXISTS " + keyspace + "." + table + ";")


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
