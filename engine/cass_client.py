from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import json

RATINGS_TABLE = 'ratings'
AVG_RATINGS_TABLE = 'avg_ratings'

def create_keyspace(session, keyspace):
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS """ + keyspace + """
    WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
    """)


def create_table_ratings(session, keyspace):
    session.execute("""
    CREATE TABLE IF NOT EXISTS """ + keyspace + """.""" + RATINGS_TABLE + """ (
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

def create_table_avg_ratings(session, keyspace):
    session.execute("""
    CREATE TABLE IF NOT EXISTS """ + keyspace + """.""" + AVG_RATINGS_TABLE + """ (
    "userID" int ,
    genre_Action float ,
    genre_Adventure float ,
    genre_Animation float ,
    genre_Children float ,
    genre_Comedy float ,
    genre_Crime float ,
    genre_Documentary float ,
    genre_Drama float ,
    genre_Fantasy float ,
    genre_FilmNoir float ,
    genre_Horror float ,
    genre_Musical float ,
    genre_Mystery float ,
    genre_Romance float ,
    genre_SciFi float ,
    genre_Thriller float ,
    genre_War float ,
    genre_Western float ,
    PRIMARY KEY("userID")
    )
    """)

def push_rating(session, keyspace, rating):
    # print(json.dumps(rating.to_dict()))
    rating = json.loads(rating)
    session.execute(
        """
        INSERT INTO """ + keyspace + """.""" + RATINGS_TABLE + """ ("userID", "movieID", rating, genre_Action, genre_Adventure, 
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


def push_avg_ratings(session, keyspace, user_id, rating):
    rating = json.loads(rating)
    print(rating)
    session.execute(
        """
        INSERT INTO """ + keyspace + """.""" + AVG_RATINGS_TABLE + """ ("userID", genre_Action, genre_Adventure, 
        genre_Animation, genre_Children , genre_Comedy, genre_Crime, genre_Documentary, genre_Drama, genre_Fantasy,
        genre_FilmNoir, genre_Horror, genre_Musical, genre_Mystery, genre_Romance, genre_SciFi, genre_Thriller,
        genre_War, genre_Western )
    VALUES (%(userID)s, %(genre_Action)s, %(genre_Adventure)s, %(genre_Animation)s, 
    %(genre_Children)s, %(genre_Comedy)s, %(genre_Crime)s, %(genre_Documentary)s, %(genre_Drama)s, %(genre_Fantasy)s, 
    %(genre_FilmNoir)s, %(genre_Horror)s, %(genre_Musical)s, %(genre_Mystery)s, %(genre_Romance)s, %(genre_SciFi)s, 
    %(genre_Thriller)s, %(genre_War)s, %(genre_Western)s
    )
    """,
        {
            'userID': int(user_id),
            'genre_Action': float(rating.get('genre_action')),
            'genre_Adventure': float(rating.get('genre_adventure')),
            'genre_Animation': float(rating.get('genre_animation')),
            'genre_Children': float(rating.get('genre_children')),
            'genre_Comedy': float(rating.get('genre_comedy')),
            'genre_Crime': float(rating.get('genre_crime')),
            'genre_Documentary': float(rating.get('genre_documentary')),
            'genre_Drama': float(rating.get('genre_drama')),
            'genre_Fantasy': float(rating.get('genre_fantasy')),
            'genre_FilmNoir': float(rating.get('genre_filmnoir')),
            'genre_Horror': float(rating.get('genre_horror')),
            'genre_Musical': float(rating.get('genre_musical')),
            'genre_Mystery': float(rating.get('genre_mystery')),
            'genre_Romance': float(rating.get('genre_romance')),
            'genre_SciFi': float(rating.get('genre_scifi')),
            'genre_Thriller': float(rating.get('genre_thriller')),
            'genre_War': float(rating.get('genre_war')),
            'genre_Western': float(rating.get('genre_western'))
        }
    )
    print('Added average ratings: userID=' + str(user_id))


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

    # create_keyspace(session, keyspace)

    # ustawienie używanego keyspace w sesji
    session.set_keyspace(keyspace)

    # użycie dict_factory pozwala na zwracanie słowników
    # znanych z języka Python przy zapytaniach do bazy danych
    session.row_factory = dict_factory

    print(get_data_table(session, keyspace, AVG_RATINGS_TABLE).current_rows)

    # clear_table(session, keyspace, table)

    # delete_table(session, keyspace, table)
