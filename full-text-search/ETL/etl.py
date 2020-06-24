import sqlite3
import json

from pathlib import Path
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


DB_FILENAME = 'db.sqlite'
INDEX_SCHEMA_FILENAME = 'index_schema.json'
BASE_DIRECTORY = Path(__file__).parent
DB_PATH = BASE_DIRECTORY.joinpath(DB_FILENAME)
INDEX_SCHEMA_PATH = BASE_DIRECTORY.joinpath(INDEX_SCHEMA_FILENAME)
INDEX_NAME = 'movies'


def get_db_connection_and_cursor(db_path):
    conn = sqlite3.connect(db_path)
    return conn, conn.cursor()


def get_initial_movies_list(cursor):
    movies = []
    cursor.execute("""
                SELECT id, imdb_rating, genre, title, plot, director, writer, writers
                FROM movies""")
    for row in cursor.fetchall():
        movies.append(
            {
                'id': row[0],
                'imdb_rating': row[1],
                'genre': row[2],
                'title': row[3],
                'description': row[4],
                'director': row[5],
                'writer': row[6],
                'writers': row[7],
            }
        )

    return movies


def edit_writers_in_movie(cursor, movie):
    writers = []

    if movie['writer']:
        writers.append({'id': movie['writer']})

    if movie['writers']:
        writers.extend(json.loads(movie['writers']))

    if writers:
        unique_writers = list({writer['id']: writer for writer in writers}.values())
        for writer in unique_writers:
            cursor.execute('SELECT name FROM writers WHERE id=?', (writer['id'],))
            writer['name'] = cursor.fetchone()[0]

        movie['writers'] = unique_writers
        movie['writers_names'] = ', '.join([writer['name'] for writer in unique_writers])

    del movie['writer']


def add_actors_to_movies(cursor, movie):
    cursor.execute("""
                SELECT a.id, a.name
                FROM actors a
                INNER JOIN movie_actors m_a ON a.id = m_a.actor_id
                WHERE m_a.movie_id=?
                """, (movie['id'],))
    movie['actors'] = [{'id': row[0], 'name': row[1]} for row in cursor.fetchall()]
    movie['actors_names'] = ', '.join([actor['name'] for actor in movie['actors']])


def process_movies(cursor, movies):
    for movie in movies:
        edit_writers_in_movie(cursor, movie)
        add_actors_to_movies(cursor, movie)
        try:
            movie['imdb_rating'] = float(movie['imdb_rating'])
        except ValueError:
            del movie['imdb_rating']

    return movies


def create_index(client):
    with open(INDEX_SCHEMA_PATH, 'r') as f:
        index_schema = f.read()

    client.indices.create(index=INDEX_NAME, body=index_schema, ignore=400)


def generate_actions(movies):
    for movie in movies:
        movie['_id'] = movie['id']
        yield movie


def main():
    print('Loading data from database...')
    conn, cursor = get_db_connection_and_cursor(DB_PATH)
    initial_movies = get_initial_movies_list(cursor)
    processed_movies = process_movies(cursor, initial_movies)
    conn.close()

    client = Elasticsearch()
    print('Creating "movies" index if it does not exist...')
    create_index(client)
    print("Indexing documents...")
    result = bulk(client, index=INDEX_NAME, actions=generate_actions(processed_movies))
    print(f'Successfully indexed {result[0]} documents, {len(result[1])} errors')


if __name__ == '__main__':
    main()
