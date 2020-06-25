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


def edit_writers_in_movie(movie, all_writers):
    writers = []

    if movie['writer']:
        writers.append({'id': movie['writer']})

    if movie['writers']:
        writers.extend(json.loads(movie['writers']))

    if writers:
        unique_writers = list({writer['id']: writer for writer in writers}.values())
        for writer in unique_writers:
            writer['name'] = next(w['name'] for w in all_writers if w['id'] == writer['id'])

        movie['writers'] = unique_writers
        movie['writers_names'] = ', '.join([writer['name'] for writer in unique_writers])

    del movie['writer']


def add_actors_to_movie(movie, actors):
    movie['actors'] = [{'id': actor['id'], 'name': actor['name']}
                       for actor in actors if actor['movie_id'] == movie['id']]
    movie['actors_names'] = ', '.join([actor['name'] for actor in movie['actors']])


def generate_actions(movies):
    for movie in movies:
        movie['_id'] = movie['id']
        yield movie


def extract(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, imdb_rating, genre, title, plot as description, director, writer, writers
        FROM movies""")
    movies = [dict(movie) for movie in cursor.fetchall()]

    cursor.execute('SELECT id, name FROM writers')
    writers = [dict(writer) for writer in cursor.fetchall()]

    cursor.execute("""
        SELECT a.id, a.name, m_a.movie_id
        FROM actors a
        INNER JOIN movie_actors m_a ON a.id = m_a.actor_id""")
    actors = [dict(actor) for actor in cursor.fetchall()]

    conn.close()
    return movies, writers, actors


def transform(movies, writers, actors):
    for movie in movies:
        edit_writers_in_movie(movie, writers)
        add_actors_to_movie(movie, actors)
        try:
            movie['imdb_rating'] = float(movie['imdb_rating'])
        except ValueError:
            del movie['imdb_rating']

    return movies


def load(movies, index_schema_path, index_name):
    client = Elasticsearch()
    with open(index_schema_path, 'r') as f:
        client.indices.create(index=index_name, body=f.read(), ignore=400)

    return bulk(client, index=INDEX_NAME, actions=generate_actions(movies))


def main():
    movies, writers, actors = extract(DB_PATH)
    transformed_movies = transform(movies, writers, actors)
    result = load(transformed_movies, INDEX_SCHEMA_PATH, INDEX_NAME)
    print(f'Successfully indexed {result[0]} documents, {len(result[1])} errors')


if __name__ == '__main__':
    main()
