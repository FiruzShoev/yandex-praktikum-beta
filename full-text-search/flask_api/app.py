from flask import Flask, request, abort, jsonify
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
from validators import validate_param_is_positive_int, validate_param_is_allowed
from api_errors import FILM_NOT_FOUND

INDEX_NAME = 'movies'

app = Flask(__name__)


@app.route('/api/v1/movies/<movie_id>')
def movie_by_id(movie_id):
    es = Elasticsearch()
    try:
        document = es.get(INDEX_NAME, movie_id, _source_excludes=['writers_names', 'actors_names'])
    except NotFoundError:
        abort(*FILM_NOT_FOUND)
    else:
        return document['_source']


@app.route('/api/v1/movies/')
@app.route('/api/v1/movies')
def movies_by_query():
    search = request.args.get('search')
    limit = request.args.get('limit')
    page = request.args.get('page')
    sort = request.args.get('sort')
    sort_order = request.args.get('sort_order')

    size = validate_param_is_positive_int(limit) if limit else 50
    page = validate_param_is_positive_int(page) if page else 1
    sort = validate_param_is_allowed(sort, ('id', 'title', 'imdb_rating')) if sort else 'id'
    sort_order = validate_param_is_allowed(sort_order, ('asc', 'desc')) if sort_order else 'asc'
    es_sort = [f'{sort}:{sort_order}']
    from_ = size * (page - 1)

    es_body = None
    if search:
        es_body = {
            "query": {
                "multi_match": {
                    "query": search,
                    "fields": ["title"]
                }
            }
        }
    es_params = {
        '_source_includes': ['id', 'title', 'imdb_rating'],
        'from_': from_,
        'size': size,
        'sort': es_sort
    }
    es = Elasticsearch()
    documents = es.search(es_body, INDEX_NAME, **es_params)
    return jsonify([document['_source'] for document in documents['hits']['hits']])


if __name__ == '__main__':
    app.run(debug=True)
