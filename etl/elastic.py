from uuid import UUID
from elasticsearch import Elasticsearch
import logging
from typing import List

from elasticsearch.helpers import bulk
from pydantic import TypeAdapter

from etl.settings import Movie

logger = logging.getLogger(__name__)

def get_elasticsearch_client(config: dict) -> Elasticsearch:
    try:
        es = Elasticsearch(
            config.es_host,
            http_compress=True,
            headers={"Content-Type": "application/x-ndjson"}
        )
        """es = Elasticsearch([config.es_host], http_compress=True)"""
        if es.ping():
            logger.info("Successfully connected to Elasticsearch.")
        else:
            logger.error("Unable to connect to Elasticsearch.")
        return es
    except Exception as e:
        logger.error("Error connecting to Elasticsearch: %s" % e)
        raise


def transform_data(data: List[dict]):
    bulk_data = []

    # Создаем TypeAdapter для модели Movie
    movie_adapter = TypeAdapter(Movie)

    movie_objects = [movie_adapter.validate_python(movie) for movie in data]

    for movie in movie_objects:
        action = {
            "_op_type": "index",
            "_index": "movies",
            "_id": str(movie.id),
            "_source": {
                "title": movie.title,
                "description": movie.description,
                "imdb_rating": movie.imdb_rating,
                "genres": movie.genres,
                "directors": [{"id": director.id, "name": director.name} for director in movie.directors],
                "actors": [{"id": actor.id, "name": actor.name} for actor in movie.actors],
                "writers": [{"id": writer.id, "name": writer.name} for writer in movie.writers],
                "directors_names": movie.directors_names,
                "actors_names": movie.actors_names,
                "writers_names": movie.writers_names
            }
        }

        bulk_data.append(action)

    return bulk_data


def load_data_to_elasticsearch(es, data):
    try:
        bulk_data = transform_data(data)

        success, failed = bulk(es, bulk_data, request_timeout=200)
        logger.info("Successfully loaded %s documents to Elasticsearch." % success)
        if failed:
            logger.warning(f"Failed to load %s documents." % failed)
    except Exception as e:
        logger.error(f"Failed to load data to Elasticsearch: {e}")

def create_index(es, index_name):
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)
        print("Индекс %s успешно создан." % index_name)
    else:
        print("Индекс %s уже существует." % index_name)