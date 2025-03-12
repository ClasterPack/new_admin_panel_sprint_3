import logging
from typing import Any, Dict, List, Type

import backoff
from elasticsearch import Elasticsearch, RequestError, NotFoundError
from elasticsearch.helpers import bulk
from pydantic import TypeAdapter, BaseModel

from etl.settings import Movie, Settings, type_map
from etl.state import State

logger = logging.getLogger(__name__)

@backoff.on_exception(backoff.expo, Exception, max_tries=5, jitter=None)
def get_elasticsearch_client(config: Settings) -> Elasticsearch:
    try:
        es = Elasticsearch(
            config.ES_HOST,
            http_compress=True,
            headers={"Content-Type": "application/x-ndjson"}
        )
        if es.ping():
            logger.info("Successfully connected to Elasticsearch.")
        else:
            logger.error("Unable to connect to Elasticsearch.")
        return es
    except Exception as e:
        logger.error("Error connecting to Elasticsearch: %s" % e)
        raise


@backoff.on_exception(backoff.expo, Exception, max_tries=5, jitter=None)
def transform_data(data: List[dict]):
    bulk_data = []
    # Создаем TypeAdapter для модели Movie
    movie_adapter = TypeAdapter(Movie)

    movie_objects = [movie_adapter.validate_python(movie) for movie in data]


    for movie in movie_objects:
        action = {
            "_op_type": "index",
            "_index": "movies",
            "_id": movie.id,
            "_source": {
                "title": movie.title,
                "description": movie.description,
                "imdb_rating": movie.imdb_rating,
                "genres": movie.genres,
                "directors_names": movie.directors_names,
                "actors_names": movie.actors_names,
                "writers_names": movie.writers_names
            }
        }
        bulk_data.append(action)

    logger.info(f"Transforming {len(bulk_data)} movies to Elasticsearch...")
    return bulk_data


@backoff.on_exception(backoff.expo, Exception, max_tries=5, jitter=None)
def load_data_to_elasticsearch(
        es: Elasticsearch, data: List[Dict[str, Any]], batch_size: int, state: State
) -> None:
    """
    Загружает данные в Elasticsearch с использованием bulk-запроса.

    :param es: Клиент Elasticsearch.
    :param data: Список словарей с данными для загрузки.
    :param batch_size: Размер пакета данных для загрузки.
    :return: None
    """
    last_loaded_id = state.get_state("last_loaded_id")

    try:
        total = len(data)
        logger.info("Loading %s records from Elasticsearch." % total)
        start_index = 0
        if last_loaded_id:
            for i, item in enumerate(data):
                if item.get('id') == last_loaded_id:
                    start_index = i + 1
                    break
        for i in range(start_index, total, batch_size):
            batch = data[i:i+batch_size]
            bulk_data = transform_data(batch)
            try:
                success, failed = bulk(
                    es,
                    bulk_data,
                    request_timeout=200,
                    headers={"Content-Type": "application/x-ndjson"}
                )
                logger.info(f"Successfully loaded {success} documents in batch.")
                if failed:
                    logger.warning(f"Failed to load {failed} documents in batch.")
            except Exception as e:
                logger.error(f"Failed to load batch starting at index %s: %s", i, e)
            if batch:
                last_loaded_id = batch[-1].get('id')
                state.set_state("last_loaded_id", last_loaded_id)  # Сохраняем ID в состоянии
    except Exception as e:
        logger.error("Failed to load data to Elasticsearch: %s" % e)


def create_es_mapping(pydantic_model: BaseModel):
    mapping = {
        "properties": {}
    }

    for field, field_type in pydantic_model.__annotations__.items():
        es_field_type = type_map.get(field_type)

        if es_field_type:
            mapping["properties"][field] = {"type": es_field_type}
        elif hasattr(field_type, "__origin__"):
            if field_type.__origin__ == List:
                list_item_type = field_type.__args__[0]
                if isinstance(list_item_type, type) and issubclass(list_item_type, BaseModel):
                    mapping["properties"][field] = {
                        "type": "nested",
                        "properties": create_es_mapping(list_item_type)["properties"]
                    }
                else:
                    mapping["properties"][field] = {"type": "keyword"}
            else:
                mapping["properties"][field] = {"type": "keyword"}
        elif isinstance(field_type, type) and issubclass(field_type, BaseModel):
            mapping["properties"][field] = {
                "type": "nested",
                "properties": create_es_mapping(field_type)["properties"]
            }
        else:
            logger.warning(f"Field {field} has no type defined, defaulting to 'keyword'.")
            mapping["properties"][field] = {"type": "keyword"}

    return mapping


@backoff.on_exception(backoff.expo, Exception, max_tries=5, jitter=None)
def create_index(model: BaseModel, index_name: str, es: Elasticsearch, settings: dict = None):
    # Generate the mapping for the model
    mapping = {
        "mappings": {
            "properties": create_es_mapping(model)
        }
    }

    if settings:
        mapping["settings"] = settings

    try:
        logger.info("Creating index '%s'..." % index_name)
        if not es.indices.exists(index=index_name):
            es.indices.create(index=index_name, body=mapping)
            logger.info("Index: '%s' is created." % index_name)
        else:
            logger.info("Index: '%s' already exists." % index_name)
    except RequestError as e:
        logger.error(f"Failed to create index: %s. Error: %s" % ({index_name}, str(e)))
    except NotFoundError as e:
        logger.error(f"Elasticsearch service not found. Error: %s" % str(e))
    except Exception as e:
        logger.error(f"An unexpected error occurred while creating the index: %s" % str(e))