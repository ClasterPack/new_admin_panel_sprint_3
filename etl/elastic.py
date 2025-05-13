import logging
from typing import Any, Dict, List, Type

import backoff
from elasticsearch import Elasticsearch, NotFoundError, RequestError
from elasticsearch.helpers import bulk
from pydantic import BaseModel

from etl.settings import Settings, type_map
from etl.state import State

logger = logging.getLogger(__name__)


class ElasticSearchLoader:
    @backoff.on_exception(backoff.expo, Exception, max_tries=5, jitter=None)
    def __init__(self, config: Settings, state: State):
        self.state = state
        try:
            es = Elasticsearch(
                config.es_host,
                http_compress=True,
                headers={"Content-Type": "application/x-ndjson"},
            )
            if es.ping():
                logger.info("Successfully connected to Elasticsearch.")
                self.es = es
            else:
                logger.error("Unable to connect to Elasticsearch.")

        except Exception as e:
            logger.error("Error connecting to Elasticsearch: %s" % e)
            raise

    @backoff.on_exception(backoff.expo, Exception, max_tries=5, jitter=None)
    def load_data_to_elasticsearch(
        self,
        data: List[Dict[str, Any]],
        batch_size: int,
        state_key: str,
        state_param: str,
    ) -> None:
        """
        Загружает данные в Elasticsearch с использованием bulk-запроса.

        :param data: Список словарей с данными для загрузки.
        :param batch_size: Размер пакета данных для загрузки.
        :param state_key: Ключ состояния.
        :param state_param Ключ по полю для поиска.
        :return: None
        """
        last_loaded = self.state.get_state(state_key)

        try:
            total = len(data)
            logger.info("Loading %s records from Elasticsearch." % total)
            start_index = 0
            if last_loaded:
                for i, item in enumerate(data):
                    if item.get(state_param) == last_loaded:
                        start_index = i + 1
                        break
            for i in range(start_index, total, batch_size):
                batch = data[i : i + batch_size]
                bulk_data = batch
                try:
                    success, failed = bulk(
                        self.es,
                        bulk_data,
                        request_timeout=200,
                        headers={"Content-Type": "application/x-ndjson"},
                    )
                    logger.info(f"Successfully loaded {success} documents in batch.")
                    if failed:
                        logger.warning(f"Failed to load {failed} documents in batch.")
                except Exception as e:
                    logger.error(f"Failed to load batch starting at index %s: %s", i, e)
                if batch:
                    if state_param == "id":
                        state_param = "_id"
                    last_loaded = batch[-1].get(state_param)
                    self.state.set_state(state_key, last_loaded)
        except Exception as e:
            logger.error("Failed to load data to Elasticsearch: %s" % e)

    def create_es_mapping(self, pydantic_model: Type[BaseModel], index_name: str):
        mapping = {}
        index_type_map = type_map.get(index_name, {})

        for field, field_type in pydantic_model.__annotations__.items():
            es_field_type = index_type_map.get(field_type)

            if es_field_type:
                mapping[field] = {"type": es_field_type}
            elif hasattr(field_type, "__origin__"):
                if field_type.__origin__ == list:
                    list_item_type = field_type.__args__[0]
                    if isinstance(list_item_type, type) and issubclass(
                        list_item_type, BaseModel
                    ):
                        mapping[field] = {
                            "type": "nested",
                            "properties": self.create_es_mapping(
                                list_item_type, index_name
                            ),
                        }
                    else:
                        mapping[field] = {"type": "keyword"}
                else:
                    mapping[field] = {"type": "keyword"}
            elif isinstance(field_type, type) and issubclass(field_type, BaseModel):
                mapping[field] = {
                    "type": "nested",
                    "properties": self.create_es_mapping(field_type, index_name),
                }
            else:
                logger.warning(
                    f"Field {field} has no type defined, defaulting to 'keyword'."
                )
                mapping[field] = {"type": "keyword"}

        return mapping

    @backoff.on_exception(backoff.expo, Exception, max_tries=5, jitter=None)
    def create_index(
        self, model: Type[BaseModel], index_name: str, settings: dict = None
    ):
        settings = settings or {}
        settings["index_name"] = index_name
        if settings[index_name]:
            mapping = settings[index_name]
        else:
            mapping = self.create_es_mapping(model, index_name)

        try:
            logger.info("Creating index '%s'..." % index_name)
            if not self.es.indices.exists(index=index_name):
                self.es.indices.create(index=index_name, body=mapping)
                logger.info("Index: '%s' is created." % index_name)
            else:
                logger.info("Index: '%s' already exists." % index_name)
        except RequestError as e:
            logger.error(
                f"Failed to create index: %s. Error: %s" % ({index_name}, str(e))
            )
        except NotFoundError as e:
            logger.error(f"Elasticsearch service not found. Error: %s" % str(e))
        except Exception as e:
            logger.error(
                f"An unexpected error occurred while creating the index: %s" % str(e)
            )

    def load_updates(self, data, state_key, last_loaded):
        """
        Загружает данные в Elasticsearch с использованием bulk-запроса.

        :param data: Список словарей с данными для загрузки.
        :param state_key: Ключ состояния.
        :param last_loaded дата последнего обновления загружаемых данных.
        :return: None
        """
        logger.info(f"Found {len(data)} updates from Movies.")
        try:
            success, failed = bulk(
                self.es,
                data,
                request_timeout=200,
                headers={"Content-Type": "application/x-ndjson"},
            )
            logger.info(f"Successfully loaded {success} documents in batch.")
            self.state.set_state(state_key, last_loaded.isoformat())
            if failed:
                logger.warning(f"Failed to load {failed} documents in batch.")

        except Exception as e:
            logger.error(f"Failed to load batch starting at index %s: %s", e)
