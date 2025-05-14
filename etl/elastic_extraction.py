import logging
from time import sleep
from typing import Type

from pydantic import BaseModel

from etl.data_transform import DataTransform
from etl.db_extractions import DBExtractions
from etl.elastic import ElasticSearchLoader
from etl.settings import Settings
from etl.state import State

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ElasticExtraction:
    def __init__(
        self,
        setup: Settings,
        state: State,
        data_transform: DataTransform,
    ):
        self.setup = setup
        self.state = state
        self.pg = DBExtractions(self.setup, self.state)
        self.es = ElasticSearchLoader(self.setup, self.state)
        self.data_transform = data_transform

    def transform_data_from_pg(
        self, model: Type[BaseModel], index_name, settings, state_key
    ):
        self.es.create_index(model, index_name, settings)
        logger.info("Extracting data from PostgreSQL...")
        pg_data = self.pg.extract_data()
        last_update = pg_data[-1]["modified"]
        transformed_movies_data = self.data_transform.transform_fw_data(pg_data)
        if transformed_movies_data and transformed_movies_data[-1][
            "_id"
        ] == self.state.get_state(key=state_key):
            logger.info("Movie data already loaded, skipping.")
        else:
            self.es.load_data_to_elasticsearch(
                data=transformed_movies_data,
                batch_size=self.setup.batch_size,
                state_key=state_key,
                state_param="id",
            )
            self.state.set_state("last_update", last_update.isoformat())
            logger.info("Data successfully loaded to Elasticsearch.")

    def update_fw_data(self, state_key):
        if self.state.get_state(state_key) is None:
            db_last_update = self.state.get_state("last_update")
            self.state.set_state(state_key, db_last_update)
        while self.pg.get_updated_fw(state_key):
            updated_fw_data, last_update = self.pg.get_updated_fw(state_key)
            bulk_movies_data = self.data_transform.transform_updates_fw(updated_fw_data)
            self.es.load_updates(bulk_movies_data, state_key, last_update)
        else:
            logger.info("No updated film work data found. Waiting for the next update.")

    def update_genre(self, state_key):
        while self.pg.get_updated_objects_ids("genre", state_key):
            genre_updates, last_update_genre = self.pg.get_updated_objects_ids(
                "genre", state_key
            )
            updates_from_pg = self.pg.updates_genres(genre_updates)
            transformed_genre_data = self.data_transform.transform_genres_updates(
                updates_from_pg
            )
            self.es.load_updates(transformed_genre_data, state_key, last_update_genre)
        else:
            logger.info("No updated genres data found. Waiting for the next update.")

    def update_person(self, state_key):
        while self.pg.get_updated_objects_ids("person", state_key):
            person_updates, last_update = self.pg.get_updated_objects_ids(
                "person", state_key
            )
            updt_from_pg = self.pg.update_persons(person_updates)
            if person_updates:
                transformed_person_data = self.data_transform.transform_persons_updates(
                    updt_from_pg
                )
                self.es.load_updates(transformed_person_data, state_key, last_update)
                if person_updates is None:
                    logger.info(
                        "Last update is None, but no errors occurred. Please check the data source."
                    )
                else:
                    logger.info("Last update processed successfully.")
        else:
            logger.info("No updated persons data found. Waiting for the next update.")
