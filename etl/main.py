import logging

from pydantic.experimental.pipeline import transform

from db_extractions import extract_data_from_postgres, get_postgres_connection, get_updated_objects_ids
from elastic import get_elasticsearch_client, load_data_to_elasticsearch, create_index
from etl.data_transform import DataTransform
from settings import Settings, Movie, state

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(setup: Settings):
    try:
        conn = get_postgres_connection(setup)
        es = get_elasticsearch_client(setup)

        logger.info("Extracting data from PostgreSQL...")
        movies_data = extract_data_from_postgres(conn)
        transformed_movies_data = DataTransform(Movie).transform_fw_data(movies_data)

        create_index(Movie, "movies", es)

        load_data_to_elasticsearch(
            es, transformed_movies_data,
            batch_size=100,
            state=state, state_key='last_loaded_id', state_param='id'
        )
        logger.info("Data successfully loaded to Elasticsearch.")
        while True:
            updated_ids=get_updated_objects_ids(
                conn, state,'person','person_update'
            )


    except Exception as e:
        logger.error("An error occurred: %s" % str(e))


if __name__ == "__main__":
    try:
        my_settings = Settings()
        main(my_settings)
    except Exception as e:
        logger.error("Failed to start application: %s", str(e))
