import logging

from db_extractions import extract_data_from_postgres, get_postgres_connection
from elastic import get_elasticsearch_client, load_data_to_elasticsearch, create_index
from settings import Settings, Movie

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(setup: Settings):
    try:
        conn = get_postgres_connection(setup)
        es = get_elasticsearch_client(setup)

        logger.info("Extracting data from PostgreSQL...")
        movies_data = extract_data_from_postgres(conn)

        logger.info("Creating index in Elasticsearch...")
        create_index(Movie, 'movies', es)


        logger.info("Loading data to Elasticsearch...")
        load_data_to_elasticsearch(es, movies_data, batch_size=100)
        logger.info("Data successfully loaded to Elasticsearch.")

    except Exception as e:
        logger.error("An error occurred: %s" % str(e))


if __name__ == "__main__":
    try:
        my_settings = Settings()
        main(my_settings)
    except Exception as e:
        logger.error("Failed to start application: %s", str(e))