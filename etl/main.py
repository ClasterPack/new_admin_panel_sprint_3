from settings import Config, my_config
from elastic import get_elasticsearch_client, load_data_to_elasticsearch , transform_data ,create_index
from db_extractions import get_postgres_connection, extract_data_from_postgres
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(config: Config):
    try:
        conn = get_postgres_connection(my_config)

        es = get_elasticsearch_client(my_config)

        logger.info("Extracting data from PostgreSQL...")
        movies_data = extract_data_from_postgres(conn)

        logger.info("Transforming data...")
        transformed_data = transform_data(movies_data)

        logger.info("Loading data to Elasticsearch...")
        create_index(es=es, index_name='movies')
        load_data_to_elasticsearch(es, transformed_data)
        logger.info("Data successfully loaded to Elasticsearch.")

    except Exception as e:
        logger.error("An error occurred: %s" % str(e))

if __name__ == "__main__":
    main(my_config)

