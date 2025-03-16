import logging
from time import sleep

from django import setup
from settings import Movie, Settings, state

from etl.data_transform import DataTransform
from etl.elastic_extraction import ElasticExtraction
from etl.settings import elastic_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(elastic_extraction: ElasticExtraction):
    elastic_extraction.transform_data_from_pg(
        model=Movie,
        index_name="movies",
        settings=elastic_settings,
        state_key="last_loaded_id",
    )
    while True:
        elastic_extraction.update_fw_data("last_update_fw")
        elastic_extraction.update_genre("last_updated_genre")
        elastic_extraction.update_person("last_updated_person")
        sleep(elastic_extraction.setup.UPDATE_FREQUENCY)


if __name__ == "__main__":
    try:
        data_transform = DataTransform(Movie)
        my_settings = Settings()
        elastic_extraction = ElasticExtraction(my_settings, state, data_transform)

        main(elastic_extraction)
    except Exception as e:
        logger.error("Failed to start application: %s", str(e))
