import logging
from typing import List, Type

from pydantic import BaseModel, TypeAdapter

logger = logging.getLogger(__name__)


class DataTransform:
    def __init__(self, model: Type[BaseModel]):
        self.data_model = model
        self.type_adapter = TypeAdapter(model)

    def transform_fw_data(self, data: List[dict]) -> List[dict] or None:
        bulk_data = []
        fw_objects = [self.type_adapter.validate_python(movie) for movie in data]
        for movie in fw_objects:
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
                    "writers_names": movie.writers_names,
                    "directors": (
                        [
                            {"id": director.id, "name": director.name}
                            for director in movie.directors
                        ]
                        if movie.directors
                        else None
                    ),
                    "actors": (
                        [{"id": actor.id, "name": actor.name} for actor in movie.actors]
                        if movie.actors
                        else None
                    ),
                    "writers": (
                        [
                            {"id": writer.id, "name": writer.name}
                            for writer in movie.writers
                        ]
                        if movie.writers
                        else None
                    ),
                },
            }
            bulk_data.append(action)
        logger.info(f"Transforming {len(bulk_data)} movies to Elasticsearch...")
        return bulk_data

    def transform_updates_fw(self, data: List[dict]) -> List[dict] or None:
        bulk_data = []
        fw_objects = [self.type_adapter.validate_python(movie) for movie in data]
        for movie in fw_objects:
            action = {
                "_op_type": "update",
                "_index": "movies",
                "_id": movie.id,
                "doc": {
                    "title": movie.title,
                    "description": movie.description,
                    "imdb_rating": movie.imdb_rating,
                },
            }
            bulk_data.append(action)
        return bulk_data

    def transform_genres_updates(self, movies: List[dict]) -> List[dict] or None:
        bulk_data = []
        fw_objects = [self.type_adapter.validate_python(movie) for movie in movies]
        for movie in fw_objects:
            action = {
                "_op_type": "update",
                "_index": "movies",
                "_id": movie.id,
                "doc": {
                    "genres": movie.genres,
                },
            }
            bulk_data.append(action)
        return bulk_data

    def transform_persons_updates(self, movies: List[dict]) -> List[dict] or None:
        bulk_data = []
        fw_objects = [self.type_adapter.validate_python(movie) for movie in movies]
        for movie in fw_objects:
            action = {
                "_op_type": "update",
                "_index": "movies",
                "_id": movie.id,
                "doc": {
                    "directors_names": movie.directors_names,
                    "actors_names": movie.actors_names,
                    "writers_names": movie.writers_names,
                    "directors": (
                        [
                            {"id": director.id, "name": director.name}
                            for director in movie.directors
                        ]
                        if movie.directors
                        else None
                    ),
                    "actors": (
                        [{"id": actor.id, "name": actor.name} for actor in movie.actors]
                        if movie.actors
                        else None
                    ),
                    "writers": (
                        [
                            {"id": writer.id, "name": writer.name}
                            for writer in movie.writers
                        ]
                        if movie.writers
                        else None
                    ),
                },
            }
            bulk_data.append(action)
        return bulk_data
