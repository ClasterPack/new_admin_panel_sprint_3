import os
from datetime import datetime
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, Field
from pydantic.v1 import BaseSettings

from etl.state import JsonFileStorage, State


class Settings(BaseSettings):
    POSTGRES_DB: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_HOST: str
    ES_HOST: str
    ES_INDEX: str

    class Config:
        env_file = os.path.join(os.path.dirname(__file__), "../.env")
        env_file_encoding = "utf-8"


class Person(BaseModel):
    id: UUID
    name: str

class Movie(BaseModel):
    id: UUID
    title: str = None
    description: Optional[str] = None
    imdb_rating: Optional[float] = None
    genres: List[str] = None
    directors_names: List[str] =None
    actors_names: List[str] = None
    writers_names: List[str] = None
    directors: List[Person] = None
    actors: List[Person] = None
    writers: List[Person] = None

type_map = {
    "movies": {
        str: "text",
        Optional[str]: "text",
        float: "float",
        Optional[float]: "float",
        UUID: "keyword",
        List[str]: "keyword",
        List[Optional[str]]: "keyword",
        List[float]: "float",
        List[UUID]: "keyword",
    },
}

elastic_settings = {
    "movies":{
        "refresh_interval": "1s",
            "analysis": {
                "filter": {
                    "english_stop": {
                        "type": "stop",
                        "stopwords": "_english_"
                    },
                    "english_stemmer": {
                        "type": "stemmer",
                        "language": "english"
                    },
                    "english_possessive_stemmer": {
                        "type": "stemmer",
                        "language": "possessive_english"
                    },
                    "russian_stop": {
                        "type": "stop",
                        "stopwords": "_russian_"
                    },
                    "russian_stemmer": {
                        "type": "stemmer",
                        "language": "russian"
                    }
                },
                "analyzer": {
                    "ru_en": {
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "english_stop",
                            "english_stemmer",
                            "english_possessive_stemmer",
                            "russian_stop",
                            "russian_stemmer"
                        ]
                    }
                }
            }
        }
    }


state_storage = JsonFileStorage("state.json")
state = State(state_storage)
