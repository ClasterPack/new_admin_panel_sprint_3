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
    SQL_HOST: str
    ES_HOST: str
    ES_INDEX: str
    BATCH_SIZE: int
    UPDATE_FREQUENCY: int

    class Config:
        env_file = os.path.join(os.path.dirname(__file__), "../.env")
        env_file_encoding = "utf-8"


class Person(BaseModel):
    id: UUID
    name: str = None


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
    modified: datetime = None


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
    'movies':{
          "settings": {
            "refresh_interval": "1s",
            "analysis": {
              "filter": {
                "english_stop": {
                  "type":       "stop",
                  "stopwords":  "_english_"
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
                  "type":       "stop",
                  "stopwords":  "_russian_"
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
          },
          "mappings": {
            "dynamic": "strict",
            "properties": {
              "id": {
                "type": "keyword"
              },
              "imdb_rating": {
                "type": "float"
              },
              "genres": {
                "type": "keyword"
              },
              "title": {
                "type": "text",
                "analyzer": "ru_en",
                "fields": {
                  "raw": {
                    "type":  "keyword"
                  }
                }
              },
              "description": {
                "type": "text",
                "analyzer": "ru_en"
              },
              "directors_names": {
                "type": "text",
                "analyzer": "ru_en"
              },
              "actors_names": {
                "type": "text",
                "analyzer": "ru_en"
              },
              "writers_names": {
                "type": "text",
                "analyzer": "ru_en"
              },
              "directors": {
                "type": "nested",
                "dynamic": "strict",
                "properties": {
                  "id": {
                    "type": "keyword"
                  },
                  "name": {
                    "type": "text",
                    "analyzer": "ru_en"
                  }
                }
              },
              "actors": {
                "type": "nested",
                "dynamic": "strict",
                "properties": {
                  "id": {
                    "type": "keyword"
                  },
                  "name": {
                    "type": "text",
                    "analyzer": "ru_en"
                  }
                }
              },
              "writers": {
                "type": "nested",
                "dynamic": "strict",
                "properties": {
                  "id": {
                    "type": "keyword"
                  },
                  "name": {
                    "type": "text",
                    "analyzer": "ru_en"
                  }
                }
              }
            }
          }
        }
        }





state_storage = JsonFileStorage("state.json")
state = State(state_storage)
