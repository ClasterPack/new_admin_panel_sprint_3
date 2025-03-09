import os
from uuid import UUID
from typing import List, Optional

from pydantic import BaseModel
from dotenv import load_dotenv
load_dotenv()

class Config(BaseModel):
    postgres_host: str
    postgres_db: str
    postgres_user: str
    postgres_password: str
    es_host: str
    es_index: str

class Person(BaseModel):
    id: UUID
    name: str

class Movie(BaseModel):
    id: UUID
    title: str
    description: Optional[str] = None
    imdb_rating: Optional[float] = None
    genres: List[Optional[str]] = []
    directors: List[Optional[Person]] = []
    actors: List[Optional[Person]] = []
    writers: List[Optional[Person]] = []
    directors_names: List[str]
    actors_names: List[str]
    writers_names: List[str]


my_config = Config(
    postgres_host=os.getenv("POSTGRES_HOST"),
    postgres_db=os.getenv("POSTGRES_DB"),
    postgres_user=os.getenv("POSTGRES_USER"),
    postgres_password=os.getenv("POSTGRES_PASSWORD"),
    es_host=os.getenv("ES_HOST"),
    es_index=os.getenv("ES_INDEX"),
)
