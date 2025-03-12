import os
from datetime import datetime
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel
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
    title: str
    description: Optional[str] = None
    imdb_rating: Optional[float] = None
    genres: List[Optional[str]] = None
    directors_names: Optional[List[str]] = None
    actors_names: Optional[List[str]] = None
    writers_names: Optional[List[str]] = None


type_map = {
    str: "keyword",
    datetime: "date",
    int: "long",
    float: "float",
    bool: "boolean",
    UUID: "keyword",
    list: "keyword",
    dict: "nested",
    BaseModel: "nested",
}

state_storage = JsonFileStorage("state.json")
state = State(state_storage)
