import datetime
import logging
from contextlib import closing
from typing import Any, Dict, List, Literal

import psycopg
from psycopg.rows import dict_row

from etl.settings import Movie, Settings
from etl.state import State

logger = logging.getLogger(__name__)


def extract_data_from_postgres(conn) -> List[Dict[str, Any]]:
    query = """
    SELECT
        fw.id,
        fw.title,
        fw.description,
        fw.rating AS imdb_rating,
        COALESCE(array_agg(DISTINCT g.name), '{}') AS genres,
        COALESCE(array_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role='director'), '{}') AS directors_names,
        COALESCE(array_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role='actor'), '{}') AS actors_names,
        COALESCE(array_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role='writer'), '{}') AS writers_names,
        COALESCE(
            json_agg(
                DISTINCT jsonb_build_object(
                    'id', p.id,
                    'name', p.full_name
                )
            ) FILTER (WHERE pfw.role='director'),
            '[]'
        ) AS directors,
        COALESCE(
            json_agg(
                DISTINCT jsonb_build_object(
                    'id', p.id,
                    'name', p.full_name
                )
            ) FILTER (WHERE pfw.role='actor'),
            '[]'
        ) AS actors,
        COALESCE(
            json_agg(
                DISTINCT jsonb_build_object(
                    'id', p.id,
                    'name', p.full_name
                )
            ) FILTER (WHERE pfw.role='writer'),
            '[]'
        ) AS writers
    FROM content.film_work fw
    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
    LEFT JOIN content.person p ON p.id = pfw.person_id
    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
    LEFT JOIN content.genre g ON g.id = gfw.genre_id
    GROUP BY fw.id;
    """
    with conn.cursor(row_factory=dict_row) as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

    data = []
    for row in rows:
        movie = Movie(
            id=row["id"],
            title=row["title"],
            description=row.get("description"),
            imdb_rating=row.get("imdb_rating"),
            genres=row["genres"],
            directors_names=row["directors_names"],
            actors_names=row["actors_names"],
            writers_names=row["writers_names"],
            directors=row["directors"],
            actors=row["actors"],
            writers=row["writers"],
        )
        data.append(movie.model_dump())

    return data

def get_updated_objects_ids(
        conn, state: State,
        table: Literal['person', 'genre'],
        key_state: str
) -> tuple[list, datetime.datetime]:
    with closing(conn.cursor()) as cursor:
        time = state.get_state(key_state)
        if time is None:
            time = datetime.datetime.min
        query=f"""
        SELECT id, modified
                FROM content.{table}
                WHERE modified > '{time}'
                ORDER BY modified
                LIMIT 100
        """
        cursor.execute(query)
        results = cursor.fetchall()
        if results:
            last_update_time = results[-1][1]
        ids=list(i[0]for i in results)
        return ids, last_update_time





def get_postgres_connection(config: Settings) -> psycopg.connection:
    try:
        conn = psycopg.connect(
            host=config.POSTGRES_HOST,
            dbname=config.POSTGRES_DB,
            user=config.POSTGRES_USER,
            password=config.POSTGRES_PASSWORD,
        )
        logger.info("Successfully connected to PostgreSQL.")
        return conn
    except Exception as e:
        logger.error("Error connecting to PostgreSQL: %s" % str(e))
        raise
