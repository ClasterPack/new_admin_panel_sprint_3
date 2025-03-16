import datetime
import logging
from contextlib import closing
from typing import Any, Dict, List, Literal

import psycopg
from psycopg.rows import dict_row

from etl.settings import Movie, Settings
from etl.state import State

logger = logging.getLogger(__name__)


class DBExtractions:
    def __init__(self,config: Settings , state: State):
        self.state: State = state
        self.config = config

        try:
            conn = psycopg.connect(
                host=config.SQL_HOST,
                dbname=config.POSTGRES_DB,
                user=config.POSTGRES_USER,
                password=config.POSTGRES_PASSWORD,
            )
            logger.info("Successfully connected to PostgreSQL.")
            self.conn = conn
        except Exception as e:
            logger.error("Error connecting to PostgreSQL: %s" % str(e))
            raise


    def extract_data(self) -> List[Dict[str, Any]]:
        query = """
        SELECT
            fw.id,
            fw.title,
            fw.description,
            fw.rating AS imdb_rating,
            fw.modified,
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
        with self.conn.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query)
            rows= cursor.fetchall()
            data =[]
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
                    modified=row["modified"]
                )
                data.append(movie.model_dump())
            data.sort(key=lambda x: x["modified"])
        return data

    def get_updated_objects_ids(
            self,
            table: Literal['person','genre'],
            key_state: str
    ) -> tuple[list, datetime.datetime] or None:
        with closing(self.conn.cursor()) as psql_cursor:
            time = self.state.get_state(key_state)
            if time is None:
                time = datetime.datetime.min
            query=f"""
            SELECT id, modified
            FROM content.{table}
            WHERE modified > '{time}'
            ORDER BY modified
            LIMIT '{self.config.BATCH_SIZE}';
            """
            psql_cursor.execute(query)
            results = psql_cursor.fetchall()
            if results:
                time = results[-1][1]
                ids=list(i[0]for i in results)
                return ids, time
            else:
                return None

    def get_updated_fw(
            self, state_key: str
    ) -> tuple[list, datetime.datetime] or None:
        with self.conn.cursor(row_factory=dict_row) as psql_cursor:
            time = self.state.get_state(state_key)
            if time is None:
                time = datetime.datetime.min
            query = f"""
            SELECT
                fw.id,
                fw.title,
                fw.description,
                fw.rating AS imdb_rating,
                fw.modified
            FROM content.film_work fw
            WHERE fw.modified > '{time}'
            ORDER BY fw.modified
            LIMIT '{self.config.BATCH_SIZE}';
            """
            psql_cursor.execute(query)

            rows = psql_cursor.fetchall()
            if rows:
                last_update_time = rows[-1]['modified']
                return rows, last_update_time
            else:
                return None


    def updates_genres(self,updated_ids):
        str_updated_ids = ', '.join([f"'{uuid}'" for uuid in updated_ids])
        logger.info(f'Updating Genres : %s', len(updated_ids))
        query = f"""
        SELECT
            fw.id,
            COALESCE(array_agg(DISTINCT g.name), '{{}}') AS genres
        FROM content.film_work fw
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        LEFT JOIN content.genre g ON g.id = gfw.genre_id
        WHERE g.id IN ({str_updated_ids})
        GROUP BY fw.id
        """
        with self.conn.cursor(row_factory=dict_row) as psql_cursor:
            psql_cursor.execute(query)
            rows = psql_cursor.fetchall()
            return rows

    def update_persons(self,updated_ids):
        str_updated_ids = ', '.join([f"'{uuid}'" for uuid in updated_ids])
        query = f"""
                SELECT
                    fw.id,
                    COALESCE(array_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role='director'), '{{}}') AS directors_names,
                    COALESCE(array_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role='actor'), '{{}}') AS actors_names,
                    COALESCE(array_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role='writer'), '{{}}') AS writers_names,
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
                WHERE p.id IN ({str_updated_ids})
                GROUP BY fw.id
                """
        with self.conn.cursor(row_factory=dict_row) as psql_cursor:
            psql_cursor.execute(query)
            rows = psql_cursor.fetchall()
            return rows








