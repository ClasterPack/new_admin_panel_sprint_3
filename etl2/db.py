import datetime
from contextlib import closing
from typing import Literal

import psycopg
from psycopg import abc

from etl2.state import State
from etl2.config import personage


class InitMixin(abc.ABC):
    def __init__(self, connection: psycopg.Connection, state: State):
        self.connection = connection
        self.state = state


class GetUpdateObjectMixin(InitMixin):
    def get_update_objects_ids(
            self, table : Literal['person', 'genre'], key_state: str
    ) -> tuple[list, datetime.datetime]:

        with closing(self.connection.cursor()) as psql_cursor:
            time = self.state.get_state(key_state)
            query=f"""
            SELECT id, modified
            FROM content.{table}
            WHERE modified > '{time}'
            ORDER BY modified
            LIMIT 100
            """
            psql_cursor.execute(query)
            results = psql_cursor.fetchall()
            state = None
            if results:
                state = results[-1][1]
            ids= list(i[0] for i in results)
            return ids, state

class PostgresProducers(GetUpdateObjectMixin):
    def get_person_update_ids(self)->tuple[list, datetime.datetime]:
        person = self.get_update_objects_ids(table='person', key_state=personage)
        return person

    def get_genre_update_ids(self)->tuple[list, datetime.datetime]:
        genre = self.get_update_objects_ids(table='genre', key_state=personage)
        return genre


class PostgresEnricher(InitMixin):
    def get_fw_based_on_related_table(self)->tuple[list, dict[str, datetime.datetime]]:
        with closing(self.connection.cursor()) as psql_cursor:
            time= self.state.get_state(key_state='person')
            query = """
            SELECT fw.id, fw.modified
            FROM content.film_work fw
            """