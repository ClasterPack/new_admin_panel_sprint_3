import psycopg
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)


def extract_data_from_postgres(conn) -> List[Dict[str, Any]]:
    query = """
    SELECT
        fn.id,
        fn.title,
        fn.description,
        fn.rating AS imdb_rating,
        COALESCE(array_agg(DISTINCT g.name), '{}') AS genres,
        COALESCE(array_agg(DISTINCT p.full_name) FILTER (WHERE pfn.role='director'), '{}') AS directors_names,
        COALESCE(array_agg(DISTINCT p.full_name) FILTER (WHERE pfn.role='actor'), '{}') AS actors_names,
        COALESCE(array_agg(DISTINCT p.full_name) FILTER (WHERE pfn.role='writer'), '{}') AS writers_names
    FROM content.film_work fn
    LEFT JOIN content.person_film_work pfn ON pfn.film_work_id = fn.id
    LEFT JOIN content.person p ON p.id = pfn.person_id
    LEFT JOIN content.genre_film_work gfn ON gfn.film_work_id = fn.id
    LEFT JOIN content.genre g ON g.id = gfn.genre_id
    GROUP BY fn.id;
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

    data = []
    for row in rows:
        movie = {
            'id': row[0],
            'title': row[1],
            'description': row[2],
            'imdb_rating': row[3],
            'genres': row[4],
            'directors_names': row[5],
            'actors_names': row[6],
            'writers_names': row[7]
        }


        data.append(movie)

    return data

def get_postgres_connection(config: dict):
    try:
        conn = psycopg.connect(
            host=config.postgres_host,
            dbname=config.postgres_db,
            user=config.postgres_user,
            password=config.postgres_password
        )
        logger.info("Successfully connected to PostgreSQL.")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL: {e}")
        raise
