#!/bin/sh

echo "Waiting for postgres..."
while ! nc -z $SQL_HOST $SQL_PORT; do
  sleep 0.1
done
echo "PostgreSQL started"

# Set PYTHONPATH and DJANGO_SETTINGS_MODULE
export PYTHONPATH="/code:$PYTHONPATH"
export DJANGO_SETTINGS_MODULE="config.settings"

echo "Current PYTHONPATH: $PYTHONPATH"

chmod -R 775 /code


poetry run python manage.py migrate --fake --fake movies_admin 0001
poetry run python manage.py migrate --no-input

DJANGO_SUPERUSER_USERNAME=admin \
  DJANGO_SUPERUSER_PASSWORD=123123 \
  DJANGO_SUPERUSER_EMAIL=mail@mail.ru \
  poetry run python manage.py createsuperuser --noinput || true

poetry run gunicorn config.wsgi:application --bind 0.0.0.0:8000 --reload

exec "$@"
