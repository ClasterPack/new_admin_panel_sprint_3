FROM --platform=linux/amd64 python:3.12.3

WORKDIR /code

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV DJANGO_SETTINGS_MODULE 'config.settings'

RUN apt update && apt install -y netcat-openbsd
RUN pip install --upgrade pip && pip install poetry

ENV TZ=Europe/Moscow
RUN apt install tzdata
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY ./pyproject.toml ./poetry.lock ./

RUN poetry install --no-root

COPY ./docker/api/entrypoint.sh /web/entrypoint.sh
#COPY ./app/ .

ENTRYPOINT ["/web/entrypoint.sh"]


