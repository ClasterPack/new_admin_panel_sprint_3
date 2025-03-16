FROM --platform=linux/amd64 python:3.12.3

WORKDIR /app

RUN apt update && apt install -y netcat-openbsd
RUN pip install --upgrade pip && pip install poetry

COPY poetry.lock pyproject.toml /app/

RUN poetry install --no-root

COPY etl /app/etl/

ENV PYTHONPATH=/app:$PYTHONPATH

CMD ["poetry", "run", "python", "etl/main.py"]

