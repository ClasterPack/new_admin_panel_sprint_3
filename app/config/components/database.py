import os

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.getenv("postgres_db"),
        "USER": os.getenv("postgres_user"),
        "PASSWORD": os.getenv("postgres_password"),
        "HOST": os.getenv("sql_host", "127.0.0.1"),
        "PORT": os.getenv("SQL_PORT", 5432),
        "OPTIONS": {
            "options": os.getenv("SQL_OPTIONS"),
        },
    }
}
