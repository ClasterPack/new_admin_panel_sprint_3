import http
import json
from enum import StrEnum, auto

import requests
from django.conf import settings
from django.contrib.auth import get_user_model, login
from django.contrib.auth.backends import BaseBackend
from django.db import IntegrityError


class Roles(StrEnum):
    ADMIN = auto()
    SUBSCRIBER = auto()



class CustomBackend(BaseBackend):
    def authenticate(self, request, username=None, password=None):
        User = get_user_model()
        url = settings.AUTH_API_LOGIN_URL
        payload = {'login': username, 'password': password}
        response = requests.post(url, data=json.dumps(payload))

        if response.status_code != http.HTTPStatus.OK:
            return None

        data = response.json()

        try:
            # Проверяем, существует ли пользователь с данным email
            user = User.objects.filter(email=data.get('email')).first()

            # Если пользователь не существует, создаём его
            if not user:
                user = User(id=data['id'], email=data.get('email'))
                user.first_name = data.get('first_name')
                user.last_name = data.get('last_name')
                user.is_admin = data.get('is_superuser', False)
                user.is_active = data.get('is_active')
                user.save()

        except IntegrityError as e:
            # Логируем ошибку в случае нарушения уникальности
            return f"Ошибка при сохранении пользователя: {str(e)}"
        except Exception as e:
            # Логируем другие исключения
            return f"Ошибка при аутентификации: {str(e)}"

        # Если пользователь активен, логиним его
        if user.is_active:
            return user

        return None

    def get_user(self, user_id):
        User = get_user_model()
        try:
            return User.objects.get(pk=user_id)
        except User.DoesNotExist:
            return None
