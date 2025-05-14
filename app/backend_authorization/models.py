import uuid

from backend_authorization.managers import MyUserManager
from django.contrib.auth.models import AbstractBaseUser
from django.db import models


class User(AbstractBaseUser):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    email = models.EmailField(verbose_name='email address', max_length=255, unique=True)
    is_active = models.BooleanField(default=True)
    is_admin = models.BooleanField(default=False)
    first_name = models.CharField(max_length=255)
    last_name = models.CharField(max_length=255)

    # строка с именем поля модели, которая используется в качестве уникального идентификатора
    USERNAME_FIELD = 'email'

    # менеджер модели
    objects = MyUserManager()

    @property
    def is_staff(self):
        return self.is_admin

    def __str__(self):
        return f'{self.email} {self.id}'

    def has_perm(self, perm, obj=None):
        return True

    def has_module_perms(self, app_label):
        return True


class Permission(models.Model):
    id = models.IntegerField(primary_key=True)
    name = models.CharField(max_length=100, unique=True, db_index=True)
    description = models.TextField(null=True, blank=True)
    code = models.CharField(max_length=100, unique=True)

    def __str__(self):
        return self.name


class Role(models.Model):
    id = models.IntegerField(primary_key=True)
    name = models.CharField(max_length=100, unique=True, db_index=True)
    description = models.TextField(null=True, blank=True)
    users = models.ManyToManyField('backend_authorization.User', related_name='roles', blank=True)
    permissions = models.ManyToManyField(Permission, related_name='roles', blank=True)

    def __str__(self):
        return self.name
