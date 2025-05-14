import requests
from django.contrib import admin
from django.conf import settings
from django.contrib.auth.admin import UserAdmin

from .models import User, RemoteRole, Role


@admin.register(User)
class CustomUserAdmin(UserAdmin):
    list_display = ('email', 'first_name', 'last_name', 'is_admin', 'is_active')
    search_fields = ('email', 'first_name', 'last_name')
    ordering = ('email',)

    filter_horizontal = ()
    list_filter = ('is_admin', 'is_active')

    fieldsets = (
        (None, {'fields': ('email', 'password')}),
        ('Personal info', {'fields': ('first_name', 'last_name')}),
        ('Permissions', {'fields': ('is_admin', 'is_active')}),
    )

    add_fieldsets = (
        (None, {
            'classes': ('wide',),
            'fields': ('email', 'first_name', 'last_name', 'password1', 'password2', 'is_admin', 'is_active'),
        }),
    )


