from django.contrib import admin

from .models import Filmwork, Genre, GenreFilmwork, Person, PersonFilmwork


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    pass


class GenreFilmworkInline(admin.TabularInline):
    model = GenreFilmwork


class PersonInline(admin.TabularInline):
    model = PersonFilmwork


@admin.register(Filmwork)
class FilmworkAdmin(admin.ModelAdmin):
    inlines = (GenreFilmworkInline,)
    list_display = ("title", "type", "creation_date", "rating", "created", "modified")
    search_fields = ("title", "description", "id")
    list_filter = ("type",)


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    inlines = (PersonInline,)
    list_display = ("full_name", "created", "modified")
    search_fields = ("full_name", "created", "id")
    list_filter = ("full_name",)
