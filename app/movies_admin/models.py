import uuid

from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.utils.translation import gettext_lazy as _

from .mixins import TimeStampedMixin, UUIDMixin


class Genre(UUIDMixin, TimeStampedMixin):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(_("name"), max_length=255)
    description = models.TextField(_("description"), blank=True)

    def __str__(self):
        return self.name

    class Meta:
        db_table = 'content"."genre'
        verbose_name = _("genre")
        verbose_name_plural = _("genres")


class Filmwork(UUIDMixin, TimeStampedMixin):
    title = models.CharField(_("title"), max_length=255, blank=False)
    description = models.TextField(_("description"), blank=True)
    creation_date = models.DateTimeField(_("creation_date"), auto_now_add=True)
    rating = models.FloatField(
        _("rating"),
        blank=True,
        validators=[MinValueValidator(0), MaxValueValidator(10)],
    )
    type = models.CharField(_("type"), max_length=100, blank=False)
    genres = models.ManyToManyField(Genre, through="GenreFilmwork")

    def __str__(self):
        return self.title

    class Meta:
        db_table = 'content"."film_work'
        verbose_name = _("film work")
        verbose_name_plural = _("film works")


class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.CharField(_("full_name"), max_length=255, blank=False)

    def __str__(self):
        return self.full_name

    class Meta:
        db_table = 'content"."person'
        verbose_name = _("person")
        verbose_name_plural = _("persons")


class GenreFilmwork(UUIDMixin):
    film_work = models.ForeignKey("Filmwork", on_delete=models.CASCADE)
    genre = models.ForeignKey("Genre", on_delete=models.CASCADE)
    created = models.DateTimeField(_("created"), auto_now_add=True)

    class Meta:
        db_table = 'content"."genre_film_work'


class PersonFilmwork(UUIDMixin):
    person = models.ForeignKey("Person", on_delete=models.CASCADE)
    film_work = models.ForeignKey("Filmwork", on_delete=models.CASCADE)
    role = models.TextField(_("role"), null=True)
    created = models.DateTimeField(_("created"), auto_now_add=True)

    class Meta:
        db_table = 'content"."person_film_work'
