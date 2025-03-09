from movies_admin.models import Filmwork, Genre, Person
from rest_framework import serializers


class GenreSerializer(serializers.ModelSerializer):
    class Meta:
        model = Genre
        fields = ["name"]


class PersonSerializer(serializers.ModelSerializer):
    class Meta:
        model = Person
        fields = ["full_name"]


class FilmWorkSerializer(serializers.ModelSerializer):
    genres = serializers.SlugRelatedField(
        many=True, slug_field="name", queryset=Genre.objects.all()
    )
    actors = serializers.SerializerMethodField()
    directors = serializers.SerializerMethodField()
    writers = serializers.SerializerMethodField()

    class Meta:
        model = Filmwork
        fields = (
            "id",
            "title",
            "description",
            "creation_date",
            "rating",
            "type",
            "genres",
            "actors",
            "directors",
            "writers",
        )

    def get_actors(self, obj):
        return self._get_people_by_role(obj, "actor")

    def get_directors(self, obj):
        return self._get_people_by_role(obj, "director")

    def get_writers(self, obj):
        return self._get_people_by_role(obj, "writer")

    def _get_people_by_role(self, obj, role):
        people = obj.personfilmwork_set.filter(role=role).select_related("person")
        return [person.person.full_name for person in people]
