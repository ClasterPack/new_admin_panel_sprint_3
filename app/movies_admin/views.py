from movies_admin.models import Filmwork, Genre
from movies_admin.serializers import FilmWorkSerializer, GenreSerializer
from rest_framework import filters
from rest_framework.permissions import IsAuthenticatedOrReadOnly
from rest_framework.viewsets import ModelViewSet


class GenreViewSet(ModelViewSet):
    queryset = Genre.objects.all().order_by("id")
    serializer_class = GenreSerializer


class FilmWorkListViewSet(ModelViewSet):
    queryset = Filmwork.objects.all().order_by("id")
    serializer_class = FilmWorkSerializer
    permission_classes = (IsAuthenticatedOrReadOnly,)
    filter_backends = (
        filters.SearchFilter,
        filters.OrderingFilter,
    )
    search_fields = ("title", "created", "genres")
    ordering_fields = ("title", "rating", "created")
