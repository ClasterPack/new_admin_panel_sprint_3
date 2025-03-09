from django.urls import include, path
from movies_admin.views import FilmWorkListViewSet
from rest_framework.routers import DefaultRouter

router = DefaultRouter()
router.register("movies", FilmWorkListViewSet)
urlpatterns = [
    path("", include(router.urls)),
]
