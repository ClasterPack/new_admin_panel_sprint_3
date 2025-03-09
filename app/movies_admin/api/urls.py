from django.urls import include, path

urlpatterns = [
    path("v1/", include("movies_admin.api.v1.urls")),
]
