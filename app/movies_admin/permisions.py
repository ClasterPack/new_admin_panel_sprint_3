from rest_framework.permissions import SAFE_METHODS, BasePermission


class UpdateMoviesPermission(BasePermission):
    def has_permission(self, request, obj):
        if request.method in SAFE_METHODS:
            return True
