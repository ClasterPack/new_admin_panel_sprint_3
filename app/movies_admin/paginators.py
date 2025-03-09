from rest_framework import pagination
from rest_framework.response import Response


class TotalPagesCountPaginator(pagination.PageNumberPagination):
    def get_paginated_response(self, data):
        previous_page_number = (
            self.page.paginator.page(self.page.number - 1).number
            if self.page.has_previous()
            else None
        )
        next_page_number = (
            self.page.paginator.page(self.page.number + 1).number
            if self.page.has_next()
            else None
        )
        return Response(
            {
                "count": self.page.paginator.count,
                "total_pages": self.page.paginator.num_pages,
                "prev": previous_page_number,
                "next": next_page_number,
                "results": data,
            }
        )
