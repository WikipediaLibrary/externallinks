import csv

from django.contrib.contenttypes.models import ContentType
from django.db.models import Q, Sum
from django.http import HttpResponse
from django.views.generic import View

from extlinks.aggregates.models import (
    LinkAggregate,
    PageProjectAggregate,
    UserAggregate,
)
from extlinks.common.helpers import build_queryset_filters
from extlinks.links.models import URLPattern, LinkEvent
from extlinks.organisations.models import Collection, Organisation
from extlinks.programs.models import Program


# CSV views borrowed from
# https://github.com/WikipediaLibrary/TWLight/blob/master/TWLight/graphs/views.py
# These views are a little hacky in how they determine whether we need the CSV
# for an organisation or partner page, but this seems to work.
class _CSVDownloadView(View):
    """
    Base view powering CSV downloads. Not intended to be used directly.
    URLs should point at subclasses of this view. Subclasses should implement a
    _write_data() method.
    """

    def get(self, request, *args, **kwargs):
        # Create the HttpResponse object with the appropriate CSV header.
        response = HttpResponse(content_type="text/csv")
        response["Content-Disposition"] = 'attachment; filename="data.csv"'

        self._write_data(response)

        return response

    def _write_data(self, response):
        raise NotImplementedError


class CSVOrgTotals(_CSVDownloadView):
    def _write_data(self, response):
        program_pk = self.kwargs["pk"]
        queryset_filter = _get_queryset_filter(
            program_pk, self.request.build_absolute_uri(), self.request.GET
        )

        top_orgs = (
            LinkAggregate.objects.filter(queryset_filter)
            .values("organisation__pk", "organisation__name")
            .annotate(
                links_added=Sum("total_links_added"),
                links_removed=Sum("total_links_removed"),
                links_diff=Sum("total_links_added") - Sum("total_links_removed"),
            )
            .order_by("-links_diff", "-links_added", "-links_removed")
        )

        writer = csv.writer(response)

        writer.writerow(["Organisation", "Links added", "Links removed", "Net Change"])

        for org in top_orgs:
            writer.writerow(
                [
                    org["organisation__name"],
                    org["links_added"],
                    org["links_removed"],
                    org["links_diff"],
                ]
            )


class CSVPageTotals(_CSVDownloadView):
    def _write_data(self, response):
        pk = self.kwargs["pk"]
        queryset_filter = _get_queryset_filter(
            pk, self.request.build_absolute_uri(), self.request.GET
        )

        top_pages = (
            PageProjectAggregate.objects.filter(queryset_filter)
            .values("project_name", "page_name")
            .annotate(
                links_added=Sum("total_links_added"),
                links_removed=Sum("total_links_removed"),
                links_diff=Sum("total_links_added") - Sum("total_links_removed"),
            )
            .order_by("-links_diff", "-links_added", "-links_removed")
        )
        writer = csv.writer(response)

        writer.writerow(
            ["Page title", "Project", "Links added", "Links removed", "Net Change"]
        )

        for page in top_pages:
            writer.writerow(
                [
                    page["page_name"],
                    page["project_name"],
                    page["links_added"],
                    page["links_removed"],
                    page["links_diff"],
                ]
            )


class CSVProjectTotals(_CSVDownloadView):
    def _write_data(self, response):
        pk = self.kwargs["pk"]
        queryset_filter = _get_queryset_filter(
            pk, self.request.build_absolute_uri(), self.request.GET
        )

        top_projects = (
            PageProjectAggregate.objects.filter(queryset_filter)
            .values("project_name")
            .annotate(
                links_added=Sum("total_links_added"),
                links_removed=Sum("total_links_removed"),
                links_diff=Sum("total_links_added") - Sum("total_links_removed"),
            )
            .order_by("-links_diff", "-links_added", "-links_removed")
        )
        writer = csv.writer(response)

        writer.writerow(["Project", "Links added", "Links removed", "Net Change"])

        for project in top_projects:
            writer.writerow(
                [
                    project["project_name"],
                    project["links_added"],
                    project["links_removed"],
                    project["links_diff"],
                ]
            )


class CSVUserTotals(_CSVDownloadView):
    def _write_data(self, response):
        pk = self.kwargs["pk"]
        queryset_filter = _get_queryset_filter(
            pk, self.request.build_absolute_uri(), self.request.GET
        )

        top_users = (
            UserAggregate.objects.filter(queryset_filter)
            .values("username")
            .annotate(
                links_added=Sum("total_links_added"),
                links_removed=Sum("total_links_removed"),
                links_diff=Sum("total_links_added") - Sum("total_links_removed"),
            )
            .order_by("-links_diff", "-links_added", "-links_removed")
        )
        writer = csv.writer(response)

        writer.writerow(["Username", "Links added", "Links removed", "Net Change"])

        for user in top_users:
            writer.writerow(
                [
                    user["username"],
                    user["links_added"],
                    user["links_removed"],
                    user["links_diff"],
                ]
            )


def _get_queryset_filter(pk, uri, filters):
    """
    This function returns a Q object with filters depending on which URL a user
    is requesting information from

    Parameters
    ----------
    pk: int
        The primary key of a collection or a program, depending on the origin of
        the request

    uri: str
        The origin URL from the request. If the URL is from the organisations view,
        then we will obtain the collection. Otherwise, if the URL is from the
        programs view, we will obtain the organisations associated to that program

    filters: dict
        The filters (if there are any) that were passed in the request

    Returns
    -------
    Q : A Q object which will filter the aggregates queries
    """
    # If we came from an organisation page, then we are passing the collection id
    if "/organisations" in uri:
        collection = Collection.objects.get(pk=pk)
        queryset_filter = build_queryset_filters(filters, {"collection": collection})
    else:
        program = Program.objects.prefetch_related("organisation_set").get(pk=pk)
        queryset_filter = build_queryset_filters(
            filters, {"organisations": program.organisation_set.all()}
        )

    return queryset_filter
