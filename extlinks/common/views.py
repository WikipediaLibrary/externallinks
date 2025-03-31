import csv

from dateutil.relativedelta import relativedelta
from django.db.models import Sum
from django.http import HttpResponse
from django.views.generic import View

from extlinks.aggregates.models import (
    PageProjectAggregate,
    ProgramTopOrganisationsTotal,
    ProgramTopProjectsTotal,
    ProgramTopUsersTotal,
    UserAggregate,
)
from extlinks.aggregates.storage import calculate_totals, download_aggregates
from extlinks.common.helpers import build_queryset_filters, last_day
from extlinks.organisations.models import Collection
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
            ProgramTopOrganisationsTotal.objects.filter(queryset_filter)
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
        aggregates = PageProjectAggregate.objects.filter(queryset_filter)

        top_pages = {
            (top_page["project_name"], top_page["page_name"]): top_page
            for top_page in aggregates.values("project_name", "page_name").annotate(
                links_added=Sum("total_links_added"),
                links_removed=Sum("total_links_removed"),
                links_diff=Sum("total_links_added") - Sum("total_links_removed"),
            )
        }

        to_date = None
        if aggregates.exists():
            earliest_aggregate_date = aggregates.earliest("full_date").full_date
            to_date = earliest_aggregate_date - relativedelta(months=1)
            to_date = to_date.replace(day=last_day(to_date))

        totals = calculate_totals(
            download_aggregates(
                prefix="aggregates_pageprojectaggregate",
                queryset_filter=queryset_filter,
                to_date=to_date,
            ),
            group_by=lambda record: (record["project_name"], record["page_name"]),
        )
        for total in totals:
            key = (total["project_name"], total["page_name"])

            if key in top_pages:
                top_pages[key]["links_added"] += total["total_links_added"]
                top_pages[key]["links_removed"] += total["total_links_removed"]
                top_pages[key]["links_diff"] += total["links_diff"]
            else:
                # We can't use the same key names as the aggregate fields in
                # the table itself so copy the totals into fields matching
                # those in the annotate call.
                top_pages[key] = total.copy()
                top_pages[key]["links_added"] = total["total_links_added"]
                top_pages[key]["links_removed"] = total["total_links_removed"]

        top_pages = sorted(
            top_pages.values(),
            key=lambda x: (
                x["links_diff"],
                x["links_added"],
                x["links_removed"],
            ),
            reverse=True,
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
        uri = self.request.build_absolute_uri()
        queryset_filter = _get_queryset_filter(pk, uri, self.request.GET)
        Model = ProgramTopProjectsTotal if "/programs" in uri else PageProjectAggregate
        aggregates = Model.objects.filter(queryset_filter)

        top_projects = {
            top_page["project_name"]: top_page
            for top_page in (
                aggregates.values("project_name").annotate(
                    links_added=Sum("total_links_added"),
                    links_removed=Sum("total_links_removed"),
                    links_diff=Sum("total_links_added") - Sum("total_links_removed"),
                )
            )
        }

        # Only factor in archived aggregate data if we're returning totals for
        # a collection and not a program. All program totals are available in
        # the database and aren't stored in object storage.
        if "/programs" not in uri:
            to_date = None
            if aggregates.exists():
                earliest_aggregate_date = aggregates.earliest("full_date").full_date
                to_date = earliest_aggregate_date - relativedelta(months=1)
                to_date = to_date.replace(day=last_day(to_date))

            totals = calculate_totals(
                download_aggregates(
                    prefix="aggregates_pageprojectaggregate",
                    queryset_filter=queryset_filter,
                    to_date=to_date,
                ),
                group_by=lambda record: record["project_name"],
            )
            for total in totals:
                key = total["project_name"]

                if key in top_projects:
                    top_projects[key]["links_added"] += total["total_links_added"]
                    top_projects[key]["links_removed"] += total["total_links_removed"]
                    top_projects[key]["links_diff"] += total["links_diff"]
                else:
                    # We can't use the same key names as the aggregate fields in
                    # the table itself so copy the totals into fields matching
                    # those in the annotate call.
                    top_projects[key] = total.copy()
                    top_projects[key]["links_added"] = total["total_links_added"]
                    top_projects[key]["links_removed"] = total["total_links_removed"]

        top_projects = sorted(
            top_projects.values(),
            key=lambda x: (
                x["links_diff"],
                x["links_added"],
                x["links_removed"],
            ),
            reverse=True,
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
        uri = self.request.build_absolute_uri()
        queryset_filter = _get_queryset_filter(pk, uri, self.request.GET)
        Model = ProgramTopUsersTotal if "/programs" in uri else UserAggregate
        aggregates = Model.objects.filter(queryset_filter)

        top_users = {
            top_user["username"]: top_user
            for top_user in (
                aggregates.values("username").annotate(
                    links_added=Sum("total_links_added"),
                    links_removed=Sum("total_links_removed"),
                    links_diff=Sum("total_links_added") - Sum("total_links_removed"),
                )
            )
        }

        # Only factor in archived aggregate data if we're returning totals for
        # a collection and not a program. All program totals are available in
        # the database and aren't stored in object storage.
        if "/programs" not in uri:
            to_date = None
            if aggregates.exists():
                earliest_aggregate_date = aggregates.earliest("full_date").full_date
                to_date = earliest_aggregate_date - relativedelta(months=1)
                to_date = to_date.replace(day=last_day(to_date))

            totals = calculate_totals(
                download_aggregates(
                    prefix="aggregates_useraggregate",
                    queryset_filter=queryset_filter,
                    to_date=to_date,
                ),
                group_by=lambda record: record["username"],
            )
            for total in totals:
                key = total["username"]

                if key in top_users:
                    top_users[key]["links_added"] += total["total_links_added"]
                    top_users[key]["links_removed"] += total["total_links_removed"]
                    top_users[key]["links_diff"] += total["links_diff"]
                else:
                    # We can't use the same key names as the aggregate fields in
                    # the table itself so copy the totals into fields matching
                    # those in the annotate call.
                    top_users[key] = total.copy()
                    top_users[key]["links_added"] = total["total_links_added"]
                    top_users[key]["links_removed"] = total["total_links_removed"]

        top_users = sorted(
            top_users.values(),
            key=lambda x: (
                x["links_diff"],
                x["links_added"],
                x["links_removed"],
            ),
            reverse=True,
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
        queryset_filter = build_queryset_filters(
            filters, {"program": Program.objects.get(pk=pk)}
        )

    return queryset_filter
