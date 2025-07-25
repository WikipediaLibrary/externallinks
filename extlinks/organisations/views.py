import json
import re

from datetime import datetime, date, timedelta
from logging import getLogger

from dateutil.relativedelta import relativedelta
from django.contrib import messages
from django.db.models import Count, Sum, Q, Prefetch, CharField
from django.db.models.functions import Cast
from django.http import JsonResponse
from django.views.generic import ListView, DetailView

import extlinks.aggregates.storage as storage

from extlinks.aggregates.models import (
    LinkAggregate,
    PageProjectAggregate,
    UserAggregate,
)
from extlinks.aggregates.storage import (
    find_unique,
    calculate_totals,
    download_aggregates,
)
from extlinks.common.forms import FilterForm
from extlinks.common.helpers import (
    get_linksearchtotal_data_by_time,
    filter_linksearchtotals,
    build_queryset_filters,
    last_day,
)
from extlinks.links.models import LinkSearchTotal
from .models import Organisation, Collection

logger = getLogger("django")


class OrganisationListView(ListView):
    model = Organisation

    def get_queryset(self, **kwargs):
        queryset = (
            Organisation.objects.all()
            .annotate(collection_count=Count("collection"))
            .order_by("name")
        )
        return queryset


class OrganisationDetailView(DetailView):
    model = Organisation
    form_class = FilterForm

    def get_queryset(self, **kwargs):
        queryset = Organisation.objects.prefetch_related(
            "collection_set", "collection_set__url"
        )
        return queryset

    # This is almost, but not exactly, the same as the program view.
    # As such, most context gathering is split out to a helper.
    def get_context_data(self, **kwargs):
        messages.warning(
            self.request,
            "We have modified where Wikilink obtains its data from. Since some of this work is "
            "still in flight, the data shown in Wikilink is currently erroneous. ",
            fail_silently=True,
        )
        context = super(OrganisationDetailView, self).get_context_data(**kwargs)
        form = self.form_class(self.request.GET)
        context["form"] = form

        organisation_collections = self.object.collection_set.all()

        # Here we have a slightly more complex context dictionary setup, where
        # each collection has its own dictionary of data.
        context["collections"] = {}
        for collection in organisation_collections:
            this_collection_linksearchtotals = LinkSearchTotal.objects.prefetch_related(
                "url"
            ).filter(url__collections__name__contains=collection.name)

            form_data = None
            if form.is_valid():
                form_data = form.cleaned_data
                this_collection_linksearchtotals = filter_linksearchtotals(
                    this_collection_linksearchtotals, form_data
                )
                context["form_data"] = json.dumps(form_data, default=str)

            # Replace all special characters that might confuse JS with an
            # underscore.
            collection_key = re.sub("[^0-9a-zA-Z]+", "_", collection.name)

            context["collections"][collection_key] = {}
            context["collections"][collection_key]["object"] = collection
            context["collections"][collection_key]["collection_id"] = collection.pk
            context["collections"][collection_key][
                "urls"
            ] = collection.get_url_patterns()

            context["collections"][collection_key] = (
                self._build_collection_context_dictionary(
                    collection, context["collections"][collection_key], form_data
                )
            )

            start_date_request = self.request.GET.get("start_date")
            end_date_request = self.request.GET.get("end_date")
            start_date = (
                datetime.strptime(start_date_request, "%Y-%m-%d").date().replace(day=1)
                if start_date_request
                else None
            )
            end_date = (
                datetime.strptime(end_date_request, "%Y-%m-%d").date().replace(day=1)
                if end_date_request
                else None
            )
            # LinkSearchTotal chart data
            dates, linksearch_data = get_linksearchtotal_data_by_time(
                this_collection_linksearchtotals, start_date, end_date
            )

            context["collections"][collection_key]["linksearch_dates"] = dates
            context["collections"][collection_key]["linksearch_data"] = linksearch_data

            # Statistics
            if linksearch_data:
                total_start = linksearch_data[0]
                total_current = linksearch_data[-1]
                total_diff = total_current - total_start
                start_date_object = datetime.strptime(dates[0], "%Y-%m-%d")
                start_date = start_date_object.strftime("%B %Y")
            # If we haven't collected any LinkSearchTotals yet, then set
            # these variables to None so we don't show them in the statistics
            # box
            else:
                total_start = None
                total_current = None
                total_diff = None
                start_date = None
            context["collections"][collection_key][
                "linksearch_total_start"
            ] = total_start
            context["collections"][collection_key][
                "linksearch_total_current"
            ] = total_current
            context["collections"][collection_key]["linksearch_total_diff"] = total_diff
            context["collections"][collection_key]["linksearch_start_date"] = start_date

            context["query_string"] = self.request.META["QUERY_STRING"]

        return context

    def _build_collection_context_dictionary(self, collection, context, form_data):
        """
        This function builds the context dictionary that will populate the
        OrganisationDetailView

        Parameters
        ----------
        collection : Collection
            A collection that the aggregate data will be filtered from

        context : dict
            The context dictionary that the function will be adding information to

        form_data: dict|None
            If the filter form has valid filters, then there will be a dictionary
            to filter the aggregates tables by dates

        Returns
        -------
        dict : The context dictionary with the relevant statistics
        """
        if form_data:
            queryset_filter = build_queryset_filters(
                form_data, {"collection": collection}
            )
        else:
            queryset_filter = Q(collection=collection)

        context = self._fill_chart_context(context, queryset_filter)

        return context

    def _fill_chart_context(self, context, queryset_filter):
        """
        This function adds the chart information to the context
        dictionary to display in ProgramDetailView

        Parameters
        ----------
        context : dict
            The context dictionary that the function will be adding information to

        queryset_filter: Q
            If the information is filtered, this set of filters will filter it.
            The default is only filtering by the collection that is part of
            the organisation

        Returns
        -------
        dict : The context dictionary with the relevant statistics
        """
        dates = []
        existing_link_aggregates = {}
        eventstream_dates = []
        eventstream_net_change = []
        filtered_link_aggregate = LinkAggregate.objects.filter(queryset_filter)
        to_date = None

        # Figure out what date the graph should end on.
        date_cursor = self.request.GET.get("end_date")
        if date_cursor:
            date_cursor = datetime.strptime(date_cursor, "%Y-%m-%d").date()
        else:
            date_cursor = date.today()

        if filtered_link_aggregate.exists():
            earliest_link_date = filtered_link_aggregate.earliest("full_date").full_date

            # Figure out the cutoff date for archives since we only want to
            # download archived aggregates up to the point until aggregate data
            # is present in the DB.
            to_date = earliest_link_date - relativedelta(months=1)
            to_date = to_date.replace(day=last_day(to_date))
        else:
            # No link information from that collection, so setting earliest_link_date
            # to the first of the current month
            earliest_link_date = date_cursor.replace(day=1)

        links_aggregated_date = []

        # Download aggregates from object storage and calculate totals grouped
        # by year and month.
        totals = calculate_totals(
            download_aggregates(
                prefix="aggregates_linkaggregate",
                queryset_filter=queryset_filter,
                to_date=to_date,
            ),
            group_by=lambda record: (record["year"], record["month"]),
        )
        links_aggregated_date.extend(totals)

        links_aggregated_date = []

        # Download aggregates from object storage and calculate totals grouped
        # by year and month.
        totals = storage.calculate_totals(
            storage.download_aggregates(
                prefix="aggregates_linkaggregate",
                queryset_filter=queryset_filter,
                to_date=to_date,
            ),
            group_by=lambda record: (record["year"], record["month"]),
        )
        links_aggregated_date.extend(totals)

        # Fetch remaining aggregates that are present in the database and
        # append them after the aggregates from the archives.
        links_aggregated_date.extend(
            LinkAggregate.objects.filter(queryset_filter)
            .values("month", "year")
            .annotate(
                links_diff=Sum("total_links_added") - Sum("total_links_removed"),
            )
            .order_by("year", "month")
        )

        # Adjust the earliest link date to the date of the earliest known
        # archive so the chart includes the archive data.
        for total in totals:
            full_date = datetime.strptime(total["full_date"], "%Y-%m-%d").date()
            if full_date < earliest_link_date:
                earliest_link_date = full_date

        # Filling an array of dates that should be in the chart
        while date_cursor >= earliest_link_date:
            dates.append(date_cursor.strftime("%Y-%m"))
            # Figure out what the last month is regardless of today's date
            date_cursor = date_cursor.replace(day=1) - timedelta(days=1)

        dates = dates[::-1]

        for link in links_aggregated_date:
            if link["month"] < 10:
                date_combined = f"{link['year']}-0{link['month']}"
            else:
                date_combined = f"{link['year']}-{link['month']}"

            existing_link_aggregates[date_combined] = link["links_diff"]

        for month_year in dates:
            eventstream_dates.append(month_year)
            if month_year in existing_link_aggregates:
                eventstream_net_change.append(existing_link_aggregates[month_year])
            else:
                eventstream_net_change.append(0)

        # These stats are for filling the program net change chart
        context["eventstream_dates"] = eventstream_dates
        context["eventstream_net_change"] = eventstream_net_change

        return context


def get_editor_count(request):
    """
    request : dict
    Ajax request for editor count (found in the Statistics table)
    """
    form_data = json.loads(request.GET.get("form_data", "{}"))
    collection_id = int(request.GET.get("collection", None))
    collection = Collection.objects.get(id=collection_id)

    queryset_filter = build_queryset_filters(form_data, {"collection": collection})
    aggregates = UserAggregate.objects.filter(queryset_filter)
    usernames = set(aggregates.values_list("username", flat=True).distinct())
    to_date = None

    # Create a filter to only download archives for missing months.
    if aggregates.exists():
        earliest_aggregate_date = aggregates.earliest("full_date").full_date
        to_date = earliest_aggregate_date - relativedelta(months=1)
        to_date = to_date.replace(day=last_day(to_date))

    # Add unique usernames from the archived aggregates.
    usernames.update(
        storage.find_unique(
            storage.download_aggregates(
                prefix="aggregates_useraggregate",
                queryset_filter=queryset_filter,
                to_date=to_date,
            ),
            group_by=lambda record: (record["username"]),
        )
    )

    response = {"editor_count": len(usernames)}

    return JsonResponse(response)


def get_project_count(request):
    """
    request : dict
    Ajax request for project count (found in the Statistics table)
    """
    form_data = json.loads(request.GET.get("form_data", "{}"))
    collection_id = int(request.GET.get("collection", None))
    collection = Collection.objects.get(id=collection_id)

    queryset_filter = build_queryset_filters(form_data, {"collection": collection})
    aggregates = PageProjectAggregate.objects.filter(queryset_filter)
    projects = set(aggregates.values_list("project_name", flat=True).distinct())
    to_date = None

    # Create a filter to only download archives for missing months.
    if aggregates.exists():
        earliest_aggregate_date = aggregates.earliest("full_date").full_date
        to_date = earliest_aggregate_date - relativedelta(months=1)
        to_date = to_date.replace(day=last_day(to_date))

    # Add unique project names from the archived aggregates.
    projects.update(
        storage.find_unique(
            storage.download_aggregates(
                prefix="aggregates_pageprojectaggregate",
                queryset_filter=queryset_filter,
                to_date=to_date,
            ),
            group_by=lambda record: (record["project_name"]),
        )
    )

    response = {"project_count": len(projects)}

    return JsonResponse(response)


def get_links_count(request):
    """
    request : dict
    Ajax request for links count (found in the Statistics table)
    """
    form_data = json.loads(request.GET.get("form_data", "{}"))
    collection_id = int(request.GET.get("collection", None))
    collection = Collection.objects.get(id=collection_id)

    queryset_filter = build_queryset_filters(form_data, {"collection": collection})
    aggregates = LinkAggregate.objects.filter(queryset_filter)
    links_added_removed = aggregates.aggregate(
        links_added=Sum("total_links_added"),
        links_removed=Sum("total_links_removed"),
        links_diff=Sum("total_links_added") - Sum("total_links_removed"),
    )
    links_added = links_added_removed["links_added"] or 0
    links_removed = links_added_removed["links_removed"] or 0
    links_diff = links_added_removed["links_diff"] or 0

    # Create a filter to only download archives for missing months.
    to_date = None
    if aggregates.exists():
        earliest_aggregate_date = aggregates.earliest("full_date").full_date
        to_date = earliest_aggregate_date - relativedelta(months=1)
        to_date = to_date.replace(day=last_day(to_date))

    # Mix in archive totals with the database totals.
    totals = storage.calculate_totals(
        storage.download_aggregates(
            prefix="aggregates_linkaggregate",
            queryset_filter=queryset_filter,
            to_date=to_date,
        ),
    )
    if len(totals) > 0:
        links_added += totals[0]["total_links_added"]
        links_removed += totals[0]["total_links_removed"]
        links_diff += totals[0]["links_diff"]

    response = {
        "links_added": links_added,
        "links_removed": links_removed,
        "links_diff": links_diff,
    }

    return JsonResponse(response)


def get_top_pages(request):
    """
    request : dict
    Ajax request for the top pages table for a given collection
    """
    form_data = json.loads(request.GET.get("form_data", "{}"))
    collection_id = int(request.GET.get("collection", None))
    collection = Collection.objects.get(id=collection_id)

    queryset_filter = build_queryset_filters(form_data, {"collection": collection})
    aggregates = PageProjectAggregate.objects.filter(queryset_filter)

    # Calculate the top pages using just aggregates from the database to start.
    top_pages = {
        (top_page["project_name"], top_page["page_name"]): top_page
        for top_page in (
            aggregates.values("project_name", "page_name").annotate(
                links_diff=Sum("total_links_added") - Sum("total_links_removed"),
            )
        )
    }

    # Create a filter to only download archives for missing months.
    to_date = None
    if aggregates.exists():
        earliest_aggregate_date = aggregates.earliest("full_date").full_date
        to_date = earliest_aggregate_date - relativedelta(months=1)
        to_date = to_date.replace(day=last_day(to_date))

    # Calculate top pages from archive data and merge it with the DB totals.
    totals = storage.calculate_totals(
        storage.download_aggregates(
            prefix="aggregates_pageprojectaggregate",
            queryset_filter=queryset_filter,
            to_date=to_date,
        ),
        group_by=lambda record: (record["project_name"], record["page_name"]),
    )
    for total in totals:
        key = (total["project_name"], total["page_name"])

        if key in top_pages:
            top_pages[key]["links_diff"] += total["links_diff"]
        else:
            top_pages[key] = total.copy()

    # Sort the completed set of results and return the top 5.
    serialized_pages = json.dumps(
        list(
            sorted(top_pages.values(), key=lambda x: x["links_diff"], reverse=True)[:5]
        )
    )
    response = {"top_pages": serialized_pages}

    return JsonResponse(response)


def get_top_projects(request):
    """
    request : dict
    Ajax request for the top projects table for a given collection
    """
    form_data = json.loads(request.GET.get("form_data", "{}"))
    collection_id = int(request.GET.get("collection", None))
    collection = Collection.objects.get(id=collection_id)

    queryset_filter = build_queryset_filters(form_data, {"collection": collection})
    aggregates = PageProjectAggregate.objects.filter(queryset_filter)

    # Calculate the top projects using just aggregates from the database to start.
    top_projects = {
        top_project["project_name"]: top_project
        for top_project in (
            aggregates.values("project_name").annotate(
                links_diff=Sum("total_links_added") - Sum("total_links_removed"),
            )
        )
    }

    # Create a filter to only download archives for missing months.
    to_date = None
    if aggregates.exists():
        earliest_aggregate_date = aggregates.earliest("full_date").full_date
        to_date = earliest_aggregate_date - relativedelta(months=1)
        to_date = to_date.replace(day=last_day(to_date))

    # Calculate top pages from archive data and merge it with the DB totals.
    totals = storage.calculate_totals(
        storage.download_aggregates(
            prefix="aggregates_pageprojectaggregate",
            queryset_filter=queryset_filter,
            to_date=to_date,
        ),
        group_by=lambda record: record["project_name"],
    )
    for total in totals:
        key = total["project_name"]

        if key in top_projects:
            top_projects[key]["links_diff"] += total["links_diff"]
        else:
            top_projects[key] = total.copy()

    # Sort the completed set of results and return the top 5.
    serialized_projects = json.dumps(
        list(
            sorted(top_projects.values(), key=lambda x: x["links_diff"], reverse=True)[
                :5
            ]
        )
    )
    response = {"top_projects": serialized_projects}

    return JsonResponse(response)


def get_top_users(request):
    """
    request : dict
    Ajax request for the top users table for a given collection
    """
    form_data = json.loads(request.GET.get("form_data", "{}"))
    collection_id = int(request.GET.get("collection", None))
    collection = Collection.objects.get(id=collection_id)

    queryset_filter = build_queryset_filters(form_data, {"collection": collection})
    aggregates = UserAggregate.objects.filter(queryset_filter)

    # Calculate the top users using just aggregates from the database to start.
    top_users = {
        top_user["username"]: top_user
        for top_user in (
            aggregates.values("username").annotate(
                links_diff=Sum("total_links_added") - Sum("total_links_removed"),
            )
        )
    }

    # Create a filter to only download archives for missing months.
    to_date = None
    if aggregates.exists():
        earliest_aggregate_date = aggregates.earliest("full_date").full_date
        to_date = earliest_aggregate_date - relativedelta(months=1)
        to_date = to_date.replace(day=last_day(to_date))

    # Calculate top pages from archive data and merge it with the DB totals.
    totals = storage.calculate_totals(
        storage.download_aggregates(
            prefix="aggregates_useraggregate",
            queryset_filter=queryset_filter,
            to_date=to_date,
        ),
        group_by=lambda record: record["username"],
    )
    for total in totals:
        key = total["username"]

        if key in top_users:
            top_users[key]["links_diff"] += total["links_diff"]
        else:
            top_users[key] = total.copy()

    # Sort the completed set of results and return the top 5.
    serialized_users = json.dumps(
        list(
            sorted(top_users.values(), key=lambda x: x["links_diff"], reverse=True)[:5]
        )
    )
    response = {"top_users": serialized_users}

    return JsonResponse(response)


def get_latest_link_events(request):
    """
    request : dict
    Ajax request for the latest link events for a given collection
    """
    form_data = json.loads(request.GET.get("form_data", "{}"))
    collection_id = int(request.GET.get("collection", None))
    collection = Collection.objects.get(id=collection_id)

    linkevents = collection.get_linkevents()
    if form_data:
        linkevents_filter = build_queryset_filters(form_data, {"linkevents": ""})
    latest_link_events = (
        linkevents.select_related("username")
        .prefetch_related(
            "urlpattern",
            Prefetch(
                "urlpattern__collection",
                queryset=Collection.objects.select_related("organisation").filter(
                    id=collection.id
                ),
            ),
        )
        .filter(linkevents_filter)
        .values(
            "link", "domain", "page_title", "rev_id", "username__username", "change"
        )
        .annotate(date=Cast("timestamp", CharField()))
        .order_by("-date")[:10]
    )

    serialized_latest_link_events = json.dumps(list(latest_link_events))
    response = {"latest_link_events": serialized_latest_link_events}

    return JsonResponse(response)
