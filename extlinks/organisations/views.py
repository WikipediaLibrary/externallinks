from datetime import datetime, date, timedelta
import json
import re

from django.db.models import Count, Sum, Q, Prefetch, CharField
from django.db.models.functions import Cast
from django.http import JsonResponse
from django.views.generic import ListView, DetailView

from extlinks.aggregates.models import (
    LinkAggregate,
    PageProjectAggregate,
    UserAggregate,
)
from extlinks.common.forms import FilterForm
from extlinks.common.helpers import (
    get_linksearchtotal_data_by_time,
    filter_linksearchtotals,
    build_queryset_filters,
)
from extlinks.links.models import LinkSearchTotal
from .models import Organisation, Collection


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
            ).filter(url__collections__name__contains=collection)

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
            context["collections"][collection_key]["urls"] = collection.get_url_patterns()

            context["collections"][collection_key] = (
                self._build_collection_context_dictionary(
                    collection, context["collections"][collection_key], form_data
                )
            )

            # LinkSearchTotal chart data
            dates, linksearch_data = get_linksearchtotal_data_by_time(
                this_collection_linksearchtotals
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
        current_date = date.today()
        filtered_link_aggregate = LinkAggregate.objects.filter(queryset_filter)

        if filtered_link_aggregate:
            earliest_link_date = filtered_link_aggregate.earliest("full_date").full_date
        else:
            # No link information from that collection, so setting earliest_link_date
            # to the first of the current month
            earliest_link_date = current_date.replace(day=1)

        links_aggregated_date = (
            LinkAggregate.objects.filter(queryset_filter)
            .values("month", "year")
            .annotate(
                net_change=Sum("total_links_added") - Sum("total_links_removed"),
            )
            .order_by("year", "month")
        )

        # Filling an array of dates that should be in the chart
        while current_date >= earliest_link_date:
            dates.append(current_date.strftime("%Y-%m"))
            # Figure out what the last month is regardless of today's date
            current_date = current_date.replace(day=1) - timedelta(days=1)

        dates = dates[::-1]

        for link in links_aggregated_date:
            if link["month"] < 10:
                date_combined = f"{link['year']}-0{link['month']}"
            else:
                date_combined = f"{link['year']}-{link['month']}"

            existing_link_aggregates[date_combined] = link["net_change"]

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
    editor_count = UserAggregate.objects.filter(queryset_filter).aggregate(
        editor_count=Count("username", distinct=True)
    )
    response = {"editor_count": editor_count["editor_count"]}

    return JsonResponse(response)


def get_project_count(request):
    form_data = json.loads(request.GET.get("form_data", "{}"))
    collection_id = int(request.GET.get("collection", None))
    collection = Collection.objects.get(id=collection_id)

    queryset_filter = build_queryset_filters(form_data, {"collection": collection})
    project_count = PageProjectAggregate.objects.filter(queryset_filter).aggregate(
        project_count=Count("project_name", distinct=True)
    )
    response = {"project_count": project_count["project_count"]}

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
    links_added_removed = LinkAggregate.objects.filter(queryset_filter).aggregate(
        links_added=Sum("total_links_added"),
        links_removed=Sum("total_links_removed"),
        links_diff=Sum("total_links_added") - Sum("total_links_removed"),
    )
    response = {
        "links_added": links_added_removed["links_added"],
        "links_removed": links_added_removed["links_removed"],
        "links_diff": links_added_removed["links_diff"],
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
    top_pages = (
        PageProjectAggregate.objects.filter(queryset_filter)
        .values("project_name", "page_name")
        .annotate(
            links_diff=Sum("total_links_added") - Sum("total_links_removed"),
        )
        .order_by("-links_diff")
    )[:5]

    serialized_pages = json.dumps(list(top_pages))
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
    top_projects = (
        PageProjectAggregate.objects.filter(queryset_filter)
        .values("project_name")
        .annotate(
            links_diff=Sum("total_links_added") - Sum("total_links_removed"),
        )
        .order_by("-links_diff")
    )[:5]

    serialized_projects = json.dumps(list(top_projects))
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
    top_users = (
        UserAggregate.objects.filter(queryset_filter)
        .values("username")
        .annotate(
            links_diff=Sum("total_links_added") - Sum("total_links_removed"),
        )
        .order_by("-links_diff")
    )[:5]

    serialized_users = json.dumps(list(top_users))
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
