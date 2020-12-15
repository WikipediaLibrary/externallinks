from datetime import datetime
import re

from django.db.models import Count, Sum, Q
from django.views.generic import ListView, DetailView
from django.views.decorators.cache import cache_page
from django.utils.decorators import method_decorator

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
        queryset = Organisation.objects.all().annotate(
            collection_count=Count("collection")
        )
        return queryset


# @method_decorator(cache_page(60 * 60), name="dispatch")
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
            this_collection_linksearchtotals = LinkSearchTotal.objects.filter(
                url__collection=collection
            )

            form_data = None
            if form.is_valid():
                form_data = form.cleaned_data
                this_collection_linksearchtotals = filter_linksearchtotals(
                    this_collection_linksearchtotals, form_data
                )

            # Replace all special characters that might confuse JS with an
            # underscore.
            collection_key = re.sub("[^0-9a-zA-Z]+", "_", collection.name)

            context["collections"][collection_key] = {}
            context["collections"][collection_key]["object"] = collection
            context["collections"][collection_key]["urls"] = collection.url.all()

            context["collections"][
                collection_key
            ] = self._build_collection_context_dictionary(
                collection, context["collections"][collection_key], form_data
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

        context = self._fill_chart_context(collection, context, queryset_filter)
        context = self._fill_statistics_table_context(context, queryset_filter)
        context = self._fill_totals_tables(context, queryset_filter)
        context = self._fill_latest_linkevents(collection, context)

        return context

    def _fill_chart_context(self, collection, context, queryset_filter):
        """
        This function adds the chart information to the context
        dictionary to display in ProgramDetailView

        Parameters
        ----------
        collection : Collection
            A collection that the aggregate data will be filtered from

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
        try:
            earliest_link_date = (
                LinkAggregate.objects.filter(queryset_filter)
                .earliest("full_date")
                .full_date
            )
        except LinkAggregate.DoesNotExist:
            earliest_link_date = (
                LinkAggregate.objects.filter(collection=collection)
                .earliest("full_date")
                .full_date
            )

        links_aggregated_date = (
            LinkAggregate.objects.filter(
                queryset_filter & Q(full_date__gte=earliest_link_date),
            )
            .values("month", "year")
            .annotate(
                net_change=Sum("total_links_added") - Sum("total_links_removed"),
            )
        )

        eventstream_dates = []
        eventstream_net_change = []
        for link in links_aggregated_date:
            date_combined = f"{link['year']}-{link['month']}"
            eventstream_dates.append(date_combined)
            eventstream_net_change.append(link["net_change"])

        # These stats are for filling the program net change chart
        context["eventstream_dates"] = eventstream_dates
        context["eventstream_net_change"] = eventstream_net_change

        return context

    def _fill_statistics_table_context(self, context, queryset_filter):
        """
        This function adds the Statistics table information to the context
        dictionary to display in OrganisationDetailView

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
        links_added_removed = LinkAggregate.objects.filter(queryset_filter).aggregate(
            links_added=Sum("total_links_added"),
            links_removed=Sum("total_links_removed"),
            links_diff=Sum("total_links_added") - Sum("total_links_removed"),
        )
        context["total_added"] = links_added_removed["links_added"]
        context["total_removed"] = links_added_removed["links_removed"]
        context["total_diff"] = links_added_removed["links_diff"]

        editor_count = UserAggregate.objects.filter(queryset_filter).aggregate(
            editor_count=Count("username", distinct=True)
        )
        context["total_editors"] = editor_count["editor_count"]

        project_count = PageProjectAggregate.objects.filter(queryset_filter).aggregate(
            project_count=Count("project_name", distinct=True)
        )
        context["total_projects"] = project_count["project_count"]

        return context

    def _fill_totals_tables(self, context, queryset_filter):
        """
        This function adds the information for the Totals tables to the context
        dictionary to display in OrganisationDetailView

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
        context["top_projects"] = (
            PageProjectAggregate.objects.filter(queryset_filter)
            .values("project_name")
            .annotate(
                links_added=Sum("total_links_added"),
                links_removed=Sum("total_links_removed"),
                links_diff=Sum("total_links_added") - Sum("total_links_removed"),
            )
            .order_by("-links_diff", "-links_added", "-links_removed")
        )[:5]

        context["top_pages"] = (
            PageProjectAggregate.objects.filter(queryset_filter)
            .values("project_name", "page_name")
            .annotate(
                links_added=Sum("total_links_added"),
                links_removed=Sum("total_links_removed"),
                links_diff=Sum("total_links_added") - Sum("total_links_removed"),
            )
            .order_by("-links_diff", "-links_added", "-links_removed")
        )[:5]

        context["top_users"] = (
            UserAggregate.objects.filter(queryset_filter)
            .values("username")
            .annotate(
                links_added=Sum("total_links_added"),
                links_removed=Sum("total_links_removed"),
                links_diff=Sum("total_links_added") - Sum("total_links_removed"),
            )
            .order_by("-links_diff", "-links_added", "-links_removed")
        )[:5]

        return context

    def _fill_latest_linkevents(self, collection, context):
        """
        This function gets the latest linkevents

        Parameters
        ----------
        collection : Collection
            A collection that the aggregate data will be filtered from

        context : dict
            The context dictionary that the function will be adding information to

        Returns
        -------
        dict : The context dictionary with the relevant statistics
        """
        linkevents = collection.get_linkevents()
        context["latest_links"] = linkevents.prefetch_related(
            "username", "url", "url__collection", "url__collection__organisation"
        ).order_by("-timestamp")[:10]

        return context
