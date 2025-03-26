from datetime import date, timedelta
import json

from django.db.models import Sum, Count, Q
from django.http import JsonResponse
from django.views.generic import ListView, DetailView
from django.views.decorators.cache import cache_page
from django.utils.decorators import method_decorator

from extlinks.aggregates.models import (
    LinkAggregate,
    ProgramTopOrganisationsTotal,
    ProgramTopProjectsTotal,
    ProgramTopUsersTotal,
)
from extlinks.common.forms import FilterForm
from extlinks.common.helpers import build_queryset_filters
from .models import Program

from logging import getLogger

logger = getLogger("django")


class ProgramListView(ListView):
    model = Program

    def get_queryset(self, **kwargs):
        queryset = Program.objects.all().annotate(
            organisation_count=Count("organisation")
        )
        return queryset


@method_decorator(cache_page(60 * 60), name="dispatch")
class ProgramDetailView(DetailView):
    model = Program
    form_class = FilterForm

    def get_queryset(self, **kwargs):
        queryset = Program.objects.prefetch_related("organisation_set")
        return queryset

    def get_context_data(self, **kwargs):
        context = super(ProgramDetailView, self).get_context_data(**kwargs)
        this_program_organisations = self.object.organisation_set.all()
        context["organisations"] = this_program_organisations
        context["program_id"] = self.object.pk
        form = self.form_class(self.request.GET)
        context["form"] = form

        form_data = None
        # Filter queryset based on form, if used
        if form.is_valid():
            form_data = form.cleaned_data
            context["form_data"] = json.dumps(form_data, default=str)

        context = self._build_context_dictionary(
            this_program_organisations, context, form_data
        )

        context["query_string"] = self.request.META["QUERY_STRING"]

        return context

    def _build_context_dictionary(self, organisations, context, form_data):
        """
        This function builds the context dictionary that will populate the
        ProgramDetailView

        Parameters
        ----------
        organisations : List[Organisation]
            A list of organisations that belong to the program

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
                form_data, {"organisations": organisations}
            )
        else:
            queryset_filter = Q(organisation__in=organisations)

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
            The default is only filtering by the organisations that are part of
            the program

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

        if filtered_link_aggregate.exists():
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
    Ajax request for editor count (found in the Statistics table)
    """
    form_data = json.loads(request.GET.get("form_data", None))
    program = request.GET.get("program", None)

    queryset_filter = build_queryset_filters(form_data, {"program": program})

    editor_count = ProgramTopUsersTotal.objects.filter(queryset_filter).aggregate(
        editor_count=Count("username", distinct=True)
    )

    response = {"editor_count": editor_count["editor_count"]}

    return JsonResponse(response)


def get_project_count(request):
    """
    Ajax request for project count (found in the Statistics table)
    """
    form_data = json.loads(request.GET.get("form_data", None))
    program = request.GET.get("program", None)

    queryset_filter = build_queryset_filters(form_data, {"program": program})

    project_count = ProgramTopProjectsTotal.objects.filter(queryset_filter).aggregate(
        project_count=Count("project_name", distinct=True)
    )

    response = {"project_count": project_count["project_count"]}

    return JsonResponse(response)


def get_links_count(request):
    """
    Ajax request for link events counts (found in the Statistics table)
    """
    form_data = json.loads(request.GET.get("form_data", None))
    program = request.GET.get("program", None)

    queryset_filter = build_queryset_filters(form_data, {"program": program})

    links_added_removed = ProgramTopOrganisationsTotal.objects.filter(
        queryset_filter
    ).aggregate(
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


def get_top_organisations(request):
    """
    Ajax request to fill the top organisations table
    """
    form_data = json.loads(request.GET.get("form_data", None))
    program = request.GET.get("program", None)

    queryset_filter = build_queryset_filters(form_data, {"program": program})

    top_organisations = (
        ProgramTopOrganisationsTotal.objects.filter(queryset_filter)
        .values("organisation__pk", "organisation__name")
        .annotate(
            links_diff=Sum("total_links_added") - Sum("total_links_removed"),
        )
        .order_by("-links_diff")
    )[:5]

    serialized_orgs = json.dumps(list(top_organisations))

    response = {"top_organisations": serialized_orgs}

    return JsonResponse(response)


def get_top_projects(request):
    """
    Ajax request to fill the top organisations table
    """
    form_data = json.loads(request.GET.get("form_data", None))
    program = request.GET.get("program", None)

    queryset_filter = build_queryset_filters(form_data, {"program": program})

    top_projects = (
        ProgramTopProjectsTotal.objects.filter(queryset_filter)
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
    Ajax request to fill the top organisations table
    """
    form_data = json.loads(request.GET.get("form_data", None))
    program = request.GET.get("program", None)

    queryset_filter = build_queryset_filters(form_data, {"program": program})

    top_users = (
        ProgramTopUsersTotal.objects.filter(queryset_filter)
        .values("username")
        .annotate(
            links_diff=Sum("total_links_added") - Sum("total_links_removed"),
        )
        .order_by("-links_diff")
    )[:5]

    serialized_users = json.dumps(list(top_users))

    response = {"top_users": serialized_users}

    return JsonResponse(response)
