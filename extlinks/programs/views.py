from django.views.generic import ListView, DetailView
from django.views.decorators.cache import cache_page
from django.utils.decorators import method_decorator
from django.db.models import Sum, Count

from extlinks.aggregates.models import (
    LinkAggregate,
    PageProjectAggregate,
    UserAggregate,
)
from extlinks.common.forms import FilterForm
from extlinks.organisations.models import Organisation
from extlinks.common.helpers import (
    get_linkevent_context,
    top_organisations,
    filter_queryset,
)
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
        form = self.form_class(self.request.GET)
        context["form"] = form

        # Filter queryset based on form, if used
        if form.is_valid():
            form_data = form.cleaned_data
            # TODO: Add filters

        context = self._build_context_dictionary(this_program_organisations, context)

        context["query_string"] = self.request.META["QUERY_STRING"]

        return context

    def _build_context_dictionary(self, organisations, context):
        """
        This function builds the context dictionary that will populate the
        ProgramDetailView

        Parameters
        ----------
        organisations : List[Organisation]
            A list of organisations that belong to the program

        context : dict
            The context dictionary that the function will be adding information to

        Returns
        -------
        dict : The context dictionary with the relevant statistics
        """

        context = self._fill_chart_context(organisations, context)
        context = self._fill_statistics_table_context(organisations, context)
        context = self._fill_totals_tables(organisations, context)

        return context

    def _fill_chart_context(self, organisations, context):
        """
        This function adds the chart information to the context
        dictionary to display in ProgramDetailView

        Parameters
        ----------
        organisations : List[Organisation]
            A list of organisations that belong to the program

        context : dict
            The context dictionary that the function will be adding information to

        Returns
        -------
        dict : The context dictionary with the relevant statistics
        """
        earliest_link_date = (
            LinkAggregate.objects.filter(organisation__in=organisations)
            .earliest("full_date")
            .full_date
        )

        links_aggregated_date = (
            LinkAggregate.objects.filter(
                organisation__in=organisations,
                full_date__gte=earliest_link_date,
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

    def _fill_statistics_table_context(self, organisations, context):
        """
        This function adds the Statistics table information to the context
        dictionary to display in ProgramDetailView

        Parameters
        ----------
        organisations : List[Organisation]
            A list of organisations that belong to the program

        context : dict
            The context dictionary that the function will be adding information to

        Returns
        -------
        dict : The context dictionary with the relevant statistics
        """
        links_added_removed = LinkAggregate.objects.filter(
            organisation__in=organisations
        ).aggregate(
            links_added=Sum("total_links_added"),
            links_removed=Sum("total_links_removed"),
            links_diff=Sum("total_links_added") - Sum("total_links_removed"),
        )
        context["total_added"] = links_added_removed["links_added"]
        context["total_removed"] = links_added_removed["links_removed"]
        context["total_diff"] = links_added_removed["links_diff"]

        editor_count = UserAggregate.objects.filter(
            organisation__in=organisations
        ).aggregate(editor_count=Count("username", distinct=True))
        context["total_editors"] = editor_count["editor_count"]

        project_count = PageProjectAggregate.objects.filter(
            organisation__in=organisations
        ).aggregate(project_count=Count("project_name", distinct=True))
        context["total_projects"] = project_count["project_count"]

        return context

    def _fill_totals_tables(self, organisations, context):
        """
        This function adds the information for the Totals tables to the context
        dictionary to display in ProgramDetailView

        Parameters
        ----------
        organisations : List[Organisation]
            A list of organisations that belong to the program

        context : dict
            The context dictionary that the function will be adding information to

        Returns
        -------
        dict : The context dictionary with the relevant statistics
        """
        context["top_organisations"] = (
            LinkAggregate.objects.filter(organisation__in=organisations)
            .values("organisation__pk", "organisation__name")
            .annotate(
                links_added=Sum("total_links_added"),
                links_removed=Sum("total_links_removed"),
                links_diff=Sum("total_links_added") - Sum("total_links_removed"),
            )
            .order_by("-links_diff", "-links_added", "-links_removed")
        )[:5]

        context["top_projects"] = (
            PageProjectAggregate.objects.filter(organisation__in=organisations)
            .values("project_name")
            .annotate(
                links_added=Sum("total_links_added"),
                links_removed=Sum("total_links_removed"),
                links_diff=Sum("total_links_added") - Sum("total_links_removed"),
            )
            .order_by("-links_diff", "-links_added", "-links_removed")
        )[:5]

        context["top_users"] = (
            UserAggregate.objects.filter(organisation__in=organisations)
            .values("username")
            .annotate(
                links_added=Sum("total_links_added"),
                links_removed=Sum("total_links_removed"),
                links_diff=Sum("total_links_added") - Sum("total_links_removed"),
            )
            .order_by("-links_diff", "-links_added", "-links_removed")
        )[:5]

        return context
