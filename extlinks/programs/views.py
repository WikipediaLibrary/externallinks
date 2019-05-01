from django.db.models import Count, Q
from django.views.generic import ListView, DetailView

from extlinks.links.models import LinkEvent

from .forms import FilterForm
from .helpers import get_change_data_by_time
from .models import Program, Organisation

from logging import getLogger

logger = getLogger('django')


class ProgramListView(ListView):
    model = Program


class ProgramDetailView(DetailView):
    model = Program
    form_class = FilterForm

    def get_context_data(self, **kwargs):
        context = super(ProgramDetailView, self).get_context_data(**kwargs)
        this_program_organisations = Organisation.objects.filter(
            program=self.object)
        context['organisations'] = this_program_organisations
        form = self.form_class(self.request.GET)
        context['form'] = form

        this_program_linkevents = LinkEvent.objects.filter(
            url__collection__organisation__program=self.object
        )

        if form.is_valid():
            form_data = form.cleaned_data
            start_date = form_data['start_date']

            if start_date:
                this_program_linkevents = this_program_linkevents.filter(
                    timestamp__gte=start_date
                )
            end_date = form_data['end_date']

            if end_date:
                this_program_linkevents = this_program_linkevents.filter(
                    timestamp__lte=end_date
                )

            limit_to_user_list = form_data['limit_to_user_list']
            if limit_to_user_list:
                this_program_linkevents = this_program_linkevents.filter(
                    on_user_list=True
                )

        context['top_3_organisations'] = this_program_organisations.annotate(
            links_added=Count('collection__url__linkevent',
                              filter=Q(
                                  collection__url__linkevent__in=this_program_linkevents,
                                  collection__url__linkevent__change=LinkEvent.ADDED)),
            links_removed=Count('collection__url__linkevent',
                                filter=Q(
                                    collection__url__linkevent__in=this_program_linkevents,
                                    collection__url__linkevent__change=LinkEvent.REMOVED)),
        ).order_by('-links_added')[:5]

        context['top_3_projects'] = this_program_linkevents.values(
            'domain').annotate(
                links_added=Count('change',
                    filter=Q(change=LinkEvent.ADDED)),
                links_removed=Count('change',
                                  filter=Q(change=LinkEvent.REMOVED))).order_by(
            '-links_added'
        )[:5]

        context['top_3_users'] = this_program_linkevents.values(
            'username').annotate(
                links_added=Count('change',
                    filter=Q(change=LinkEvent.ADDED)),
                links_removed=Count('change',
                                  filter=Q(change=LinkEvent.REMOVED))).order_by(
            '-links_added'
        )[:5]

        context['latest_linkevents'] = this_program_linkevents.order_by(
            '-timestamp')[:10]

        # EventStream chart data
        dates, added_data_series, removed_data_series = get_change_data_by_time(
            this_program_linkevents)

        context['eventstream_dates'] = dates
        context['eventstream_added_data'] = added_data_series
        context['eventstream_removed_data'] = removed_data_series

        # Stat block
        context['total_added'] = sum(added_data_series)
        context['total_removed'] = sum(removed_data_series)
        context['total_editors'] = this_program_linkevents.values_list(
            'username').distinct().count()

        return context


class OrganisationListView(ListView):
    model = Organisation


class OrganisationDetailView(DetailView):
    model = Organisation
