from django.db.models import Count, Q
from django.views.generic import ListView, DetailView

from extlinks.links.models import LinkEvent, LinkSearchTotal, URLPattern
from .forms import FilterForm
from .helpers import get_linkevent_context, get_linksearchtotal_data_by_time
from .models import Program, Organisation, Collection

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

        context = get_linkevent_context(context,
                                        this_program_linkevents,
                                        form)

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

        return context


class OrganisationListView(ListView):
    model = Organisation


class OrganisationDetailView(DetailView):
    model = Organisation
    form_class = FilterForm

    # This is almost, but not exactly, the same as the program view.
    # As such, most context gathering is split out to a helper.
    def get_context_data(self, **kwargs):
        context = super(OrganisationDetailView, self).get_context_data(**kwargs)
        form = self.form_class(self.request.GET)
        context['form'] = form

        organisation_collections = Collection.objects.filter(
            organisation=self.object
        )

        context['collections'] = {}
        for collection in organisation_collections:
            this_collection_linkevents = LinkEvent.objects.filter(
                url__collection=collection
            )
            collection_key = collection.name.replace(" ", "_")

            context['collections'][collection_key] = {}
            context['collections'][collection_key]['object'] = collection
            context['collections'][collection_key]['urls'] = URLPattern.objects.filter(
                collection=collection
            )
            context['collections'][collection_key] = get_linkevent_context(
                context['collections'][collection_key],
                this_collection_linkevents,
                form)

            context['collections'][collection_key]['top_3_pages'] = this_collection_linkevents.values(
                'page_title', 'domain').annotate(
                links_added=Count('change',
                                  filter=Q(change=LinkEvent.ADDED)),
                links_removed=Count('change',
                                    filter=Q(change=LinkEvent.REMOVED))).order_by(
                '-links_added'
            )[:5]

            # totalLinks chart data
            this_collection_linksearchtotals = LinkSearchTotal.objects.filter(
                url__collection=collection
            )

            dates, linksearch_data = get_linksearchtotal_data_by_time(
                this_collection_linksearchtotals)

            context['collections'][collection_key]['linksearch_dates'] = dates
            context['collections'][collection_key]['linksearch_data'] = linksearch_data

        return context
