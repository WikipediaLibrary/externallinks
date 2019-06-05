from django.views.generic import ListView, DetailView

from extlinks.common.forms import FilterForm
from extlinks.common.helpers import (filter_queryset,
                                     get_linkevent_context,
                                     annotate_top,
                                     get_linksearchtotal_data_by_time,
                                     filter_linksearchtotals)
from extlinks.links.models import LinkEvent, LinkSearchTotal, URLPattern
from .models import Organisation, Collection


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
            this_collection_linksearchtotals = LinkSearchTotal.objects.filter(
                url__collection=collection
            )

            if form.is_valid():
                form_data = form.cleaned_data
                this_collection_linkevents = filter_queryset(
                    this_collection_linkevents,
                    form_data)
                this_collection_linksearchtotals = filter_linksearchtotals(
                    this_collection_linksearchtotals,
                    form_data
                )

            collection_key = collection.name.replace(" ", "_")

            context['collections'][collection_key] = {}
            context['collections'][collection_key]['object'] = collection
            context['collections'][collection_key]['urls'] = URLPattern.objects.filter(
                collection=collection
            )
            context['collections'][collection_key] = get_linkevent_context(
                context['collections'][collection_key],
                this_collection_linkevents)

            context['collections'][collection_key]['top_pages'] = annotate_top(
                this_collection_linkevents,
                '-links_added',
                ['page_title', 'domain'],
                num_results=5,
            )

            # totalLinks chart data
            dates, linksearch_data = get_linksearchtotal_data_by_time(
                this_collection_linksearchtotals)

            context['collections'][collection_key]['linksearch_dates'] = dates
            context['collections'][collection_key]['linksearch_data'] = linksearch_data

            # Statistics
            if linksearch_data:
                total_start = linksearch_data[0]
                total_current = linksearch_data[-1]
                total_diff = total_current - total_start
            else:
                total_start = None
                total_current = None
                total_diff = None
            context['collections'][collection_key]['linksearch_total_start'] = total_start
            context['collections'][collection_key]['linksearch_total_current'] = total_current
            context['collections'][collection_key]['linksearch_total_diff'] = total_diff

            context['query_string'] = self.request.META['QUERY_STRING']

        return context
