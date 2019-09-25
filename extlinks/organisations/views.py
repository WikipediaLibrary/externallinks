from datetime import datetime
import re

from django.db.models import Count
from django.views.generic import ListView, DetailView
from django.views.decorators.cache import cache_page
from django.utils.decorators import method_decorator

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

    def get_queryset(self, **kwargs):
        queryset = Organisation.objects.all().annotate(
            collection_count=Count('collection')
        )
        return queryset


@method_decorator(cache_page(60 * 5), name='dispatch')
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

        # Here we have a slightly more complex context dictionary setup, where
        # each collection has its own dictionary of data.
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

            # Replace all special characters that might confuse JS with an
            # underscore.
            collection_key = re.sub('[^0-9a-zA-Z]+', '_', collection.name)

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

            # LinkSearchTotal chart data
            dates, linksearch_data = get_linksearchtotal_data_by_time(
                this_collection_linksearchtotals)

            context['collections'][collection_key]['linksearch_dates'] = dates
            context['collections'][collection_key]['linksearch_data'] = linksearch_data

            # Statistics
            if linksearch_data:
                total_start = linksearch_data[0]
                total_current = linksearch_data[-1]
                total_diff = total_current - total_start
                start_date_object = datetime.strptime(dates[0], '%Y-%m-%d')
                start_date = start_date_object.strftime('%B %Y')
            # If we haven't collected any LinkSearchTotals yet, then set
            # these variables to None so we don't show them in the statistics
            # box
            else:
                total_start = None
                total_current = None
                total_diff = None
                start_date = None
            context['collections'][collection_key]['linksearch_total_start'] = total_start
            context['collections'][collection_key]['linksearch_total_current'] = total_current
            context['collections'][collection_key]['linksearch_total_diff'] = total_diff
            context['collections'][collection_key]['linksearch_start_date'] = start_date

            context['query_string'] = self.request.META['QUERY_STRING']

        return context
