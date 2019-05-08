import csv

from django.http import HttpResponse
from django.views.generic import ListView, DetailView, View

from extlinks.links.models import LinkEvent, LinkSearchTotal, URLPattern
from .forms import FilterForm
from .helpers import (get_linkevent_context,
                      get_linksearchtotal_data_by_time,
                      annotate_top,
                      top_organisations,
                      filter_queryset)
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

        if form.is_valid():
            form_data = form.cleaned_data
            this_program_linkevents = filter_queryset(this_program_linkevents,
                                                      form_data)

        context = get_linkevent_context(context,
                                        this_program_linkevents)

        context['top_organisations'] = top_organisations(
            this_program_organisations,
            this_program_linkevents,
            num_results=5)

        context['query_string'] = self.request.META['QUERY_STRING']

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
            if form.is_valid():
                form_data = form.cleaned_data
                this_collection_linkevents = filter_queryset(
                    this_collection_linkevents,
                    form_data)
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
            this_collection_linksearchtotals = LinkSearchTotal.objects.filter(
                url__collection=collection
            )

            dates, linksearch_data = get_linksearchtotal_data_by_time(
                this_collection_linksearchtotals)

            context['collections'][collection_key]['linksearch_dates'] = dates
            context['collections'][collection_key]['linksearch_data'] = linksearch_data

            context['query_string'] = self.request.META['QUERY_STRING']

        return context


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
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename="data.csv"'

        self._write_data(response)

        return response

    def _write_data(self, response):
        raise NotImplementedError


class CSVOrgTotals(_CSVDownloadView):
    def _write_data(self, response):
        program_pk = self.kwargs['pk']
        this_program_orgs = Organisation.objects.filter(
            program__pk=program_pk)
        this_program_linkevents = LinkEvent.objects.filter(
            url__collection__organisation__program__pk=program_pk
        )
        this_program_linkevents = filter_queryset(this_program_linkevents,
                                                  self.request.GET)
        top_orgs = top_organisations(
            this_program_orgs,
            this_program_linkevents)

        writer = csv.writer(response)

        writer.writerow(['Organisation', 'Links added', 'Links removed'])

        for org in top_orgs:
            writer.writerow([org.name, org.links_added, org.links_removed])


class CSVPageTotals(_CSVDownloadView):
    def _write_data(self, response):
        org_pk = self.kwargs['pk']
        linkevents = LinkEvent.objects.filter(
            url__collection__organisation__pk=org_pk)

        linkevents = filter_queryset(linkevents,
                                     self.request.GET)

        top_pages = annotate_top(
            linkevents,
            '-links_added',
            ['page_title', 'domain'],
            )
        writer = csv.writer(response)

        writer.writerow(['Page title', 'Project', 'Links added', 'Links removed'])

        for page in top_pages:
            writer.writerow([page['page_title'], page['domain'],
                             page['links_added'], page['links_removed']])


class CSVProjectTotals(_CSVDownloadView):
    def _write_data(self, response):
        pk = self.kwargs['pk']
        # If we came from an organisation page:
        if '/organisation' in self.request.build_absolute_uri():
            linkevents = LinkEvent.objects.filter(
                url__collection__organisation__pk=pk)
        else:
            linkevents = LinkEvent.objects.filter(
                url__collection__organisation__program__pk=pk)

        linkevents = filter_queryset(linkevents,
                                     self.request.GET)

        top_projects = annotate_top(linkevents,
                                    '-links_added',
                                    ['domain'])
        writer = csv.writer(response)

        writer.writerow(['Project', 'Links added', 'Links removed'])

        for project in top_projects:
            writer.writerow([project['domain'], project['links_added'],
                             project['links_removed']])


class CSVUserTotals(_CSVDownloadView):
    def _write_data(self, response):
        pk = self.kwargs['pk']
        # If we came from an organisation page:
        if '/organisation' in self.request.build_absolute_uri():
            linkevents = LinkEvent.objects.filter(
                url__collection__organisation__pk=pk)
        else:
            linkevents = LinkEvent.objects.filter(
                url__collection__organisation__program__pk=pk)

        linkevents = filter_queryset(linkevents,
                                     self.request.GET)

        top_users = annotate_top(linkevents,
                                    '-links_added',
                                    ['username'])
        writer = csv.writer(response)

        writer.writerow(['Username', 'Links added', 'Links removed'])

        for user in top_users:
            writer.writerow([user['username'], user['links_added'],
                             user['links_removed']])


class CSVAllLinkEvents(_CSVDownloadView):
    def _write_data(self, response):
        pk = self.kwargs['pk']
        # If we came from an organisation page:
        if '/organisation' in self.request.build_absolute_uri():
            linkevents = LinkEvent.objects.filter(
                url__collection__organisation__pk=pk)
        else:
            linkevents = LinkEvent.objects.filter(
                url__collection__organisation__program__pk=pk)

        linkevents = filter_queryset(linkevents,
                                     self.request.GET)

        writer = csv.writer(response)

        writer.writerow(['Link', 'User', 'Page title',
                         'Project', 'Timestamp', 'Revision ID',
                         'Change'])

        for link in linkevents.order_by('-timestamp'):
            writer.writerow([link.link, link.username, link.page_title,
                             link.domain, link.timestamp, link.rev_id,
                             link.change])
