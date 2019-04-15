from django.views.generic import ListView, DetailView

from .models import Program, Organisation


class ProgramListView(ListView):
    model = Program


class ProgramDetailView(DetailView):
    model = Program


class OrganisationListView(ListView):
    model = Organisation


class OrganisationDetailView(DetailView):
    model = Organisation
