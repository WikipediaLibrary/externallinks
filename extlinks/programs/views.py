from django.views.generic import ListView, DetailView
from django.views.decorators.cache import cache_page
from django.utils.decorators import method_decorator

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


@method_decorator(cache_page(60 * 60), name="dispatch")
class ProgramDetailView(DetailView):
    model = Program
    form_class = FilterForm

    def get_context_data(self, **kwargs):
        context = super(ProgramDetailView, self).get_context_data(**kwargs)
        this_program_organisations = Organisation.objects.filter(program=self.object)
        context["organisations"] = this_program_organisations
        form = self.form_class(self.request.GET)
        context["form"] = form

        this_program_linkevents = self.get_object().get_linkevents()

        # Filter queryset based on form, if used
        if form.is_valid():
            form_data = form.cleaned_data
            this_program_linkevents = filter_queryset(
                this_program_linkevents, form_data
            )

        context = get_linkevent_context(context, this_program_linkevents)

        context["top_organisations"] = top_organisations(
            this_program_organisations, this_program_linkevents, num_results=5
        )

        context["query_string"] = self.request.META["QUERY_STRING"]

        return context
