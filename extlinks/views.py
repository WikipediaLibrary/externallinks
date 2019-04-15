from django.views.generic import TemplateView

from extlinks.programs.models import Program, Organisation


class Homepage(TemplateView):
    template_name = 'homepage.html'
