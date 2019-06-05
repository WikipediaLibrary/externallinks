from django.views.generic import TemplateView


class Homepage(TemplateView):
    template_name = 'homepage.html'


class Documentation(TemplateView):
    template_name = 'documentation.html'
