from django.views.decorators.csrf import requires_csrf_token
from django.views.generic import TemplateView
from django.views.defaults import server_error


class Homepage(TemplateView):
    template_name = "homepage.html"


class Documentation(TemplateView):
    template_name = "documentation.html"

@requires_csrf_token
def custom_server_error(request):
    return server_error(request, "500/500.html")
