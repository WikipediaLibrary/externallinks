from django import template
from django.template.defaultfilters import stringfilter

register = template.Library()


@register.filter
@stringfilter
def replace_underscores(string):
    return string.replace('_', ' ')
