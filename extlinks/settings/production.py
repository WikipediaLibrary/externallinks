import sentry_sdk
from sentry_sdk.integrations.django import DjangoIntegration
from extlinks.settings.helpers import sentry_before_send

from .base import *
from .logging import *

DEBUG = False

ALLOWED_HOSTS = ["wikilink.wmflabs.org"]

# Redirect HTTP to HTTPS
# SECURE_PROXY_SSL_HEADER is required because we're behind a proxy
SECURE_SSL_REDIRECT = True
SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")

DEFAULT_FROM_EMAIL = "Wikilink Production <noreply@wikilink.wmflabs.org>"

sentry_sdk.init(
    dsn="https://cdabef0803434e3c97cb2c15f9a7da37@glitchtip-wikilink.wmflabs.org/1",
    integrations=[DjangoIntegration()],
    before_send=sentry_before_send,
)
