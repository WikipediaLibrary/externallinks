import sentry_sdk
from sentry_sdk.integrations.django import DjangoIntegration
from extlinks.settings.helpers import sentry_before_send

from .base import *
from .logging import *

DEBUG = False

ALLOWED_HOSTS = ["wikilink.wmflabs.org"]

# We can only run this in production, because we're connecting to the database
# replicas internally.
CRON_CLASSES += [
    "extlinks.common.cron.BackupCron",
    "extlinks.links.cron.TotalLinksCron",
]

# Redirect HTTP to HTTPS
# SECURE_PROXY_SSL_HEADER is required because we're behind a proxy
SECURE_SSL_REDIRECT = True
SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")

DEFAULT_FROM_EMAIL = "Wikilink Production <noreply@wikilink.wmflabs.org>"

sentry_sdk.init(
    dsn="https://a33ceca60d69401998f52637fc69a754@glitchtip-wikilink.wmflabs.org/1",
    integrations=[DjangoIntegration()],
    before_send=sentry_before_send,
)
