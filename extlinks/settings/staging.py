from .base import *
from .logging import *

DEBUG = False

ALLOWED_HOSTS = ["wikilink-staging.wmflabs.org"]

# We can only run this in production, because we're connecting to the database
# replicas internally.
CRON_CLASSES += ["extlinks.links.cron.TotalLinksCron"]

# Redirect HTTP to HTTPS
# SECURE_PROXY_SSL_HEADER is required because we're behind a proxy
SECURE_SSL_REDIRECT = True
SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")

SERVER_EMAIL = "Wikilink <noreply@wikilink-staging.wmflabs.org>"
DEFAULT_FROM_EMAIL = SERVER_EMAIL
