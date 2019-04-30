from .base import *

DEBUG = True

ALLOWED_HOSTS += ['wikilink.wmflabs.org']

# Redirect HTTP to HTTPS
# SECURE_PROXY_SSL_HEADER is required because we're behind a proxy
SECURE_SSL_REDIRECT = True
SECURE_PROXY_SSL_HEADER = ('HTTP_X_FORWARDED_PROTO', 'https')
