from .base import *
from .logging import *

DEBUG = True

SERVER_EMAIL = "Wikilink Local <wikilink.local@localhost.localdomain>"
DEFAULT_FROM_EMAIL = SERVER_EMAIL


# Django Debug Toolbar config
# ------------------------------------------------------------------------------

# Sometimes, developers do not want the debug toolbar on their local environments,
# so we can disable it by not passing a REQUIREMENTS_FILE variable when building
# the docker containers
if os.environ["REQUIREMENTS_FILE"] == "local.txt":
    INSTALLED_APPS += [
        "debug_toolbar",
        "django_extensions",
    ]

    MIDDLEWARE += [
        "debug_toolbar.middleware.DebugToolbarMiddleware",
    ]

    INTERNAL_IPS = ["127.0.0.1", "localhost", "0.0.0.0"]

    def show_toolbar(request):
        return True

    DEBUG_TOOLBAR_CONFIG = {
        "SHOW_TOOLBAR_CALLBACK": show_toolbar,
    }
    # Dummy Cache
    CACHES = {
        "default": {
            "BACKEND": "django.core.cache.backends.dummy.DummyCache",
        }
    }
