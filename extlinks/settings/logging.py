import os
import logging.config

# LOGGING CONFIGURATION
# ------------------------------------------------------------------------------
# We're replacing the default logging config to get better control of the
# mail_admins behavior.
# Logging is in another file since Django 3.1 because of https://code.djangoproject.com/ticket/32016

LOGGING_CONFIG = None

logging.config.dictConfig(
    {
        "version": 1,
        "disable_existing_loggers": False,
        "filters": {
            "require_debug_false": {"()": "django.utils.log.RequireDebugFalse"},
            "require_debug_true": {"()": "django.utils.log.RequireDebugTrue"},
        },
        "formatters": {
            "django.server": {
                "()": "django.utils.log.ServerFormatter",
                "format": "[%(levelname)s] %(message)s",
            }
        },
        "handlers": {
            "nodebug_console": {
                "level": "INFO",
                "filters": ["require_debug_false"],
                "class": "logging.StreamHandler",
                "formatter": "django.server",
            },
            "debug_console": {
                "level": "INFO",
                "filters": ["require_debug_true"],
                "class": "logging.StreamHandler",
                "formatter": "django.server",
            },
            "django.server": {
                "level": "INFO",
                "class": "logging.StreamHandler",
                "formatter": "django.server",
            },
        },
        "loggers": {
            "django": {
                "handlers": ["nodebug_console", "debug_console"],
                "level": os.environ.get("DJANGO_LOG_LEVEL", "INFO"),
            },
            "django.server": {
                "handlers": ["django.server"],
                "level": os.environ.get("DJANGO_LOG_LEVEL", "INFO"),
                "propagate": False,
            },
            "Wikilink": {
                "handlers": ["nodebug_console", "debug_console"],
                "level": os.environ.get("DJANGO_LOG_LEVEL", "INFO"),
            },
        },
    }
)
