"""
Django settings for extlinks project.
"""

import os

# Usually we'd define this relative to the settings file, but we're always
# starting from /app in Docker.
BASE_DIR = "/app"

SECRET_KEY = os.environ["SECRET_KEY"]

ALLOWED_HOSTS = ["127.0.0.1", "localhost", "0.0.0.0"]


# Application definition

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "extlinks.common",
    "extlinks.links",
    "extlinks.organisations",
    "extlinks.programs",
    "django_cron",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "extlinks.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [os.path.join(BASE_DIR, "extlinks", "templates")],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "extlinks.wsgi.application"


# Database
# https://docs.djangoproject.com/en/2.2/ref/settings/#databases

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.mysql",
        "NAME": os.environ["MYSQL_DATABASE"],
        "USER": "root",
        "PASSWORD": os.environ["MYSQL_ROOT_PASSWORD"],
        "HOST": "db",
        "PORT": "3306",
        "OPTIONS": {"charset": "utf8mb4"},
    }
}


# Password validation
# https://docs.djangoproject.com/en/2.2/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]


# Internationalization
# https://docs.djangoproject.com/en/2.2/topics/i18n/

LANGUAGE_CODE = "en-us"

TIME_ZONE = "UTC"

USE_I18N = True

USE_L10N = True

USE_TZ = True

# Cache

CACHES = {
    "default": {
        "BACKEND": "django.core.cache.backends.memcached.MemcachedCache",
        "LOCATION": "cache:11211",
    }
}

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/2.2/howto/static-files/

STATIC_URL = "/static/"
STATIC_ROOT = os.path.join(BASE_DIR, "static")

# Cron config
CRON_CLASSES = ["extlinks.organisations.cron.UserListsCron"]

# LOGGING CONFIGURATION
# ------------------------------------------------------------------------------
# We're replacing the default logging config to get better control of the
# mail_admins behavior.

ADMINS = [("Wikilink Developers", "librarycard-dev@lists.wikimedia.org")]
DJANGO_EMAIL_ADMINS_BACKEND = os.environ.get(
    "DJANGO_EMAIL_ADMINS_BACKEND", "django.core.mail.backends.console.EmailBackend"
)
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
                "format": "[%(server_time)s] %(message)s",
            }
        },
        "handlers": {
            "nodebug_console": {
                "level": "WARNING",
                "filters": ["require_debug_false"],
                "class": "logging.StreamHandler",
            },
            "debug_console": {
                "level": "INFO",
                "filters": ["require_debug_true"],
                "class": "logging.StreamHandler",
            },
            "django.server": {
                "level": "INFO",
                "class": "logging.StreamHandler",
                "formatter": "django.server",
            },
            "mail_admins": {
                "level": "ERROR",
                "filters": ["require_debug_false"],
                "class": "django.utils.log.AdminEmailHandler",
                "email_backend": DJANGO_EMAIL_ADMINS_BACKEND,
            },
        },
        "loggers": {
            "django": {
                "handlers": ["nodebug_console", "debug_console", "mail_admins"],
                "level": os.environ.get("DJANGO_LOG_LEVEL", "INFO"),
            },
            "django.server": {
                "handlers": ["django.server"],
                "level": os.environ.get("DJANGO_LOG_LEVEL", "INFO"),
                "propagate": False,
            },
            "Wikilink": {
                "handlers": ["nodebug_console", "debug_console", "mail_admins"],
                "level": os.environ.get("DJANGO_LOG_LEVEL", "INFO"),
            },
        },
    }
)
