import hashlib
import logging
from datetime import date
from json import loads

from django.apps import apps
from django.core.cache import cache
from django.db import models
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.utils.functional import cached_property

logger = logging.getLogger("django")


class URLPatternManager(models.Manager):
    def cached(self):
        cached_patterns = cache.get("url_pattern_cache")
        if not cached_patterns:
            cached_patterns = (
                self.all()
                .select_related("collection")
                .annotate(
                    organisation=models.F("collection__organisation"),
                )
            )
            logger.info("set url_pattern_cache")
            cache.set("url_pattern_cache", cached_patterns, None)
        return cached_patterns

    def matches(self, link):
        # Queryset of all URL patterns matching this link
        tracked_urls = self.cached()
        excluded_ids = []
        for pattern in tracked_urls:
            if pattern.url not in link and pattern.get_proxied_url not in link:
                excluded_ids.add(pattern.id)
        url_patterns = tracked_urls.exclude(id__in=excluded_ids)
        return url_patterns


class URLPattern(models.Model):
    class Meta:
        app_label = "links"
        verbose_name = "URL pattern"
        verbose_name_plural = "URL patterns"

    objects = URLPatternManager()
    # This doesn't have to look like a 'real' URL so we'll use a CharField.
    url = models.CharField(max_length=150)

    collection = models.ForeignKey(
        "organisations.Collection",
        null=True,
        on_delete=models.SET_NULL,
        related_name="url",
    )

    @property
    # @TODO: This is both slow and broken
    def linkevent(self):
        return LinkEvent.objects.filter(url_patterns__id__contains=self.id)

    def __str__(self):
        return self.url

    @cached_property
    def get_proxied_url(self):
        # This isn't everything that happens, but it's good enough
        # for us to make a decision about whether we have a match.
        return self.url.replace(".", "-")


@receiver(post_save, sender=URLPattern)
def delete_url_pattern_cache(sender, instance, **kwargs):
    if cache.delete("url_pattern_cache"):
        logger.info("delete url_pattern_cache")


class LinkSearchTotal(models.Model):
    class Meta:
        app_label = "links"
        verbose_name = "LinkSearch total"
        verbose_name_plural = "LinkSearch totals"
        # We only want one record for each URL on any particular date
        constraints = [
            models.UniqueConstraint(fields=["url", "date"], name="unique_date_total")
        ]

    url = models.ForeignKey(URLPattern, null=True, on_delete=models.SET_NULL)

    date = models.DateField(default=date.today)
    total = models.PositiveIntegerField()


class LinkEvent(models.Model):
    """
    Stores data from the page-links-change EventStream

    https://stream.wikimedia.org/?doc#!/Streams/get_v2_stream_page_links_change
    """

    class Meta:
        app_label = "links"
        get_latest_by = "timestamp"
        indexes = [
            models.Index(
                fields=[
                    "hash_link_event_id",
                ]
            ),
            models.Index(
                fields=[
                    "timestamp",
                ]
            ),
        ]

    def url_patterns_default():
        """
        JSONField requires a callable wrapper for default value
        """
        return []

    url = models.ManyToManyField(URLPattern, related_name="linkevent")

    # Contains a snapshot of matching URL patterns at ingest time
    url_patterns = models.JSONField(default=url_patterns_default)

    # URLs should have a max length of 2083
    link = models.CharField(max_length=2083)
    timestamp = models.DateTimeField()
    domain = models.CharField(max_length=32, db_index=True)
    username = models.ForeignKey(
        "organisations.User", null=True, on_delete=models.SET_NULL
    )
    # rev_id has null=True because some tracked revisions don't have a
    # revision ID, like page moves.
    rev_id = models.PositiveIntegerField(null=True)
    # IPs have no user_id, so this can be blank too.
    user_id = models.PositiveIntegerField(null=True)
    page_title = models.CharField(max_length=255)
    page_namespace = models.IntegerField()
    event_id = models.CharField(max_length=36)
    user_is_bot = models.BooleanField(default=False)
    hash_link_event_id = models.CharField(max_length=256, blank=True)

    # Were links added or removed?
    REMOVED = 0
    ADDED = 1

    CHANGE_CHOICES = (
        (REMOVED, "Removed"),
        (ADDED, "Added"),
    )

    change = models.IntegerField(choices=CHANGE_CHOICES, db_index=True)

    # Flags whether this event was from a user on the user list for the
    # organisation tracking its URL.
    on_user_list = models.BooleanField(default=False)

    def url_add(self, instance):
        match = False
        for url_pattern in self.url_patterns:
            if url_pattern.get("id") == instance.id:
                match = True
                break
        url_pattern = URLPattern.objects.filter(id=instance.id).values().first()
        if url_pattern is None:
            # @TODO: raise a not found error here
            return
        self.url_patterns.append(url_pattern)
        self.save()

    @property
    def get_url_patterns(self):
        id_list = [url_pattern.get("id") for url_pattern in self.url_patterns]
        return URLPattern.objects.filter(pk__in=id_list)

    @property
    def get_collection(self):
        url_pattern = loads(self.url_patterns)[0]
        if url_pattern is None:
            return None
        id = url.get("collection")
        if id is None:
            return None
        Collection = apps.get_model("organisations", "collection")
        return Collection.objects.filter(id=id).first()

    @property
    def get_organisation(self):
        url_pattern = self.url_patterns[0]
        if url_pattern is None:
            return None
        id = url_pattern.get("organisation")
        if id is None:
            return None
        Organisation = apps.get_model("organisations", "organisation")
        return Organisation.objects.filter(id=id).first()

    def save(self, **kwargs):
        link_event_id = self.link + self.event_id
        hash = hashlib.sha256()
        hash.update(link_event_id.encode("utf-8"))
        self.hash_link_event_id = hash.hexdigest()
        super().save(**kwargs)
