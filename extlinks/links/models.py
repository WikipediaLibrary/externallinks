from datetime import date

from django.db import models
from django.utils.functional import cached_property


class URLPattern(models.Model):
    class Meta:
        app_label = "links"
        verbose_name = "URL pattern"
        verbose_name_plural = "URL patterns"

    # This doesn't have to look like a 'real' URL so we'll use a CharField.
    url = models.CharField(max_length=150)

    collection = models.ForeignKey('organisations.Collection', null=True,
                                   on_delete=models.SET_NULL,
                                   related_name='url')

    def __str__(self):
        return self.url

    @cached_property
    def get_proxied_url(self):
        # This isn't everything that happens, but it's good enough
        # for us to make a decision about whether we have a match.
        return self.url.replace(".", "-")


class LinkSearchTotal(models.Model):
    class Meta:
        app_label = "links"
        verbose_name = "LinkSearch total"
        verbose_name_plural = "LinkSearch totals"
        # We only want one record for each URL on any particular date
        constraints = [
            models.UniqueConstraint(fields=['url', 'date'],
                                    name='unique_date_total')
        ]

    url = models.ForeignKey(URLPattern, null=True,
                            on_delete=models.SET_NULL)

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

    url = models.ManyToManyField(URLPattern,
                                 related_name='linkevent')

    # URLs should have a max length of 2083
    link = models.CharField(max_length=2083)
    timestamp = models.DateTimeField()
    domain = models.CharField(max_length=32, db_index=True)
    username = models.ForeignKey('organisations.User', null=True,
                                 on_delete=models.SET_NULL)
    # rev_id has null=True because some tracked revisions don't have a
    # revision ID, like page moves.
    rev_id = models.PositiveIntegerField(null=True)
    # IPs have no user_id, so this can be blank too.
    user_id = models.PositiveIntegerField(null=True)
    page_title = models.CharField(max_length=255)
    page_namespace = models.IntegerField()
    event_id = models.CharField(max_length=36)
    user_is_bot = models.BooleanField(default=False)

    # Were links added or removed?
    REMOVED = 0
    ADDED = 1

    CHANGE_CHOICES = (
        (REMOVED, 'Removed'),
        (ADDED, 'Added'),
    )

    change = models.IntegerField(choices=CHANGE_CHOICES, db_index=True)

    # Flags whether this event was from a user on the user list for the
    # organisation tracking its URL.
    on_user_list = models.BooleanField(default=False)

    @property
    def get_organisation(self):
        url_patterns = self.url.all()
        return url_patterns[0].collection.organisation
