import logging
import os
import requests

from extlinks.common.management.commands import BaseCommand
from django.db import close_old_connections, transaction

from extlinks.organisations.models import Organisation, User

logger = logging.getLogger("django")

# (connect, read) in seconds.
REQUEST_TIMEOUT = (10, 60)


class Command(BaseCommand):
    help = "Updates organisation user lists who have a user_list_url"

    def _handle(self, *args, **options):
        # TODO: Hacky way to get TWL working, needs to be flexible.
        auth_key = os.environ["TWL_API_TOKEN"]

        user_list_orgs = Organisation.objects.filter(username_list_url__isnull=False)

        # Catch per organisation so one bad response does not send the whole
        # batch back through BaseCommand's retries.
        for organisation in user_list_orgs:
            try:
                self._sync_organisation(organisation, auth_key)
            except Exception:
                logger.exception(
                    f"users_update_lists: failed to update {organisation.name}"
                )

        close_old_connections()

    def _fetch_usernames(self, organisation, auth_key):
        response = requests.get(
            organisation.username_list_url,
            headers={"Authorization": "Token {}".format(auth_key)},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        json_response = response.json()

        if not isinstance(json_response, list):
            raise ValueError(
                f"Expected a list, got {type(json_response).__name__}"
            )
        return {result["wp_username"] for result in json_response}

    def _sync_organisation(self, organisation, auth_key):
        incoming = self._fetch_usernames(organisation, auth_key)
        current = set(organisation.username_list.values_list("username", flat=True))

        # An empty list is far more likely an upstream fault than every
        # user losing access, so do not wipe the list.
        if not incoming and current:
            raise ValueError(
                f"Refusing to empty a list of {len(current)} users"
            )

        to_add = incoming - current
        to_remove = current - incoming

        # Diff rather than clear and re-add, so a no-op sync inserts no rows
        # into the join table (T434529).
        with transaction.atomic():
            if to_add:
                existing = set(
                    User.objects.filter(username__in=to_add).values_list(
                        "username", flat=True
                    )
                )
                User.objects.bulk_create(
                    [User(username=username) for username in to_add - existing]
                )
                organisation.username_list.add(
                    *User.objects.filter(username__in=to_add)
                )
            if to_remove:
                organisation.username_list.remove(
                    *User.objects.filter(username__in=to_remove)
                )
            # Useful for health check
            organisation.save()

        logger.info(
            f"users_update_lists: {organisation.name}: "
            f"{len(to_add)} added, {len(to_remove)} removed"
        )
