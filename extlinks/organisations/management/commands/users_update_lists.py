import os
import requests

from django.core.management import BaseCommand

from extlinks.organisations.models import Organisation, User


class Command(BaseCommand):
    help = "Updates organisation user lists who have a user_list_url"

    def handle(self, *args, **options):
        user_list_orgs = Organisation.objects.filter(
            username_list_url__isnull=False
        )

        for organisation in user_list_orgs:
            username_list_url = organisation.username_list_url

            # TODO: Hacky way to get TWL working, needs to be flexible.
            auth_key = os.environ['TWL_API_TOKEN']
            response = requests.get(username_list_url,
                                    headers={
                                        'Authorization': "Token {}".format(
                                            auth_key)
                                    })
            if response.status_code == 200:
                json_response = response.json()
            else:
                continue

            # If we got a valid response, clear the previous username list
            organisation.username_list.clear()

            for result in json_response:
                username = result['wp_username']

                user_object, _ = User.objects.get_or_create(
                    username=username
                )

                organisation.username_list.add(user_object)
