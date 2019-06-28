import requests

from django.core.management import BaseCommand

from extlinks.organisations.models import Organisation, User


class Command(BaseCommand):
    help = "Updates organisation user lists who have a user_list_url"

    def handle(self, *args, **options):
        user_list_orgs = Organisation.objects.filter(
            username_list__isnull=False
        )

        for organisation in user_list_orgs:
            username_list_url = organisation.username_list_url

            response = requests.get(username_list_url)
            if response.status_code == 200:
                json_response = response.json()
            else:
                continue

            # If we got a valid response, clear the previous username list
            organisation.username_list.clear()

            for result in json_response:
                username = result['username']

                user_object = User.objects.get_or_create(
                    username=username
                )

                organisation.username_list.add(user_object)
