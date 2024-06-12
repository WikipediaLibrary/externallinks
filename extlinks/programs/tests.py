from datetime import datetime, timezone
import json

from django.test import TestCase, RequestFactory
from django.urls import reverse
from django.core.management import call_command

from extlinks.aggregates.factories import (
    LinkAggregateFactory,
    PageProjectAggregateFactory,
    UserAggregateFactory,
)
from extlinks.common.views import (
    CSVOrgTotals,
    CSVProjectTotals,
    CSVUserTotals,
    CSVAllLinkEvents,
)
from extlinks.links.factories import LinkEventFactory, URLPatternFactory
from extlinks.links.models import LinkEvent
from extlinks.organisations.factories import (
    UserFactory,
    OrganisationFactory,
    CollectionFactory,
)
from .factories import ProgramFactory
from .views import (
    ProgramListView,
    ProgramDetailView,
)


class ProgramListTest(TestCase):
    def setUp(self):
        self.program_one = ProgramFactory()
        self.program_two = ProgramFactory()

    def test_program_list_view(self):
        """
        Test that we can simply load the program list page successfully
        """
        factory = RequestFactory()

        request = factory.get(reverse("homepage"))
        response = ProgramListView.as_view()(request)

        self.assertEqual(response.status_code, 200)

    def test_program_list_contents(self):
        """
        Test that the program list page contains the programs we expect.
        """

        factory = RequestFactory()

        request = factory.get(reverse("homepage"))
        response = ProgramListView.as_view()(request)

        self.assertContains(response, self.program_one.name)
        self.assertContains(response, self.program_two.name)


class ProgramDetailTest(TestCase):
    def setUp(self):
        self.program1 = ProgramFactory()
        self.organisation1 = OrganisationFactory(name="Org 1", program=(self.program1,))
        self.organisation2 = OrganisationFactory(name="Org 2", program=(self.program1,))
        self.url1 = reverse("programs:detail", kwargs={"pk": self.program1.pk})

        self.collection1 = CollectionFactory(organisation=self.organisation1)
        self.collection2 = CollectionFactory(organisation=self.organisation2)
        self.collection3 = CollectionFactory(organisation=self.organisation2)

        urlpattern1 = URLPatternFactory(collection=self.collection1)
        urlpattern2 = URLPatternFactory(collection=self.collection2)
        urlpattern3 = URLPatternFactory(collection=self.collection3)

        user = UserFactory(username="Jim")

        self.linkevent1 = LinkEventFactory(
            link=urlpattern1.url + "/test",
            change=LinkEvent.ADDED,
            username=user,
            timestamp=datetime(2019, 1, 15, tzinfo=timezone.utc),
            page_title="Event 1",
            user_is_bot=True,
        )
        self.linkevent1.url.add(urlpattern1)
        self.linkevent1.save()

        self.linkevent2 = LinkEventFactory(
            link=urlpattern1.url + "/test",
            change=LinkEvent.ADDED,
            username=user,
            timestamp=datetime(2019, 1, 10, tzinfo=timezone.utc),
            page_title="Event 1",
        )
        self.linkevent2.url.add(urlpattern1)
        self.linkevent2.save()

        self.linkevent3 = LinkEventFactory(
            link=urlpattern1.url + "/test",
            change=LinkEvent.REMOVED,
            username=UserFactory(username="Bob"),
            timestamp=datetime(2017, 5, 5, tzinfo=timezone.utc),
            page_title="Event 2",
        )
        self.linkevent3.url.add(urlpattern1)
        self.linkevent3.save()

        self.linkevent4 = LinkEventFactory(
            link=urlpattern1.url + "/test",
            change=LinkEvent.ADDED,
            username=UserFactory(username="Mary"),
            timestamp=datetime(2019, 3, 1, tzinfo=timezone.utc),
            on_user_list=True,
            page_title="Event 2",
            page_namespace=1,
        )
        self.linkevent4.url.add(urlpattern1)
        self.linkevent4.save()

        # Running the tables aggregates commands to fill aggregate tables
        call_command("fill_link_aggregates")
        call_command("fill_pageproject_aggregates")
        call_command("fill_user_aggregates")

    def test_program_detail_view(self):
        """
        Test that we can simply load a program detail page successfully
        """
        factory = RequestFactory()

        request = factory.get(self.url1)
        response = ProgramDetailView.as_view()(request, pk=self.program1.pk)

        self.assertEqual(response.status_code, 200)

    def test_program_detail_links_added(self):
        """
        Test that we're counting the correct total number of added links
        for this program.
        """
        factory = RequestFactory()

        request = factory.get(self.url1)
        form_data = "{}"
        organisations = self.program1.organisation_set.all()
        org_values = [org.pk for org in organisations]
        str_org_values = ",".join(map(str, org_values))

        url = reverse("programs:links_count")
        url_with_params = (
            "{url}?organisations={org_values}&form_data={form_data}".format(
                url=url, org_values=str_org_values, form_data=form_data
            )
        )
        response = self.client.get(url_with_params)

        self.assertEqual(json.loads(response.content)["links_added"], 3)

    def test_program_detail_links_removed(self):
        """
        Test that we're counting the correct total number of removed links
        for this program.
        """
        form_data = "{}"
        organisations = self.program1.organisation_set.all()
        org_values = [org.pk for org in organisations]
        str_org_values = ",".join(map(str, org_values))

        url = reverse("programs:links_count")
        url_with_params = (
            "{url}?organisations={org_values}&form_data={form_data}".format(
                url=url, org_values=str_org_values, form_data=form_data
            )
        )
        response = self.client.get(url_with_params)

        self.assertEqual(json.loads(response.content)["links_removed"], 1)

    def test_program_detail_total_editors(self):
        """
        Test that we're counting the correct total number of editors
        for this program.
        """
        form_data = "{}"
        organisations = self.program1.organisation_set.all()
        org_values = [org.pk for org in organisations]
        str_org_values = ",".join(map(str, org_values))

        url = reverse("programs:editor_count")
        url_with_params = (
            "{url}?organisations={org_values}&form_data={form_data}".format(
                url=url, org_values=str_org_values, form_data=form_data
            )
        )
        response = self.client.get(url_with_params)

        self.assertEqual(json.loads(response.content)["editor_count"], 3)

    def test_program_detail_date_form(self):
        """
        Test that the date limiting form works on the program detail page.
        """
        form_data = '{"start_date": "2019-01-01", "end_date": "2019-02-01"}'
        organisations = self.program1.organisation_set.all()
        org_values = [org.pk for org in organisations]
        str_org_values = ",".join(map(str, org_values))

        url = reverse("programs:links_count")
        url_with_params = (
            "{url}?organisations={org_values}&form_data={form_data}".format(
                url=url, org_values=str_org_values, form_data=form_data
            )
        )
        response = self.client.get(url_with_params)

        self.assertEqual(json.loads(response.content)["links_added"], 2)
        self.assertEqual(json.loads(response.content)["links_removed"], 0)

    def test_program_detail_user_list_form(self):
        """
        Test that the user list limiting form works on the program detail page.
        """
        form_data = '{"limit_to_user_list": true}'
        organisations = self.program1.organisation_set.all()
        org_values = [org.pk for org in organisations]
        str_org_values = ",".join(map(str, org_values))

        url = reverse("programs:links_count")
        url_with_params = (
            "{url}?organisations={org_values}&form_data={form_data}".format(
                url=url, org_values=str_org_values, form_data=form_data
            )
        )
        response = self.client.get(url_with_params)

        self.assertEqual(json.loads(response.content)["links_added"], 1)
        self.assertEqual(json.loads(response.content)["links_removed"], 0)

    def test_program_detail_namespace_form(self):
        """
        Test that the namespace id limiting form works on the program detail page.
        """
        # TODO: Skipping this test until we have implemented the namespace filter
        self.skipTest("Skipping test")
        factory = RequestFactory()

        data = {"namespace_id": 1}

        request = factory.get(self.url1, data)
        response = ProgramDetailView.as_view()(request, pk=self.program1.pk)

        self.assertEqual(response.context_data["total_added"], 1)
        self.assertEqual(response.context_data["total_removed"], 0)

    def test_top_organisations_csv(self):
        """
        Test that the top pages CSV returns the expected data
        """
        factory = RequestFactory()

        csv_url = reverse(
            "programs:csv_org_totals", kwargs={"pk": self.organisation1.pk}
        )

        request = factory.get(csv_url)
        response = CSVOrgTotals.as_view()(request, pk=self.program1.pk)
        csv_content = response.content.decode("utf-8")

        expected_output = (
            "Organisation,Links added,Links removed,Net Change\r\n" "Org 1,3,1,2\r\n"
        )

        self.assertEqual(csv_content, expected_output)

    def test_top_organisations_csv_filtered(self):
        """
        Test that the top pages CSV returns the expected data
        """
        factory = RequestFactory()

        csv_url = reverse(
            "programs:csv_org_totals", kwargs={"pk": self.organisation1.pk}
        )

        data = {"start_date": "2019-01-01", "end_date": "2019-02-01"}
        request = factory.get(csv_url, data)
        response = CSVOrgTotals.as_view()(request, pk=self.program1.pk)
        csv_content = response.content.decode("utf-8")

        expected_output = (
            "Organisation,Links added,Links removed,Net Change\r\n" "Org 1,2,0,2\r\n"
        )

        self.assertEqual(csv_content, expected_output)

    def test_top_projects_csv(self):
        """
        Test that the top projects CSV returns the expected data
        """
        factory = RequestFactory()

        csv_url = reverse(
            "programs:csv_project_totals", kwargs={"pk": self.program1.pk}
        )

        request = factory.get(csv_url)
        response = CSVProjectTotals.as_view()(request, pk=self.program1.pk)
        csv_content = response.content.decode("utf-8")

        expected_output = (
            "Project,Links added,Links removed,Net Change\r\n"
            "en.wikipedia.org,3,1,2\r\n"
        )

        self.assertEqual(csv_content, expected_output)

    def test_top_projects_csv_filtered(self):
        """
        Test that the top projects CSV returns the expected data
        """
        factory = RequestFactory()

        csv_url = reverse(
            "programs:csv_project_totals", kwargs={"pk": self.program1.pk}
        )

        data = {"start_date": "2019-01-01", "end_date": "2019-02-01"}
        request = factory.get(csv_url, data)
        response = CSVProjectTotals.as_view()(request, pk=self.program1.pk)
        csv_content = response.content.decode("utf-8")

        expected_output = (
            "Project,Links added,Links removed,Net Change\r\n"
            "en.wikipedia.org,2,0,2\r\n"
        )

        self.assertEqual(csv_content, expected_output)

    def test_top_users_csv(self):
        """
        Test that the top users CSV returns the expected data
        """
        factory = RequestFactory()

        csv_url = reverse("programs:csv_user_totals", kwargs={"pk": self.program1.pk})

        request = factory.get(csv_url)
        response = CSVUserTotals.as_view()(request, pk=self.program1.pk)
        csv_content = response.content.decode("utf-8")

        expected_output = (
            "Username,Links added,Links removed,Net Change\r\n"
            "Jim,2,0,2\r\n"
            "Mary,1,0,1\r\n"
            "Bob,0,1,-1\r\n"
        )
        self.assertEqual(csv_content, expected_output)

    def test_top_users_csv_filtered(self):
        """
        Test that the top users CSV returns the expected data
        """
        factory = RequestFactory()

        csv_url = reverse("programs:csv_user_totals", kwargs={"pk": self.program1.pk})

        data = {"start_date": "2019-01-01", "end_date": "2019-02-01"}
        request = factory.get(csv_url, data)
        response = CSVUserTotals.as_view()(request, pk=self.program1.pk)
        csv_content = response.content.decode("utf-8")

        expected_output = (
            "Username,Links added,Links removed,Net Change\r\n" "Jim,2,0,2\r\n"
        )
        self.assertEqual(csv_content, expected_output)

    def test_program_detail_bot_edits(self):
        """
        Test that the user list limiting form works on the program detail page.
        """
        # TODO: Skipping this test until we have implemented the bot filter
        self.skipTest("Skipping test")
        factory = RequestFactory()

        data = {"exclude_bots": True}

        request = factory.get(self.url1, data)
        response = ProgramDetailView.as_view()(request, pk=self.program1.pk)

        self.assertEqual(response.context_data["total_added"], 2)
        self.assertEqual(response.context_data["total_removed"], 1)
