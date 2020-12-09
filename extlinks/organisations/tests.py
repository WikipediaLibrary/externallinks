from datetime import datetime

from django.core.management import call_command
from django.test import TestCase, RequestFactory
from django.urls import reverse

from extlinks.common.views import (
    CSVPageTotals,
    CSVProjectTotals,
    CSVUserTotals,
    CSVAllLinkEvents,
)
from extlinks.links.factories import LinkEventFactory, URLPatternFactory
from extlinks.links.models import LinkEvent
from extlinks.programs.factories import ProgramFactory
from .factories import UserFactory, OrganisationFactory, CollectionFactory
from .views import OrganisationListView, OrganisationDetailView


class OrganisationListTest(TestCase):
    def setUp(self):
        self.program = ProgramFactory()
        self.organisation_one = OrganisationFactory(program=(self.program,))
        self.organisation_two = OrganisationFactory(program=(self.program,))

    def test_organisation_list_view(self):
        """
        Test that we can simply load the organisation list page successfully
        """
        factory = RequestFactory()

        request = factory.get(reverse("organisations:list"))
        response = OrganisationListView.as_view()(request)

        self.assertEqual(response.status_code, 200)

    def test_organisation_list_contents(self):
        """
        Test that the organisation list page contains the programs we expect.
        """

        factory = RequestFactory()

        request = factory.get(reverse("organisations:list"))
        response = OrganisationListView.as_view()(request)

        self.assertContains(response, self.organisation_one.name)
        self.assertContains(response, self.organisation_two.name)


class OrganisationDetailTest(TestCase):
    """
    Mostly the same tests as for programs, at least for now.
    """

    def setUp(self):
        self.program1 = ProgramFactory()
        self.organisation1 = OrganisationFactory(program=(self.program1,))
        self.url1 = reverse(
            "organisations:detail", kwargs={"pk": self.organisation1.pk}
        )

        self.collection1 = CollectionFactory(organisation=self.organisation1)
        self.collection1_key = self.collection1.name

        urlpattern1 = URLPatternFactory(collection=self.collection1)

        user = UserFactory(username="Jim")

        self.linkevent1 = LinkEventFactory(
            link=urlpattern1.url + "/test",
            change=LinkEvent.ADDED,
            username=user,
            timestamp=datetime(2019, 1, 15),
            page_title="Event 1",
            user_is_bot=True,
        )
        self.linkevent1.url.add(urlpattern1)
        self.linkevent1.save()

        self.linkevent2 = LinkEventFactory(
            link=urlpattern1.url + "/test",
            change=LinkEvent.ADDED,
            username=user,
            timestamp=datetime(2019, 1, 10),
            page_title="Event 1",
        )
        self.linkevent2.url.add(urlpattern1)
        self.linkevent2.save()

        self.linkevent3 = LinkEventFactory(
            link=urlpattern1.url + "/test",
            change=LinkEvent.REMOVED,
            username=UserFactory(username="Bob"),
            timestamp=datetime(2017, 5, 5),
            page_title="Event 2",
        )
        self.linkevent3.url.add(urlpattern1)
        self.linkevent3.save()

        self.linkevent4 = LinkEventFactory(
            link=urlpattern1.url + "/test",
            change=LinkEvent.ADDED,
            username=UserFactory(username="Mary"),
            timestamp=datetime(2019, 3, 1),
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

    def test_organisation_detail_view(self):
        """
        Test that we can simply load a organisation detail page successfully
        """
        factory = RequestFactory()

        request = factory.get(self.url1)
        response = OrganisationDetailView.as_view()(request, pk=self.organisation1.pk)

        self.assertEqual(response.status_code, 200)

    def test_organisation_detail_links_added(self):
        """
        Test that we're counting the correct total number of added links
        for this organisation.
        """
        factory = RequestFactory()

        request = factory.get(self.url1)
        response = OrganisationDetailView.as_view()(request, pk=self.organisation1.pk)

        self.assertEqual(
            response.context_data["collections"][self.collection1_key]["total_added"], 3
        )

    def test_organisation_detail_links_removed(self):
        """
        Test that we're counting the correct total number of removed links
        for this organisation.
        """
        factory = RequestFactory()

        request = factory.get(self.url1)
        response = OrganisationDetailView.as_view()(request, pk=self.organisation1.pk)

        self.assertEqual(
            response.context_data["collections"][self.collection1_key]["total_removed"],
            1,
        )

    def test_organisation_detail_total_editors(self):
        """
        Test that we're counting the correct total number of editors
        for this organisation.
        """
        factory = RequestFactory()

        request = factory.get(self.url1)
        response = OrganisationDetailView.as_view()(request, pk=self.organisation1.pk)

        self.assertEqual(
            response.context_data["collections"][self.collection1_key]["total_editors"],
            3,
        )

    def test_organisation_detail_date_form(self):
        """
        Test that the date limiting form works on the organisation detail page.
        """
        factory = RequestFactory()

        data = {"start_date": "2019-01-01", "end_date": "2019-02-01"}

        request = factory.get(self.url1, data)
        response = OrganisationDetailView.as_view()(request, pk=self.organisation1.pk)

        self.assertEqual(
            response.context_data["collections"][self.collection1_key]["total_added"], 2
        )
        self.assertEqual(
            response.context_data["collections"][self.collection1_key]["total_removed"],
            0,
        )

    def test_organisation_detail_user_list_form(self):
        """
        Test that the user list limiting form works on the organisation detail page.
        """
        # TODO: Skipping this test until we have implemented the user list filter
        self.skipTest("Skipping test")
        factory = RequestFactory()

        data = {"limit_to_user_list": True}

        request = factory.get(self.url1, data)
        response = OrganisationDetailView.as_view()(request, pk=self.organisation1.pk)

        self.assertEqual(
            response.context_data["collections"][self.collection1_key]["total_added"], 1
        )
        self.assertEqual(
            response.context_data["collections"][self.collection1_key]["total_removed"],
            0,
        )

    def test_organisation_detail_namespace_form(self):
        """
        Test that the namespace id limiting form works on the organisation detail page.
        """
        # TODO: Skipping this test until we have implemented the namespace filter
        self.skipTest("Skipping test")
        factory = RequestFactory()

        data = {"namespace_id": 0}

        request = factory.get(self.url1, data)
        response = OrganisationDetailView.as_view()(request, pk=self.organisation1.pk)

        self.assertEqual(
            response.context_data["collections"][self.collection1_key]["total_added"], 2
        )
        self.assertEqual(
            response.context_data["collections"][self.collection1_key]["total_removed"],
            1,
        )

    def test_top_pages_csv(self):
        """
        Test that the top pages CSV returns the expected data
        """
        factory = RequestFactory()

        csv_url = reverse(
            "organisations:csv_page_totals", kwargs={"pk": self.collection1.pk}
        )

        request = factory.get(csv_url)
        response = CSVPageTotals.as_view()(request, pk=self.collection1.pk)
        csv_content = response.content.decode("utf-8")

        expected_output = (
            "Page title,Project,Links added,Links removed,Net Change\r\n"
            "Event 1,en.wikipedia.org,2,0,2\r\n"
            "Event 2,en.wikipedia.org,1,1,0\r\n"
        )

        self.assertEqual(csv_content, expected_output)

    def test_top_projects_csv(self):
        """
        Test that the top projects CSV returns the expected data
        """
        factory = RequestFactory()

        csv_url = reverse(
            "organisations:csv_project_totals", kwargs={"pk": self.collection1.pk}
        )

        request = factory.get(csv_url)
        response = CSVProjectTotals.as_view()(request, pk=self.collection1.pk)
        csv_content = response.content.decode("utf-8")

        expected_output = (
            "Project,Links added,Links removed,Net Change\r\n"
            "en.wikipedia.org,3,1,2\r\n"
        )

        self.assertEqual(csv_content, expected_output)

    def test_top_users_csv(self):
        """
        Test that the top users CSV returns the expected data
        """
        factory = RequestFactory()

        csv_url = reverse(
            "organisations:csv_user_totals", kwargs={"pk": self.collection1.pk}
        )

        request = factory.get(csv_url)
        response = CSVUserTotals.as_view()(request, pk=self.collection1.pk)
        csv_content = response.content.decode("utf-8")

        expected_output = (
            "Username,Links added,Links removed,Net Change\r\n"
            "Jim,2,0,2\r\n"
            "Mary,1,0,1\r\n"
            "Bob,0,1,-1\r\n"
        )
        self.assertEqual(csv_content, expected_output)

    def test_latest_links_csv(self):
        """
        Test that the top users CSV returns the expected data
        """
        factory = RequestFactory()

        csv_url = reverse(
            "organisations:csv_all_links", kwargs={"pk": self.organisation1.pk}
        )

        request = factory.get(csv_url)
        response = CSVAllLinkEvents.as_view()(request, pk=self.organisation1.pk)

        self.assertContains(response, self.linkevent1.link)
        self.assertContains(response, self.linkevent2.link)
        self.assertContains(response, self.linkevent3.link)
        self.assertContains(response, self.linkevent4.link)

        self.assertContains(response, self.linkevent1.username.username)

    def test_bot_edits_form(self):
        """
        Test that the bot list limiting form works on the organisation detail page.
        """
        # TODO: Skipping this test until we have implemented the bot filter
        self.skipTest("Skipping test")
        factory = RequestFactory()

        data = {"exclude_bots": True}

        request = factory.get(self.url1, data)
        response = OrganisationDetailView.as_view()(request, pk=self.organisation1.pk)

        self.assertEqual(
            response.context_data["collections"][self.collection1_key]["total_added"], 2
        )
        self.assertEqual(
            response.context_data["collections"][self.collection1_key]["total_removed"],
            1,
        )
