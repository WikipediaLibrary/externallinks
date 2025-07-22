from datetime import datetime, timezone
import json
from unittest import mock

from django.core.management import call_command
from django.test import TestCase, RequestFactory, TransactionTestCase
from django.urls import reverse
from django.utils.http import urlencode

from extlinks.common.views import (
    CSVPageTotals,
    CSVProjectTotals,
    CSVUserTotals,
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


class OrganisationDetailTest(TransactionTestCase):
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

        urlpattern1 = URLPatternFactory()
        urlpattern1.collections.add(self.collection1)
        urlpattern1.save()

        user = UserFactory(username="Jim")

        self.linkevent1 = LinkEventFactory(
            link=urlpattern1.url + "/test",
            content_object=urlpattern1,
            change=LinkEvent.ADDED,
            username=user,
            timestamp=datetime(2019, 1, 15, tzinfo=timezone.utc),
            page_title="Event 1",
            user_is_bot=True,
        )

        self.linkevent2 = LinkEventFactory(
            link=urlpattern1.url + "/test",
            content_object=urlpattern1,
            change=LinkEvent.ADDED,
            username=user,
            timestamp=datetime(2019, 1, 10, tzinfo=timezone.utc),
            page_title="Event 1",
        )

        self.linkevent3 = LinkEventFactory(
            link=urlpattern1.url + "/test",
            content_object=urlpattern1,
            change=LinkEvent.REMOVED,
            username=UserFactory(username="Bob"),
            timestamp=datetime(2017, 5, 5, tzinfo=timezone.utc),
            page_title="Event 2",
        )

        self.linkevent4 = LinkEventFactory(
            link=urlpattern1.url + "/test",
            content_object=urlpattern1,
            change=LinkEvent.ADDED,
            username=UserFactory(username="Mary"),
            timestamp=datetime(2019, 3, 1, tzinfo=timezone.utc),
            on_user_list=True,
            page_title="Event 2",
            page_namespace=1,
        )

        # Running the tables aggregates commands to fill aggregate tables
        call_command("fill_link_aggregates")
        call_command("fill_pageproject_aggregates")
        call_command("fill_user_aggregates")

    @mock.patch("swiftclient.Connection")
    def test_organisation_detail_view(self, mock_swift_connection):
        """
        Test that we can simply load a organisation detail page successfully
        """

        mock_swift_connection.side_effect = RuntimeError("Swift is disabled")

        factory = RequestFactory()

        request = factory.get(self.url1)
        response = OrganisationDetailView.as_view()(request, pk=self.organisation1.pk)

        self.assertEqual(response.status_code, 200)

    @mock.patch("swiftclient.Connection")
    def test_organisation_detail_links_added(self, mock_swift_connection):
        """
        Test that we're counting the correct total number of added links
        for this organisation.
        """

        mock_swift_connection.side_effect = RuntimeError("Swift is disabled")

        form_data = "{}"
        collection_id = self.collection1.id

        url = reverse("organisations:links_count")
        url_with_params = "{url}?collection={collection}&form_data={form_data}".format(
            url=url, collection=collection_id, form_data=form_data
        )
        response = self.client.get(url_with_params)

        self.assertEqual(json.loads(response.content)["links_added"], 3)

    @mock.patch("swiftclient.Connection")
    def test_organisation_detail_links_removed(self, mock_swift_connection):
        """
        Test that we're counting the correct total number of removed links
        for this organisation.
        """

        mock_swift_connection.side_effect = RuntimeError("Swift is disabled")

        form_data = "{}"
        collection_id = self.collection1.id

        url = reverse("organisations:links_count")
        url_with_params = "{url}?collection={collection}&form_data={form_data}".format(
            url=url, collection=collection_id, form_data=form_data
        )
        response = self.client.get(url_with_params)

        self.assertEqual(json.loads(response.content)["links_removed"], 1)

    @mock.patch("swiftclient.Connection")
    def test_organisation_detail_total_editors(self, mock_swift_connection):
        """
        Test that we're counting the correct total number of editors
        for this organisation.
        """

        mock_swift_connection.side_effect = RuntimeError("Swift is disabled")

        form_data = "{}"
        collection_id = self.collection1.id

        url = reverse("organisations:editor_count")
        url_with_params = "{url}?collection={collection}&form_data={form_data}".format(
            url=url, collection=collection_id, form_data=form_data
        )
        response = self.client.get(url_with_params)

        self.assertEqual(json.loads(response.content)["editor_count"], 3)

    @mock.patch("swiftclient.Connection")
    def test_organisation_detail_date_form(self, mock_swift_connection):
        """
        Test that the date limiting form works on the organisation detail page.
        """

        mock_swift_connection.side_effect = RuntimeError("Swift is disabled")

        form_data = '{"start_date": "2019-01-01", "end_date": "2019-02-01"}'
        collection_id = self.collection1.id

        url = reverse("organisations:links_count")
        url_with_params = "{url}?collection={collection}&form_data={form_data}".format(
            url=url, collection=collection_id, form_data=form_data
        )
        response = self.client.get(url_with_params)

        self.assertEqual(json.loads(response.content)["links_added"], 2)
        self.assertEqual(json.loads(response.content)["links_removed"], 0)

    @mock.patch("swiftclient.Connection")
    def test_organisation_detail_user_list_form(self, mock_swift_connection):
        """
        Test that the user list limiting form works on the organisation detail page.
        """

        mock_swift_connection.side_effect = RuntimeError("Swift is disabled")

        form_data = '{"limit_to_user_list": true}'
        collection_id = self.collection1.id

        url = reverse("organisations:links_count")
        url_with_params = "{url}?collection={collection}&form_data={form_data}".format(
            url=url, collection=collection_id, form_data=form_data
        )
        response = self.client.get(url_with_params)

        self.assertEqual(json.loads(response.content)["links_added"], 1)
        self.assertEqual(json.loads(response.content)["links_removed"], 0)

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

    @mock.patch("swiftclient.Connection")
    def test_top_pages_csv(self, mock_swift_connection):
        """
        Test that the top pages CSV returns the expected data
        """

        mock_swift_connection.side_effect = RuntimeError("Swift is disabled")

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

    @mock.patch("swiftclient.Connection")
    def test_top_pages_csv_filtered(self, mock_swift_connection):
        """
        Test that the top pages CSV returns the expected data
        """

        mock_swift_connection.side_effect = RuntimeError("Swift is disabled")

        factory = RequestFactory()

        csv_url = reverse(
            "organisations:csv_page_totals", kwargs={"pk": self.collection1.pk}
        )

        data = {"start_date": "2019-01-01", "end_date": "2019-02-01"}
        request = factory.get(csv_url, data)
        response = CSVPageTotals.as_view()(request, pk=self.collection1.pk)
        csv_content = response.content.decode("utf-8")

        expected_output = (
            "Page title,Project,Links added,Links removed,Net Change\r\n"
            "Event 1,en.wikipedia.org,2,0,2\r\n"
        )

        self.assertEqual(csv_content, expected_output)

    @mock.patch("extlinks.aggregates.storage.download_aggregates")
    @mock.patch("swiftclient.Connection")
    def test_top_pages_csv_with_archives(
        self, mock_swift_connection, mock_download_aggregates
    ):
        """
        Test that the top pages CSV returns the expected data when
        incorporating archived data
        """

        mock_swift_connection.side_effect = RuntimeError("Swift is disabled")

        mock_download_aggregates.return_value = [
            {
                "project_name": "en.wikipedia.org",
                "page_name": "Event 2",
                "full_date": "2021-01-01",
                "total_links_added": 2,
                "total_links_removed": 1,
                "on_user_list": False,
            },
            {
                "project_name": "en.wikipedia.org",
                "page_name": "Event 3",
                "full_date": "2021-01-01",
                "total_links_added": 1,
                "total_links_removed": 2,
                "on_user_list": False,
            },
        ]

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
            "Event 2,en.wikipedia.org,3,2,1\r\n"
            "Event 3,en.wikipedia.org,1,2,-1\r\n"
        )

        self.assertEqual(csv_content, expected_output)

    @mock.patch("swiftclient.Connection")
    def test_top_projects_csv(self, mock_swift_connection):
        """
        Test that the top projects CSV returns the expected data
        """

        mock_swift_connection.side_effect = RuntimeError("Swift is disabled")

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

    @mock.patch("swiftclient.Connection")
    def test_top_projects_csv_filtered(self, mock_swift_connection):
        """
        Test that the top projects CSV returns the expected data
        """

        mock_swift_connection.side_effect = RuntimeError("Swift is disabled")

        factory = RequestFactory()

        csv_url = reverse(
            "organisations:csv_project_totals", kwargs={"pk": self.collection1.pk}
        )

        data = {"start_date": "2019-01-01", "end_date": "2019-02-01"}
        request = factory.get(csv_url, data)
        response = CSVProjectTotals.as_view()(request, pk=self.collection1.pk)
        csv_content = response.content.decode("utf-8")

        expected_output = (
            "Project,Links added,Links removed,Net Change\r\n"
            "en.wikipedia.org,2,0,2\r\n"
        )

        self.assertEqual(csv_content, expected_output)

    @mock.patch("extlinks.aggregates.storage.download_aggregates")
    @mock.patch("swiftclient.Connection")
    def test_top_projects_csv_with_archives(
        self, mock_swift_connection, mock_download_aggregates
    ):
        """
        Test that the top projects CSV returns the expected data when
        incorporating archived data
        """

        mock_swift_connection.side_effect = RuntimeError("Swift is disabled")

        mock_download_aggregates.return_value = [
            {
                "project_name": "en.wikipedia.org",
                "full_date": "2021-01-01",
                "total_links_added": 2,
                "total_links_removed": 1,
                "on_user_list": False,
            },
            {
                "project_name": "fr.wikipedia.org",
                "full_date": "2021-01-01",
                "total_links_added": 2,
                "total_links_removed": 3,
                "on_user_list": False,
            },
        ]

        factory = RequestFactory()

        csv_url = reverse(
            "organisations:csv_project_totals", kwargs={"pk": self.collection1.pk}
        )

        request = factory.get(csv_url)
        response = CSVProjectTotals.as_view()(request, pk=self.collection1.pk)
        csv_content = response.content.decode("utf-8")

        expected_output = (
            "Project,Links added,Links removed,Net Change\r\n"
            "en.wikipedia.org,5,2,3\r\n"
            "fr.wikipedia.org,2,3,-1\r\n"
        )

        self.assertEqual(csv_content, expected_output)

    @mock.patch("swiftclient.Connection")
    def test_top_users_csv(self, mock_swift_connection):
        """
        Test that the top users CSV returns the expected data
        """

        mock_swift_connection.side_effect = RuntimeError("Swift is disabled")

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

    @mock.patch("swiftclient.Connection")
    def test_top_users_csv_filtered(self, mock_swift_connection):
        """
        Test that the top users CSV returns the expected data
        """

        mock_swift_connection.side_effect = RuntimeError("Swift is disabled")

        factory = RequestFactory()

        csv_url = reverse(
            "organisations:csv_user_totals", kwargs={"pk": self.collection1.pk}
        )

        data = {"start_date": "2019-01-01", "end_date": "2019-02-01"}
        request = factory.get(csv_url, data)
        response = CSVUserTotals.as_view()(request, pk=self.collection1.pk)
        csv_content = response.content.decode("utf-8")

        expected_output = (
            "Username,Links added,Links removed,Net Change\r\n" "Jim,2,0,2\r\n"
        )
        self.assertEqual(csv_content, expected_output)

    @mock.patch("extlinks.aggregates.storage.download_aggregates")
    @mock.patch("swiftclient.Connection")
    def test_top_users_csv_with_archives(
        self, mock_swift_connection, mock_download_aggregates
    ):
        """
        Test that the top users CSV returns the expected data
        """

        mock_swift_connection.side_effect = RuntimeError("Swift is disabled")

        mock_download_aggregates.return_value = [
            {
                "username": "Jim",
                "full_date": "2021-01-01",
                "total_links_added": 2,
                "total_links_removed": 1,
                "on_user_list": False,
            },
            {
                "username": "Alice",
                "full_date": "2021-01-01",
                "total_links_added": 2,
                "total_links_removed": 4,
                "on_user_list": False,
            },
        ]

        factory = RequestFactory()

        csv_url = reverse(
            "organisations:csv_user_totals", kwargs={"pk": self.collection1.pk}
        )

        request = factory.get(csv_url)
        response = CSVUserTotals.as_view()(request, pk=self.collection1.pk)
        csv_content = response.content.decode("utf-8")

        expected_output = (
            "Username,Links added,Links removed,Net Change\r\n"
            "Jim,4,1,3\r\n"
            "Mary,1,0,1\r\n"
            "Bob,0,1,-1\r\n"
            "Alice,2,4,-2\r\n"
        )
        self.assertEqual(csv_content, expected_output)

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

    @mock.patch("swiftclient.Connection")
    def test_top_projects(self, mock_swift_connection):
        """
        Test that the top projects view returns the expected data.
        """

        mock_swift_connection.side_effect = RuntimeError("Swift is disabled")

        url = reverse("organisations:top_projects")
        params = {"collection": self.collection1.id, "form_data": "{}"}
        response = self.client.get(f"{url}?{urlencode(params)}")

        self.assertEqual(response.status_code, 200)

        response_data = json.loads(response.content)
        self.assertIn("top_projects", response_data)

        top_projects = json.loads(response_data["top_projects"])

        self.assertEqual(len(top_projects), 1)
        self.assertEqual(top_projects[0]["project_name"], "en.wikipedia.org")
        self.assertEqual(top_projects[0]["links_diff"], 2)

    @mock.patch("swiftclient.Connection")
    def test_top_projects_date_filtered(self, mock_swift_connection):
        """
        Test that the top projects view handles date filtering correctly.
        """

        mock_swift_connection.side_effect = RuntimeError("Swift is disabled")

        form_data = {"start_date": "2019-01-01", "end_date": "2019-02-01"}

        url = reverse("organisations:top_projects")
        params = {"collection": self.collection1.id, "form_data": json.dumps(form_data)}
        response = self.client.get(f"{url}?{urlencode(params)}")

        self.assertEqual(response.status_code, 200)

        response_data = json.loads(response.content)
        top_projects = json.loads(response_data["top_projects"])

        self.assertEqual(len(top_projects), 1)
        self.assertEqual(top_projects[0]["project_name"], "en.wikipedia.org")
        self.assertEqual(top_projects[0]["links_diff"], 2)

    @mock.patch("extlinks.aggregates.storage.download_aggregates")
    @mock.patch("swiftclient.Connection")
    def test_top_projects_with_archives(
        self, mock_swift_connection, mock_download_aggregates
    ):
        """
        Test that the top projects view correctly merges database and archive data.
        """

        mock_swift_connection.side_effect = RuntimeError("Swift is disabled")

        # Mock the return value of download_aggregates to simulate archived data
        mock_download_aggregates.return_value = [
            {
                "project_name": "en.wikipedia.org",
                "full_date": "2021-01-01",
                "total_links_added": 2,
                "total_links_removed": 1,
                "on_user_list": False,
            },
            {
                "project_name": "fr.wikipedia.org",
                "full_date": "2021-01-01",
                "total_links_added": 3,
                "total_links_removed": 1,
                "on_user_list": False,
            },
        ]

        url = reverse("organisations:top_projects")
        params = {"collection": self.collection1.id, "form_data": "{}"}
        response = self.client.get(f"{url}?{urlencode(params)}")

        self.assertEqual(response.status_code, 200)

        response_data = json.loads(response.content)
        top_projects = json.loads(response_data["top_projects"])

        # We should have two projects now. 'en.wikipedia.org' from the database
        # and archive data, and 'fr.wikipedia.org' from archive data.
        self.assertEqual(len(top_projects), 2)

        # Find each project in the results
        en_project = next(
            (p for p in top_projects if p["project_name"] == "en.wikipedia.org"), None
        )
        fr_project = next(
            (p for p in top_projects if p["project_name"] == "fr.wikipedia.org"), None
        )

        # Verify project data. 'en.wikipedia' has both DB and archive data, and
        # 'fr.wikipedia' only has archive data.
        self.assertIsNotNone(en_project)
        self.assertEqual(en_project["links_diff"], 3)  # 2 from DB + (2-1) from archive

        self.assertIsNotNone(fr_project)
        self.assertEqual(fr_project["links_diff"], 2)  # (3-1) from archive

    @mock.patch("swiftclient.Connection")
    def test_top_users(self, mock_swift_connection):
        """
        Test that the top users view returns the expected data.
        """

        mock_swift_connection.side_effect = RuntimeError("Swift is disabled")

        url = reverse("organisations:top_users")
        params = {"collection": self.collection1.id, "form_data": "{}"}
        response = self.client.get(f"{url}?{urlencode(params)}")

        self.assertEqual(response.status_code, 200)

        response_data = json.loads(response.content)
        self.assertIn("top_users", response_data)

        top_users = json.loads(response_data["top_users"])
        self.assertEqual(len(top_users), 3)

        # Find the users in the results.
        jim = next((u for u in top_users if u["username"] == "Jim"), None)
        mary = next((u for u in top_users if u["username"] == "Mary"), None)
        bob = next((u for u in top_users if u["username"] == "Bob"), None)

        self.assertIsNotNone(jim)
        self.assertEqual(jim["links_diff"], 2)

        self.assertIsNotNone(mary)
        self.assertEqual(mary["links_diff"], 1)

        self.assertIsNotNone(bob)
        self.assertEqual(bob["links_diff"], -1)

    @mock.patch("swiftclient.Connection")
    def test_top_users_date_filtered(self, mock_swift_connection):
        """
        Test that the top users view handles date filtering correctly.
        """

        mock_swift_connection.side_effect = RuntimeError("Swift is disabled")

        form_data = {"start_date": "2019-01-01", "end_date": "2019-02-01"}

        url = reverse("organisations:top_users")
        params = {"collection": self.collection1.id, "form_data": json.dumps(form_data)}
        response = self.client.get(f"{url}?{urlencode(params)}")

        self.assertEqual(response.status_code, 200)

        response_data = json.loads(response.content)
        top_users = json.loads(response_data["top_users"])

        # Only Jim should be in results due to the date filter.
        self.assertEqual(len(top_users), 1)
        self.assertEqual(top_users[0]["username"], "Jim")
        self.assertEqual(top_users[0]["links_diff"], 2)

    @mock.patch("extlinks.aggregates.storage.download_aggregates")
    @mock.patch("swiftclient.Connection")
    def test_top_users_with_archives(
        self, mock_swift_connection, mock_download_aggregates
    ):
        """
        Test that the top users view correctly merges database and archive data.
        """

        mock_swift_connection.side_effect = RuntimeError("Swift is disabled")

        # Mock the return of 'download_aggregates' to simulate archived data.
        mock_download_aggregates.return_value = [
            {
                "username": "Jim",
                "full_date": "2021-01-01",
                "total_links_added": 2,
                "total_links_removed": 1,
                "on_user_list": False,
            },
            {
                "username": "Alice",
                "full_date": "2021-01-01",
                "total_links_added": 2,
                "total_links_removed": 4,
                "on_user_list": False,
            },
        ]

        url = reverse("organisations:top_users")
        params = {"collection": self.collection1.id, "form_data": "{}"}
        response = self.client.get(f"{url}?{urlencode(params)}")

        self.assertEqual(response.status_code, 200)

        response_data = json.loads(response.content)
        top_users = json.loads(response_data["top_users"])

        # We should have four users now. Three from the database and one
        # additional user (Alice) from archive data.
        self.assertEqual(len(top_users), 4)

        # Find users in the results.
        jim = next((u for u in top_users if u["username"] == "Jim"), None)
        mary = next((u for u in top_users if u["username"] == "Mary"), None)
        bob = next((u for u in top_users if u["username"] == "Bob"), None)
        alice = next((u for u in top_users if u["username"] == "Alice"), None)

        # Verify that user changes from both sources are accurate. Jim has both
        # DB and archive data while the others have only one source each.
        self.assertIsNotNone(jim)
        self.assertEqual(jim["links_diff"], 3)  # 2 from DB + (2-1) from archive

        self.assertIsNotNone(mary)
        self.assertEqual(mary["links_diff"], 1)  # Only from DB

        self.assertIsNotNone(bob)
        self.assertEqual(bob["links_diff"], -1)  # Only from DB

        self.assertIsNotNone(alice)
        self.assertEqual(alice["links_diff"], -2)  # (2-4) from archive

    @mock.patch("swiftclient.Connection")
    def test_top_pages(self, mock_swift_connection):
        """
        Test that the top pages view returns the expected data.
        """

        mock_swift_connection.side_effect = RuntimeError("Swift is disabled")

        url = reverse("organisations:top_pages")
        params = {"collection": self.collection1.id, "form_data": "{}"}
        response = self.client.get(f"{url}?{urlencode(params)}")

        self.assertEqual(response.status_code, 200)

        response_data = json.loads(response.content)
        self.assertIn("top_pages", response_data)

        top_pages = json.loads(response_data["top_pages"])
        self.assertEqual(len(top_pages), 2)

        # Find the pages in the results.
        event1 = next((p for p in top_pages if p["page_name"] == "Event 1"), None)
        event2 = next((p for p in top_pages if p["page_name"] == "Event 2"), None)

        self.assertIsNotNone(event1)
        self.assertEqual(event1["links_diff"], 2)

        self.assertIsNotNone(event2)
        self.assertEqual(event2["links_diff"], 0)

    @mock.patch("swiftclient.Connection")
    def test_top_pages_date_filtered(self, mock_swift_connection):
        """
        Test that the top pages view handles date filtering correctly.
        """

        mock_swift_connection.side_effect = RuntimeError("Swift is disabled")

        form_data = {"start_date": "2019-01-01", "end_date": "2019-02-01"}

        url = reverse("organisations:top_pages")
        params = {"collection": self.collection1.id, "form_data": json.dumps(form_data)}
        response = self.client.get(f"{url}?{urlencode(params)}")

        self.assertEqual(response.status_code, 200)

        response_data = json.loads(response.content)
        top_pages = json.loads(response_data["top_pages"])

        # Only 'Event 1' should be in results due to the date filter.
        self.assertEqual(len(top_pages), 1)
        self.assertEqual(top_pages[0]["page_name"], "Event 1")
        self.assertEqual(top_pages[0]["project_name"], "en.wikipedia.org")
        self.assertEqual(top_pages[0]["links_diff"], 2)

    @mock.patch("extlinks.aggregates.storage.download_aggregates")
    @mock.patch("swiftclient.Connection")
    def test_top_pages_with_archives(
        self, mock_swift_connection, mock_download_aggregates
    ):
        """
        Test that the top pages view correctly merges database and archive data.
        """

        mock_swift_connection.side_effect = RuntimeError("Swift is disabled")

        # Mock the return of 'download_aggregates' to simulate archived data.
        mock_download_aggregates.return_value = [
            {
                "project_name": "en.wikipedia.org",
                "page_name": "Event 2",
                "full_date": "2021-01-01",
                "total_links_added": 2,
                "total_links_removed": 1,
                "on_user_list": False,
            },
            {
                "project_name": "en.wikipedia.org",
                "page_name": "Event 3",
                "full_date": "2021-01-01",
                "total_links_added": 1,
                "total_links_removed": 2,
                "on_user_list": False,
            },
        ]

        url = reverse("organisations:top_pages")
        params = {"collection": self.collection1.id, "form_data": "{}"}
        response = self.client.get(f"{url}?{urlencode(params)}")

        self.assertEqual(response.status_code, 200)

        response_data = json.loads(response.content)
        top_pages = json.loads(response_data["top_pages"])

        # We should have three pages now. Two from the database and one
        # additional page (Event 3) from the archive data.
        self.assertEqual(len(top_pages), 3)

        # Find pages in the results.
        event1 = next((p for p in top_pages if p["page_name"] == "Event 1"), None)
        event2 = next((p for p in top_pages if p["page_name"] == "Event 2"), None)
        event3 = next((p for p in top_pages if p["page_name"] == "Event 3"), None)

        # Verify that page changes from both sources are accurate. Event 2 has
        # both DB and archive data while the others have only one source each.
        self.assertIsNotNone(event1)
        self.assertEqual(event1["links_diff"], 2)  # Only from DB

        self.assertIsNotNone(event2)
        self.assertEqual(event2["links_diff"], 1)  # 0 from DB + (2-1) from archive

        self.assertIsNotNone(event3)
        self.assertEqual(event3["links_diff"], -1)  # (1-2) from archive
