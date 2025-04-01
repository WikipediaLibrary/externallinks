import json, tempfile, glob, gzip, os

from datetime import datetime, date, timezone

from django.core.management import call_command
from django.test import TestCase, TransactionTestCase

from unittest import mock

from extlinks.aggregates.models import (
    LinkAggregate,
    PageProjectAggregate,
    UserAggregate,
)
from extlinks.organisations.factories import (
    OrganisationFactory,
    CollectionFactory,
    UserFactory,
)
from .factories import LinkEventFactory, URLPatternFactory
from .helpers import link_is_tracked, split_url_for_query
from .models import URLPattern, LinkEvent


class LinksHelpersTest(TestCase):
    def test_split_url_for_query_1(self):
        """
        Given a URL pattern, ensure that our helper function converts it
        to the expected format for querying replica databases
        """
        url = "testurl.com"

        output = split_url_for_query(url)

        self.assertEqual(output, ("com.testurl.%", "%"))

    def test_split_url_for_query_2(self):
        """
        Given a URL pattern with a path, ensure that our helper function
        converts it to the expected format for querying replica databases
        """
        url = "testurl.com/test"

        output = split_url_for_query(url)

        self.assertEqual(output, ("com.testurl.%", "%./test%"))

    def test_split_url_for_query_3(self):
        """
        Given a URL pattern starting "*.", ensure that our helper function
        converts it to the expected format for querying replica databases
        """
        url = "*.testurl.com/test"

        output = split_url_for_query(url)

        self.assertEqual(output, ("com.testurl.%", "%./test%"))

    def test_get_organisation(self):
        """
        Test the get_organisation function on the LinkEvent model.
        """
        organisation1 = OrganisationFactory()
        organisation2 = OrganisationFactory()

        CollectionFactory(organisation=organisation1)
        collection2 = CollectionFactory(organisation=organisation2)

        URLPatternFactory()
        URLPatternFactory()
        url_pattern = URLPatternFactory(collection=collection2)

        new_link = LinkEventFactory(content_object=url_pattern)

        self.assertEqual(new_link.get_organisation, organisation2)


class LinksTrackedTest(TestCase):
    def setUp(self):
        _ = URLPatternFactory(url="test.com")

    def test_link_is_tracked_true(self):
        """
        Test that link_is_tracked returns True when we have a matching
        URLPattern
        """
        self.assertTrue(link_is_tracked("https://test.com/testurl"))

    def test_link_is_tracked_true_with_subdomain(self):
        """
        Test that link_is_tracked returns True when we have a matching
        URLPattern even when the link has a subdomain
        """
        self.assertTrue(link_is_tracked("https://foo.test.com/testurl"))

    def test_link_is_tracked_true_with_www(self):
        """
        Test that link_is_tracked returns True when we have a matching
        URLPattern even when the link has www
        """
        self.assertTrue(link_is_tracked("https://www.test.com/testurl"))

    def test_link_is_tracked_false(self):
        """
        Test that link_is_tracked returns False when we have no matching
        URLPattern
        """
        self.assertFalse(link_is_tracked("https://www.foo.com/"))

    def test_link_is_tracked_false_not_domain(self):
        """
        Test that link_is_tracked returns False when we have a partial
        match but the match isn't for the actual domain
        """
        self.assertFalse(link_is_tracked("https://thisisatest.com/"))

    def test_link_is_tracked_false_archive(self):
        """
        Test that link_is_tracked returns False when we have a partial
        match based on multiple protocols
        """
        self.assertFalse(link_is_tracked("https://web.archive.org/https://test.com/"))

    def test_link_is_tracked_true_proxy(self):
        """
        Test that link_is_tracked returns True when we have a proxied URL
        that matches a URLPattern
        """
        self.assertTrue(
            link_is_tracked("https://www-test-com.wikipedialibrary.idm.oclc.org/")
        )

    def test_link_is_tracked_false_other_proxy(self):
        """
        Test that link_is_tracked returns False when we have a proxied URL
        that matches our URLPattern but isn't the TWL proxy
        """
        self.assertFalse(
            link_is_tracked("https://www-test-com.university.idm.oclc.org/")
        )


class URLPatternModelTest(TestCase):
    def test_get_proxied_url_1(self):
        """
        Test that URLPattern.get_proxied_url transforms a URL correctly
        """
        test_urlpattern = URLPattern(url="gale.com")
        self.assertEqual(test_urlpattern.get_proxied_url, "gale-com")

    def test_get_proxied_url_2(self):
        """
        Test that URLPattern.get_proxied_url transforms a URL correctly
        when it has a subdomain
        """
        test_urlpattern = URLPattern(url="platform.almanhal.com")
        self.assertEqual(test_urlpattern.get_proxied_url, "platform-almanhal-com")


class LinkEventsCollectCommandTest(TestCase):
    def setUp(self):
        self.organisation1 = OrganisationFactory(name="JSTOR")
        self.collection1 = CollectionFactory(
            name="JSTOR", organisation=self.organisation1
        )
        self.url = URLPatternFactory(url="www.jstor.org")
        self.url.collections.add(self.collection1)
        self.url.save()

        self.event_data1 = {
            "$schema": "/mediawiki/page/links-change/1.0.0",
            "meta": {
                "uri": "https://en.wikipedia.org/wiki/Tanya_Tuzova",
                "request_id": "db830ce7-1aa8-4984-baa3-7598ea7113d2",
                "id": "4100e9a8-af77-405f-ab13-ec0957a7c24c",
                "dt": "2020-08-20T21:22:46Z",
                "domain": "en.wikipedia.org",
                "stream": "mediawiki.page-links-change",
                "topic": "eqiad.mediawiki.page-links-change",
                "partition": 0,
                "offset": 840218818,
            },
            "database": "enwiki",
            "page_id": 63608061,
            "page_title": "Page1",
            "page_namespace": 0,
            "page_is_redirect": False,
            "rev_id": 974060045,
            "performer": {
                "user_text": "User1",
                "user_groups": ["extendedconfirmed", "*", "user", "autoconfirmed"],
                "user_is_bot": False,
                "user_id": 32001896,
                "user_registration_dt": "2017-09-24T15:12:43Z",
                "user_edit_count": 2099,
            },
            "removed_links": [
                {
                    "link": "/wiki/Wikipedia:Articles_for_deletion/Tanya_Tuzova",
                    "external": False,
                },
                {"link": "/wiki/Wikipedia:Deletion_policy", "external": False},
                {"link": "/wiki/Wikipedia:Guide_to_deletion", "external": False},
                {"link": "/wiki/Wikipedia:Page_blanking", "external": False},
                {
                    "link": "//scholar.google.com/scholar%3Fq%3D%2522Tanya%2BTuzova%2522",
                    "external": True,
                },
                {
                    "link": "https://www.jstor.org/action/doBasicSearch%3FQuery%3D%2522Tanya%2BTuzova%2522%26acc%3Don%26wc%3Don",
                    "external": True,
                },
            ],
        }
        self.event_data2 = {
            "$schema": "/mediawiki/page/links-change/1.0.0",
            "meta": {
                "uri": "https://en.wikipedia.org/wiki/Tanya_Tuzova",
                "request_id": "db830ce7-1aa8-4984-baa3-7598ea7113d2",
                "id": "4100e9a8-af77-405f-ab13-ec0957a7c24c",
                "dt": "2020-08-20T21:22:46Z",
                "domain": "en.wikipedia.org",
                "stream": "mediawiki.page-links-change",
                "topic": "eqiad.mediawiki.page-links-change",
                "partition": 0,
                "offset": 840218818,
            },
            "database": "enwiki",
            "page_id": 63608061,
            "page_title": "Page1",
            "page_namespace": 0,
            "page_is_redirect": False,
            "rev_id": 974060041,
            "performer": {
                "user_text": "User1",
                "user_groups": ["extendedconfirmed", "*", "user", "autoconfirmed"],
                "user_is_bot": False,
                "user_id": 32001896,
                "user_registration_dt": "2017-09-24T15:12:43Z",
                "user_edit_count": 6550,
            },
            "added_links": [
                {
                    "link": "/wiki/Wikipedia:Articles_for_deletion/Tanya_Tuzova",
                    "external": False,
                },
                {"link": "/wiki/Wikipedia:Deletion_policy", "external": False},
                {"link": "/wiki/Wikipedia:Guide_to_deletion", "external": False},
                {"link": "/wiki/Wikipedia:Page_blanking", "external": False},
                {
                    "link": "https://www-jstor-org.wikipedialibrary.idm.oclc.org/stable/27903775?Search=yes&resultItemClick=true&searchText=%22Evil+as+an+Explanatory+Concept%22&searchUri=/action/doBasicSearch?Query%3D%2522Evil%2Bas%2Ban%2BExplanatory%2BConcept%2522%26acc%3Don%26wc%3Don%26fc%3Doff%26group%3Dnone%26refreqid%3Dsearch%253Aad56779891b6147f11436f1629fe704d&ab_segments=0/basic_SYC-5187_SYC-5188/5188&refreqid=fastly-default:21cde56a64c1528014fba9e12d054b80&seq=1#metadata_info_tab_contents",
                    "external": True,
                },
                {
                    "link": "https://www-jstor-org.wikipedialibrary.idm.oclc.org/stable/dklsajdlkajslkdjaslkdjalks",
                    "external": True,
                },
            ],
        }

    def test_management_command_non_proxy(self):
        self.assertEqual(LinkEvent.objects.count(), 0)
        with self.assertRaises(SystemExit):
            call_command("linkevents_collect", test=self.event_data1)
        self.assertEqual(LinkEvent.objects.count(), 1)

    def test_management_command_proxy_urls(self):
        self.assertEqual(LinkEvent.objects.count(), 0)
        with self.assertRaises(SystemExit):
            call_command("linkevents_collect", test=self.event_data2)
        self.assertEqual(LinkEvent.objects.count(), 2)


class LinkEventsArchiveCommandTest(TransactionTestCase):
    def setUp(self):
        self.user = UserFactory(username="jonsnow")

        self.jstor_organisation = OrganisationFactory(name="JSTOR")
        self.jstor_collection = CollectionFactory(
            name="JSTOR", organisation=self.jstor_organisation
        )
        self.jstor_url_pattern = URLPatternFactory(url="www.jstor.org")
        self.jstor_url_pattern.collections.add(self.jstor_collection)
        self.jstor_url_pattern.save()

    @mock.patch("swiftclient.client.Connection")
    def test_dump_with_date(self, mock_swift_connection):
        """
        Test that LinkEvents are dumped and removed from the database when
        using the 'linkevents_archive' management command. LinkEvent dumps
        should be grouped together by date.
        """
        mock_conn = mock_swift_connection.return_value
        mock_conn.get_account.return_value = ({}, [])

        # Add the LinkEvent data that will be dumped.

        for i in range(5):
            LinkEventFactory(
                content_object=self.jstor_url_pattern,
                link=f"www.jstor.org/something_16_{i}",
                timestamp=datetime(2021, 1, 16, 0, 0, 0, tzinfo=timezone.utc),
                page_title=f"Page_{i}",
                username=self.user,
            )

        for i in range(5):
            LinkEventFactory(
                content_object=self.jstor_url_pattern,
                link=f"www.jstor.org/something_17_{i}",
                timestamp=datetime(2021, 1, 17, 0, 0, 0, tzinfo=timezone.utc),
                page_title=f"Page_{i}",
                username=self.user,
            )

        for i in range(5):
            LinkEventFactory(
                content_object=self.jstor_url_pattern,
                link=f"www.jstor.org/something_18_{i}",
                timestamp=datetime(2021, 1, 18, 0, 0, 0, tzinfo=timezone.utc),
                page_title=f"Page_{i}",
                username=self.user,
            )

        temp_dir = tempfile.gettempdir()

        try:
            # Call the command which dumps the events we just added.
            call_command(
                "linkevents_archive",
                "dump",
                date=date(year=2021, month=1, day=18),
                output=temp_dir,
            )

            # Ensure the expected archives were generated.
            jan_16_archive = os.path.join(
                temp_dir, "links_linkevent_20210116_0.json.gz"
            )
            self.assertTrue(os.path.isfile(jan_16_archive))
            jan_17_archive = os.path.join(
                temp_dir, "links_linkevent_20210117_0.json.gz"
            )
            self.assertTrue(os.path.isfile(jan_17_archive))
            jan_18_archive = os.path.join(
                temp_dir, "links_linkevent_20210118_0.json.gz"
            )
            self.assertTrue(os.path.isfile(jan_18_archive))

            # Make sure the events that were archived got removed from the db.
            self.assertEqual(LinkEvent.objects.count(), 0)
        finally:
            pattern = os.path.join(temp_dir, "links_linkevent_*.json.gz")

            for file in glob.glob(pattern):
                os.remove(file)

    @mock.patch("swiftclient.client.Connection")
    def test_dump_without_date(self, mock_swift_connection):
        """
        Test that LinkEvents are dumped and removed from the database when
        using the 'linkevents_archive' management command even when a date
        option is not provided. A date should be inferred from the job logs.
        """
        mock_conn = mock_swift_connection.return_value
        mock_conn.get_account.return_value = ({}, [])

        # Add the LinkEvent data that will be dumped.

        for i in range(5):
            LinkEventFactory(
                content_object=self.jstor_url_pattern,
                link=f"www.jstor.org/something_16_{i}",
                timestamp=datetime(2025, 1, 16, 0, 0, 0, tzinfo=timezone.utc),
                page_title=f"Page_{i}",
                username=self.user,
            )

        for i in range(5):
            LinkEventFactory(
                content_object=self.jstor_url_pattern,
                link=f"www.jstor.org/something_17_{i}",
                timestamp=datetime(2025, 1, 17, 0, 0, 0, tzinfo=timezone.utc),
                page_title=f"Page_{i}",
                username=self.user,
            )

        for i in range(5):
            LinkEventFactory(
                content_object=self.jstor_url_pattern,
                link=f"www.jstor.org/something_17_{i}",
                timestamp=datetime(2025, 1, 19, 0, 0, 0, tzinfo=timezone.utc),
                page_title=f"Page_{i}",
                username=self.user,
            )

        # Call the commands to fill the aggregates tables
        call_command("fill_link_aggregates")
        call_command("fill_pageproject_aggregates")
        call_command("fill_user_aggregates")

        temp_dir = tempfile.gettempdir()

        try:
            # Call the command which dumps the events we just added.
            call_command("linkevents_archive", "dump", output=temp_dir)

            # Ensure the expected archives were generated.
            jan_16_archive = os.path.join(
                temp_dir, "links_linkevent_20250116_0.json.gz"
            )
            self.assertTrue(os.path.isfile(jan_16_archive))
            jan_17_archive = os.path.join(
                temp_dir, "links_linkevent_20250117_0.json.gz"
            )
            self.assertTrue(os.path.isfile(jan_17_archive))

            # Make sure the events that were archived got removed from the db.
            # We expect 5 since 10 of the 15 events were inserted before the
            # jobs started, but the remaining 5 were inserted afterwards.
            self.assertEqual(LinkEvent.objects.count(), 5)
        finally:
            pattern = os.path.join(temp_dir, "links_linkevent_*.json.gz")

            for file in glob.glob(pattern):
                os.remove(file)

    @mock.patch("swiftclient.client.Connection")
    def test_dump_with_partial_jobs(self, mock_swift_connection):
        """
        Test that no links are archived if not all of the jobs have run yet.
        """
        mock_conn = mock_swift_connection.return_value
        mock_conn.get_account.return_value = ({}, [])

        # Add the LinkEvent data that will be dumped.

        for i in range(5):
            LinkEventFactory(
                content_object=self.jstor_url_pattern,
                link=f"www.jstor.org/something_16_{i}",
                timestamp=datetime(2025, 1, 16, 0, 0, 0, tzinfo=timezone.utc),
                page_title=f"Page_{i}",
                username=self.user,
            )

        for i in range(5):
            LinkEventFactory(
                content_object=self.jstor_url_pattern,
                link=f"www.jstor.org/something_17_{i}",
                timestamp=datetime(2025, 1, 17, 0, 0, 0, tzinfo=timezone.utc),
                page_title=f"Page_{i}",
                username=self.user,
            )

        # Add cron job log entries since these are needed to automatically
        # determine safe dates for the job to filter by. One of the required
        # jobs is missing.

        # Call the commands to fill the aggregates tables
        call_command("fill_link_aggregates")
        call_command("fill_user_aggregates")

        temp_dir = tempfile.gettempdir()

        try:
            # Call the command which dumps the events we just added.
            call_command("linkevents_archive", "dump", output=temp_dir)

            # Ensure that no archives were generated.
            jan_16_archive = os.path.join(
                temp_dir, "links_linkevent_20250116_0.json.gz"
            )
            self.assertFalse(os.path.isfile(jan_16_archive))
            jan_17_archive = os.path.join(
                temp_dir, "links_linkevent_20250117_0.json.gz"
            )
            self.assertFalse(os.path.isfile(jan_17_archive))

            # Ensure that no LinkEvents were deleted.
            self.assertEqual(LinkEvent.objects.count(), 10)
        finally:
            pattern = os.path.join(temp_dir, "links_linkevent_*.json.gz")

            for file in glob.glob(pattern):
                os.remove(file)

    @mock.patch("swiftclient.client.Connection")
    def test_dump_with_no_jobs(self, mock_swift_connection):
        """
        Test that no links are archived if none of the jobs have run yet.
        """
        mock_conn = mock_swift_connection.return_value
        mock_conn.get_account.return_value = ({}, [])

        # Add the LinkEvent data that will be dumped.

        for i in range(5):
            LinkEventFactory(
                content_object=self.jstor_url_pattern,
                link=f"www.jstor.org/something_16_{i}",
                timestamp=datetime(2025, 1, 16, 0, 0, 0, tzinfo=timezone.utc),
                page_title=f"Page_{i}",
                username=self.user,
            )

        for i in range(5):
            LinkEventFactory(
                content_object=self.jstor_url_pattern,
                link=f"www.jstor.org/something_17_{i}",
                timestamp=datetime(2025, 1, 17, 0, 0, 0, tzinfo=timezone.utc),
                page_title=f"Page_{i}",
                username=self.user,
            )

        # Add cron job log entries since these are needed to automatically
        # determine safe dates for the job to filter by. None of the required
        # jobs are present.

        temp_dir = tempfile.gettempdir()

        try:
            # Call the command which dumps the events we just added.
            call_command("linkevents_archive", "dump", output=temp_dir)

            # Ensure that no archives were generated.
            jan_16_archive = os.path.join(
                temp_dir, "links_linkevent_20250116_0.json.gz"
            )
            self.assertFalse(os.path.isfile(jan_16_archive))
            jan_17_archive = os.path.join(
                temp_dir, "links_linkevent_20250117_0.json.gz"
            )
            self.assertFalse(os.path.isfile(jan_17_archive))

            # Ensure that no LinkEvents were deleted.
            self.assertEqual(LinkEvent.objects.count(), 10)
        finally:
            pattern = os.path.join(temp_dir, "links_linkevent_*.json.gz")

            for file in glob.glob(pattern):
                os.remove(file)

    @mock.patch.dict(
        os.environ,
        {
            "OPENSTACK_AUTH_URL": "fakeurl",
            "SWIFT_APPLICATION_CREDENTIAL_ID": "fakecredid",
            "SWIFT_APPLICATION_CREDENTIAL_SECRET": "fakecredsecret",
        },
    )
    @mock.patch("swiftclient.client.Connection")
    def test_dump_creates_swift_container(self, mock_swift_connection):
        """
        Test that Swift container is created if missing
        """
        for i in range(5):
            LinkEventFactory(
                content_object=self.jstor_url_pattern,
                link=f"www.jstor.org/something_16_{i}",
                timestamp=datetime(2021, 1, 16, 0, 0, 0, tzinfo=timezone.utc),
                page_title=f"Page_{i}",
                username=self.user,
            )

        mock_conn = mock_swift_connection.return_value
        # Simulate different containers
        mock_conn.get_account.return_value = (
            {},
            [
                {"name": "linkevents-backup-202012"},
                {"name": "linkevents-backup-202102"},
            ],
        )

        temp_dir = tempfile.gettempdir()

        try:
            # Call the command which dumps the events only for the 16th.
            call_command(
                "linkevents_archive",
                "dump",
                date=date(year=2021, month=1, day=16),
                output=temp_dir,
            )

            # Ensure `put_container` was called
            mock_conn.put_container.assert_called_with("archive-linkevents")

        finally:
            pattern = os.path.join(temp_dir, "links_linkevent_*.json.gz")

            for file in glob.glob(pattern):
                os.remove(file)

    @mock.patch("swiftclient.client.Connection")
    def test_dump_does_not_create_swift_container(self, mock_swift_connection):
        """
        Test that Swift container creation should not be called if
        container already exists
        """
        for i in range(5):
            LinkEventFactory(
                content_object=self.jstor_url_pattern,
                link=f"www.jstor.org/something_16_{i}",
                timestamp=datetime(2021, 1, 16, 0, 0, 0, tzinfo=timezone.utc),
                page_title=f"Page_{i}",
                username=self.user,
            )

        mock_conn = mock_swift_connection.return_value
        # Simulate existing container
        mock_conn.get_account.return_value = (
            {},
            [{"name": "archive-linkevents"}],
        )

        temp_dir = tempfile.gettempdir()

        try:
            # Call the command which dumps the events only for the 16th.
            call_command(
                "linkevents_archive",
                "dump",
                date=date(year=2021, month=1, day=16),
                output=temp_dir,
            )

            # Ensure `put_container` was NOT called
            mock_conn.put_container.assert_not_called()

        finally:
            pattern = os.path.join(temp_dir, "links_linkevent_*.json.gz")

            for file in glob.glob(pattern):
                os.remove(file)

    @mock.patch.dict(
        os.environ,
        {
            "OPENSTACK_AUTH_URL": "fakeurl",
            "SWIFT_APPLICATION_CREDENTIAL_ID": "fakecredid",
            "SWIFT_APPLICATION_CREDENTIAL_SECRET": "fakecredsecret",
        },
    )
    @mock.patch("swiftclient.client.Connection")
    def test_dump_creates_file_object_in_swift(self, mock_swift_connection):
        """
        Test that Swift object is created
        """
        for i in range(5):
            LinkEventFactory(
                content_object=self.jstor_url_pattern,
                link=f"www.jstor.org/something_16_{i}",
                timestamp=datetime(2021, 1, 16, 0, 0, 0, tzinfo=timezone.utc),
                page_title=f"Page_{i}",
                username=self.user,
            )

        mock_conn = mock_swift_connection.return_value
        # Simulate existing container
        mock_conn.get_account.return_value = (
            {},
            [{"name": "linkevents-backup-202101"}],
        )

        temp_dir = tempfile.gettempdir()

        try:
            # Call the command which dumps the events only for the 16th.
            call_command(
                "linkevents_archive",
                "dump",
                date=date(year=2021, month=1, day=16),
                output=temp_dir,
            )

            # Ensure `put_object` was called
            mock_conn.put_object.assert_called()

        finally:
            pattern = os.path.join(temp_dir, "links_linkevent_*.json.gz")

            for file in glob.glob(pattern):
                os.remove(file)

    @mock.patch.dict(
        os.environ,
        {
            "OPENSTACK_AUTH_URL": "fakeurl",
            "SWIFT_APPLICATION_CREDENTIAL_ID": "fakecredid",
            "SWIFT_APPLICATION_CREDENTIAL_SECRET": "fakecredsecret",
        },
    )
    @mock.patch("swiftclient.client.Connection")
    def test_dump_deletes_local_file(self, mock_swift_connection):
        """
        Test that local file is deleted if --object_storage_only flag
        is True
        """
        for i in range(5):
            LinkEventFactory(
                content_object=self.jstor_url_pattern,
                link=f"www.jstor.org/something_16_{i}",
                timestamp=datetime(2021, 1, 16, 0, 0, 0, tzinfo=timezone.utc),
                page_title=f"Page_{i}",
                username=self.user,
            )

        mock_conn = mock_swift_connection.return_value
        # Simulate existing container
        mock_conn.get_account.return_value = (
            {},
            [{"name": "linkevents-backup-202101"}],
        )

        temp_dir = tempfile.gettempdir()

        try:
            # Call the command which dumps the events only for the 16th.
            call_command(
                "linkevents_archive",
                "dump",
                "--object-storage-only",
                date=date(year=2021, month=1, day=16),
                output=temp_dir,
            )

            jan_16_archive = os.path.join(
                temp_dir, "links_linkevent_20210116_0.json.gz"
            )
            self.assertFalse(os.path.isfile(jan_16_archive))

        finally:
            # Just ensure cleanup
            pattern = os.path.join(temp_dir, "links_linkevent_*.json.gz")

            for file in glob.glob(pattern):
                os.remove(file)

    @mock.patch("swiftclient.client.Connection")
    def test_load(self, mock_swift_connection):
        """
        Test that we can load LinkEvents from an archive in the filesystem.
        Generate and dump some test data and, load it, and verify it looks as
        we expect it to.
        """
        mock_conn = mock_swift_connection.return_value
        mock_conn.get_account.return_value = ({}, [])

        # Generate some test data that we'll be dumping and loading.

        for i in range(5):
            LinkEventFactory(
                content_object=self.jstor_url_pattern,
                link=f"www.jstor.org/something_16_{i}",
                timestamp=datetime(2021, 1, 16, 0, 0, 0, tzinfo=timezone.utc),
                page_title=f"Page_{i}",
                username=self.user,
            )

        temp_dir = tempfile.gettempdir()

        try:
            # Call the command which dumps the events we just added.
            call_command(
                "linkevents_archive",
                "dump",
                date=date(year=2021, month=1, day=16),
                output=temp_dir,
            )

            # Make sure the events that were archived got removed from the db.
            self.assertEqual(LinkEvent.objects.count(), 0)

            # Load the LinkEvents from the archive we just created into the db.
            call_command(
                "linkevents_archive",
                "load",
                os.path.join(temp_dir, "links_linkevent_20210116_0.json.gz"),
            )

            # Verify that the expected amount of LinkEvents now exist.
            self.assertEqual(LinkEvent.objects.count(), 5)
        finally:
            pattern = os.path.join(temp_dir, "links_linkevent_*.json.gz")

            for file in glob.glob(pattern):
                os.remove(file)

    @mock.patch.dict(
        os.environ,
        {
            "OPENSTACK_AUTH_URL": "fakeurl",
            "SWIFT_APPLICATION_CREDENTIAL_ID": "fakecredid",
            "SWIFT_APPLICATION_CREDENTIAL_SECRET": "fakecredsecret",
        },
    )
    @mock.patch("swiftclient.client.Connection")
    def test_upload_successful(self, mock_swift_connection):
        """
        Test that we can upload a LinkEvents archive to Swift.
        """
        mock_conn = mock_swift_connection.return_value
        mock_conn.get_account.return_value = (
            {},
            [{"name": "linkevents-backup-202101"}],
        )

        temp_dir = tempfile.gettempdir()
        archive_filename = "links_linkevent_20210116_0.json.gz"
        archive_path = os.path.join(temp_dir, archive_filename)
        json_data = [
            {
                "model": "links.linkevent",
                "pk": 1,
                "fields": {
                    "link": "https://www.jstor.org/something_16",
                    "timestamp": "2021-01-16T00:00:00Z",
                    "domain": "en.wikipedia.org",
                    "content_type": None,
                    "object_id": None,
                    "username": 1,
                    "rev_id": None,
                    "user_id": None,
                    "page_title": "Page",
                    "page_namespace": 0,
                    "event_id": "event-id-1",
                    "user_is_bot": False,
                    "hash_link_event_id": "fakehash",
                    "change": 1,
                    "on_user_list": False,
                    "url": [123],
                },
            }
        ]

        with gzip.open(archive_path, "wt", encoding="utf-8") as f:
            json.dump(json_data, f)

        try:
            # Load the LinkEvents from the archive we just created into the db.
            call_command(
                "linkevents_archive",
                "upload",
                archive_path,
            )
            mock_conn.put_object.assert_called_once()

        finally:
            pattern = os.path.join(temp_dir, "links_linkevent_*.json.gz")

            for file in glob.glob(pattern):
                os.remove(file)

    @mock.patch("swiftclient.client.Connection")
    def test_upload_skip_non_existent_file(self, mock_swift_connection):
        """
        Test that non-existent files are skipped.
        """
        mock_conn = mock_swift_connection.return_value
        mock_conn.get_account.return_value = (
            {},
            [{"name": "linkevents-backup-202101"}],
        )

        fake_path = "/tmp/nonexistent_file.json.gz"

        with self.assertLogs("django", level="ERROR") as cm:
            # Load the LinkEvents from the archive we just created into the db.
            call_command(
                "linkevents_archive",
                "upload",
                fake_path,
            )

        self.assertTrue(
            any(
                f"File {fake_path} does not exist. Skipping" in msg for msg in cm.output
            )
        )

    @mock.patch.dict(
        os.environ,
        {
            "OPENSTACK_AUTH_URL": "",
            "SWIFT_APPLICATION_CREDENTIAL_ID": "",
            "SWIFT_APPLICATION_CREDENTIAL_SECRET": "",
        },
    )
    @mock.patch("swiftclient.client.Connection")
    def test_swift_credentials_not_set(self, mock_swift_connection):
        """
        Test that non-existent Swift credentials should output a message a skip upload.
        """
        mock_conn = mock_swift_connection.return_value
        mock_conn.get_account.return_value = (
            {},
            [{"name": "linkevents-backup-202101"}],
        )

        temp_dir = tempfile.gettempdir()
        archive_filename = "links_linkevent_20210116_test.json.gz"
        archive_path = os.path.join(temp_dir, archive_filename)
        json_data = [
            {
                "model": "links.linkevent",
                "pk": 1,
                "fields": {
                    "link": "https://www.jstor.org/something_16",
                    "timestamp": "2021-01-16T00:00:00Z",
                    "domain": "en.wikipedia.org",
                    "content_type": None,
                    "object_id": None,
                    "username": 1,
                    "rev_id": None,
                    "user_id": None,
                    "page_title": "Page",
                    "page_namespace": 0,
                    "event_id": "event-id-1",
                    "user_is_bot": False,
                    "hash_link_event_id": "fakehash",
                    "change": 1,
                    "on_user_list": False,
                    "url": [123],
                },
            }
        ]

        with gzip.open(archive_path, "wt", encoding="utf-8") as f:
            json.dump(json_data, f)

        try:
            with self.assertLogs("django", level="INFO") as cm:
                # Load the LinkEvents from the archive we just created into the db.
                call_command(
                    "linkevents_archive",
                    "upload",
                    archive_path,
                )
                self.assertTrue(
                    any(
                        "Swift credentials not provided. Skipping upload." in msg
                        for msg in cm.output
                    )
                )
        finally:
            pattern = os.path.join(temp_dir, "links_linkevent_*.json.gz")

            for file in glob.glob(pattern):
                os.remove(file)


class EZProxyRemovalCommandTest(TransactionTestCase):
    def setUp(self):
        self.user = UserFactory(username="jonsnow")

        self.jstor_organisation = OrganisationFactory(name="JSTOR")
        self.jstor_collection = CollectionFactory(
            name="JSTOR", organisation=self.jstor_organisation
        )
        self.jstor_url_pattern = URLPatternFactory(url="www.jstor.org")
        self.jstor_url_pattern.collections.add(self.jstor_collection)
        self.jstor_url_pattern.save()

        self.proquest_organisation = OrganisationFactory(name="ProQuest")
        self.proquest_collection = CollectionFactory(
            name="ProQuest", organisation=self.proquest_organisation
        )
        self.proquest_url_pattern = URLPatternFactory(
            url="www.proquest.com",
        )
        self.proquest_url_pattern.collections.add(self.proquest_collection)
        self.proquest_url_pattern.save()

        self.proxy_organisation = OrganisationFactory(
            name="Wikipedia Library OCLC EZProxy"
        )
        self.proxy_collection = CollectionFactory(
            name="EZProxy", organisation=self.proxy_organisation
        )
        self.proxy_url_pattern = URLPatternFactory(
            url="wikipedialibrary.idm.oclc",
        )
        self.proxy_url_pattern.collections.add(self.proxy_collection)
        self.proxy_url_pattern.save()

        self.linkevent_jstor1 = LinkEventFactory(
            content_object=self.jstor_url_pattern,
            link="www.jstor.org/dgsajdga",
            timestamp=datetime(2021, 1, 1, 15, 30, 35, tzinfo=timezone.utc),
            page_title="Page1",
            username=self.user,
        )

        self.linkevent_jstor2 = LinkEventFactory(
            content_object=self.jstor_url_pattern,
            link="www.jstor.org/eiuqwyeiu445",
            timestamp=datetime(2021, 1, 1, 18, 30, 35, tzinfo=timezone.utc),
            page_title="Page1",
            username=self.user,
        )

        self.linkevent_jstor_proxy = LinkEventFactory(
            content_object=self.proxy_url_pattern,
            link="www-jstor-org.wikipedialibrary.idm.oclc/eiuqwyeiu445",
            timestamp=datetime(2021, 1, 1, 22, 30, 35, tzinfo=timezone.utc),
            page_title="Page1",
            username=self.user,
        )

        self.linkevent_proquest1 = LinkEventFactory(
            link="www.proquest.com/vclmxvldfgf465",
            content_object=self.proquest_url_pattern,
            timestamp=datetime(2021, 1, 1, 15, 30, 35, tzinfo=timezone.utc),
            page_title="Page1",
            username=self.user,
        )
        self.linkevent_proquest2 = LinkEventFactory(
            link="www.proquest.com/dkjsahdj2893",
            content_object=self.proquest_url_pattern,
            timestamp=datetime(2021, 1, 1, 18, 30, 35, tzinfo=timezone.utc),
            page_title="Page1",
            username=self.user,
        )
        self.linkevent_proquest_proxy = LinkEventFactory(
            link="www-proquest-com.wikipedialibrary.idm.oclc/dhsauiydiuq8273",
            content_object=self.proxy_url_pattern,
            timestamp=datetime(2021, 1, 1, 22, 30, 35, tzinfo=timezone.utc),
            page_title="Page1",
            username=self.user,
        )

    def test_change_linkevents_to_non_ezproxy_collections_command(self):
        # Call the commands to fill the aggregates tables
        call_command("fill_link_aggregates")
        call_command("fill_pageproject_aggregates")
        call_command("fill_user_aggregates")

        # Assert the correct number of LinkEvents before deleting the proxy url
        # and collection
        self.assertEqual(self.jstor_url_pattern.link_events.count(), 2)
        self.assertEqual(self.proquest_url_pattern.link_events.count(), 2)
        self.assertEqual(self.proxy_url_pattern.link_events.count(), 2)

        # Assert the correct number of aggregates before deleting the proxy url
        # and collection
        self.assertEqual(LinkAggregate.objects.count(), 3)
        self.assertEqual(UserAggregate.objects.count(), 3)
        self.assertEqual(PageProjectAggregate.objects.count(), 3)

        call_command("remove_ezproxy_collection")

        self.assertEqual(self.jstor_url_pattern.link_events.count(), 3)
        self.assertEqual(self.proquest_url_pattern.link_events.count(), 3)
        self.assertEqual(LinkAggregate.objects.count(), 2)
        self.assertEqual(UserAggregate.objects.count(), 2)
        self.assertEqual(PageProjectAggregate.objects.count(), 2)


class FixOnUserListCommandTest(TransactionTestCase):
    def setUp(self):
        self.user1 = UserFactory(username="jonsnow")

        self.jstor_organisation = OrganisationFactory(name="JSTOR")
        self.jstor_organisation.username_list.add(self.user1)

        self.jstor_collection = CollectionFactory(
            name="JSTOR", organisation=self.jstor_organisation
        )
        self.jstor_url_pattern = URLPatternFactory(
            url="www.jstor.org", collection=self.jstor_collection
        )
        self.jstor_url_pattern.collections.set([self.jstor_collection])

        self.proquest_organisation = OrganisationFactory(name="ProQuest")
        self.proquest_organisation.username_list.add(self.user1)

        self.proquest_collection = CollectionFactory(
            name="ProQuest", organisation=self.proquest_organisation
        )
        self.proquest_url_pattern = URLPatternFactory(url="www.proquest.com")
        self.proquest_url_pattern.collections.set([self.proquest_collection])

        self.linkevent_jstor1 = LinkEventFactory(
            content_object=self.jstor_url_pattern,
            link="www.jstor.org/dgsajdga",
            timestamp=datetime(2021, 1, 1, 15, 30, 35, tzinfo=timezone.utc),
            page_title="Page1",
            username=self.user1,
            on_user_list=True,
        )
        self.linkevent_jstor2 = LinkEventFactory(
            link="www.jstor.org/eiuqwyeiu445",
            content_object=self.jstor_url_pattern,
            timestamp=datetime(2021, 1, 1, 18, 30, 35, tzinfo=timezone.utc),
            page_title="Page1",
            username=self.user1,
            on_user_list=True,
        )
        self.linkevent_jstor_proxy = LinkEventFactory(
            link="www-jstor-org.wikipedialibrary.idm.oclc/eiuqwyeiu445",
            content_object=self.jstor_url_pattern,
            timestamp=datetime(2021, 1, 1, 22, 30, 35, tzinfo=timezone.utc),
            page_title="Page1",
            username=self.user1,
            on_user_list=False,
        )
        self.linkevent_proquest1 = LinkEventFactory(
            link="www.proquest.com/vclmxvldfgf465",
            content_object=self.proquest_url_pattern,
            timestamp=datetime(2021, 1, 1, 15, 30, 35, tzinfo=timezone.utc),
            page_title="Page1",
            username=self.user1,
            on_user_list=True,
        )
        self.linkevent_proquest2 = LinkEventFactory(
            link="www.proquest.com/dkjsahdj2893",
            content_object=self.proquest_url_pattern,
            timestamp=datetime(2021, 1, 1, 18, 30, 35, tzinfo=timezone.utc),
            page_title="Page1",
            username=self.user1,
            on_user_list=True,
        )
        self.linkevent_proquest_proxy = LinkEventFactory(
            link="www-proquest-com.wikipedialibrary.idm.oclc/dhsauiydiuq8273",
            content_object=self.proquest_url_pattern,
            timestamp=datetime(2021, 1, 1, 22, 30, 35, tzinfo=timezone.utc),
            page_title="Page1",
            username=self.user1,
            on_user_list=False,
        )

    def test_fix_proxy_linkevents_on_user_list_command(self):
        # Call the commands to fill the aggregates tables
        call_command("fill_link_aggregates")
        call_command("fill_pageproject_aggregates")
        call_command("fill_user_aggregates")

        # Assert the correct number of LinkEvents before running the command to
        # fix the user list
        self.assertEqual(
            self.jstor_url_pattern.link_events.filter(on_user_list=True).count(),
            2,
        )
        self.assertEqual(
            self.jstor_url_pattern.link_events.filter(on_user_list=False).count(),
            1,
        )
        self.assertEqual(
            self.proquest_url_pattern.link_events.filter(on_user_list=True).count(),
            2,
        )
        self.assertEqual(
            self.proquest_url_pattern.link_events.filter(on_user_list=False).count(),
            1,
        )

        # Assert the correct number of aggregates before running the fix command
        self.assertEqual(LinkAggregate.objects.count(), 4)
        self.assertEqual(UserAggregate.objects.count(), 4)
        self.assertEqual(PageProjectAggregate.objects.count(), 4)

        call_command("fix_proxy_linkevents_on_user_list")

        self.assertEqual(
            self.jstor_url_pattern.link_events.filter(on_user_list=True).count(),
            3,
        )
        self.assertEqual(
            self.proquest_url_pattern.link_events.filter(on_user_list=True).count(),
            3,
        )
        self.assertEqual(
            self.jstor_url_pattern.link_events.filter(on_user_list=False).count(),
            0,
        )
        self.assertEqual(
            self.proquest_url_pattern.link_events.filter(on_user_list=False).count(),
            0,
        )
        self.assertEqual(LinkAggregate.objects.count(), 2)
        self.assertEqual(UserAggregate.objects.count(), 2)
        self.assertEqual(PageProjectAggregate.objects.count(), 2)
