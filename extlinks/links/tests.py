from datetime import datetime, timezone

from django.core.management import call_command
from django.test import TestCase

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

        new_link = LinkEventFactory(urlpattern=url_pattern)

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
        self.url = URLPatternFactory(url="www.jstor.org", collection=self.collection1)
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


class EZProxyRemovalCommandTest(TestCase):
    def setUp(self):
        self.user = UserFactory(username="jonsnow")

        self.jstor_organisation = OrganisationFactory(name="JSTOR")
        self.jstor_collection = CollectionFactory(
            name="JSTOR", organisation=self.jstor_organisation
        )
        self.jstor_url_pattern = URLPatternFactory(
            url="www.jstor.org", collection=self.jstor_collection
        )

        self.proquest_organisation = OrganisationFactory(name="ProQuest")
        self.proquest_collection = CollectionFactory(
            name="ProQuest", organisation=self.proquest_organisation
        )
        self.proquest_url_pattern = URLPatternFactory(
            url="www.proquest.com", collection=self.proquest_collection
        )

        self.proxy_organisation = OrganisationFactory(
            name="Wikipedia Library OCLC EZProxy"
        )
        self.proxy_collection = CollectionFactory(
            name="EZProxy", organisation=self.proxy_organisation
        )
        self.proxy_url_pattern = URLPatternFactory(
            url="wikipedialibrary.idm.oclc", collection=self.proxy_collection
        )

        self.linkevent_jstor1 = LinkEventFactory(
            urlpattern=self.jstor_url_pattern,
            link="www.jstor.org/dgsajdga",
            timestamp=datetime(2021, 1, 1, 15, 30, 35, tzinfo=timezone.utc),
            page_title="Page1",
            username=self.user,
        )

        self.linkevent_jstor2 = LinkEventFactory(
            urlpattern=self.jstor_url_pattern,
            link="www.jstor.org/eiuqwyeiu445",
            timestamp=datetime(2021, 1, 1, 18, 30, 35, tzinfo=timezone.utc),
            page_title="Page1",
            username=self.user,
        )

        self.linkevent_jstor_proxy = LinkEventFactory(
            urlpattern=self.proxy_url_pattern,
            link="www-jstor-org.wikipedialibrary.idm.oclc/eiuqwyeiu445",
            timestamp=datetime(2021, 1, 1, 22, 30, 35, tzinfo=timezone.utc),
            page_title="Page1",
            username=self.user,
        )

        self.linkevent_proquest1 = LinkEventFactory(
            link="www.proquest.com/vclmxvldfgf465",
            urlpattern=self.proquest_url_pattern,
            timestamp=datetime(2021, 1, 1, 15, 30, 35, tzinfo=timezone.utc),
            page_title="Page1",
            username=self.user,
        )
        self.linkevent_proquest2 = LinkEventFactory(
            link="www.proquest.com/dkjsahdj2893",
            urlpattern=self.proquest_url_pattern,
            timestamp=datetime(2021, 1, 1, 18, 30, 35, tzinfo=timezone.utc),
            page_title="Page1",
            username=self.user,
        )
        self.linkevent_proquest_proxy = LinkEventFactory(
            link="www-proquest-com.wikipedialibrary.idm.oclc/dhsauiydiuq8273",
            urlpattern=self.proxy_url_pattern,
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
        self.assertEqual(
            LinkEvent.objects.filter(urlpattern=self.jstor_url_pattern).count(), 2
        )
        self.assertEqual(
            LinkEvent.objects.filter(urlpattern=self.proquest_url_pattern).count(), 2
        )
        self.assertEqual(
            LinkEvent.objects.filter(urlpattern=self.proxy_url_pattern).count(), 2
        )

        # Assert the correct number of aggregates before deleting the proxy url
        # and collection
        self.assertEqual(LinkAggregate.objects.count(), 3)
        self.assertEqual(UserAggregate.objects.count(), 3)
        self.assertEqual(PageProjectAggregate.objects.count(), 3)

        call_command("remove_ezproxy_collection")

        self.assertEqual(
            LinkEvent.objects.filter(urlpattern=self.jstor_url_pattern).count(), 3
        )
        self.assertEqual(
            LinkEvent.objects.filter(urlpattern=self.proquest_url_pattern).count(), 3
        )
        self.assertEqual(LinkAggregate.objects.count(), 2)
        self.assertEqual(UserAggregate.objects.count(), 2)
        self.assertEqual(PageProjectAggregate.objects.count(), 2)


class FixOnUserListCommandTest(TestCase):
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

        self.proquest_organisation = OrganisationFactory(name="ProQuest")
        self.proquest_organisation.username_list.add(self.user1)

        self.proquest_collection = CollectionFactory(
            name="ProQuest", organisation=self.proquest_organisation
        )
        self.proquest_url_pattern = URLPatternFactory(
            url="www.proquest.com", collection=self.proquest_collection
        )

        self.linkevent_jstor1 = LinkEventFactory(
            urlpattern=self.jstor_url_pattern,
            link="www.jstor.org/dgsajdga",
            timestamp=datetime(2021, 1, 1, 15, 30, 35, tzinfo=timezone.utc),
            page_title="Page1",
            username=self.user1,
            on_user_list=True,
        )
        self.linkevent_jstor2 = LinkEventFactory(
            link="www.jstor.org/eiuqwyeiu445",
            urlpattern=self.jstor_url_pattern,
            timestamp=datetime(2021, 1, 1, 18, 30, 35, tzinfo=timezone.utc),
            page_title="Page1",
            username=self.user1,
            on_user_list=True,
        )
        self.linkevent_jstor_proxy = LinkEventFactory(
            link="www-jstor-org.wikipedialibrary.idm.oclc/eiuqwyeiu445",
            urlpattern=self.jstor_url_pattern,
            timestamp=datetime(2021, 1, 1, 22, 30, 35, tzinfo=timezone.utc),
            page_title="Page1",
            username=self.user1,
            on_user_list=False,
        )
        self.linkevent_proquest1 = LinkEventFactory(
            link="www.proquest.com/vclmxvldfgf465",
            urlpattern=self.proquest_url_pattern,
            timestamp=datetime(2021, 1, 1, 15, 30, 35, tzinfo=timezone.utc),
            page_title="Page1",
            username=self.user1,
            on_user_list=True,
        )
        self.linkevent_proquest2 = LinkEventFactory(
            link="www.proquest.com/dkjsahdj2893",
            urlpattern=self.proquest_url_pattern,
            timestamp=datetime(2021, 1, 1, 18, 30, 35, tzinfo=timezone.utc),
            page_title="Page1",
            username=self.user1,
            on_user_list=True,
        )
        self.linkevent_proquest_proxy = LinkEventFactory(
            link="www-proquest-com.wikipedialibrary.idm.oclc/dhsauiydiuq8273",
            urlpattern=self.proquest_url_pattern,
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
            LinkEvent.objects.filter(
                urlpattern=self.jstor_url_pattern, on_user_list=True
            ).count(),
            2,
        )
        self.assertEqual(
            LinkEvent.objects.filter(
                urlpattern=self.jstor_url_pattern, on_user_list=False
            ).count(),
            1,
        )
        self.assertEqual(
            LinkEvent.objects.filter(
                urlpattern=self.proquest_url_pattern, on_user_list=True
            ).count(),
            2,
        )
        self.assertEqual(
            LinkEvent.objects.filter(
                urlpattern=self.proquest_url_pattern, on_user_list=False
            ).count(),
            1,
        )

        # Assert the correct number of aggregates before running the fix command
        self.assertEqual(LinkAggregate.objects.count(), 4)
        self.assertEqual(UserAggregate.objects.count(), 4)
        self.assertEqual(PageProjectAggregate.objects.count(), 4)

        call_command("fix_proxy_linkevents_on_user_list")

        self.assertEqual(
            LinkEvent.objects.filter(
                urlpattern=self.jstor_url_pattern, on_user_list=True
            ).count(),
            3,
        )
        self.assertEqual(
            LinkEvent.objects.filter(
                urlpattern=self.proquest_url_pattern, on_user_list=True
            ).count(),
            3,
        )
        self.assertEqual(
            LinkEvent.objects.filter(
                urlpattern=self.jstor_url_pattern, on_user_list=False
            ).count(),
            0,
        )
        self.assertEqual(
            LinkEvent.objects.filter(
                urlpattern=self.proquest_url_pattern, on_user_list=False
            ).count(),
            0,
        )
        self.assertEqual(LinkAggregate.objects.count(), 2)
        self.assertEqual(UserAggregate.objects.count(), 2)
        self.assertEqual(PageProjectAggregate.objects.count(), 2)
