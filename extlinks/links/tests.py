from django.core.management import call_command
from django.test import TestCase

from extlinks.organisations.factories import (OrganisationFactory,
                                              CollectionFactory)
from .factories import LinkEventFactory, URLPatternFactory
from .helpers import link_is_tracked, split_url_for_query
from .models import URLPattern


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
        organisation3 = OrganisationFactory()

        collection1 = CollectionFactory(organisation=organisation1)
        collection2 = CollectionFactory(organisation=organisation2)
        collection3 = CollectionFactory(organisation=organisation2)

        urlpattern1 = URLPatternFactory(collection=collection2)
        urlpattern2 = URLPatternFactory(collection=collection2)
        urlpattern3 = URLPatternFactory(collection=collection3)

        new_link = LinkEventFactory()
        new_link.url.add(urlpattern1)
        new_link.url.add(urlpattern3)

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
        self.assertTrue(link_is_tracked("https://www-test-com.wikipedialibrary.idm.oclc.org/"))

    def test_link_is_tracked_false_other_proxy(self):
        """
        Test that link_is_tracked returns False when we have a proxied URL
        that matches our URLPattern but isn't the TWL proxy
        """
        self.assertFalse(link_is_tracked("https://www-test-com.university.idm.oclc.org/"))


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
        self.assertEqual(test_urlpattern.get_proxied_url,
                         "platform-almanhal-com")
