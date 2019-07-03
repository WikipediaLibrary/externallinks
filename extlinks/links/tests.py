from django.core.management import call_command
from django.test import TestCase

from extlinks.organisations.factories import (OrganisationFactory,
                                              CollectionFactory)
from .factories import LinkEventFactory, URLPatternFactory
from .helpers import split_url_for_query
from .models import LinkEvent


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

    def test_get_organisations(self):
        """
        Test the get_organisations function on the LinkEvent model.
        """
        organisation1 = OrganisationFactory()
        organisation2 = OrganisationFactory()
        organisation3 = OrganisationFactory()

        collection1 = CollectionFactory(organisation=organisation1)
        collection2 = CollectionFactory(organisation=organisation2)
        collection3 = CollectionFactory(organisation=organisation2)

        urlpattern1 = URLPatternFactory(collection=collection1)
        urlpattern2 = URLPatternFactory(collection=collection2)
        urlpattern3 = URLPatternFactory(collection=collection3)

        new_link = LinkEventFactory()
        new_link.url.add(urlpattern1)
        new_link.url.add(urlpattern3)

        self.assertEqual(new_link.get_organisations,
                         [organisation1, organisation2])
