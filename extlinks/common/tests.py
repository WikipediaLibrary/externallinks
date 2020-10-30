from django.test import TestCase

from extlinks.links.factories import URLPatternFactory, LinkEventFactory
from extlinks.links.models import LinkEvent
from extlinks.organisations.factories import OrganisationFactory, CollectionFactory
from .helpers import annotate_top

# Write tests for annotate_top to check what happens with (distinct'ed)
# querysets, like we feed it currently, but where the underlying linkevents
# have multiple collections. Is it returning the right numbers?


class HelpersTest(TestCase):
    def setUp(self):
        self.org = OrganisationFactory()
        collection = CollectionFactory(organisation=self.org)
        collection2 = CollectionFactory(organisation=self.org)
        self.url1 = URLPatternFactory(url="foo.test.com", collection=collection)
        self.url2 = URLPatternFactory(url="test.com", collection=collection2)
        self.event1 = LinkEventFactory(link="test.com/url")
        self.event1.url.add(self.url1)
        self.event2 = LinkEventFactory(link="test.com/url2")
        self.event2.url.add(self.url1)
        self.event3 = LinkEventFactory(link="test.com/url3", domain="de.wikipedia.org")
        self.event3.url.add(self.url1)

    def test_annotate_top_simple(self):
        """
        Make sure annotate_top sends back data in the expected format, given
        some simple input.
        """
        queryset = LinkEvent.objects.filter(
            url__collection__organisation__pk=self.org.pk
        ).distinct()
        annotate_results = annotate_top(queryset, "-links_added", ["domain"])
        self.assertEqual(
            annotate_results[0],
            {"domain": "en.wikipedia.org", "links_added": 2, "links_removed": 0},
        )
        self.assertEqual(
            annotate_results[1],
            {"domain": "de.wikipedia.org", "links_added": 1, "links_removed": 0},
        )

    def test_annotate_top_multi_collection(self):
        """
        Make sure annotate_top sends back data in the expected format, when
        we have LinkEvents that correspond to multiple URLPatterns in the same
        organisation.
        """
        event4 = LinkEventFactory(link="test.com/url4")
        event4.url.add(self.url1)
        event4.url.add(self.url2)
        queryset = LinkEvent.objects.filter(
            url__collection__organisation__pk=self.org.pk
        ).distinct()
        print(len(queryset), "events go in.")
        annotate_results = annotate_top(queryset, "-links_added", ["domain"])
        self.assertEqual(
            annotate_results[0],
            {"domain": "en.wikipedia.org", "links_added": 3, "links_removed": 0},
        )
        self.assertEqual(
            annotate_results[1],
            {"domain": "de.wikipedia.org", "links_added": 1, "links_removed": 0},
        )
