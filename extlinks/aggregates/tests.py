import factory
from datetime import datetime, date, timedelta

from django.core.management import call_command
from django.test import TestCase

from .factories import (
    LinkAggregateFactory,
    UserAggregateFactory,
    PageProjectAggregateFactory,
)
from .models import LinkAggregate, UserAggregate, PageProjectAggregate
from extlinks.links.factories import LinkEventFactory, URLPatternFactory
from extlinks.organisations.factories import (
    CollectionFactory,
    OrganisationFactory,
    UserFactory,
)


class LinkAggregateCommandTest(TestCase):
    def setUp(self):
        # Creating one Collection
        self.organisation = OrganisationFactory(name="ACME Org")
        self.collection = CollectionFactory(name="ACME", organisation=self.organisation)
        self.url = URLPatternFactory(url="www.google.com", collection=self.collection)
        # Adding LinkEvents so that the command has information to parse.
        link_event_1 = LinkEventFactory(timestamp=datetime(2020, 1, 1, 15, 30, 35))
        link_event_1.url.add(self.url)
        link_event_1.save()

        link_event_2 = LinkEventFactory(timestamp=datetime(2020, 1, 1, 17, 40, 55))
        link_event_2.url.add(self.url)
        link_event_2.save()

        link_event_3 = LinkEventFactory(timestamp=datetime(2020, 1, 1, 19, 5, 42))
        link_event_3.url.add(self.url)
        link_event_3.save()

        link_event_4 = LinkEventFactory(timestamp=datetime(2020, 9, 10, 12, 9, 14))
        link_event_4.url.add(self.url)
        link_event_4.save()

        link_event_5 = LinkEventFactory(timestamp=datetime(2020, 9, 10, 12, 40, 50))
        link_event_5.url.add(self.url)
        link_event_5.save()

        link_event_6 = LinkEventFactory(timestamp=datetime(2020, 9, 10, 16, 52, 49))
        link_event_6.url.add(self.url)
        link_event_6.save()

        link_event_7 = LinkEventFactory(timestamp=datetime(2020, 9, 10, 17, 16, 30))
        link_event_7.url.add(self.url)
        link_event_7.save()

        link_event_8 = LinkEventFactory(timestamp=datetime(2020, 9, 10, 22, 36, 15))
        link_event_8.url.add(self.url)
        link_event_8.save()

    # Test when LinkAggregate table is empty
    def test_link_aggregate_table_empty(self):
        self.assertEqual(LinkAggregate.objects.count(), 0)

        call_command("fill_link_aggregates")

        self.assertEqual(LinkAggregate.objects.count(), 2)

        # Getting both LinkAggregates to check information is correct
        link_event_january = LinkAggregate.objects.get(day=1, month=1, year=2020)
        link_event_september = LinkAggregate.objects.get(day=10, month=9, year=2020)

        self.assertEqual(link_event_january.total_links_added, 3)
        self.assertEqual(link_event_september.total_links_added, 5)

    # Test when LinkAggregate table isn't empty
    def test_link_aggregate_table_with_data(self):
        yesterday = datetime.today() - timedelta(days=1)
        self.assertEqual(LinkAggregate.objects.count(), 0)
        # Add LinkAggregates NOTE: the last LinkAggregate date here needs to be
        # greater than the latest LinkEvent date in the setUp
        LinkAggregateFactory(full_date=date(2020, 1, 1))
        LinkAggregateFactory(full_date=date(2020, 2, 12))
        LinkAggregateFactory(full_date=date(2020, 2, 15))
        LinkAggregateFactory(full_date=date(2020, 9, 10))
        LinkAggregateFactory(full_date=date(2020, 9, 30))
        LinkAggregateFactory(full_date=date(2020, 9, 30))

        self.assertEqual(LinkAggregate.objects.count(), 6)

        # Add a LinkEvent from yesterday, a LinkAggregate with this date does not
        # exist yet
        yesterday_datetime = datetime(
            yesterday.year, yesterday.month, yesterday.day, 9, 18, 47
        )
        link_event = LinkEventFactory(timestamp=yesterday_datetime)
        link_event.url.add(self.url)
        link_event.save()

        call_command("fill_link_aggregates")

        self.assertEqual(LinkAggregate.objects.count(), 7)

        yesterday_link_aggregate = LinkAggregate.objects.get(
            full_date=yesterday.date(),
            collection=self.collection,
            organisation=self.organisation,
        )
        self.assertEqual(yesterday_link_aggregate.total_links_added, 1)

        # The eventstream container crashed and a LinkEvent was not added to
        # yesterday's aggregate
        yesterday_late_datetime = datetime(
            yesterday.year, yesterday.month, yesterday.day, 12, 30, 56
        )
        link_event_2 = LinkEventFactory(timestamp=yesterday_late_datetime)
        link_event_2.url.add(self.url)
        link_event_2.save()

        # That should be picked up by the command and add the new count
        call_command("fill_link_aggregates")

        # Now, the link aggregate should have 2 event links added in
        updated_link_aggregate = LinkAggregate.objects.get(
            organisation=self.organisation,
            collection=self.collection,
            full_date=yesterday.date(),
        )

        self.assertEqual(updated_link_aggregate.total_links_added, 2)
        self.assertEqual(updated_link_aggregate.total_links_removed, 0)


class UserAggregateCommandTest(TestCase):
    def setUp(self):
        # Creating one Collection
        self.organisation = OrganisationFactory(name="ACME Org")
        self.collection = CollectionFactory(name="ACME", organisation=self.organisation)
        self.url = URLPatternFactory(url="www.google.com", collection=self.collection)
        self.user = UserFactory(username="juannieve")
        self.user2 = UserFactory(username="jonsnow")

        # Adding LinkEvents so that the command has information to parse.
        link_event_1 = LinkEventFactory(timestamp=datetime(2020, 1, 1, 15, 30, 35))
        link_event_1.url.add(self.url)
        link_event_1.username = self.user
        link_event_1.save()

        link_event_2 = LinkEventFactory(timestamp=datetime(2020, 1, 1, 17, 40, 55))
        link_event_2.url.add(self.url)
        link_event_2.username = self.user2
        link_event_2.save()

        link_event_3 = LinkEventFactory(timestamp=datetime(2020, 1, 1, 19, 5, 42))
        link_event_3.url.add(self.url)
        link_event_3.username = self.user
        link_event_3.save()

        link_event_4 = LinkEventFactory(timestamp=datetime(2020, 9, 10, 12, 9, 14))
        link_event_4.url.add(self.url)
        link_event_4.username = self.user2
        link_event_4.save()

        link_event_5 = LinkEventFactory(timestamp=datetime(2020, 9, 10, 12, 40, 50))
        link_event_5.url.add(self.url)
        link_event_5.username = self.user
        link_event_5.save()

        link_event_6 = LinkEventFactory(timestamp=datetime(2020, 9, 10, 16, 52, 49))
        link_event_6.url.add(self.url)
        link_event_6.username = self.user
        link_event_6.save()

        link_event_7 = LinkEventFactory(timestamp=datetime(2020, 9, 10, 17, 16, 30))
        link_event_7.url.add(self.url)
        link_event_7.username = self.user
        link_event_7.save()

        link_event_8 = LinkEventFactory(timestamp=datetime(2020, 9, 10, 22, 36, 15))
        link_event_8.url.add(self.url)
        link_event_8.username = self.user2
        link_event_8.save()

    # Test when UserAggregate.table is empty
    def test_page_aggregate_table_empty(self):
        self.assertEqual(UserAggregate.objects.count(), 0)

        call_command("fill_user_aggregates")

        self.assertEqual(UserAggregate.objects.count(), 4)

        # Getting UserAggregate. to check information is correct
        juan_aggregate_january = UserAggregate.objects.get(
            day=1, month=1, year=2020, username="juannieve"
        )
        jon_aggregate_january = UserAggregate.objects.get(
            day=1, month=1, year=2020, username="jonsnow"
        )
        self.assertEqual(juan_aggregate_january.total_links_added, 2)
        self.assertEqual(jon_aggregate_january.total_links_added, 1)

        juan_aggregate_september = UserAggregate.objects.get(
            day=10, month=9, year=2020, username="juannieve"
        )
        jon_aggregate_september = UserAggregate.objects.get(
            day=10, month=9, year=2020, username="jonsnow"
        )
        self.assertEqual(juan_aggregate_september.total_links_added, 3)
        self.assertEqual(jon_aggregate_september.total_links_added, 2)

    def test_page_aggregate_table_with_data(self):
        yesterday = datetime.today() - timedelta(days=1)
        self.assertEqual(UserAggregate.objects.count(), 0)
        # Add UserAggregate. NOTE: the last UserAggregate.date here needs to be
        # greater than the latest LinkEvent date in the setUp
        UserAggregateFactory(full_date=date(2020, 1, 1))
        UserAggregateFactory(full_date=date(2020, 2, 12))
        UserAggregateFactory(full_date=date(2020, 2, 15))
        UserAggregateFactory(full_date=date(2020, 9, 10))
        UserAggregateFactory(full_date=date(2020, 9, 30))
        UserAggregateFactory(full_date=date(2020, 9, 30))

        self.assertEqual(UserAggregate.objects.count(), 6)

        # Add a LinkEvent from yesterday, a UserAggregate.with this date does not
        # exist yet
        yesterday_datetime = datetime(
            yesterday.year, yesterday.month, yesterday.day, 9, 18, 47
        )
        link_event = LinkEventFactory(timestamp=yesterday_datetime, username=self.user)
        link_event.url.add(self.url)
        link_event.save()

        call_command("fill_user_aggregates")

        self.assertEqual(UserAggregate.objects.count(), 7)

        yesterday_page_aggregate = UserAggregate.objects.get(
            full_date=yesterday.date(),
            username=self.user,
            collection=self.collection,
            organisation=self.organisation,
        )
        self.assertEqual(yesterday_page_aggregate.total_links_added, 1)

        # The eventstream container crashed and a LinkEvent was not added to
        # yesterday's aggregate
        yesterday_late_datetime = datetime(
            yesterday.year, yesterday.month, yesterday.day, 12, 30, 56
        )
        link_event_2 = LinkEventFactory(
            timestamp=yesterday_late_datetime, username=self.user
        )
        link_event_2.url.add(self.url)
        link_event_2.save()

        # That should be picked up by the command and add the new count
        call_command("fill_user_aggregates")

        # Now, the link aggregate should have 2 event links added in
        updated_page_aggregate = UserAggregate.objects.get(
            organisation=self.organisation,
            collection=self.collection,
            username=self.user,
            full_date=yesterday.date(),
        )

        self.assertEqual(updated_page_aggregate.total_links_added, 2)
        self.assertEqual(updated_page_aggregate.total_links_removed, 0)


class PageProjectAggregateCommandTest(TestCase):
    def setUp(self):
        # Creating one Collection
        self.organisation = OrganisationFactory(name="ACME Org")
        self.collection = CollectionFactory(name="ACME", organisation=self.organisation)
        self.url = URLPatternFactory(url="www.google.com", collection=self.collection)

        # Adding LinkEvents so that the command has information to parse.
        link_event_1 = LinkEventFactory(
            domain="en.wiki.org",
            page_title="Page1",
            timestamp=datetime(2020, 1, 1, 15, 30, 35),
        )
        link_event_1.url.add(self.url)
        link_event_1.save()

        link_event_2 = LinkEventFactory(
            domain="es.wiki.org",
            page_title="Page1",
            timestamp=datetime(2020, 1, 1, 17, 40, 55),
        )
        link_event_2.url.add(self.url)
        link_event_2.save()

        link_event_3 = LinkEventFactory(
            domain="es.wiki.org",
            page_title="Page1",
            timestamp=datetime(2020, 1, 1, 19, 5, 42),
        )
        link_event_3.url.add(self.url)
        link_event_3.save()

        link_event_4 = LinkEventFactory(
            domain="en.wiki.org",
            page_title="Page2",
            timestamp=datetime(2020, 9, 10, 12, 9, 14),
        )
        link_event_4.url.add(self.url)
        link_event_4.save()

        link_event_5 = LinkEventFactory(
            domain="en.wiki.org",
            page_title="Page1",
            timestamp=datetime(2020, 9, 10, 12, 40, 50),
        )
        link_event_5.url.add(self.url)
        link_event_5.save()

        link_event_6 = LinkEventFactory(
            domain="en.wiki.org",
            page_title="Page2",
            timestamp=datetime(2020, 9, 10, 16, 52, 49),
        )
        link_event_6.url.add(self.url)
        link_event_6.save()

        link_event_7 = LinkEventFactory(
            domain="en.wiki.org",
            page_title="Page1",
            timestamp=datetime(2020, 9, 10, 17, 16, 30),
        )
        link_event_7.url.add(self.url)
        link_event_7.save()

        link_event_8 = LinkEventFactory(
            domain="es.wiki.org",
            page_title="Page2",
            timestamp=datetime(2020, 9, 10, 22, 36, 15),
        )
        link_event_8.url.add(self.url)
        link_event_8.save()

    # Test when PageProjectAggregate table is empty
    def test_page_aggregate_table_empty(self):
        self.assertEqual(PageProjectAggregate.objects.count(), 0)

        call_command("fill_pageproject_aggregates")

        self.assertEqual(PageProjectAggregate.objects.count(), 5)

        # Getting PageProjectAggregates to check information is correct
        enwiki_page1_aggregate_january = PageProjectAggregate.objects.get(
            day=1, month=1, year=2020, project_name="en.wiki.org", page_name="Page1"
        )
        eswiki_page1_aggregate_january = PageProjectAggregate.objects.get(
            day=1, month=1, year=2020, project_name="es.wiki.org", page_name="Page1"
        )
        self.assertEqual(enwiki_page1_aggregate_january.total_links_added, 1)
        self.assertEqual(eswiki_page1_aggregate_january.total_links_added, 2)

        enwiki_page1_aggregate_september = PageProjectAggregate.objects.get(
            day=10, month=9, year=2020, project_name="en.wiki.org", page_name="Page1"
        )
        enwiki_page2_aggregate_september = PageProjectAggregate.objects.get(
            day=10, month=9, year=2020, project_name="en.wiki.org", page_name="Page2"
        )
        eswiki_page2_aggregate_september = PageProjectAggregate.objects.get(
            day=10, month=9, year=2020, project_name="es.wiki.org", page_name="Page2"
        )
        self.assertEqual(enwiki_page1_aggregate_september.total_links_added, 2)
        self.assertEqual(enwiki_page2_aggregate_september.total_links_added, 2)
        self.assertEqual(eswiki_page2_aggregate_september.total_links_added, 1)

    def test_page_aggregate_table_with_data(self):
        yesterday = datetime.today() - timedelta(days=1)
        self.assertEqual(PageProjectAggregate.objects.count(), 0)
        # Add PageProjectAggregates NOTE: the last PageProjectAggregate date here needs to be
        # greater than the latest LinkEvent date in the setUp
        PageProjectAggregateFactory(full_date=date(2020, 1, 1))
        PageProjectAggregateFactory(full_date=date(2020, 2, 12))
        PageProjectAggregateFactory(full_date=date(2020, 2, 15))
        PageProjectAggregateFactory(full_date=date(2020, 9, 10))
        PageProjectAggregateFactory(full_date=date(2020, 9, 30))
        PageProjectAggregateFactory(full_date=date(2020, 9, 30))

        self.assertEqual(PageProjectAggregate.objects.count(), 6)

        # Add a LinkEvent from yesterday, a PageProjectAggregate with this date does not
        # exist yet
        yesterday_datetime = datetime(
            yesterday.year, yesterday.month, yesterday.day, 9, 18, 47
        )
        link_event = LinkEventFactory(
            timestamp=yesterday_datetime, domain="en.wiki.org", page_title="Page1"
        )
        link_event.url.add(self.url)
        link_event.save()

        call_command("fill_pageproject_aggregates")

        self.assertEqual(PageProjectAggregate.objects.count(), 7)

        yesterday_page_aggregate = PageProjectAggregate.objects.get(
            full_date=yesterday.date(),
            project_name="en.wiki.org",
            page_name="Page1",
            collection=self.collection,
            organisation=self.organisation,
        )
        self.assertEqual(yesterday_page_aggregate.total_links_added, 1)

        # The eventstream container crashed and a LinkEvent was not added to
        # yesterday's aggregate
        yesterday_late_datetime = datetime(
            yesterday.year, yesterday.month, yesterday.day, 12, 30, 56
        )
        link_event_2 = LinkEventFactory(
            timestamp=yesterday_late_datetime, domain="en.wiki.org", page_title="Page1"
        )
        link_event_2.url.add(self.url)
        link_event_2.save()

        # That should be picked up by the command and add the new count
        call_command("fill_pageproject_aggregates")

        # Now, the link aggregate should have 2 event links added in
        updated_page_aggregate = PageProjectAggregate.objects.get(
            organisation=self.organisation,
            collection=self.collection,
            project_name="en.wiki.org",
            page_name="Page1",
            full_date=yesterday.date(),
        )

        self.assertEqual(updated_page_aggregate.total_links_added, 2)
        self.assertEqual(updated_page_aggregate.total_links_removed, 0)
