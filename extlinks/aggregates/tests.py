import os
import shutil
import tempfile
import time_machine
import glob
import gzip
import json
import swiftclient

from datetime import datetime, date, timedelta, timezone
from dateutil.relativedelta import relativedelta
from unittest import mock

from django.contrib.contenttypes.models import ContentType
from django.core.management import call_command, CommandError
from django.test import TransactionTestCase

from extlinks.aggregates.management.helpers import (
    validate_link_aggregate_archive,
    validate_pageproject_aggregate_archive,
    validate_user_aggregate_archive,
)

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
from extlinks.organisations.models import Organisation
from ..links.models import URLPattern, LinkEvent


class BaseTransactionTest(TransactionTestCase):
    @classmethod
    def setUpClass(cls):
        super(BaseTransactionTest, cls).setUpClass()
        cls.tenacity_patcher = mock.patch('tenacity.nap.time')
        cls.mock_tenacity = cls.tenacity_patcher.start()

    @classmethod
    def tearDownClass(cls):
        super(BaseTransactionTest, cls).tearDownClass()
        cls.tenacity_patcher.stop()

class LinkAggregateCommandTest(BaseTransactionTest):
    def setUp(self):
        # Creating one Collection
        self.organisation = OrganisationFactory(name="ACME Org")
        self.collection = CollectionFactory(name="ACME", organisation=self.organisation)
        self.url = URLPatternFactory(url="www.google.com")
        self.url.collections.add(self.collection)
        self.url.save()
        # Adding LinkEvents so that the command has information to parse.
        LinkEventFactory(
            content_object=self.url,
            timestamp=datetime(2020, 1, 1, 15, 30, 35, tzinfo=timezone.utc),
        )
        LinkEventFactory(
            content_object=self.url,
            timestamp=datetime(2020, 1, 1, 17, 40, 55, tzinfo=timezone.utc),
        )
        LinkEventFactory(
            content_object=self.url,
            timestamp=datetime(2020, 1, 1, 19, 5, 42, tzinfo=timezone.utc),
        )
        LinkEventFactory(
            content_object=self.url,
            timestamp=datetime(2020, 9, 10, 12, 9, 14, tzinfo=timezone.utc),
        )
        LinkEventFactory(
            content_object=self.url,
            timestamp=datetime(2020, 9, 10, 12, 40, 50, tzinfo=timezone.utc),
        )
        LinkEventFactory(
            content_object=self.url,
            timestamp=datetime(2020, 9, 10, 16, 52, 49, tzinfo=timezone.utc),
        )
        LinkEventFactory(
            content_object=self.url,
            timestamp=datetime(2020, 9, 10, 17, 16, 30, tzinfo=timezone.utc),
        )
        LinkEventFactory(
            content_object=self.url,
            timestamp=datetime(2020, 9, 10, 22, 36, 15, tzinfo=timezone.utc),
        )

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
        LinkAggregateFactory(full_date=date(2020, 1, 11))
        LinkAggregateFactory(full_date=date(2020, 2, 12))
        LinkAggregateFactory(full_date=date(2020, 2, 15))
        LinkAggregateFactory(full_date=date(2020, 9, 10))
        LinkAggregateFactory(full_date=date(2020, 9, 30))
        LinkAggregateFactory(full_date=date(2020, 9, 30))

        self.assertEqual(LinkAggregate.objects.count(), 6)

        # Add a LinkEvent from yesterday, a LinkAggregate with this date does not
        # exist yet
        yesterday_datetime = datetime(
            yesterday.year,
            yesterday.month,
            yesterday.day,
            9,
            18,
            47,
            tzinfo=timezone.utc,
        )
        LinkEventFactory(content_object=self.url, timestamp=yesterday_datetime)

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
            yesterday.year,
            yesterday.month,
            yesterday.day,
            12,
            30,
            56,
            tzinfo=timezone.utc,
        )
        LinkEventFactory(content_object=self.url, timestamp=yesterday_late_datetime)

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

    def test_link_aggregate_with_argument(self):
        # Create a new collection and some LinkEvents associated with it
        new_collection = CollectionFactory(name="Monsters Inc")
        url_pattern = URLPatternFactory(
            url="www.duckduckgo.com",
        )
        url_pattern.collections.add(new_collection)
        url_pattern.save()

        LinkEventFactory(
            content_object=url_pattern,
            timestamp=datetime(2020, 1, 1, 15, 30, 35, tzinfo=timezone.utc),
        )
        LinkEventFactory(
            content_object=url_pattern,
            timestamp=datetime(2020, 4, 23, 8, 50, 13, tzinfo=timezone.utc),
        )

        self.assertEqual(
            LinkAggregate.objects.filter(collection=new_collection).count(), 0
        )

        call_command("fill_link_aggregates", collections=[new_collection.pk])

        self.assertEqual(
            LinkAggregate.objects.filter(collection=new_collection).count(), 2
        )

    def test_link_aggregate_with_argument_error(self):
        # Calling the command with a collection that doesn't exist
        with self.assertRaises(CommandError):
            call_command("fill_link_aggregates", collections=[9999999])

    def test_link_aggregate_with_argument_delete_org(self):
        # Create a new collection and some LinkEvents associated with it
        # Then delete the organisation
        new_organisation = OrganisationFactory(name="Pixar")
        new_collection = CollectionFactory(
            name="Monsters Inc", organisation=new_organisation
        )
        url_pattern = URLPatternFactory(url="www.pixar.com", collection=new_collection)
        url_pattern.collections.add(new_collection)
        url_pattern.save()

        LinkEventFactory(
            content_object=url_pattern,
            timestamp=datetime(2020, 1, 1, 15, 30, 35, tzinfo=timezone.utc),
        )
        LinkEventFactory(
            content_object=url_pattern,
            timestamp=datetime(2020, 4, 23, 8, 50, 13, tzinfo=timezone.utc),
        )

        self.assertEqual(
            LinkAggregate.objects.filter(collection=new_collection).count(), 0
        )

        call_command("fill_link_aggregates", collections=[new_collection.pk])

        self.assertEqual(
            LinkAggregate.objects.filter(collection=new_collection).count(), 2
        )

        LinkEventFactory(
            content_object=url_pattern,
            timestamp=datetime(2020, 4, 25, 8, 50, 13, tzinfo=timezone.utc),
        )

        # Delete the organisation
        Organisation.objects.filter(pk=new_organisation.pk).delete()

        with self.assertRaises(CommandError):
            # No collection was returned
            call_command("fill_link_aggregates", collections=[new_collection.pk])


class UserAggregateCommandTest(BaseTransactionTest):
    def setUp(self):
        # Creating one Collection
        self.organisation = OrganisationFactory(name="ACME Org")
        self.collection = CollectionFactory(name="ACME", organisation=self.organisation)
        self.url = URLPatternFactory(url="www.google.com")
        self.url.collections.add(self.collection)
        self.url.save()
        self.user = UserFactory(username="juannieve")
        self.user2 = UserFactory(username="jonsnow")

        # Adding LinkEvents so that the command has information to parse.
        LinkEventFactory(
            content_object=self.url,
            username=self.user,
            timestamp=datetime(2020, 1, 1, 15, 30, 35, tzinfo=timezone.utc),
        )

        LinkEventFactory(
            content_object=self.url,
            username=self.user2,
            timestamp=datetime(2020, 1, 1, 17, 40, 55, tzinfo=timezone.utc),
        )

        LinkEventFactory(
            content_object=self.url,
            username=self.user,
            timestamp=datetime(2020, 1, 1, 19, 5, 42, tzinfo=timezone.utc),
        )

        LinkEventFactory(
            content_object=self.url,
            username=self.user2,
            timestamp=datetime(2020, 9, 10, 12, 9, 14, tzinfo=timezone.utc),
        )

        LinkEventFactory(
            content_object=self.url,
            username=self.user,
            timestamp=datetime(2020, 9, 10, 12, 40, 50, tzinfo=timezone.utc),
        )

        LinkEventFactory(
            content_object=self.url,
            username=self.user,
            timestamp=datetime(2020, 9, 10, 16, 52, 49, tzinfo=timezone.utc),
        )

        LinkEventFactory(
            content_object=self.url,
            username=self.user,
            timestamp=datetime(2020, 9, 10, 17, 16, 30, tzinfo=timezone.utc),
        )

        LinkEventFactory(
            content_object=self.url,
            username=self.user2,
            timestamp=datetime(2020, 9, 10, 22, 36, 15, tzinfo=timezone.utc),
        )

    # Test when UserAggregate.table is empty
    def test_user_aggregate_table_empty(self):
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

    def test_user_aggregate_table_with_data(self):
        yesterday = datetime.today() - timedelta(days=1)
        self.assertEqual(UserAggregate.objects.count(), 0)
        # Add UserAggregate. NOTE: the last UserAggregate.date here needs to be
        # greater than the latest LinkEvent date in the setUp
        UserAggregateFactory(full_date=date(2020, 1, 11))
        UserAggregateFactory(full_date=date(2020, 2, 12))
        UserAggregateFactory(full_date=date(2020, 2, 15))
        UserAggregateFactory(full_date=date(2020, 9, 10))
        UserAggregateFactory(full_date=date(2020, 9, 30))
        UserAggregateFactory(full_date=date(2020, 9, 30))

        self.assertEqual(UserAggregate.objects.count(), 6)

        # Add a LinkEvent from yesterday, a UserAggregate.with this date does not
        # exist yet
        yesterday_datetime = datetime(
            yesterday.year,
            yesterday.month,
            yesterday.day,
            9,
            18,
            47,
            tzinfo=timezone.utc,
        )
        LinkEventFactory(
            content_object=self.url, timestamp=yesterday_datetime, username=self.user
        )

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
            yesterday.year,
            yesterday.month,
            yesterday.day,
            12,
            30,
            56,
            tzinfo=timezone.utc,
        )
        LinkEventFactory(
            content_object=self.url,
            timestamp=yesterday_late_datetime,
            username=self.user,
        )

        # That should be picked up by the command and add the new count
        call_command("fill_user_aggregates")

        # Now, the link aggregate should have 2 event links added in
        updated_user_aggregate = UserAggregate.objects.get(
            organisation=self.organisation,
            collection=self.collection,
            username=self.user,
            full_date=yesterday.date(),
        )

        self.assertEqual(updated_user_aggregate.total_links_added, 2)
        self.assertEqual(updated_user_aggregate.total_links_removed, 0)

    def test_user_aggregate_with_argument(self):
        # Create a new collection and some LinkEvents associated with it
        new_collection = CollectionFactory(name="Monsters Inc")
        url_pattern = URLPatternFactory(
            url="www.duckduckgo.com", collection=new_collection
        )
        url_pattern.collections.add(new_collection)
        url_pattern.save()
        # Creating a collection and LinkEvent that won't be run in the command
        other_collection = CollectionFactory(name="Unused")
        other_url_pattern = URLPatternFactory(
            url="www.notusingthis.com", collection=other_collection
        )
        other_url_pattern.collections.add(other_collection)
        other_url_pattern.save()

        LinkEventFactory(
            content_object=url_pattern,
            timestamp=datetime(2020, 1, 1, 15, 30, 35, tzinfo=timezone.utc),
        )

        LinkEventFactory(
            content_object=url_pattern,
            timestamp=datetime(2020, 4, 23, 8, 50, 13, tzinfo=timezone.utc),
        )

        LinkEventFactory(content_object=other_url_pattern)

        self.assertEqual(
            UserAggregate.objects.filter(collection=new_collection).count(),
            0,
        )

        call_command("fill_user_aggregates", collections=[new_collection.pk])

        self.assertEqual(
            UserAggregate.objects.filter(collection=new_collection).count(),
            2,
        )

    def test_user_aggregate_with_argument_error(self):
        # Calling the command with a collection that doesn't exist
        with self.assertRaises(CommandError):
            call_command("fill_user_aggregates", collections=[9999999])

    def test_user_aggregate_with_argument_delete_org(self):
        # Create a new collection and some LinkEvents associated with it
        # Then delete the organisation
        new_organisation = OrganisationFactory(name="Pixar")
        new_collection = CollectionFactory(
            name="Monsters Inc", organisation=new_organisation
        )
        url_pattern = URLPatternFactory(url="www.pixar.com", collection=new_collection)
        url_pattern.collections.add(new_collection)
        url_pattern.save()

        LinkEventFactory(
            content_object=url_pattern,
            timestamp=datetime(2020, 1, 1, 15, 30, 35, tzinfo=timezone.utc),
        )

        LinkEventFactory(
            content_object=url_pattern,
            timestamp=datetime(2020, 4, 23, 8, 50, 13, tzinfo=timezone.utc),
        )

        self.assertEqual(
            UserAggregate.objects.filter(collection=new_collection).count(), 0
        )

        call_command("fill_user_aggregates", collections=[new_collection.pk])

        self.assertEqual(
            UserAggregate.objects.filter(collection=new_collection).count(), 2
        )

        LinkEventFactory(
            content_object=url_pattern,
            timestamp=datetime(2020, 4, 25, 8, 50, 13, tzinfo=timezone.utc),
        )

        # Delete the organisation
        Organisation.objects.filter(pk=new_organisation.pk).delete()

        with self.assertRaises(CommandError):
            # No collection was returned
            call_command("fill_user_aggregates", collections=[new_collection.pk])


class PageProjectAggregateCommandTest(BaseTransactionTest):
    def setUp(self):
        # Creating one Collection
        self.organisation = OrganisationFactory(name="ACME Org")
        self.collection = CollectionFactory(name="ACME", organisation=self.organisation)
        self.url = URLPatternFactory(url="www.google.com")
        self.url.collections.add(self.collection)
        self.url.save()
        # Adding LinkEvents so that the command has information to parse.
        LinkEventFactory(
            content_object=self.url,
            domain="en.wiki.org",
            page_title="Page1",
            timestamp=datetime(2020, 1, 1, 15, 30, 35, tzinfo=timezone.utc),
        )

        LinkEventFactory(
            content_object=self.url,
            domain="es.wiki.org",
            page_title="Page1",
            timestamp=datetime(2020, 1, 1, 17, 40, 55, tzinfo=timezone.utc),
        )

        LinkEventFactory(
            content_object=self.url,
            domain="es.wiki.org",
            page_title="Page1",
            timestamp=datetime(2020, 1, 1, 19, 5, 42, tzinfo=timezone.utc),
        )

        LinkEventFactory(
            content_object=self.url,
            domain="en.wiki.org",
            page_title="Page2",
            timestamp=datetime(2020, 9, 10, 12, 9, 14, tzinfo=timezone.utc),
        )

        LinkEventFactory(
            content_object=self.url,
            domain="en.wiki.org",
            page_title="Page1",
            timestamp=datetime(2020, 9, 10, 12, 40, 50, tzinfo=timezone.utc),
        )

        LinkEventFactory(
            content_object=self.url,
            domain="en.wiki.org",
            page_title="Page2",
            timestamp=datetime(2020, 9, 10, 16, 52, 49, tzinfo=timezone.utc),
        )

        LinkEventFactory(
            content_object=self.url,
            domain="en.wiki.org",
            page_title="Page1",
            timestamp=datetime(2020, 9, 10, 17, 16, 30, tzinfo=timezone.utc),
        )

        LinkEventFactory(
            content_object=self.url,
            domain="es.wiki.org",
            page_title="Page2",
            timestamp=datetime(2020, 9, 10, 22, 36, 15, tzinfo=timezone.utc),
        )

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
        PageProjectAggregateFactory(full_date=date(2020, 1, 11))
        PageProjectAggregateFactory(full_date=date(2020, 2, 12))
        PageProjectAggregateFactory(full_date=date(2020, 2, 15))
        PageProjectAggregateFactory(full_date=date(2020, 9, 10))
        PageProjectAggregateFactory(full_date=date(2020, 9, 30))
        PageProjectAggregateFactory(full_date=date(2020, 9, 30))

        self.assertEqual(PageProjectAggregate.objects.count(), 6)

        # Add a LinkEvent from yesterday, a PageProjectAggregate with this date does not
        # exist yet
        yesterday_datetime = datetime(
            yesterday.year,
            yesterday.month,
            yesterday.day,
            9,
            18,
            47,
            tzinfo=timezone.utc,
        )
        LinkEventFactory(
            content_object=self.url,
            timestamp=yesterday_datetime,
            domain="en.wiki.org",
            page_title="Page1",
        )

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
            yesterday.year,
            yesterday.month,
            yesterday.day,
            12,
            30,
            56,
            tzinfo=timezone.utc,
        )
        LinkEventFactory(
            content_object=self.url,
            timestamp=yesterday_late_datetime,
            domain="en.wiki.org",
            page_title="Page1",
        )

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

    def test_pageproject_aggregate_with_argument(self):
        # Create a new collection and some LinkEvents associated with it
        new_collection = CollectionFactory(name="Monsters Inc")
        url_pattern = URLPatternFactory(url="www.duckduckgo.com")
        url_pattern.collections.add(new_collection)
        url_pattern.save()
        # Creating a collection and LinkEvent that won't be run in the command
        other_collection = CollectionFactory(name="Unused")
        other_url_pattern = URLPatternFactory(url="www.notusingthis.com")
        other_url_pattern.collections.add(other_collection)
        other_url_pattern.save()

        LinkEventFactory(
            content_object=url_pattern,
            timestamp=datetime(2020, 1, 1, 15, 30, 35, tzinfo=timezone.utc),
        )

        LinkEventFactory(
            content_object=url_pattern,
            timestamp=datetime(2020, 4, 23, 8, 50, 13, tzinfo=timezone.utc),
        )

        LinkEventFactory(content_object=other_url_pattern)

        self.assertEqual(
            PageProjectAggregate.objects.filter(collection=new_collection).count(),
            0,
        )

        call_command("fill_pageproject_aggregates", collections=[new_collection.pk])

        self.assertEqual(
            PageProjectAggregate.objects.filter(collection=new_collection).count(),
            2,
        )

    def test_pageproject_aggregate_with_argument_error(self):
        # Calling the command with a collection that doesn't exist
        with self.assertRaises(CommandError):
            call_command("fill_pageproject_aggregates", collections=[9999999])

    def test_pageproject_aggregate_with_argument_delete_org(self):
        # Create a new collection and some LinkEvents associated with it
        # Then delete the organisation
        new_organisation = OrganisationFactory(name="Pixar")
        new_collection = CollectionFactory(
            name="Monsters Inc", organisation=new_organisation
        )
        url_pattern = URLPatternFactory(url="www.pixar.com")
        url_pattern.collections.add(new_collection)
        url_pattern.save()

        LinkEventFactory(
            content_object=url_pattern,
            timestamp=datetime(2020, 1, 1, 15, 30, 35, tzinfo=timezone.utc),
        )

        LinkEventFactory(
            content_object=url_pattern,
            timestamp=datetime(2020, 4, 23, 8, 50, 13, tzinfo=timezone.utc),
        )

        self.assertEqual(
            PageProjectAggregate.objects.filter(collection=new_collection).count(), 0
        )

        call_command("fill_pageproject_aggregates", collections=[new_collection.pk])

        self.assertEqual(
            PageProjectAggregate.objects.filter(collection=new_collection).count(), 2
        )

        LinkEventFactory(
            timestamp=datetime(2020, 4, 25, 8, 50, 13, tzinfo=timezone.utc)
        )

        # Delete the organisation
        Organisation.objects.filter(pk=new_organisation.pk).delete()

        with self.assertRaises(CommandError):
            # No collection was returned
            call_command("fill_pageproject_aggregates", collections=[new_collection.pk])


class MonthlyLinkAggregateCommandTest(BaseTransactionTest):
    def setUp(self):
        self.organisation = OrganisationFactory(name="ACME Org")
        self.collection = CollectionFactory(name="ACME", organisation=self.organisation)

        # 10 entries for Jan 2024 (first 10 days)
        self.expected_total_added = 0
        self.expected_total_removed = 0
        for day in range(1, 11):
            self.expected_total_added += day
            self.expected_total_removed += day - 1
            LinkAggregateFactory(
                full_date=date(2024, 1, day),
                organisation=self.organisation,
                collection=self.collection,
                total_links_added=day,
                total_links_removed=day - 1,
            )

    def test_aggregate_monthly_data(self):
        with time_machine.travel(date(2024, 2, 11)):
            self.assertEqual(LinkAggregate.objects.filter(day=0).count(), 0)

            call_command("fill_monthly_link_aggregates")

            self.assertEqual(LinkAggregate.objects.filter(day=0).count(), 1)

            monthly_aggregate = LinkAggregate.objects.get(year=2024, month=1, day=0)
            self.assertEqual(
                self.expected_total_added, monthly_aggregate.total_links_added
            )
            self.assertEqual(
                self.expected_total_removed, monthly_aggregate.total_links_removed
            )

    def test_no_aggregation_when_no_new_data(self):
        with time_machine.travel(date(2024, 2, 11)):
            call_command("fill_monthly_link_aggregates")

            # Running it again should NOT create duplicate entries
            call_command("fill_monthly_link_aggregates")
            self.assertEqual(LinkAggregate.objects.filter(day=0).count(), 1)

    def test_aggregate_next_month(self):
        with time_machine.travel(date(2024, 2, 11)):
            call_command("fill_monthly_link_aggregates")

        with time_machine.travel(date(2024, 3, 11)):
            # Simulate next month by adding 5 more days to the next month
            next_total_added = 0
            next_total_removed = 0
            for day in range(15, 20):
                next_total_added += day
                next_total_removed += day - 1
                LinkAggregateFactory(
                    full_date=date(2024, 2, day),
                    organisation=self.organisation,
                    collection=self.collection,
                    total_links_added=day,
                    total_links_removed=day - 1,
                )
            call_command("fill_monthly_link_aggregates")

            self.assertEqual(LinkAggregate.objects.filter(day=0).count(), 2)
            monthly_aggregate = LinkAggregate.objects.get(year=2024, month=2, day=0)
            # Should still be the same
            self.assertEqual(next_total_added, monthly_aggregate.total_links_added)
            self.assertEqual(next_total_removed, monthly_aggregate.total_links_removed)
            self.assertEqual(LinkAggregate.objects.exclude(day=0).count(), 0)

    def test_specific_collection_aggregation(self):
        with time_machine.travel(date(2024, 2, 11)):
            other_collection = CollectionFactory(
                name="Other Collection", organisation=self.organisation
            )
            for day in range(1, 6):
                LinkAggregateFactory(
                    full_date=date(2024, 1, day),
                    organisation=self.organisation,
                    collection=other_collection,
                    total_links_added=day * 2,
                    total_links_removed=day * 2 - 1,
                )

            call_command(
                "fill_monthly_link_aggregates", collections=[other_collection.pk]
            )

            self.assertEqual(
                LinkAggregate.objects.filter(
                    day=0, collection=other_collection
                ).count(),
                1,
            )
            self.assertEqual(
                LinkAggregate.objects.filter(day=0, collection=self.collection).count(),
                0,
            )

    def test_specific_year_month(self):
        # Adding different month data
        for day in range(1, 11):
            LinkAggregateFactory(
                full_date=date(2024, 2, day),
                organisation=self.organisation,
                collection=self.collection,
                total_links_added=day,
                total_links_removed=day - 1,
            )

        with time_machine.travel(date(2024, 5, 11)):
            call_command("fill_monthly_link_aggregates", year_month="2024-01")

            self.assertEqual(LinkAggregate.objects.filter(day=0).count(), 1)

            monthly_aggregate = LinkAggregate.objects.get(year=2024, month=1, day=0)
            self.assertEqual(
                self.expected_total_added, monthly_aggregate.total_links_added
            )
            self.assertEqual(
                self.expected_total_removed, monthly_aggregate.total_links_removed
            )


class MonthlyUserAggregateCommandTest(BaseTransactionTest):
    def setUp(self):
        self.organisation = OrganisationFactory(name="ACME Org")
        self.collection = CollectionFactory(name="ACME", organisation=self.organisation)
        self.user = UserFactory(username="juannieve")

        # 10 entries for Jan 2024 (first 10 days)
        self.expected_total_added = 0
        self.expected_total_removed = 0
        for day in range(1, 11):
            self.expected_total_added += day
            self.expected_total_removed += day - 1
            UserAggregateFactory(
                full_date=date(2024, 1, day),
                organisation=self.organisation,
                collection=self.collection,
                username=self.user,
                total_links_added=day,
                total_links_removed=day - 1,
            )

        # 10 more entries for Jan 2024 with a different user
        self.user2 = UserFactory(username="jonsnow")
        for day in range(1, 11):
            UserAggregateFactory(
                full_date=date(2024, 1, day),
                organisation=self.organisation,
                collection=self.collection,
                username=self.user2,
                total_links_added=day + 1,
                total_links_removed=day + 2,
            )

    def test_aggregate_monthly_data(self):
        with time_machine.travel(date(2024, 2, 11)):
            self.assertEqual(UserAggregate.objects.filter(day=0).count(), 0)

            call_command("fill_monthly_user_aggregates")

            self.assertEqual(UserAggregate.objects.filter(day=0).count(), 2)

            monthly_aggregate = UserAggregate.objects.get(
                year=2024, month=1, day=0, username=self.user
            )
            self.assertEqual(
                self.expected_total_added, monthly_aggregate.total_links_added
            )
            self.assertEqual(
                self.expected_total_removed, monthly_aggregate.total_links_removed
            )

    def test_no_aggregation_when_no_new_data(self):
        with time_machine.travel(date(2024, 2, 11)):
            call_command("fill_monthly_user_aggregates")

            # Running it again should NOT create duplicate entries
            call_command("fill_monthly_user_aggregates")
            self.assertEqual(UserAggregate.objects.filter(day=0).count(), 2)

    def test_aggregate_new_data_same_month(self):
        """
        Simulating running the script again, in case we receive new data
        """
        with time_machine.travel(date(2024, 2, 11)):
            call_command("fill_monthly_user_aggregates")

            # Adding more data to the same month should add up to the totals
            for day in range(1, 11):
                self.expected_total_added += day
                self.expected_total_removed += day - 1
                UserAggregateFactory(
                    full_date=date(2024, 1, day),
                    organisation=self.organisation,
                    collection=self.collection,
                    username=self.user,
                    total_links_added=day,
                    total_links_removed=day - 1,
                )
            call_command("fill_monthly_user_aggregates")

            monthly_aggregate = UserAggregate.objects.get(
                organisation=self.organisation,
                collection=self.collection,
                username=self.user,
                year=2024,
                month=1,
                day=0,
            )
            self.assertEqual(
                self.expected_total_added, monthly_aggregate.total_links_added
            )
            self.assertEqual(
                self.expected_total_removed, monthly_aggregate.total_links_removed
            )

    def test_aggregate_next_month(self):
        with time_machine.travel(date(2024, 2, 11)):
            call_command("fill_monthly_user_aggregates")

        with time_machine.travel(date(2024, 3, 11)):
            # Simulate next month by adding 5 more days to the next month
            next_total_added = 0
            next_total_removed = 0
            for day in range(15, 20):
                next_total_added += day
                next_total_removed += day - 1
                UserAggregateFactory(
                    full_date=date(2024, 2, day),
                    organisation=self.organisation,
                    collection=self.collection,
                    username=self.user,
                    total_links_added=day,
                    total_links_removed=day - 1,
                )
            call_command("fill_monthly_user_aggregates")

            self.assertEqual(UserAggregate.objects.filter(day=0).count(), 3)
            monthly_aggregate = UserAggregate.objects.get(
                year=2024, month=2, day=0, username=self.user
            )
            # Should still be the same
            self.assertEqual(next_total_added, monthly_aggregate.total_links_added)
            self.assertEqual(next_total_removed, monthly_aggregate.total_links_removed)
            self.assertEqual(UserAggregate.objects.exclude(day=0).count(), 0)

    def test_specific_collection_aggregation(self):
        with time_machine.travel(date(2024, 2, 11)):
            other_collection = CollectionFactory(
                name="Other Collection", organisation=self.organisation
            )
            for day in range(1, 6):
                UserAggregateFactory(
                    full_date=date(2024, 1, day),
                    organisation=self.organisation,
                    collection=other_collection,
                    username=self.user,
                    total_links_added=day * 2,
                    total_links_removed=day * 2 - 1,
                )

            call_command(
                "fill_monthly_user_aggregates", collections=[other_collection.pk]
            )

            self.assertEqual(
                UserAggregate.objects.filter(
                    day=0, collection=other_collection
                ).count(),
                1,
            )
            self.assertEqual(
                UserAggregate.objects.filter(day=0, collection=self.collection).count(),
                0,
            )

    def test_specific_year_month(self):
        # Adding different month data
        for day in range(1, 11):
            UserAggregateFactory(
                full_date=date(2024, 2, day),
                organisation=self.organisation,
                collection=self.collection,
                username=self.user,
                total_links_added=day,
                total_links_removed=day - 1,
            )

        with time_machine.travel(date(2024, 5, 11)):
            call_command("fill_monthly_user_aggregates", year_month="2024-01")

            self.assertEqual(UserAggregate.objects.filter(day=0).count(), 2)

            monthly_aggregate = UserAggregate.objects.get(
                organisation=self.organisation,
                collection=self.collection,
                username=self.user,
                year=2024,
                month=1,
                day=0,
            )
            self.assertEqual(
                self.expected_total_added, monthly_aggregate.total_links_added
            )
            self.assertEqual(
                self.expected_total_removed, monthly_aggregate.total_links_removed
            )


class MonthlyPageProjectAggregateCommandTest(BaseTransactionTest):
    def setUp(self):
        self.organisation = OrganisationFactory(name="ACME Org")
        self.collection = CollectionFactory(name="ACME", organisation=self.organisation)
        self.project_name = "en.wiki.org"
        self.page_name = "Page1"

        # 10 entries for Jan 2024 (first 10 days)
        self.expected_total_added = 0
        self.expected_total_removed = 0
        for day in range(1, 11):
            self.expected_total_added += day
            self.expected_total_removed += day - 1
            PageProjectAggregateFactory(
                full_date=date(2024, 1, day),
                organisation=self.organisation,
                collection=self.collection,
                project_name=self.project_name,
                page_name=self.page_name,
                total_links_added=day,
                total_links_removed=day - 1,
            )

        # 10 more entries for Jan 2024 with a different user
        self.project_name2 = "es.wiki.org"
        self.page_name2 = "Page1-es"
        for day in range(1, 11):
            PageProjectAggregateFactory(
                full_date=date(2024, 1, day),
                organisation=self.organisation,
                collection=self.collection,
                project_name=self.project_name2,
                page_name=self.page_name2,
                total_links_added=day + 1,
                total_links_removed=day + 2,
            )

    def test_aggregate_monthly_data(self):
        with time_machine.travel(date(2024, 2, 11)):
            self.assertEqual(PageProjectAggregate.objects.filter(day=0).count(), 0)

            call_command("fill_monthly_pageproject_aggregates")

            self.assertEqual(PageProjectAggregate.objects.filter(day=0).count(), 2)

            monthly_aggregate = PageProjectAggregate.objects.get(
                year=2024,
                month=1,
                day=0,
                project_name=self.project_name,
                page_name=self.page_name,
            )
            self.assertEqual(
                self.expected_total_added, monthly_aggregate.total_links_added
            )
            self.assertEqual(
                self.expected_total_removed, monthly_aggregate.total_links_removed
            )

    def test_no_aggregation_when_no_new_data(self):
        with time_machine.travel(date(2024, 2, 11)):
            call_command("fill_monthly_pageproject_aggregates")

            # Running it again should NOT create duplicate entries
            call_command("fill_monthly_pageproject_aggregates")
            self.assertEqual(PageProjectAggregate.objects.filter(day=0).count(), 2)

    def test_aggregate_new_data_same_month(self):
        """
        Simulating running the script again, in case we receive new data
        """
        with time_machine.travel(date(2024, 2, 11)):
            call_command("fill_monthly_pageproject_aggregates")

            # Adding more data to the same month should add up to the totals
            for day in range(1, 11):
                self.expected_total_added += day
                self.expected_total_removed += day - 1
                PageProjectAggregateFactory(
                    full_date=date(2024, 1, day),
                    organisation=self.organisation,
                    collection=self.collection,
                    project_name=self.project_name,
                    page_name=self.page_name,
                    total_links_added=day,
                    total_links_removed=day - 1,
                )
            call_command("fill_monthly_pageproject_aggregates")

            monthly_aggregate = PageProjectAggregate.objects.get(
                organisation=self.organisation,
                collection=self.collection,
                year=2024,
                month=1,
                day=0,
                project_name=self.project_name,
                page_name=self.page_name,
            )
            self.assertEqual(
                self.expected_total_added, monthly_aggregate.total_links_added
            )
            self.assertEqual(
                self.expected_total_removed, monthly_aggregate.total_links_removed
            )

    def test_aggregate_next_month(self):
        with time_machine.travel(date(2024, 2, 11)):
            call_command("fill_monthly_pageproject_aggregates")

        with time_machine.travel(date(2024, 3, 11)):
            # Simulate next month by adding 5 more days to the next month
            next_total_added = 0
            next_total_removed = 0
            for day in range(15, 20):
                next_total_added += day
                next_total_removed += day - 1
                PageProjectAggregateFactory(
                    full_date=date(2024, 2, day),
                    organisation=self.organisation,
                    collection=self.collection,
                    project_name=self.project_name,
                    page_name=self.page_name,
                    total_links_added=day,
                    total_links_removed=day - 1,
                )
            call_command("fill_monthly_pageproject_aggregates")

            self.assertEqual(PageProjectAggregate.objects.filter(day=0).count(), 3)
            monthly_aggregate = PageProjectAggregate.objects.get(
                year=2024,
                month=2,
                day=0,
                project_name=self.project_name,
                page_name=self.page_name,
            )
            # Should still be the same
            self.assertEqual(next_total_added, monthly_aggregate.total_links_added)
            self.assertEqual(next_total_removed, monthly_aggregate.total_links_removed)
            self.assertEqual(PageProjectAggregate.objects.exclude(day=0).count(), 0)

    def test_specific_collection_aggregation(self):
        with time_machine.travel(date(2024, 2, 11)):
            other_collection = CollectionFactory(
                name="Other Collection", organisation=self.organisation
            )
            for day in range(1, 6):
                PageProjectAggregateFactory(
                    full_date=date(2024, 1, day),
                    organisation=self.organisation,
                    collection=other_collection,
                    project_name=self.project_name,
                    page_name=self.page_name,
                    total_links_added=day * 2,
                    total_links_removed=day * 2 - 1,
                )

            call_command(
                "fill_monthly_pageproject_aggregates", collections=[other_collection.pk]
            )

            self.assertEqual(
                PageProjectAggregate.objects.filter(
                    day=0, collection=other_collection
                ).count(),
                1,
            )
            self.assertEqual(
                PageProjectAggregate.objects.filter(
                    day=0, collection=self.collection
                ).count(),
                0,
            )

    def test_specific_year_month(self):
        # Adding different month data
        for day in range(1, 11):
            PageProjectAggregateFactory(
                full_date=date(2024, 2, day),
                organisation=self.organisation,
                collection=self.collection,
                project_name=self.project_name,
                page_name=self.page_name,
                total_links_added=day,
                total_links_removed=day - 1,
            )

        with time_machine.travel(date(2024, 5, 11)):
            call_command("fill_monthly_pageproject_aggregates", year_month="2024-01")

            self.assertEqual(PageProjectAggregate.objects.filter(day=0).count(), 2)

            monthly_aggregate = PageProjectAggregate.objects.get(
                organisation=self.organisation,
                collection=self.collection,
                project_name=self.project_name,
                page_name=self.page_name,
                year=2024,
                month=1,
                day=0,
            )
            self.assertEqual(
                self.expected_total_added, monthly_aggregate.total_links_added
            )
            self.assertEqual(
                self.expected_total_removed, monthly_aggregate.total_links_removed
            )


class ArchiveLinkAggregatesCommandTest(BaseTransactionTest):
    def setUp(self):
        self.organisation = OrganisationFactory(name="JSTOR")
        self.collection = CollectionFactory(
            name="JSTOR",
            organisation=self.organisation,
        )
        self.output_dir = os.path.join(
            tempfile.gettempdir(), "ArchiveLinkAggregatesCommandTest"
        )

        os.mkdir(self.output_dir)

        self.jan_aggregate = LinkAggregateFactory(
            full_date=date(2023, 1, 1),
            organisation=self.organisation,
            collection=self.collection,
            total_links_added=1,
            total_links_removed=0,
        )
        self.feb_aggregate = LinkAggregateFactory(
            full_date=date(2023, 2, 1),
            organisation=self.organisation,
            collection=self.collection,
            total_links_added=5,
            total_links_removed=3,
        )
        self.mar_aggregate = LinkAggregateFactory(
            full_date=date(2023, 3, 1),
            organisation=self.organisation,
            collection=self.collection,
            total_links_added=10,
            total_links_removed=15,
        )

    def tearDown(self):
        shutil.rmtree(self.output_dir)

    @mock.patch("swiftclient.Connection")
    def test_archive_link_aggregates(self, mock_swift_connection):
        mock_conn = mock_swift_connection.return_value
        mock_conn.get_account.return_value = (
            {},
            [{"name": "archive-aggregates-test"}],
        )
        mock_conn.put_container.return_value = ({}, [])
        mock_conn.put_object.return_value = ""

        self.assertEqual(LinkAggregate.objects.count(), 3)

        call_command(
            "archive_link_aggregates",
            "dump",
            "--from",
            "2023-01",
            "--to",
            "2023-03",
            "--output",
            self.output_dir,
        )

        self.assertTrue(
            validate_link_aggregate_archive(self.jan_aggregate, self.output_dir)
        )
        self.assertTrue(
            validate_link_aggregate_archive(self.feb_aggregate, self.output_dir)
        )
        self.assertTrue(
            validate_link_aggregate_archive(self.mar_aggregate, self.output_dir)
        )

        self.assertEqual(LinkAggregate.objects.count(), 0)

    @mock.patch("swiftclient.Connection")
    def test_load_link_aggregates(self, mock_swift_connection):
        mock_conn = mock_swift_connection.return_value
        mock_conn.get_account.return_value = (
            {},
            [{"name": "archive-aggregates-test"}],
        )
        mock_conn.put_container.return_value = ({}, [])
        mock_conn.put_object.return_value = ""

        call_command(
            "archive_link_aggregates",
            "dump",
            "--from",
            "2023-01",
            "--to",
            "2023-03",
            "--output",
            self.output_dir,
        )

        self.assertEqual(LinkAggregate.objects.count(), 0)

        archives = (
            os.path.join(self.output_dir, filename)
            for filename in os.listdir(self.output_dir)
        )
        call_command(
            "archive_link_aggregates",
            "load",
            *archives,
        )

        self.assertEqual(LinkAggregate.objects.count(), 3)

    @mock.patch("swiftclient.Connection")
    def test_link_aggregate_upload(self, mock_swift_connection):
        mock_conn = mock_swift_connection.return_value
        mock_conn.head_object.side_effect = swiftclient.ClientException(
            "Mocked ClientException",
            http_status=404,
            http_reason="Not Found",
            http_response_content="Object not found",
        )
        mock_conn.get_account.return_value = (
            {},
            [{"name": "archive-aggregates-test"}],
        )
        mock_conn.put_container.return_value = ({}, [])
        mock_conn.put_object.return_value = ""

        call_command(
            "archive_link_aggregates",
            "dump",
            "--from",
            "2023-01",
            "--to",
            "2023-03",
            "--output",
            self.output_dir,
        )

        self.assertEqual(len(os.listdir(self.output_dir)), 3)

        archives = (
            os.path.join(self.output_dir, filename)
            for filename in os.listdir(self.output_dir)
        )
        call_command(
            "archive_link_aggregates",
            "upload",
            "--container",
            "fakecontainer",
            *archives,
        )

        mock_conn.put_object.assert_has_calls(
            (
                mock.call(
                    "fakecontainer",
                    filename,
                    contents=mock.ANY,
                    content_type=mock.ANY,
                )
                for filename in os.listdir(self.output_dir)
            ),
            any_order=True,
        )

    @mock.patch("swiftclient.Connection")
    def test_link_aggregate_upload_with_object_storage_only(
        self, mock_swift_connection
    ):
        mock_conn = mock_swift_connection.return_value
        mock_conn.head_object.side_effect = swiftclient.ClientException(
            "Mocked ClientException",
            http_status=404,
            http_reason="Not Found",
            http_response_content="Object not found",
        )
        mock_conn.get_account.return_value = (
            {},
            [{"name": "archive-aggregates-test"}],
        )
        mock_conn.put_container.return_value = ({}, [])
        mock_conn.put_object.return_value = ""

        call_command(
            "archive_link_aggregates",
            "dump",
            "--from",
            "2023-01",
            "--to",
            "2023-03",
            "--output",
            self.output_dir,
            "--container",
            "fakecontainer",
            "--object-storage-only",
        )

        expected_archives = [
            f"aggregates_linkaggregate_{self.organisation.id}_{self.collection.id}_2023-01-01_0.json.gz",
            f"aggregates_linkaggregate_{self.organisation.id}_{self.collection.id}_2023-02-01_0.json.gz",
            f"aggregates_linkaggregate_{self.organisation.id}_{self.collection.id}_2023-03-01_0.json.gz",
        ]

        mock_conn.put_object.assert_has_calls(
            (
                mock.call(
                    "fakecontainer",
                    filename,
                    contents=mock.ANY,
                    content_type=mock.ANY,
                )
                for filename in expected_archives
            ),
            any_order=True,
        )

        self.assertEqual(len(os.listdir(self.output_dir)), 0)

    @mock.patch("swiftclient.Connection")
    def test_archive_link_aggregates_no_dates(self, mock_swift_connection):
        mock_conn = mock_swift_connection.return_value
        mock_conn.get_account.return_value = (
            {},
            [{"name": "archive-aggregates-test"}],
        )
        mock_conn.put_container.return_value = ({}, [])
        mock_conn.put_object.return_value = ""

        # Add an additional aggregate that we won't expect to get archived as
        # it was created too recently (less than a year ago).
        LinkAggregateFactory(
            full_date=(date.today() - relativedelta(months=11)).replace(day=1),
            organisation=self.organisation,
            collection=self.collection,
            total_links_added=30,
            total_links_removed=5,
        )

        self.assertEqual(LinkAggregate.objects.count(), 4)

        call_command(
            "archive_link_aggregates",
            "dump",
            "--output",
            self.output_dir,
        )

        self.assertTrue(
            validate_link_aggregate_archive(self.jan_aggregate, self.output_dir)
        )
        self.assertTrue(
            validate_link_aggregate_archive(self.feb_aggregate, self.output_dir)
        )
        self.assertTrue(
            validate_link_aggregate_archive(self.mar_aggregate, self.output_dir)
        )

        # Expect that the "recently" created aggregate was not archived.
        self.assertEqual(len(os.listdir(self.output_dir)), 3)
        self.assertEqual(LinkAggregate.objects.count(), 1)


class ArchiveUserAggregatesCommandTest(BaseTransactionTest):
    def setUp(self):
        self.user = UserFactory(username="jonsnow")
        self.organisation = OrganisationFactory(name="JSTOR")
        self.collection = CollectionFactory(
            name="JSTOR",
            organisation=self.organisation,
        )
        self.output_dir = os.path.join(
            tempfile.gettempdir(), "ArchiveUserAggregatesCommandTest"
        )

        os.mkdir(self.output_dir)

        self.jan_aggregate = UserAggregateFactory(
            full_date=date(2023, 1, 1),
            organisation=self.organisation,
            collection=self.collection,
            total_links_added=1,
            total_links_removed=0,
            username=self.user.username,
        )
        self.feb_aggregate = UserAggregateFactory(
            full_date=date(2023, 2, 1),
            organisation=self.organisation,
            collection=self.collection,
            total_links_added=5,
            total_links_removed=3,
            username=self.user.username,
        )
        self.mar_aggregate = UserAggregateFactory(
            full_date=date(2023, 3, 1),
            organisation=self.organisation,
            collection=self.collection,
            total_links_added=10,
            total_links_removed=15,
            username=self.user.username,
        )

    def tearDown(self):
        shutil.rmtree(self.output_dir)

    @mock.patch("swiftclient.Connection")
    def test_archive_user_aggregates(self, mock_swift_connection):
        mock_conn = mock_swift_connection.return_value
        mock_conn.get_account.return_value = (
            {},
            [{"name": "archive-aggregates-test"}],
        )
        mock_conn.put_container.return_value = ({}, [])
        mock_conn.put_object.return_value = ""

        self.assertEqual(UserAggregate.objects.count(), 3)

        call_command(
            "archive_user_aggregates",
            "dump",
            "--from",
            "2023-01",
            "--to",
            "2023-03",
            "--output",
            self.output_dir,
        )

        self.assertTrue(
            validate_user_aggregate_archive(self.jan_aggregate, self.output_dir)
        )
        self.assertTrue(
            validate_user_aggregate_archive(self.feb_aggregate, self.output_dir)
        )
        self.assertTrue(
            validate_user_aggregate_archive(self.mar_aggregate, self.output_dir)
        )

        self.assertEqual(UserAggregate.objects.count(), 0)

    @mock.patch("swiftclient.Connection")
    def test_load_user_aggregates(self, mock_swift_connection):
        mock_conn = mock_swift_connection.return_value
        mock_conn.get_account.return_value = (
            {},
            [{"name": "archive-aggregates-test"}],
        )
        mock_conn.put_container.return_value = ({}, [])
        mock_conn.put_object.return_value = ""

        call_command(
            "archive_user_aggregates",
            "dump",
            "--from",
            "2023-01",
            "--to",
            "2023-03",
            "--output",
            self.output_dir,
        )

        self.assertEqual(UserAggregate.objects.count(), 0)

        archives = (
            os.path.join(self.output_dir, filename)
            for filename in os.listdir(self.output_dir)
        )
        call_command(
            "archive_user_aggregates",
            "load",
            *archives,
        )

        self.assertEqual(UserAggregate.objects.count(), 3)

    @mock.patch("swiftclient.Connection")
    def test_user_aggregate_upload(self, mock_swift_connection):
        mock_conn = mock_swift_connection.return_value
        mock_conn.head_object.side_effect = swiftclient.ClientException(
            "Mocked ClientException",
            http_status=404,
            http_reason="Not Found",
            http_response_content="Object not found",
        )
        mock_conn.get_account.return_value = (
            {},
            [{"name": "archive-aggregates-test"}],
        )
        mock_conn.put_container.return_value = ({}, [])
        mock_conn.put_object.return_value = ""

        call_command(
            "archive_user_aggregates",
            "dump",
            "--from",
            "2023-01",
            "--to",
            "2023-03",
            "--output",
            self.output_dir,
        )

        self.assertEqual(len(os.listdir(self.output_dir)), 3)

        archives = (
            os.path.join(self.output_dir, filename)
            for filename in os.listdir(self.output_dir)
        )
        call_command(
            "archive_user_aggregates",
            "upload",
            "--container",
            "fakecontainer",
            *archives,
        )

        mock_conn.put_object.assert_has_calls(
            (
                mock.call(
                    "fakecontainer",
                    filename,
                    contents=mock.ANY,
                    content_type=mock.ANY,
                )
                for filename in os.listdir(self.output_dir)
            ),
            any_order=True,
        )

    @mock.patch("swiftclient.Connection")
    def test_user_aggregate_upload_with_object_storage_only(
        self, mock_swift_connection
    ):
        mock_conn = mock_swift_connection.return_value
        mock_conn.head_object.side_effect = swiftclient.ClientException(
            "Mocked ClientException",
            http_status=404,
            http_reason="Not Found",
            http_response_content="Object not found",
        )
        mock_conn.get_account.return_value = (
            {},
            [{"name": "archive-aggregates-test"}],
        )
        mock_conn.put_container.return_value = ({}, [])
        mock_conn.put_object.return_value = ""

        call_command(
            "archive_user_aggregates",
            "dump",
            "--from",
            "2023-01",
            "--to",
            "2023-03",
            "--output",
            self.output_dir,
            "--container",
            "fakecontainer",
            "--object-storage-only",
        )

        expected_archives = [
            f"aggregates_useraggregate_{self.organisation.id}_{self.collection.id}_2023-01-01_0.json.gz",
            f"aggregates_useraggregate_{self.organisation.id}_{self.collection.id}_2023-02-01_0.json.gz",
            f"aggregates_useraggregate_{self.organisation.id}_{self.collection.id}_2023-03-01_0.json.gz",
        ]

        mock_conn.put_object.assert_has_calls(
            (
                mock.call(
                    "fakecontainer",
                    filename,
                    contents=mock.ANY,
                    content_type=mock.ANY,
                )
                for filename in expected_archives
            ),
            any_order=True,
        )

        self.assertEqual(len(os.listdir(self.output_dir)), 0)

    @mock.patch("swiftclient.Connection")
    def test_archive_user_aggregates_no_dates(self, mock_swift_connection):
        mock_conn = mock_swift_connection.return_value
        mock_conn.get_account.return_value = (
            {},
            [{"name": "archive-aggregates-test"}],
        )
        mock_conn.put_container.return_value = ({}, [])
        mock_conn.put_object.return_value = ""

        # Add an additional aggregate that we won't expect to get archived as
        # it was created too recently (less than a year ago).
        UserAggregateFactory(
            full_date=(date.today() - relativedelta(months=11)).replace(day=1),
            organisation=self.organisation,
            collection=self.collection,
            total_links_added=30,
            total_links_removed=5,
            username=self.user.username,
        )

        self.assertEqual(UserAggregate.objects.count(), 4)

        call_command(
            "archive_user_aggregates",
            "dump",
            "--output",
            self.output_dir,
        )

        self.assertTrue(
            validate_user_aggregate_archive(self.jan_aggregate, self.output_dir)
        )
        self.assertTrue(
            validate_user_aggregate_archive(self.feb_aggregate, self.output_dir)
        )
        self.assertTrue(
            validate_user_aggregate_archive(self.mar_aggregate, self.output_dir)
        )

        # Expect that the "recently" created aggregate was not archived.
        self.assertEqual(len(os.listdir(self.output_dir)), 3)
        self.assertEqual(UserAggregate.objects.count(), 1)


class ArchivePageProjectAggregatesCommandTest(BaseTransactionTest):
    def setUp(self):
        self.page = "TestPage"
        self.project = "en.wikipedia.org"
        self.organisation = OrganisationFactory(name="JSTOR")
        self.collection = CollectionFactory(
            name="JSTOR",
            organisation=self.organisation,
        )
        self.output_dir = os.path.join(
            tempfile.gettempdir(), "ArchivePageProjectAggregatesCommandTest"
        )

        os.mkdir(self.output_dir)

        self.jan_aggregate = PageProjectAggregateFactory(
            full_date=date(2023, 1, 1),
            organisation=self.organisation,
            collection=self.collection,
            total_links_added=1,
            total_links_removed=0,
            project_name=self.project,
            page_name=self.page,
        )
        self.feb_aggregate = PageProjectAggregateFactory(
            full_date=date(2023, 2, 1),
            organisation=self.organisation,
            collection=self.collection,
            total_links_added=5,
            total_links_removed=3,
            project_name=self.project,
            page_name=self.page,
        )
        self.mar_aggregate = PageProjectAggregateFactory(
            full_date=date(2023, 3, 1),
            organisation=self.organisation,
            collection=self.collection,
            total_links_added=10,
            total_links_removed=15,
            project_name=self.project,
            page_name=self.page,
        )

    def tearDown(self):
        shutil.rmtree(self.output_dir)

    @mock.patch("swiftclient.Connection")
    def test_archive_pageproject_aggregates(self, mock_swift_connection):
        mock_conn = mock_swift_connection.return_value
        mock_conn.get_account.return_value = (
            {},
            [{"name": "archive-aggregates-test"}],
        )
        mock_conn.put_container.return_value = ({}, [])
        mock_conn.put_object.return_value = ""

        self.assertEqual(PageProjectAggregate.objects.count(), 3)

        call_command(
            "archive_pageproject_aggregates",
            "dump",
            "--from",
            "2023-01",
            "--to",
            "2023-03",
            "--output",
            self.output_dir,
        )

        self.assertTrue(
            validate_pageproject_aggregate_archive(self.jan_aggregate, self.output_dir)
        )
        self.assertTrue(
            validate_pageproject_aggregate_archive(self.feb_aggregate, self.output_dir)
        )
        self.assertTrue(
            validate_pageproject_aggregate_archive(self.mar_aggregate, self.output_dir)
        )

        self.assertEqual(PageProjectAggregate.objects.count(), 0)

    @mock.patch("swiftclient.Connection")
    def test_load_pageproject_aggregates(self, mock_swift_connection):
        mock_conn = mock_swift_connection.return_value
        mock_conn.get_account.return_value = (
            {},
            [{"name": "archive-aggregates-test"}],
        )
        mock_conn.put_container.return_value = ({}, [])
        mock_conn.put_object.return_value = ""

        call_command(
            "archive_pageproject_aggregates",
            "dump",
            "--from",
            "2023-01",
            "--to",
            "2023-03",
            "--output",
            self.output_dir,
        )

        self.assertEqual(PageProjectAggregate.objects.count(), 0)

        archives = (
            os.path.join(self.output_dir, filename)
            for filename in os.listdir(self.output_dir)
        )
        call_command(
            "archive_pageproject_aggregates",
            "load",
            *archives,
        )

        self.assertEqual(PageProjectAggregate.objects.count(), 3)

    @mock.patch("swiftclient.Connection")
    def test_pageproject_aggregate_upload(self, mock_swift_connection):
        mock_conn = mock_swift_connection.return_value
        mock_conn.head_object.side_effect = swiftclient.ClientException(
            "Mocked ClientException",
            http_status=404,
            http_reason="Not Found",
            http_response_content="Object not found",
        )
        mock_conn.get_account.return_value = (
            {},
            [{"name": "archive-aggregates-test"}],
        )
        mock_conn.put_container.return_value = ({}, [])
        mock_conn.put_object.return_value = ""

        call_command(
            "archive_pageproject_aggregates",
            "dump",
            "--from",
            "2023-01",
            "--to",
            "2023-03",
            "--output",
            self.output_dir,
        )

        self.assertEqual(len(os.listdir(self.output_dir)), 3)

        archives = (
            os.path.join(self.output_dir, filename)
            for filename in os.listdir(self.output_dir)
        )
        call_command(
            "archive_pageproject_aggregates",
            "upload",
            "--container",
            "fakecontainer",
            *archives,
        )

        mock_conn.put_object.assert_has_calls(
            (
                mock.call(
                    "fakecontainer",
                    filename,
                    contents=mock.ANY,
                    content_type=mock.ANY,
                )
                for filename in os.listdir(self.output_dir)
            ),
            any_order=True,
        )

    @mock.patch("swiftclient.Connection")
    def test_pageproject_aggregate_upload_with_object_storage_only(
        self, mock_swift_connection
    ):
        mock_conn = mock_swift_connection.return_value
        mock_conn.head_object.side_effect = swiftclient.ClientException(
            "Mocked ClientException",
            http_status=404,
            http_reason="Not Found",
            http_response_content="Object not found",
        )
        mock_conn.get_account.return_value = (
            {},
            [{"name": "archive-aggregates-test"}],
        )
        mock_conn.put_container.return_value = ({}, [])
        mock_conn.put_object.return_value = ""

        call_command(
            "archive_pageproject_aggregates",
            "dump",
            "--from",
            "2023-01",
            "--to",
            "2023-03",
            "--output",
            self.output_dir,
            "--container",
            "fakecontainer",
            "--object-storage-only",
        )

        expected_archives = [
            f"aggregates_pageprojectaggregate_{self.organisation.id}_{self.collection.id}_2023-01-01_0.json.gz",
            f"aggregates_pageprojectaggregate_{self.organisation.id}_{self.collection.id}_2023-02-01_0.json.gz",
            f"aggregates_pageprojectaggregate_{self.organisation.id}_{self.collection.id}_2023-03-01_0.json.gz",
        ]

        mock_conn.put_object.assert_has_calls(
            (
                mock.call(
                    "fakecontainer",
                    filename,
                    contents=mock.ANY,
                    content_type=mock.ANY,
                )
                for filename in expected_archives
            ),
            any_order=True,
        )

        self.assertEqual(len(os.listdir(self.output_dir)), 0)

    @mock.patch("swiftclient.Connection")
    def test_archive_pageproject_aggregates_no_dates(self, mock_swift_connection):
        mock_conn = mock_swift_connection.return_value
        mock_conn.get_account.return_value = (
            {},
            [{"name": "archive-aggregates-test"}],
        )
        mock_conn.put_container.return_value = ({}, [])
        mock_conn.put_object.return_value = ""

        # Add an additional aggregate that we won't expect to get archived as
        # it was created too recently (less than a year ago).
        PageProjectAggregateFactory(
            full_date=(date.today() - relativedelta(months=11)).replace(day=1),
            organisation=self.organisation,
            collection=self.collection,
            total_links_added=30,
            total_links_removed=5,
            project_name=self.project,
            page_name=self.page,
        )

        self.assertEqual(PageProjectAggregate.objects.count(), 4)

        call_command(
            "archive_pageproject_aggregates",
            "dump",
            "--output",
            self.output_dir,
        )

        self.assertTrue(
            validate_pageproject_aggregate_archive(self.jan_aggregate, self.output_dir)
        )
        self.assertTrue(
            validate_pageproject_aggregate_archive(self.feb_aggregate, self.output_dir)
        )
        self.assertTrue(
            validate_pageproject_aggregate_archive(self.mar_aggregate, self.output_dir)
        )

        # Expect that the "recently" created aggregate was not archived.
        self.assertEqual(len(os.listdir(self.output_dir)), 3)
        self.assertEqual(PageProjectAggregate.objects.count(), 1)


class UploadAllArchivedAggregatesCommandTest(BaseTransactionTest):
    @mock.patch.dict(
        os.environ,
        {
            "OPENSTACK_AUTH_URL": "fakeurl",
            "SWIFT_APPLICATION_CREDENTIAL_ID": "fakecredid",
            "SWIFT_APPLICATION_CREDENTIAL_SECRET": "fakecredsecret",
        },
    )
    @mock.patch("swiftclient.Connection")
    def test_uploads_all_files_successfully(self, mock_swift_connection):
        mock_conn = mock_swift_connection.return_value
        mock_conn.get_account.return_value = (
            {},
            [{"name": "archive-aggregates-backup-202101"}],
        )
        mock_conn.head_object.side_effect = swiftclient.ClientException(
            "Mocked ClientException",
            http_status=404,
            http_reason="Not Found",
            http_response_content="Object not found",
        )

        temp_dir = tempfile.gettempdir()
        archive_filename = "aggregates_pageprojectaggregate_20210116_0.json.gz"
        archive_path = os.path.join(temp_dir, archive_filename)
        json_data = [
            {
                "model": "aggregates.pageprojectaggregate",
                "pk": 1,
                "fields": {
                    "organisation": 21,
                    "collection": 3,
                    "project_name": "enwiki",
                    "page_name": "Wikipedia",
                    "day": 0,
                    "month": 1,
                    "year": 2021,
                    "total_links_added": 12321,
                    "total_links_removed": 43,
                    "on_user_list": True,
                    "created_at": "2021-01-16T00:00:00Z",
                    "updated_at": "2021-01-16T00:00:00Z",
                },
            }
        ]

        with gzip.open(archive_path, "wt", encoding="utf-8") as f:
            json.dump(json_data, f)

        try:
            call_command(
                "upload_all_archived_aggregates",
                "--container",
                "archive-aggregates-test",
                "--dir",
                temp_dir,
            )
            mock_conn.put_object.assert_called_once()

        finally:
            pattern = os.path.join(temp_dir, "aggregates_*.json.gz")

            for file in glob.glob(pattern):
                os.remove(file)


class FixAggregatesForOrganisationAndMonthCommandTest(BaseTransactionTest):

    def setUp(self):
        # Creating one Collection, Organisation, and URLPattern
        self.organisation = OrganisationFactory(name="ACME Org")
        self.collection = CollectionFactory(organisation=self.organisation)
        self.user = UserFactory()
        self.user2 = UserFactory()
        self.url = URLPatternFactory(url="www.test.com")
        self.url.collection = self.collection
        self.url.save()

    @mock.patch.dict(
        os.environ,
        {
            "OPENSTACK_AUTH_URL": "fakeurl",
            "SWIFT_APPLICATION_CREDENTIAL_ID": "fakecredid",
            "SWIFT_APPLICATION_CREDENTIAL_SECRET": "fakecredsecret",
        },
    )
    @mock.patch("swiftclient.Connection")
    def test_reaggregate_link_archives_monthly(self, mock_swift_connection):
        mock_conn = mock_swift_connection.return_value
        mock_conn.get_account.return_value = (
            {},
            [{"name": "archive-aggregates-backup-2024-12-22"}],
        )
        mock_conn.get_container.return_value = ({},[])
        temp_dir = tempfile.gettempdir()
        archive_filename = "links_linkevent_20241222_0.json.gz"
        archive_path = os.path.join(temp_dir, archive_filename)

        json_data = [
            {
                "model": "links.linkevent",
                "pk": 1,
                "fields": {
                    "link": "https://www.another_domain.com/articles/",
                    "timestamp": "2024-12-16T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 2,
                "fields": {
                    "link": "https://www.test.com/",
                    "timestamp": "2024-12-16T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 3,
                "fields": {
                    "link": "https://www.test.com/3",
                    "timestamp": "2024-12-15T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 4,
                "fields": {
                    "link": "https://www.test.com/4",
                    "timestamp": "2024-12-15T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 0,
                    "on_user_list": True,
                    "url": []
                }
            },
        ]

        with gzip.open(archive_path, "wt", encoding="utf-8") as f:
            json.dump(json_data, f)

        try:
            call_command(
                "reaggregate_link_archives",
                "--month",
                "202412",
                "--organisation",
                self.organisation.id,
                "--dir",
                temp_dir,
            )
            monthly_link_aggregate = LinkAggregate.objects.all().first()
            monthly_user_aggregates = UserAggregate.objects.all().first()
            monthly_page_project_aggregates = PageProjectAggregate.objects.all().first()
            # assert only one monthly aggregate created for on_user_list=True
            self.assertEqual(1, LinkAggregate.objects.count())
            self.assertEqual(1, PageProjectAggregate.objects.count())
            self.assertEqual(1, UserAggregate.objects.count())
            # assert daily aggregates were not created
            self.assertEqual(0, LinkAggregate.objects.filter(day=16).count())
            self.assertEqual(0, LinkAggregate.objects.filter(day=15).count())
            self.assertEqual(0, PageProjectAggregate.objects.filter(day=16).count())
            self.assertEqual(0, PageProjectAggregate.objects.filter(day=15).count())
            self.assertEqual(0, UserAggregate.objects.filter(day=16).count())
            self.assertEqual(0, UserAggregate.objects.filter(day=15).count())
            # assert totals match expected totals
            self.assertEqual(2, monthly_link_aggregate.total_links_added)
            self.assertEqual(1, monthly_link_aggregate.total_links_removed)
            self.assertEqual(2, monthly_user_aggregates.total_links_added)
            self.assertEqual(1, monthly_link_aggregate.total_links_removed)
            self.assertEqual(2, monthly_page_project_aggregates.total_links_added)
            self.assertEqual(1, monthly_page_project_aggregates.total_links_removed)
        finally:
            for file in glob.glob(archive_path):
                os.remove(file)


    @mock.patch.dict(
        os.environ,
        {
            "OPENSTACK_AUTH_URL": "fakeurl",
            "SWIFT_APPLICATION_CREDENTIAL_ID": "fakecredid",
            "SWIFT_APPLICATION_CREDENTIAL_SECRET": "fakecredsecret",
        },
    )
    @mock.patch("swiftclient.Connection")
    def test_reaggregate_link_archives_monthly_multiple_projects(self, mock_swift_connection):
        mock_conn = mock_swift_connection.return_value
        mock_conn.get_account.return_value = (
            {},
            [{"name": "archive-aggregates-backup-2024-12-22"}],
        )
        mock_conn.get_container.return_value = ({},[])
        temp_dir = tempfile.gettempdir()
        archive_filename = "links_linkevent_20241222_0.json.gz"
        archive_path = os.path.join(temp_dir, archive_filename)

        json_data = [
            {
                "model": "links.linkevent",
                "pk": 1,
                "fields": {
                    "link": "https://www.another_domain.com/articles/",
                    "timestamp": "2024-12-16T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 2,
                "fields": {
                    "link": "https://www.test.com/",
                    "timestamp": "2024-12-16T09:15:27.363Z",
                    "domain": "cy.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 3,
                "fields": {
                    "link": "https://www.test.com/3",
                    "timestamp": "2024-12-15T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 4,
                "fields": {
                    "link": "https://www.test.com/4",
                    "timestamp": "2024-12-15T09:15:27.363Z",
                    "domain": "de.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 0,
                    "on_user_list": True,
                    "url": []
                }
            },
        ]

        with gzip.open(archive_path, "wt", encoding="utf-8") as f:
            json.dump(json_data, f)

        try:
            call_command(
                "reaggregate_link_archives",
                "--month",
                "202412",
                "--organisation",
                self.organisation.id,
                "--dir",
                temp_dir,
            )
            monthly_link_aggregate = LinkAggregate.objects.all().first()
            monthly_user_aggregates = UserAggregate.objects.all().first()
            monthly_page_project_aggregates_en = PageProjectAggregate.objects.filter(project_name="en.wikipedia.org").first()
            monthly_page_project_aggregates_de = PageProjectAggregate.objects.filter(project_name="de.wikipedia.org").first()
            monthly_page_project_aggregates_cy = PageProjectAggregate.objects.filter(project_name="cy.wikipedia.org").first()

            # assert only one monthly aggregate created for on_user_list=True
            self.assertEqual(1, LinkAggregate.objects.count())
            self.assertEqual(3, PageProjectAggregate.objects.count())
            self.assertEqual(1, UserAggregate.objects.count())
            # assert daily aggregates were not created
            self.assertEqual(0, LinkAggregate.objects.filter(day=16).count())
            self.assertEqual(0, LinkAggregate.objects.filter(day=15).count())
            self.assertEqual(0, PageProjectAggregate.objects.filter(day=16).count())
            self.assertEqual(0, PageProjectAggregate.objects.filter(day=15).count())
            self.assertEqual(0, UserAggregate.objects.filter(day=16).count())
            self.assertEqual(0, UserAggregate.objects.filter(day=15).count())
            # assert totals match expected totals
            self.assertEqual(2, monthly_link_aggregate.total_links_added)
            self.assertEqual(1, monthly_link_aggregate.total_links_removed)
            self.assertEqual(2, monthly_user_aggregates.total_links_added)
            self.assertEqual(1, monthly_link_aggregate.total_links_removed)
            self.assertEqual(1, monthly_page_project_aggregates_de.total_links_removed)
            self.assertEqual(0, monthly_page_project_aggregates_de.total_links_added)
            self.assertEqual(0, monthly_page_project_aggregates_en.total_links_removed)
            self.assertEqual(1, monthly_page_project_aggregates_en.total_links_added)
            self.assertEqual(0, monthly_page_project_aggregates_cy.total_links_removed)
            self.assertEqual(1, monthly_page_project_aggregates_cy.total_links_added)

        finally:
            for file in glob.glob(archive_path):
                os.remove(file)

    @mock.patch.dict(
        os.environ,
        {
            "OPENSTACK_AUTH_URL": "fakeurl",
            "SWIFT_APPLICATION_CREDENTIAL_ID": "fakecredid",
            "SWIFT_APPLICATION_CREDENTIAL_SECRET": "fakecredsecret",
        },
    )
    @mock.patch("swiftclient.Connection")
    def test_reaggregate_link_archives_monthly_multiple_pages(self, mock_swift_connection):
        mock_conn = mock_swift_connection.return_value
        mock_conn.get_account.return_value = (
            {},
            [{"name": "archive-aggregates-backup-2024-12-22"}],
        )
        mock_conn.get_container.return_value = ({}, [])
        temp_dir = tempfile.gettempdir()
        archive_filename = "links_linkevent_20241222_0.json.gz"
        archive_path = os.path.join(temp_dir, archive_filename)

        json_data = [
            {
                "model": "links.linkevent",
                "pk": 1,
                "fields": {
                    "link": "https://www.another_domain.com/articles/",
                    "timestamp": "2024-12-16T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 2,
                "fields": {
                    "link": "https://www.test.com/",
                    "timestamp": "2024-12-16T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 3,
                "fields": {
                    "link": "https://www.test.com/3",
                    "timestamp": "2024-12-15T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test2",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 4,
                "fields": {
                    "link": "https://www.test.com/4",
                    "timestamp": "2024-12-15T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 0,
                    "on_user_list": True,
                    "url": []
                }
            },
        ]

        with gzip.open(archive_path, "wt", encoding="utf-8") as f:
            json.dump(json_data, f)

        try:
            call_command(
                "reaggregate_link_archives",
                "--month",
                "202412",
                "--organisation",
                self.organisation.id,
                "--dir",
                temp_dir,
            )
            monthly_link_aggregate = LinkAggregate.objects.all().first()
            monthly_user_aggregates = UserAggregate.objects.all().first()
            monthly_page_project_aggregates_page_1 = PageProjectAggregate.objects.filter(page_name="test").first()
            monthly_page_project_aggregates_page_2 = PageProjectAggregate.objects.filter(page_name="test2").first()
            # assert only one monthly aggregate created for on_user_list=True
            self.assertEqual(1, LinkAggregate.objects.count())
            self.assertEqual(2, PageProjectAggregate.objects.count())
            self.assertEqual(1, UserAggregate.objects.count())
            # assert daily aggregates were not created
            self.assertEqual(0, LinkAggregate.objects.filter(day=16).count())
            self.assertEqual(0, LinkAggregate.objects.filter(day=15).count())
            self.assertEqual(0, PageProjectAggregate.objects.filter(day=16).count())
            self.assertEqual(0, PageProjectAggregate.objects.filter(day=15).count())
            self.assertEqual(0, UserAggregate.objects.filter(day=16).count())
            self.assertEqual(0, UserAggregate.objects.filter(day=15).count())
            # assert totals match expected totals
            self.assertEqual(2, monthly_link_aggregate.total_links_added)
            self.assertEqual(1, monthly_link_aggregate.total_links_removed)
            self.assertEqual(2, monthly_user_aggregates.total_links_added)
            self.assertEqual(1, monthly_link_aggregate.total_links_removed)
            self.assertEqual(1, monthly_page_project_aggregates_page_1.total_links_added)
            self.assertEqual(1, monthly_page_project_aggregates_page_1.total_links_removed)
            self.assertEqual(1, monthly_page_project_aggregates_page_2.total_links_added)
            self.assertEqual(0, monthly_page_project_aggregates_page_2.total_links_removed)

        finally:
            for file in glob.glob(archive_path):
                os.remove(file)


    @mock.patch.dict(
        os.environ,
        {
            "OPENSTACK_AUTH_URL": "fakeurl",
            "SWIFT_APPLICATION_CREDENTIAL_ID": "fakecredid",
            "SWIFT_APPLICATION_CREDENTIAL_SECRET": "fakecredsecret",
        },
    )
    @mock.patch("swiftclient.Connection")
    def test_reaggregate_link_archives_monthly_multiple_users(self, mock_swift_connection):
        mock_conn = mock_swift_connection.return_value
        mock_conn.get_account.return_value = (
            {},
            [{"name": "archive-aggregates-backup-2024-12-22"}],
        )
        mock_conn.get_container.return_value = ({},[])
        temp_dir = tempfile.gettempdir()
        archive_filename = "links_linkevent_20241222_0.json.gz"
        archive_path = os.path.join(temp_dir, archive_filename)

        json_data = [
            {
                "model": "links.linkevent",
                "pk": 1,
                "fields": {
                    "link": "https://www.another_domain.com/articles/",
                    "timestamp": "2024-12-16T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 2,
                "fields": {
                    "link": "https://www.test.com/",
                    "timestamp": "2024-12-16T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 3,
                "fields": {
                    "link": "https://www.test.com/3",
                    "timestamp": "2024-12-15T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user2.username,
                    "rev_id": 485489,
                    "user_id": self.user2.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 4,
                "fields": {
                    "link": "https://www.test.com/4",
                    "timestamp": "2024-12-15T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user2.username,
                    "rev_id": 485489,
                    "user_id": self.user2.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 0,
                    "on_user_list": True,
                    "url": []
                }
            },
        ]

        with gzip.open(archive_path, "wt", encoding="utf-8") as f:
            json.dump(json_data, f)

        try:
            call_command(
                "reaggregate_link_archives",
                "--month",
                "202412",
                "--organisation",
                self.organisation.id,
                "--dir",
                temp_dir,
            )
            monthly_link_aggregate = LinkAggregate.objects.all().first()
            monthly_user_aggregates_1 = UserAggregate.objects.filter(username=self.user.username).first()
            monthly_user_aggregates_2 = UserAggregate.objects.filter(username=self.user2.username).first()
            monthly_page_project_aggregates = PageProjectAggregate.objects.all().first()
            # assert only one monthly aggregate created for on_user_list=True
            self.assertEqual(1, LinkAggregate.objects.count())
            self.assertEqual(1, PageProjectAggregate.objects.count())
            self.assertEqual(2, UserAggregate.objects.count())
            # assert daily aggregates were not created
            self.assertEqual(0, LinkAggregate.objects.filter(day=16).count())
            self.assertEqual(0, LinkAggregate.objects.filter(day=15).count())
            self.assertEqual(0, PageProjectAggregate.objects.filter(day=16).count())
            self.assertEqual(0, PageProjectAggregate.objects.filter(day=15).count())
            self.assertEqual(0, UserAggregate.objects.filter(day=16).count())
            self.assertEqual(0, UserAggregate.objects.filter(day=15).count())
            # assert totals match expected totals
            self.assertEqual(2, monthly_link_aggregate.total_links_added)
            self.assertEqual(1, monthly_link_aggregate.total_links_removed)
            self.assertEqual(1, monthly_user_aggregates_1.total_links_added)
            self.assertEqual(0, monthly_user_aggregates_1.total_links_removed)
            self.assertEqual(1, monthly_user_aggregates_2.total_links_added)
            self.assertEqual(1, monthly_user_aggregates_2.total_links_removed)
            self.assertEqual(2, monthly_page_project_aggregates.total_links_added)
            self.assertEqual(1, monthly_page_project_aggregates.total_links_removed)
        finally:
            for file in glob.glob(archive_path):
                os.remove(file)

    @mock.patch.dict(
        os.environ,
        {
            "OPENSTACK_AUTH_URL": "fakeurl",
            "SWIFT_APPLICATION_CREDENTIAL_ID": "fakecredid",
            "SWIFT_APPLICATION_CREDENTIAL_SECRET": "fakecredsecret",
        },
    )
    @mock.patch("swiftclient.Connection")
    def test_reaggregate_link_archives_monthly_skips_if_uploaded(self, mock_swift_connection):
        mock_conn = mock_swift_connection.return_value
        mock_conn.get_account.return_value = (
            {},
            [{"name": "archive-aggregates-backup-2024-12-22"}],
        )
        mock_conn.get_container.return_value = ({},[{"name": "archive-aggregates-backup-2024-12-22"}])
        temp_dir = tempfile.gettempdir()
        archive_filename = "links_linkevent_20241222_0.json.gz"
        archive_path = os.path.join(temp_dir, archive_filename)
        json_data = [
            {
                "model": "links.linkevent",
                "pk": 1,
                "fields": {
                    "link": "https://www.another_domain.com/articles/",
                    "timestamp": "2024-12-16T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 2,
                "fields": {
                    "link": "https://www.test.com/",
                    "timestamp": "2024-12-16T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 3,
                "fields": {
                    "link": "https://www.test.com/3",
                    "timestamp": "2024-12-15T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 4,
                "fields": {
                    "link": "https://www.test.com/4",
                    "timestamp": "2024-12-15T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 0,
                    "on_user_list": True,
                    "url": []
                }
            },
        ]

        with gzip.open(archive_path, "wt", encoding="utf-8") as f:
            json.dump(json_data, f)

        try:
            call_command(
                "reaggregate_link_archives",
                "--month",
                "202412",
                "--organisation",
                self.organisation.id,
                "--dir",
                temp_dir,
            )
            # assert no daily or monthly aggregates created
            self.assertEqual(0, LinkAggregate.objects.count())
            self.assertEqual(0, UserAggregate.objects.count())
            self.assertEqual(0, PageProjectAggregate.objects.count())
        finally:
            for file in glob.glob(archive_path):
                os.remove(file)


    @mock.patch.dict(
        os.environ,
        {
            "OPENSTACK_AUTH_URL": "fakeurl",
            "SWIFT_APPLICATION_CREDENTIAL_ID": "fakecredid",
            "SWIFT_APPLICATION_CREDENTIAL_SECRET": "fakecredsecret",
        },
    )
    @mock.patch("swiftclient.Connection")
    def test_reaggregate_link_archives_monthly_skips_if_linkevents_for_month(self, mock_swift_connection):
        mock_conn = mock_swift_connection.return_value
        mock_conn.get_account.return_value = (
            {},
            [{"name": "archive-aggregates-backup-2024-12-22"}],
        )
        mock_conn.get_container.return_value = ({},[])
        temp_dir = tempfile.gettempdir()
        archive_filename = "links_linkevent_20241222_0.json.gz"
        archive_path = os.path.join(temp_dir, archive_filename)
        json_data = [
            {
                "model": "links.linkevent",
                "pk": 1,
                "fields": {
                    "link": "https://www.another_domain.com/articles/",
                    "timestamp": "2024-12-16T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 2,
                "fields": {
                    "link": "https://www.test.com/",
                    "timestamp": "2024-12-16T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 3,
                "fields": {
                    "link": "https://www.test.com/3",
                    "timestamp": "2024-12-15T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 4,
                "fields": {
                    "link": "https://www.test.com/4",
                    "timestamp": "2024-12-15T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 0,
                    "on_user_list": True,
                    "url": []
                }
            },
        ]

        with gzip.open(archive_path, "wt", encoding="utf-8") as f:
            json.dump(json_data, f)


        # create link events
        call_command("loaddata", archive_path)

        try:
            call_command(
                "reaggregate_link_archives",
                "--month",
                "202412",
                "--organisation",
                self.organisation.id,
                "--dir",
                temp_dir,
            )
            # assert no daily or monthly aggregates created
            self.assertEqual(0, LinkAggregate.objects.count())
            self.assertEqual(0, UserAggregate.objects.count())
            self.assertEqual(0, PageProjectAggregate.objects.count())
        finally:
            for file in glob.glob(archive_path):
                os.remove(file)


    @mock.patch.dict(
        os.environ,
        {
            "OPENSTACK_AUTH_URL": "fakeurl",
            "SWIFT_APPLICATION_CREDENTIAL_ID": "fakecredid",
            "SWIFT_APPLICATION_CREDENTIAL_SECRET": "fakecredsecret",
        },
    )
    @mock.patch("swiftclient.Connection")
    def test_reaggregate_link_archives_daily(self, mock_swift_connection):
        mock_conn = mock_swift_connection.return_value
        mock_conn.get_account.return_value = (
            {},
            [],
        )
        mock_conn.get_container.return_value = ({},[])
        temp_dir = tempfile.gettempdir()
        archive_filename = "links_linkevent_20241222_0.json.gz"
        archive_path = os.path.join(temp_dir, archive_filename)
        json_data = [
            {
                "model": "links.linkevent",
                "pk": 1,
                "fields": {
                    "link": "https://www.another_domain.com/articles/",
                    "timestamp": "2024-12-16T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 2,
                "fields": {
                    "link": "https://www.test.com/",
                    "timestamp": "2024-12-16T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 3,
                "fields": {
                    "link": "https://www.test.com/3",
                    "timestamp": "2024-12-15T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 4,
                "fields": {
                    "link": "https://www.test.com/4",
                    "timestamp": "2024-12-15T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 0,
                    "on_user_list": True,
                    "url": []
                }
            },
        ]

        with gzip.open(archive_path, "wt", encoding="utf-8") as f:
            json.dump(json_data, f)

        try:
            call_command(
                "reaggregate_link_archives",
                "--day",
                "20241215",
                "--organisation",
                self.organisation.id,
                "--dir",
                temp_dir,
            )
            daily_link_aggregate = LinkAggregate.objects.all().first()
            daily_user_aggregate = UserAggregate.objects.all().first()
            daily_pageproject_aggregate = PageProjectAggregate.objects.all().first()
            # assert no monthly aggregates created
            self.assertEqual(1, LinkAggregate.objects.count())
            self.assertEqual(1, UserAggregate.objects.count())
            self.assertEqual(1, PageProjectAggregate.objects.count())
            # assert daily aggregates were created for the correct day
            self.assertEqual(0, LinkAggregate.objects.filter(day=16).count())
            self.assertEqual(0, UserAggregate.objects.filter(day=16).count())
            self.assertEqual(0, PageProjectAggregate.objects.filter(day=16).count())
            self.assertEqual(1, LinkAggregate.objects.filter(day=15).count())
            self.assertEqual(1, UserAggregate.objects.filter(day=15).count())
            self.assertEqual(1, PageProjectAggregate.objects.filter(day=15).count())
            # assert totals match expected totals
            self.assertEqual(1, daily_link_aggregate.total_links_added)
            self.assertEqual(1, daily_link_aggregate.total_links_removed)
            self.assertEqual(1, daily_user_aggregate.total_links_added)
            self.assertEqual(1, daily_user_aggregate.total_links_removed)
            self.assertEqual(1, daily_pageproject_aggregate.total_links_added)
            self.assertEqual(1, daily_pageproject_aggregate.total_links_removed)
        finally:
            for file in glob.glob(archive_path):
                os.remove(file)


    @mock.patch.dict(
        os.environ,
        {
            "OPENSTACK_AUTH_URL": "fakeurl",
            "SWIFT_APPLICATION_CREDENTIAL_ID": "fakecredid",
            "SWIFT_APPLICATION_CREDENTIAL_SECRET": "fakecredsecret",
        },
    )
    @mock.patch("swiftclient.Connection")
    def test_reaggregate_link_archives_daily_multiple_projects(self, mock_swift_connection):
        mock_conn = mock_swift_connection.return_value
        mock_conn.get_account.return_value = (
            {},
            [],
        )
        mock_conn.get_container.return_value = ({},[])
        temp_dir = tempfile.gettempdir()
        archive_filename = "links_linkevent_20241222_0.json.gz"
        archive_path = os.path.join(temp_dir, archive_filename)
        json_data = [
            {
                "model": "links.linkevent",
                "pk": 1,
                "fields": {
                    "link": "https://www.another_domain.com/articles/",
                    "timestamp": "2024-12-16T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 2,
                "fields": {
                    "link": "https://www.test.com/",
                    "timestamp": "2024-12-16T09:15:27.363Z",
                    "domain": "cy.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 3,
                "fields": {
                    "link": "https://www.test.com/3",
                    "timestamp": "2024-12-15T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 4,
                "fields": {
                    "link": "https://www.test.com/4",
                    "timestamp": "2024-12-15T09:15:27.363Z",
                    "domain": "de.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 0,
                    "on_user_list": True,
                    "url": []
                }
            },
        ]

        with gzip.open(archive_path, "wt", encoding="utf-8") as f:
            json.dump(json_data, f)

        try:
            call_command(
                "reaggregate_link_archives",
                "--day",
                "20241215",
                "--organisation",
                self.organisation.id,
                "--dir",
                temp_dir,
            )
            daily_link_aggregate = LinkAggregate.objects.all().first()
            daily_user_aggregate = UserAggregate.objects.all().first()
            daily_pageproject_aggregate1 = PageProjectAggregate.objects.filter(project_name="en.wikipedia.org").first()
            daily_pageproject_aggregate2 = PageProjectAggregate.objects.filter(project_name="de.wikipedia.org").first()
            # assert no monthly aggregates created
            self.assertEqual(1, LinkAggregate.objects.count())
            self.assertEqual(1, UserAggregate.objects.count())
            self.assertEqual(2, PageProjectAggregate.objects.count())
            # assert daily aggregates were created for the correct day
            self.assertEqual(0, LinkAggregate.objects.filter(day=16).count())
            self.assertEqual(0, UserAggregate.objects.filter(day=16).count())
            self.assertEqual(0, PageProjectAggregate.objects.filter(day=16).count())
            self.assertEqual(1, LinkAggregate.objects.filter(day=15).count())
            self.assertEqual(1, UserAggregate.objects.filter(day=15).count())
            self.assertEqual(2, PageProjectAggregate.objects.filter(day=15).count())
            # assert totals match expected totals
            self.assertEqual(1, daily_link_aggregate.total_links_added)
            self.assertEqual(1, daily_link_aggregate.total_links_removed)
            self.assertEqual(1, daily_user_aggregate.total_links_added)
            self.assertEqual(1, daily_user_aggregate.total_links_removed)
            self.assertEqual(1, daily_pageproject_aggregate1.total_links_added)
            self.assertEqual(0, daily_pageproject_aggregate1.total_links_removed)
            self.assertEqual(0, daily_pageproject_aggregate2.total_links_added)
            self.assertEqual(1, daily_pageproject_aggregate2.total_links_removed)
        finally:
            for file in glob.glob(archive_path):
                os.remove(file)

    @mock.patch.dict(
        os.environ,
        {
            "OPENSTACK_AUTH_URL": "fakeurl",
            "SWIFT_APPLICATION_CREDENTIAL_ID": "fakecredid",
            "SWIFT_APPLICATION_CREDENTIAL_SECRET": "fakecredsecret",
        },
    )
    @mock.patch("swiftclient.Connection")
    def test_reaggregate_link_archives_daily_multiple_pages(self, mock_swift_connection):
        mock_conn = mock_swift_connection.return_value
        mock_conn.get_account.return_value = (
            {},
            [],
        )
        mock_conn.get_container.return_value = ({},[])
        temp_dir = tempfile.gettempdir()
        archive_filename = "links_linkevent_20241222_0.json.gz"
        archive_path = os.path.join(temp_dir, archive_filename)
        json_data = [
            {
                "model": "links.linkevent",
                "pk": 1,
                "fields": {
                    "link": "https://www.another_domain.com/articles/",
                    "timestamp": "2024-12-16T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 2,
                "fields": {
                    "link": "https://www.test.com/",
                    "timestamp": "2024-12-16T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 3,
                "fields": {
                    "link": "https://www.test.com/3",
                    "timestamp": "2024-12-15T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test2",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 4,
                "fields": {
                    "link": "https://www.test.com/4",
                    "timestamp": "2024-12-15T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 0,
                    "on_user_list": True,
                    "url": []
                }
            },
        ]

        with gzip.open(archive_path, "wt", encoding="utf-8") as f:
            json.dump(json_data, f)

        try:
            call_command(
                "reaggregate_link_archives",
                "--day",
                "20241215",
                "--organisation",
                self.organisation.id,
                "--dir",
                temp_dir,
            )
            daily_link_aggregate = LinkAggregate.objects.all().first()
            daily_user_aggregate = UserAggregate.objects.all().first()
            monthly_page_project_aggregates_page_1 = PageProjectAggregate.objects.filter(page_name="test").first()
            monthly_page_project_aggregates_page_2 = PageProjectAggregate.objects.filter(page_name="test2").first()
            # assert no monthly aggregates created
            self.assertEqual(1, LinkAggregate.objects.count())
            self.assertEqual(1, UserAggregate.objects.count())
            self.assertEqual(2, PageProjectAggregate.objects.count())
            # assert daily aggregates were created for the correct day
            self.assertEqual(0, LinkAggregate.objects.filter(day=16).count())
            self.assertEqual(0, UserAggregate.objects.filter(day=16).count())
            self.assertEqual(0, PageProjectAggregate.objects.filter(day=16).count())
            self.assertEqual(1, LinkAggregate.objects.filter(day=15).count())
            self.assertEqual(1, UserAggregate.objects.filter(day=15).count())
            self.assertEqual(2, PageProjectAggregate.objects.filter(day=15).count())
            # assert totals match expected totals
            self.assertEqual(1, daily_link_aggregate.total_links_added)
            self.assertEqual(1, daily_link_aggregate.total_links_removed)
            self.assertEqual(1, daily_user_aggregate.total_links_added)
            self.assertEqual(1, daily_user_aggregate.total_links_removed)
            self.assertEqual(0, monthly_page_project_aggregates_page_1.total_links_added)
            self.assertEqual(1, monthly_page_project_aggregates_page_1.total_links_removed)
            self.assertEqual(1, monthly_page_project_aggregates_page_2.total_links_added)
            self.assertEqual(0, monthly_page_project_aggregates_page_2.total_links_removed)
        finally:
            for file in glob.glob(archive_path):
                os.remove(file)

    @mock.patch.dict(
        os.environ,
        {
            "OPENSTACK_AUTH_URL": "fakeurl",
            "SWIFT_APPLICATION_CREDENTIAL_ID": "fakecredid",
            "SWIFT_APPLICATION_CREDENTIAL_SECRET": "fakecredsecret",
        },
    )
    @mock.patch("swiftclient.Connection")
    def test_reaggregate_link_archives_daily_multiple_users(self, mock_swift_connection):
        mock_conn = mock_swift_connection.return_value
        mock_conn.get_account.return_value = (
            {},
            [],
        )
        mock_conn.get_container.return_value = ({},[])
        temp_dir = tempfile.gettempdir()
        archive_filename = "links_linkevent_20241222_0.json.gz"
        archive_path = os.path.join(temp_dir, archive_filename)
        json_data = [
            {
                "model": "links.linkevent",
                "pk": 1,
                "fields": {
                    "link": "https://www.another_domain.com/articles/",
                    "timestamp": "2024-12-16T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.username,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 2,
                "fields": {
                    "link": "https://www.test.com/",
                    "timestamp": "2024-12-15T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.username,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 3,
                "fields": {
                    "link": "https://www.test.com/3",
                    "timestamp": "2024-12-15T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user2.username,
                    "rev_id": 485489,
                    "user_id": self.user2.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 4,
                "fields": {
                    "link": "https://www.test.com/4",
                    "timestamp": "2024-12-15T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user2.username,
                    "rev_id": 485489,
                    "user_id": self.user2.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 0,
                    "on_user_list": True,
                    "url": []
                }
            },
        ]

        with gzip.open(archive_path, "wt", encoding="utf-8") as f:
            json.dump(json_data, f)

        try:
            call_command(
                "reaggregate_link_archives",
                "--day",
                "20241215",
                "--organisation",
                self.organisation.id,
                "--dir",
                temp_dir,
            )
            daily_link_aggregate = LinkAggregate.objects.all().first()
            daily_user_aggregate = UserAggregate.objects.filter(username=self.user.username).first()
            daily_user_aggregate2 = UserAggregate.objects.filter(username=self.user2.username).first()
            # assert no monthly aggregates created
            self.assertEqual(1, LinkAggregate.objects.count())
            self.assertEqual(2, UserAggregate.objects.count())
            self.assertEqual(1, PageProjectAggregate.objects.count())
            # assert daily aggregates were created for the correct day
            self.assertEqual(0, LinkAggregate.objects.filter(day=16).count())
            self.assertEqual(0, UserAggregate.objects.filter(day=16).count())
            self.assertEqual(0, PageProjectAggregate.objects.filter(day=16).count())
            self.assertEqual(1, LinkAggregate.objects.filter(day=15).count())
            self.assertEqual(2, UserAggregate.objects.filter(day=15).count())
            self.assertEqual(1, PageProjectAggregate.objects.filter(day=15).count())
            # assert totals match expected totals
            self.assertEqual(2, daily_link_aggregate.total_links_added)
            self.assertEqual(1, daily_link_aggregate.total_links_removed)
            self.assertEqual(1, daily_user_aggregate.total_links_added)
            self.assertEqual(0, daily_user_aggregate.total_links_removed)
            self.assertEqual(1, daily_user_aggregate2.total_links_added)
            self.assertEqual(1, daily_user_aggregate2.total_links_removed)
        finally:
            for file in glob.glob(archive_path):
                os.remove(file)

    @mock.patch.dict(
        os.environ,
        {
            "OPENSTACK_AUTH_URL": "fakeurl",
            "SWIFT_APPLICATION_CREDENTIAL_ID": "fakecredid",
            "SWIFT_APPLICATION_CREDENTIAL_SECRET": "fakecredsecret",
        },
    )
    @mock.patch("swiftclient.Connection")
    def test_reaggregate_link_archives_daily_skips_if_uploaded(self, mock_swift_connection):
        mock_conn = mock_swift_connection.return_value
        mock_conn.get_account.return_value = (
            {},
            [],
        )
        mock_conn.get_container.return_value = ({},[{"name": "archive-aggregates-backup-2024-12-15"}])
        temp_dir = tempfile.gettempdir()
        archive_filename = "links_linkevent_20241222_0.json.gz"
        archive_path = os.path.join(temp_dir, archive_filename)
        json_data = [
            {
                "model": "links.linkevent",
                "pk": 1,
                "fields": {
                    "link": "https://www.another_domain.com/articles/",
                    "timestamp": "2024-12-16T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 2,
                "fields": {
                    "link": "https://www.test.com/",
                    "timestamp": "2024-12-16T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 3,
                "fields": {
                    "link": "https://www.test.com/3",
                    "timestamp": "2024-12-15T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 4,
                "fields": {
                    "link": "https://www.test.com/4",
                    "timestamp": "2024-12-15T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 0,
                    "on_user_list": True,
                    "url": []
                }
            },
        ]

        with gzip.open(archive_path, "wt", encoding="utf-8") as f:
            json.dump(json_data, f)

        try:
            call_command(
                "reaggregate_link_archives",
                "--day",
                "20241215",
                "--organisation",
                self.organisation.id,
                "--dir",
                temp_dir,
            )
            self.assertEqual(0, LinkAggregate.objects.count())
            self.assertEqual(0, UserAggregate.objects.count())
            self.assertEqual(0, PageProjectAggregate.objects.count())
        finally:
            for file in glob.glob(archive_path):
                os.remove(file)

    @mock.patch.dict(
        os.environ,
        {
            "OPENSTACK_AUTH_URL": "fakeurl",
            "SWIFT_APPLICATION_CREDENTIAL_ID": "fakecredid",
            "SWIFT_APPLICATION_CREDENTIAL_SECRET": "fakecredsecret",
        },
    )
    @mock.patch("swiftclient.Connection")
    def test_reaggregate_link_archives_daily_skips_if_linkevents_for_day(self, mock_swift_connection):
        mock_conn = mock_swift_connection.return_value
        mock_conn.get_account.return_value = (
            {},
            [],
        )
        mock_conn.get_container.return_value = ({},[])
        temp_dir = tempfile.gettempdir()
        archive_filename = "links_linkevent_20241222_0.json.gz"
        archive_path = os.path.join(temp_dir, archive_filename)
        json_data = [
            {
                "model": "links.linkevent",
                "pk": 1,
                "fields": {
                    "link": "https://www.another_domain.com/articles/",
                    "timestamp": "2024-12-16T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 2,
                "fields": {
                    "link": "https://www.test.com/",
                    "timestamp": "2024-12-16T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 3,
                "fields": {
                    "link": "https://www.test.com/3",
                    "timestamp": "2024-12-15T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 4,
                "fields": {
                    "link": "https://www.test.com/4",
                    "timestamp": "2024-12-15T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 0,
                    "on_user_list": True,
                    "url": []
                }
            },
        ]

        with gzip.open(archive_path, "wt", encoding="utf-8") as f:
            json.dump(json_data, f)

        # create link events
        call_command("loaddata", archive_path)

        try:
            call_command(
                "reaggregate_link_archives",
                "--day",
                "20241215",
                "--organisation",
                self.organisation.id,
                "--dir",
                temp_dir,
            )
            self.assertEqual(0, LinkAggregate.objects.count())
            self.assertEqual(0, UserAggregate.objects.count())
            self.assertEqual(0, PageProjectAggregate.objects.count())
        finally:
            for file in glob.glob(archive_path):
                os.remove(file)

    @mock.patch.dict(
        os.environ,
        {
            "OPENSTACK_AUTH_URL": "fakeurl",
            "SWIFT_APPLICATION_CREDENTIAL_ID": "fakecredid",
            "SWIFT_APPLICATION_CREDENTIAL_SECRET": "fakecredsecret",
        },
    )
    @mock.patch("swiftclient.Connection")
    def test_reaggregate_link_archives_monthly_on_and_off_user_list(self, mock_swift_connection):
        mock_conn = mock_swift_connection.return_value
        mock_conn.get_account.return_value = (
            {},
            [],
        )
        mock_conn.get_container.return_value = ({}, [])
        temp_dir = tempfile.gettempdir()
        archive_filename = "links_linkevent_20241222_0.json.gz"
        archive_path = os.path.join(temp_dir, archive_filename)
        json_data = [
            {
                "model": "links.linkevent",
                "pk": 1,
                "fields": {
                    "link": "https://www.another_domain.com/articles/",
                    "timestamp": "2024-12-16T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 2,
                "fields": {
                    "link": "https://www.test.com/",
                    "timestamp": "2024-12-16T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 3,
                "fields": {
                    "link": "https://www.test.com/3",
                    "timestamp": "2024-12-15T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 4,
                "fields": {
                    "link": "https://www.test.com/4",
                    "timestamp": "2024-12-15T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 0,
                    "on_user_list": False,
                    "url": []
                }
            },
        ]

        with gzip.open(archive_path, "wt", encoding="utf-8") as f:
            json.dump(json_data, f)

        try:
            call_command(
                "reaggregate_link_archives",
                "--month",
                "202412",
                "--organisation",
                self.organisation.id,
                "--dir",
                temp_dir,
            )
            monthly_aggregate_on_user_list = LinkAggregate.objects.filter(on_user_list=True).first()
            monthly_aggregate_not_on_user_list = LinkAggregate.objects.filter(on_user_list=False).first()
            # assert two monthly aggregates were created for on_user_list=True and on_user_list=False
            self.assertEqual(2, LinkAggregate.objects.count())
            self.assertEqual(2, LinkAggregate.objects.filter(day=0).count())
            # assert daily aggregates were not created
            self.assertEqual(0, LinkAggregate.objects.filter(day=16).count())
            self.assertEqual(0, LinkAggregate.objects.filter(day=15).count())
            # assert totals match expected totals
            self.assertEqual(2, monthly_aggregate_on_user_list.total_links_added)
            self.assertEqual(0, monthly_aggregate_on_user_list.total_links_removed)
            self.assertEqual(0, monthly_aggregate_not_on_user_list.total_links_added)
            self.assertEqual(1, monthly_aggregate_not_on_user_list.total_links_removed)
        finally:
            for file in glob.glob(archive_path):
                os.remove(file)

    @mock.patch.dict(
        os.environ,
        {
            "OPENSTACK_AUTH_URL": "fakeurl",
            "SWIFT_APPLICATION_CREDENTIAL_ID": "fakecredid",
            "SWIFT_APPLICATION_CREDENTIAL_SECRET": "fakecredsecret",
        },
    )
    @mock.patch("swiftclient.Connection")
    def test_reaggregate_link_archives_only_link_event_archives(self, mock_swift_connection):
        mock_conn = mock_swift_connection.return_value
        mock_conn.get_account.return_value = (
            {},
            [],
        )
        mock_conn.get_container.return_value = ({}, [])
        temp_dir = tempfile.gettempdir()
        archive_filename = "aggregates_20241222_0.json.gz"
        archive_path = os.path.join(temp_dir, archive_filename)
        json_data = [
            {
                "model": "links.linkevent",
                "pk": 1,
                "fields": {
                    "link": "https://www.another_domain.com/articles/",
                    "timestamp": "2024-12-16T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 2,
                "fields": {
                    "link": "https://www.test.com/",
                    "timestamp": "2024-12-16T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 3,
                "fields": {
                    "link": "https://www.test.com/3",
                    "timestamp": "2024-01-15T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
        ]

        with gzip.open(archive_path, "wt", encoding="utf-8") as f:
            json.dump(json_data, f)

        try:
            call_command(
                "reaggregate_link_archives",
                "--month",
                "202412",
                "--organisation",
                self.organisation.id,
                "--dir",
                temp_dir,
            )
            self.assertEqual(0, LinkAggregate.objects.count())
            self.assertEqual(0, UserAggregate.objects.count())
            self.assertEqual(0, PageProjectAggregate.objects.count())
        finally:
            for file in glob.glob(archive_path):
                os.remove(file)

    @mock.patch.dict(
        os.environ,
        {
            "OPENSTACK_AUTH_URL": "fakeurl",
            "SWIFT_APPLICATION_CREDENTIAL_ID": "fakecredid",
            "SWIFT_APPLICATION_CREDENTIAL_SECRET": "fakecredsecret",
        },
    )
    @mock.patch("swiftclient.Connection")
    def test_reaggregate_link_archives_only_in_correct_zipped_format(self, mock_swift_connection):
        mock_conn = mock_swift_connection.return_value
        mock_conn.get_account.return_value = (
            {},
            [],
        )
        mock_conn.get_container.return_value = ({}, [])
        temp_dir = tempfile.gettempdir()
        archive_filename = "links_linkevent_20241222_0.json"
        archive_path = os.path.join(temp_dir, archive_filename)
        json_data = [
            {
                "model": "links.linkevent",
                "pk": 1,
                "fields": {
                    "link": "https://www.another_domain.com/articles/",
                    "timestamp": "2024-12-16T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 2,
                "fields": {
                    "link": "https://www.test.com/",
                    "timestamp": "2024-12-16T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
            {
                "model": "links.linkevent",
                "pk": 3,
                "fields": {
                    "link": "https://www.test.com/3",
                    "timestamp": "2024-01-15T09:15:27.363Z",
                    "domain": "en.wikipedia.org",
                    "content_type": ContentType.objects.get_for_model(URLPattern).id,
                    "object_id": self.url.id,
                    "username": self.user.id,
                    "rev_id": 485489,
                    "user_id": self.user.id,
                    "page_title": "test",
                    "page_namespace": 0,
                    "event_id": "",
                    "user_is_bot": False,
                    "hash_link_event_id": "",
                    "change": 1,
                    "on_user_list": True,
                    "url": []
                }
            },
        ]

        with gzip.open(archive_path, "wt", encoding="utf-8") as f:
            json.dump(json_data, f)

        try:
            call_command(
                "reaggregate_link_archives",
                "--month",
                "202412",
                "--organisation",
                self.organisation.id,
                "--dir",
                temp_dir,
            )
            self.assertEqual(0, LinkAggregate.objects.count())
            self.assertEqual(0, UserAggregate.objects.count())
            self.assertEqual(0, PageProjectAggregate.objects.count())
        finally:
            for file in glob.glob(archive_path):
                os.remove(file)
