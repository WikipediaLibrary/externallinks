import datetime
import time_machine
from unittest import mock

from django.test import TestCase

from extlinks.common.helpers import get_linksearchtotal_data_by_time
from extlinks.links.factories import LinkSearchTotalFactory, URLPatternFactory
from extlinks.links.models import LinkSearchTotal


class LinkSearchDataByTimeTest(TestCase):
    def setUp(self):
        url = URLPatternFactory(url="www.acme.org")
        # Adding LinkSearch data
        LinkSearchTotalFactory(url=url, date=datetime.datetime(2020, 1, 15))
        LinkSearchTotalFactory(url=url, date=datetime.datetime(2020, 2, 1))
        LinkSearchTotalFactory(url=url, date=datetime.datetime(2020, 2, 2))
        LinkSearchTotalFactory(url=url, date=datetime.datetime(2020, 2, 18))
        LinkSearchTotalFactory(url=url, date=datetime.datetime(2020, 3, 6))
        LinkSearchTotalFactory(url=url, date=datetime.datetime(2020, 4, 16), total=0)

    def test_linksearch_data_empty_queryset(self):
        linksearch_queryset = None

        dates, linksearch_data = get_linksearchtotal_data_by_time(linksearch_queryset)

        self.assertEqual(0, len(dates))
        self.assertEqual(0, len(linksearch_data))

    def test_linksearch_data(self):
        with time_machine.travel(datetime.date(2020, 12, 31)):
            linksearch = LinkSearchTotal.objects.all()

            dates, linksearch_data = get_linksearchtotal_data_by_time(linksearch)

            self.assertEqual(12, len(dates))
            self.assertEqual(12, len(linksearch_data))
