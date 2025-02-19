from datetime import date, datetime, timezone
import time_machine
from unittest import mock

from django.core.exceptions import ValidationError
from django.test import TestCase

from extlinks.common.forms import FilterForm, YearMonthField
from extlinks.common.helpers import (
    get_linksearchtotal_data_by_time,
    get_normalized_date_for_display,
)
from extlinks.links.factories import LinkSearchTotalFactory, URLPatternFactory
from extlinks.links.models import LinkSearchTotal


class LinkSearchDataByTimeTest(TestCase):
    def setUp(self):
        url = URLPatternFactory(url="www.acme.org")
        # Adding LinkSearch data
        LinkSearchTotalFactory(url=url, date=datetime(2020, 1, 15, tzinfo=timezone.utc))
        LinkSearchTotalFactory(url=url, date=datetime(2020, 2, 1, tzinfo=timezone.utc))
        LinkSearchTotalFactory(url=url, date=datetime(2020, 2, 2, tzinfo=timezone.utc))
        LinkSearchTotalFactory(url=url, date=datetime(2020, 2, 18, tzinfo=timezone.utc))
        LinkSearchTotalFactory(url=url, date=datetime(2020, 3, 6, tzinfo=timezone.utc))
        LinkSearchTotalFactory(
            url=url, date=datetime(2020, 4, 16, tzinfo=timezone.utc), total=0
        )

    def test_linksearch_data_empty_queryset(self):
        linksearch_queryset = None

        dates, linksearch_data = get_linksearchtotal_data_by_time(linksearch_queryset)

        self.assertEqual(0, len(dates))
        self.assertEqual(0, len(linksearch_data))

    def test_linksearch_data(self):
        with time_machine.travel(date(2020, 12, 31)):
            linksearch = LinkSearchTotal.objects.all()

            dates, linksearch_data = get_linksearchtotal_data_by_time(linksearch)

            self.assertEqual(12, len(dates))
            self.assertEqual(12, len(linksearch_data))

    def test_get_normalized_date_for_display(self):
        self.assertEqual("2025-02", get_normalized_date_for_display("2025-02-01"))
        self.assertEqual("2025-01", get_normalized_date_for_display("2025-01"))
        self.assertEqual("", get_normalized_date_for_display("invalid date"))


class YearMonthFieldTest(TestCase):
    def setUp(self):
        self.field = YearMonthField(required=False)

    def test_valid_year_month(self):
        self.assertEqual(self.field.clean("2025-02"), date(2025, 2, 1))
        self.assertEqual(self.field.clean("1999-12"), date(1999, 12, 1))

    def test_invalid_format(self):
        with self.assertRaisesMessage(ValidationError, "Enter a valid year-month"):
            self.field.clean("2025/02")

        with self.assertRaisesMessage(ValidationError, "Enter a valid year-month"):
            self.field.clean("abcd-12")

        with self.assertRaisesMessage(ValidationError, "Enter a valid year-month"):
            self.field.clean("2025-13")

        with self.assertRaisesMessage(ValidationError, "Enter a valid year-month"):
            self.field.clean("2025")

    def test_empty_value(self):
        self.assertIsNone(self.field.clean(""))
        self.assertIsNone(self.field.clean(None))


class FilterFormTest(TestCase):

    def test_valid_data(self):
        form = FilterForm(
            data={
                "start_date": "2025-02",
                "end_date": "2025-06",
                "limit_to_user_list": "on",
                "namespace_id": "10",
                # "exclude_bots": "on", # omit this key to assertFalse
            }
        )
        self.assertTrue(form.is_valid())
        self.assertEqual(form.cleaned_data["start_date"], date(2025, 2, 1))
        self.assertEqual(
            form.cleaned_data["end_date"], date(2025, 6, 30)
        )  # Should return last day of month
        self.assertTrue(form.cleaned_data["limit_to_user_list"])
        self.assertEqual(form.cleaned_data["namespace_id"], 10)
        self.assertFalse(form.cleaned_data["exclude_bots"])

    def test_empty_data(self):
        form = FilterForm(data={})
        self.assertTrue(form.is_valid())
        self.assertIsNone(form.cleaned_data["start_date"])
        self.assertIsNone(form.cleaned_data["end_date"])
        self.assertFalse(form.cleaned_data["limit_to_user_list"])
        self.assertIsNone(form.cleaned_data["namespace_id"])
        self.assertFalse(form.cleaned_data["exclude_bots"])

    def test_invalid_start_date(self):
        form = FilterForm(data={"start_date": "2025/02"})
        self.assertFalse(form.is_valid())
        self.assertIn("start_date", form.errors)

    def test_invalid_end_date(self):
        form = FilterForm(data={"end_date": "abcd-12"})
        self.assertFalse(form.is_valid())
        self.assertIn("end_date", form.errors)

    def test_clean_end_date(self):
        form = FilterForm(data={"end_date": "2023-11"})
        self.assertTrue(form.is_valid())
        self.assertEqual(form.cleaned_data["end_date"], date(2023, 11, 30))

    def test_clean_end_date_leap_year(self):
        form = FilterForm(data={"end_date": "2024-02"})
        self.assertTrue(form.is_valid())
        self.assertEqual(form.cleaned_data["end_date"], date(2024, 2, 29))

    def test_clean_end_date_feb_non_leap_year(self):
        form = FilterForm(data={"end_date": "2025-02"})
        self.assertTrue(form.is_valid())
        self.assertEqual(form.cleaned_data["end_date"], date(2025, 2, 28))
