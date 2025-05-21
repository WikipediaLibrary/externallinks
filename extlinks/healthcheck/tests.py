import datetime
import json
import os
import shutil
import tempfile
import typing

from unittest import mock

from django.core.cache import cache
from django.http.response import JsonResponse
from django.test import RequestFactory, TestCase
from django.urls import reverse
from django.utils.timezone import now

from extlinks.aggregates.factories import (
    LinkAggregateFactory,
    UserAggregateFactory,
    PageProjectAggregateFactory,
)
from extlinks.healthcheck.views import (
    AggregatesCronHealthCheckView,
    CommonCronHealthCheckView,
    LinkEventHealthCheckView,
    LinksCronHealthCheckView,
    MonthlyAggregatesCronHealthCheckView,
    OrganizationsCronHealthCheckView,
)
from extlinks.links.factories import LinkEventFactory, LinkSearchTotalFactory
from extlinks.organisations.factories import OrganisationFactory
from extlinks.organisations.models import Organisation


class TestLinkEventHealthCheckView(TestCase):
    def setUp(self):
        self.url = reverse("healthcheck:link_event")

        # Clear the view cache between tests so tests don't affect each other.
        cache.clear()

    def test_successful_health_check(self):
        """
        Verify that the healthcheck passes if there are recent link events.
        """

        LinkEventFactory()

        factory = RequestFactory()
        request = factory.get(self.url)
        view = LinkEventHealthCheckView.as_view()
        response = typing.cast(JsonResponse, view(request))

        self.assertEqual(response.status_code, 200)
        self.assertIsInstance(response, JsonResponse)

        content = json.loads(response.content)
        self.assertEqual(content["status"], "ok")

    def test_missing_health_check(self):
        """
        Verify that the healthcheck fails if there are no link events.
        """

        factory = RequestFactory()
        request = factory.get(self.url)
        view = LinkEventHealthCheckView.as_view()
        response = typing.cast(JsonResponse, view(request))

        self.assertEqual(response.status_code, 404)
        self.assertIsInstance(response, JsonResponse)

        content = json.loads(response.content)
        self.assertEqual(content["status"], "not found")

    def test_stale_health_check(self):
        """
        Verify that the healthcheck fails if there are no recent link events.
        """

        LinkEventFactory(timestamp=now() - datetime.timedelta(days=2))

        factory = RequestFactory()
        request = factory.get(self.url)
        view = LinkEventHealthCheckView.as_view()
        response = typing.cast(JsonResponse, view(request))

        self.assertEqual(response.status_code, 500)
        self.assertIsInstance(response, JsonResponse)

        content = json.loads(response.content)
        self.assertEqual(content["status"], "out of date")


class TestAggregatesCronHealthCheckView(TestCase):
    def setUp(self):
        self.url = reverse("healthcheck:agg_crons")

        # Clear the view cache between tests so tests don't affect each other.
        cache.clear()

    def test_successful_health_check(self):
        """
        Verify that the healthcheck passes if there are recent aggregates.
        """

        full_date = now().date()

        LinkAggregateFactory(full_date=full_date)
        UserAggregateFactory(full_date=full_date)
        PageProjectAggregateFactory(full_date=full_date)

        factory = RequestFactory()
        request = factory.get(self.url)
        view = AggregatesCronHealthCheckView.as_view()
        response = typing.cast(JsonResponse, view(request))

        self.assertEqual(response.status_code, 200)
        self.assertIsInstance(response, JsonResponse)

        content = json.loads(response.content)
        self.assertEqual(content["status"], "ok")

    def test_missing_health_check(self):
        """
        Verify that the healthcheck fails if there are no aggregates.
        """

        factory = RequestFactory()
        request = factory.get(self.url)
        view = AggregatesCronHealthCheckView.as_view()
        response = typing.cast(JsonResponse, view(request))

        self.assertEqual(response.status_code, 404)
        self.assertIsInstance(response, JsonResponse)

        content = json.loads(response.content)
        self.assertEqual(content["status"], "not found")

    def test_stale_link_aggregate_health_check(self):
        """
        Verify that the healthcheck fails if there are no recent link
        aggregates.
        """

        valid_full_date = now().date()
        stale_full_date = (now() - datetime.timedelta(days=3)).date()

        LinkAggregateFactory(full_date=stale_full_date)
        UserAggregateFactory(full_date=valid_full_date)
        PageProjectAggregateFactory(full_date=valid_full_date)

        factory = RequestFactory()
        request = factory.get(self.url)
        view = AggregatesCronHealthCheckView.as_view()
        response = typing.cast(JsonResponse, view(request))

        self.assertEqual(response.status_code, 500)
        self.assertIsInstance(response, JsonResponse)

        content = json.loads(response.content)
        self.assertEqual(content["status"], "out of date")

    def test_stale_user_aggregate_health_check(self):
        """
        Verify that the healthcheck fails if there are no recent user
        aggregates.
        """

        valid_full_date = now().date()
        stale_full_date = (now() - datetime.timedelta(days=3)).date()

        LinkAggregateFactory(full_date=valid_full_date)
        UserAggregateFactory(full_date=stale_full_date)
        PageProjectAggregateFactory(full_date=valid_full_date)

        factory = RequestFactory()
        request = factory.get(self.url)
        view = AggregatesCronHealthCheckView.as_view()
        response = typing.cast(JsonResponse, view(request))

        self.assertEqual(response.status_code, 500)
        self.assertIsInstance(response, JsonResponse)

        content = json.loads(response.content)
        self.assertEqual(content["status"], "out of date")

    def test_stale_pageproject_aggregate_health_check(self):
        """
        Verify that the healthcheck fails if there are no recent pageproject
        aggregates.
        """

        valid_full_date = now().date()
        stale_full_date = (now() - datetime.timedelta(days=3)).date()

        LinkAggregateFactory(full_date=valid_full_date)
        UserAggregateFactory(full_date=valid_full_date)
        PageProjectAggregateFactory(full_date=stale_full_date)

        factory = RequestFactory()
        request = factory.get(self.url)
        view = AggregatesCronHealthCheckView.as_view()
        response = typing.cast(JsonResponse, view(request))

        self.assertEqual(response.status_code, 500)
        self.assertIsInstance(response, JsonResponse)

        content = json.loads(response.content)
        self.assertEqual(content["status"], "out of date")


class TestMonthlyAggregatesCronHealthCheckView(TestCase):
    def setUp(self):
        self.url = reverse("healthcheck:month_agg_crons")

        # Clear the view cache between tests so tests don't affect each other.
        cache.clear()

    def test_successful_health_check(self):
        """
        Verify that the healthcheck passes if there are recent monthly
        aggregates.
        """

        full_date = now().date()

        LinkAggregateFactory(
            full_date=full_date,
            year=full_date.year,
            month=full_date.month,
            day=0,
        )
        UserAggregateFactory(
            full_date=full_date,
            year=full_date.year,
            month=full_date.month,
            day=0,
        )
        PageProjectAggregateFactory(
            full_date=full_date,
            year=full_date.year,
            month=full_date.month,
            day=0,
        )

        factory = RequestFactory()
        request = factory.get(self.url)
        view = MonthlyAggregatesCronHealthCheckView.as_view()
        response = typing.cast(JsonResponse, view(request))

        self.assertEqual(response.status_code, 200)
        self.assertIsInstance(response, JsonResponse)

        content = json.loads(response.content)
        self.assertEqual(content["status"], "ok")

    def test_missing_health_check(self):
        """
        Verify that the healthcheck fails if there are no monthly aggregates.
        """

        factory = RequestFactory()
        request = factory.get(self.url)
        view = MonthlyAggregatesCronHealthCheckView.as_view()
        response = typing.cast(JsonResponse, view(request))

        self.assertEqual(response.status_code, 404)
        self.assertIsInstance(response, JsonResponse)

        content = json.loads(response.content)
        self.assertEqual(content["status"], "not found")

    def test_stale_link_aggregate_health_check(self):
        """
        Verify that the healthcheck fails if there are no recent monthly link
        aggregates.
        """

        valid_full_date = now().date()
        stale_full_date = (now() - datetime.timedelta(days=36)).date()

        LinkAggregateFactory(
            full_date=stale_full_date,
            year=stale_full_date.year,
            month=stale_full_date.month,
            day=0,
        )
        UserAggregateFactory(
            full_date=valid_full_date,
            year=valid_full_date.year,
            month=valid_full_date.month,
            day=0,
        )
        PageProjectAggregateFactory(
            full_date=valid_full_date,
            year=valid_full_date.year,
            month=valid_full_date.month,
            day=0,
        )

        factory = RequestFactory()
        request = factory.get(self.url)
        view = MonthlyAggregatesCronHealthCheckView.as_view()
        response = typing.cast(JsonResponse, view(request))

        self.assertEqual(response.status_code, 500)
        self.assertIsInstance(response, JsonResponse)

        content = json.loads(response.content)
        self.assertEqual(content["status"], "out of date")

    def test_stale_user_aggregate_health_check(self):
        """
        Verify that the healthcheck fails if there are no recent monthly user
        aggregates.
        """

        valid_full_date = now().date()
        stale_full_date = (now() - datetime.timedelta(days=36)).date()

        LinkAggregateFactory(
            full_date=valid_full_date,
            year=valid_full_date.year,
            month=valid_full_date.month,
            day=0,
        )
        UserAggregateFactory(
            full_date=stale_full_date,
            year=stale_full_date.year,
            month=stale_full_date.month,
            day=0,
        )
        PageProjectAggregateFactory(
            full_date=valid_full_date,
            year=valid_full_date.year,
            month=valid_full_date.month,
            day=0,
        )

        factory = RequestFactory()
        request = factory.get(self.url)
        view = MonthlyAggregatesCronHealthCheckView.as_view()
        response = typing.cast(JsonResponse, view(request))

        self.assertEqual(response.status_code, 500)
        self.assertIsInstance(response, JsonResponse)

        content = json.loads(response.content)
        self.assertEqual(content["status"], "out of date")

    def test_stale_pageproject_aggregate_health_check(self):
        """
        Verify that the healthcheck fails if there are no recent monthly
        pageproject aggregates.
        """

        valid_full_date = now().date()
        stale_full_date = (now() - datetime.timedelta(days=36)).date()

        LinkAggregateFactory(
            full_date=valid_full_date,
            year=valid_full_date.year,
            month=valid_full_date.month,
            day=0,
        )
        UserAggregateFactory(
            full_date=valid_full_date,
            year=valid_full_date.year,
            month=valid_full_date.month,
            day=0,
        )
        PageProjectAggregateFactory(
            full_date=stale_full_date,
            year=stale_full_date.year,
            month=stale_full_date.month,
            day=0,
        )

        factory = RequestFactory()
        request = factory.get(self.url)
        view = MonthlyAggregatesCronHealthCheckView.as_view()
        response = typing.cast(JsonResponse, view(request))

        self.assertEqual(response.status_code, 500)
        self.assertIsInstance(response, JsonResponse)

        content = json.loads(response.content)
        self.assertEqual(content["status"], "out of date")


HOST_BACKUP_DIR = os.path.join(tempfile.gettempdir(), "TestComonCronHealthCheckView")


class TestComonCronHealthCheckView(TestCase):
    def setUp(self):
        self.url = reverse("healthcheck:common_crons")

        # We use a custom directory to store backup files used in these tests
        # and override 'HOST_BACKUP_DIR' to point to it as the tests using the
        # same backup directory as the development server can cause flakiness.
        os.mkdir(HOST_BACKUP_DIR)

        # Clear the view cache between tests so tests don't affect each other.
        cache.clear()

    def tearDown(self):
        # Remove any backup files that were created during the tests.
        shutil.rmtree(HOST_BACKUP_DIR)

    @mock.patch.dict(os.environ, {"HOST_BACKUP_DIR": HOST_BACKUP_DIR})
    def test_successful_health_check(self):
        """
        Verify that the healthcheck passes if there has been a recent linkevent
        backup (in the last 3 days).
        """

        filename = "links_linkevent_{}_0.json.gz".format(now().strftime("%Y%m%d"))
        filepath = os.path.join(HOST_BACKUP_DIR, filename)

        with open(filepath, "w") as f:
            f.write("totally compressed contents")

        factory = RequestFactory()
        request = factory.get(self.url)
        view = CommonCronHealthCheckView.as_view()
        response = typing.cast(JsonResponse, view(request))

        self.assertEqual(response.status_code, 200)
        self.assertIsInstance(response, JsonResponse)

        content = json.loads(response.content)
        self.assertEqual(content["status"], "ok")

    @mock.patch.dict(os.environ, {"HOST_BACKUP_DIR": HOST_BACKUP_DIR})
    def test_stale_health_check(self):
        """
        Verify that the healthcheck fails if there has not been a recent
        linkevent backup (in the last 3 days).
        """

        factory = RequestFactory()
        request = factory.get(self.url)
        view = CommonCronHealthCheckView.as_view()
        response = typing.cast(JsonResponse, view(request))

        self.assertEqual(response.status_code, 500)
        self.assertIsInstance(response, JsonResponse)

        content = json.loads(response.content)
        self.assertEqual(content["status"], "out of date")


class TestLinksCronHealthCheckView(TestCase):
    def setUp(self):
        self.url = reverse("healthcheck:link_crons")

        # Clear the view cache between tests so tests don't affect each other.
        cache.clear()

    def test_successful_health_check(self):
        """
        Verify that the healthcheck passes if there are recent search totals.
        """

        LinkSearchTotalFactory()

        factory = RequestFactory()
        request = factory.get(self.url)
        view = LinksCronHealthCheckView.as_view()
        response = typing.cast(JsonResponse, view(request))

        self.assertEqual(response.status_code, 200)
        self.assertIsInstance(response, JsonResponse)

        content = json.loads(response.content)
        self.assertEqual(content["status"], "ok")

    def test_missing_health_check(self):
        """
        Verify that the healthcheck fails if there are no search totals.
        """

        factory = RequestFactory()
        request = factory.get(self.url)
        view = LinksCronHealthCheckView.as_view()
        response = typing.cast(JsonResponse, view(request))

        self.assertEqual(response.status_code, 404)
        self.assertIsInstance(response, JsonResponse)

        content = json.loads(response.content)
        self.assertEqual(content["status"], "not found")

    def test_stale_health_check(self):
        """
        Verify that the healthcheck fails if there are no recent search totals.
        """

        LinkSearchTotalFactory(
            date=datetime.datetime.today() - datetime.timedelta(days=10)
        )

        factory = RequestFactory()
        request = factory.get(self.url)
        view = LinksCronHealthCheckView.as_view()
        response = typing.cast(JsonResponse, view(request))

        self.assertEqual(response.status_code, 500)
        self.assertIsInstance(response, JsonResponse)

        content = json.loads(response.content)
        self.assertEqual(content["status"], "out of date")


class TestOrganizationsCronHealthCheckView(TestCase):
    def setUp(self):
        self.url = reverse("healthcheck:org_crons")

        # Clear the view cache between tests so tests don't affect each other.
        cache.clear()

    def test_successful_health_check(self):
        """
        Verify that the healthcheck passes if there have been any recent
        organisation updates.
        """

        OrganisationFactory(username_list_updated=now())

        factory = RequestFactory()
        request = factory.get(self.url)
        view = OrganizationsCronHealthCheckView.as_view()
        response = typing.cast(JsonResponse, view(request))

        self.assertEqual(response.status_code, 200)
        self.assertIsInstance(response, JsonResponse)

        content = json.loads(response.content)
        self.assertEqual(content["status"], "ok")

    def test_missing_health_check(self):
        """
        Verify that the healthcheck fails if there aren't any organisations.
        """

        factory = RequestFactory()
        request = factory.get(self.url)
        view = OrganizationsCronHealthCheckView.as_view()
        response = typing.cast(JsonResponse, view(request))

        self.assertEqual(response.status_code, 404)
        self.assertIsInstance(response, JsonResponse)

        content = json.loads(response.content)
        self.assertEqual(content["status"], "not found")

    def test_stale_health_check(self):
        """
        Verify that the healthcheck fails if there have not been any recent
        organisation updates.
        """

        # Due to the 'username_list_updated' field having 'auto_now=True' we
        # have to manually update the field outside of the ORM to bypass the
        # ORM overriding whatever date we set.
        organisation = OrganisationFactory()
        Organisation.objects.filter(pk=organisation.pk).update(
            username_list_updated=now() - datetime.timedelta(hours=2)
        )
        organisation.refresh_from_db()

        factory = RequestFactory()
        request = factory.get(self.url)
        view = OrganizationsCronHealthCheckView.as_view()
        response = typing.cast(JsonResponse, view(request))

        self.assertEqual(response.status_code, 500)
        self.assertIsInstance(response, JsonResponse)

        content = json.loads(response.content)
        self.assertEqual(content["status"], "out of date")
