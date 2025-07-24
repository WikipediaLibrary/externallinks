import os
import shutil
import tempfile

from datetime import date, datetime, timezone
from unittest import mock

import swiftclient
import time_machine

from django.test import TestCase

import extlinks.common.swift as swift

from extlinks.common.forms import FilterForm
from extlinks.common.helpers import get_linksearchtotal_data_by_time
from extlinks.links.factories import LinkSearchTotalFactory, URLPatternFactory
from extlinks.links.models import LinkSearchTotal

SWIFT_TEST_CREDENTIALS = {
    "OPENSTACK_AUTH_URL": "fakeauthurl",
    "SWIFT_APPLICATION_CREDENTIAL_ID": "fakecredentialid",
    "SWIFT_APPLICATION_CREDENTIAL_SECRET": "fakecredentialsecret",
}


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


class FilterFormTest(TestCase):

    def test_valid_data(self):
        form = FilterForm(
            data={
                "start_date": "2025-02-15",
                "end_date": "2025-06-21",
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
        form = FilterForm(data={"start_date": "2025/02/01"})
        self.assertFalse(form.is_valid())
        self.assertIn("start_date", form.errors)

    def test_clean_start_date(self):
        form = FilterForm(data={"start_date": "2023-11-11"})
        self.assertTrue(form.is_valid())
        self.assertEqual(form.cleaned_data["start_date"], date(2023, 11, 1))

    def test_invalid_end_date(self):
        form = FilterForm(data={"end_date": "abcd-12-ef"})
        self.assertFalse(form.is_valid())
        self.assertIn("end_date", form.errors)

    def test_clean_end_date(self):
        form = FilterForm(data={"end_date": "2023-11-01"})
        self.assertTrue(form.is_valid())
        self.assertEqual(form.cleaned_data["end_date"], date(2023, 11, 30))

    def test_clean_end_date_leap_year(self):
        form = FilterForm(data={"end_date": "2024-02-01"})
        self.assertTrue(form.is_valid())
        self.assertEqual(form.cleaned_data["end_date"], date(2024, 2, 29))

    def test_clean_end_date_feb_non_leap_year(self):
        form = FilterForm(data={"end_date": "2025-02-01"})
        self.assertTrue(form.is_valid())
        self.assertEqual(form.cleaned_data["end_date"], date(2025, 2, 28))


class SwiftConnectionTest(TestCase):
    @mock.patch.dict(os.environ, SWIFT_TEST_CREDENTIALS, clear=True)
    def test_swift_connection(self):
        conn = swift.swift_connection()
        self.assertIsInstance(conn, swiftclient.Connection)

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_swift_connection_validation(self):
        with self.assertRaises(RuntimeError):
            swift.swift_connection()


class SwiftUploadTest(TestCase):
    def setUp(self):
        self.tmpdir = os.path.join(tempfile.gettempdir(), "SwiftUploadTest")
        os.mkdir(self.tmpdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def write_file(self, path: str, contents="placeholder"):
        """
        Writes a text file to the temporary directory.
        """

        full_path = os.path.join(self.tmpdir, path)
        with open(full_path, "w") as f:
            f.write(contents)

        return full_path

    @mock.patch("swiftclient.Connection")
    @mock.patch.dict(os.environ, SWIFT_TEST_CREDENTIALS, clear=True)
    def test_swift_upload(self, mock_swift_connection):
        """
        Test that we can upload a file to Swift using the helper methods.
        """

        mock_conn = mock_swift_connection.return_value
        mock_conn.put_object.return_value = ""

        swift.upload_file(
            swift.swift_connection(), "fakecontainer", self.write_file("file.txt")
        )
        mock_conn.put_object.assert_called_once_with(
            "fakecontainer", "file.txt", contents=mock.ANY, content_type=mock.ANY
        )

    @mock.patch("swiftclient.Connection")
    @mock.patch.dict(os.environ, SWIFT_TEST_CREDENTIALS, clear=True)
    def test_swift_batch_upload(self, mock_swift_connection):
        """
        Test that we can upload a batch of files to Swift.
        """

        mock_conn = mock_swift_connection.return_value
        mock_conn.head_object.side_effect = swiftclient.ClientException(
            "Mocked ClientException",
            http_status=404,
            http_reason="Not Found",
            http_response_content="Object not found",
        )
        mock_conn.put_object.return_value = ""

        files = ["file1.txt", "file2.txt", "file3.txt", "file4.txt", "file5.txt"]
        for file in files:
            self.write_file(os.path.join(self.tmpdir, file))

        conn = swift.swift_connection()
        swift.batch_upload_files(
            conn,
            "fakecontainer",
            (os.path.join(self.tmpdir, file) for file in files),
            max_workers=3,
        )
        mock_conn.put_object.assert_has_calls(
            (
                mock.call(
                    "fakecontainer",
                    file,
                    contents=mock.ANY,
                    content_type=mock.ANY,
                )
                for file in files
            ),
            any_order=True,
        )
