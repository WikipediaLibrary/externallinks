from django.test import TestCase, RequestFactory
from django.urls import reverse

from .views import Homepage


class HomepageTest(TestCase):
    def test_homepage_view(self):
        """
        Can we simply load the homepage successfully?
        """
        factory = RequestFactory()

        request = factory.get(reverse("homepage"))
        response = Homepage.as_view()(request)

        self.assertEqual(response.status_code, 200)
