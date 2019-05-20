from datetime import datetime

from django.test import TestCase, RequestFactory
from django.urls import reverse

from extlinks.links.factories import (LinkEventFactory,
                                      URLPatternFactory)
from extlinks.links.models import LinkEvent
from .factories import ProgramFactory, OrganisationFactory, CollectionFactory
from .views import (ProgramListView,
                    ProgramDetailView,
                    OrganisationListView,
                    OrganisationDetailView)


class ProgramListTest(TestCase):

    def setUp(self):
        self.program_one = ProgramFactory()
        self.program_two = ProgramFactory()

    def test_program_list_view(self):
        """
        Test that we can simply load the program list page successfully
        """
        factory = RequestFactory()

        request = factory.get(reverse('homepage'))
        response = ProgramListView.as_view()(request)

        self.assertEqual(response.status_code, 200)

    def test_program_list_contents(self):
        """
        Test that the program list page contains the programs we expect.
        """

        factory = RequestFactory()

        request = factory.get(reverse('homepage'))
        response = ProgramListView.as_view()(request)

        self.assertContains(response, self.program_one.name)
        self.assertContains(response, self.program_two.name)


class ProgramDetailTest(TestCase):

    def setUp(self):
        self.program1 = ProgramFactory()
        self.organisation1 = OrganisationFactory(program=self.program1)
        self.organisation2 = OrganisationFactory(program=self.program1)
        self.url1 = reverse('programs:detail',
                            kwargs={'pk': self.program1.pk})

        self.collection1 = CollectionFactory(organisation=self.organisation1)
        self.collection2 = CollectionFactory(organisation=self.organisation2)
        self.collection3 = CollectionFactory(organisation=self.organisation2)

        urlpattern1 = URLPatternFactory(collection=self.collection1)
        urlpattern2 = URLPatternFactory(collection=self.collection2)
        urlpattern3 = URLPatternFactory(collection=self.collection3)

        self.linkevent1 = LinkEventFactory(
            link=urlpattern1.url + "/test",
            change=LinkEvent.ADDED,
            username='Jim',
            timestamp=datetime(2019, 1, 15))
        self.linkevent1.url.add(urlpattern1)
        self.linkevent1.save()

        self.linkevent2 = LinkEventFactory(
            link=urlpattern2.url + "/test",
            change=LinkEvent.ADDED,
            username='Jim',
            timestamp=datetime(2019, 1, 10))
        self.linkevent2.url.add(urlpattern2)
        self.linkevent2.save()

        self.linkevent3 = LinkEventFactory(
            link=urlpattern2.url + "/test",
            change=LinkEvent.REMOVED,
            username='Bob',
            timestamp=datetime(2017, 5, 5))
        self.linkevent3.url.add(urlpattern2)
        self.linkevent3.save()

        self.linkevent4 = LinkEventFactory(
            link=urlpattern3.url + "/test",
            change=LinkEvent.ADDED,
            username='Mary',
            timestamp=datetime(2019, 3, 1),
            on_user_list=True)
        self.linkevent4.url.add(urlpattern3)
        self.linkevent4.save()

    def test_program_detail_view(self):
        """
        Test that we can simply load a program detail page successfully
        """
        factory = RequestFactory()

        request = factory.get(self.url1)
        response = ProgramDetailView.as_view()(request,
                                               pk=self.program1.pk)

        self.assertEqual(response.status_code, 200)

    def test_program_detail_links_added(self):
        """
        Test that we're counting the correct total number of added links
        for this program.
        """
        factory = RequestFactory()

        request = factory.get(self.url1)
        response = ProgramDetailView.as_view()(request,
                                               pk=self.program1.pk)

        self.assertEqual(response.context_data['total_added'], 3)

    def test_program_detail_links_removed(self):
        """
        Test that we're counting the correct total number of removed links
        for this program.
        """
        factory = RequestFactory()

        request = factory.get(self.url1)
        response = ProgramDetailView.as_view()(request,
                                               pk=self.program1.pk)

        self.assertEqual(response.context_data['total_removed'], 1)

    def test_program_detail_total_editors(self):
        """
        Test that we're counting the correct total number of editors
        for this program.
        """
        factory = RequestFactory()

        request = factory.get(self.url1)
        response = ProgramDetailView.as_view()(request,
                                               pk=self.program1.pk)

        self.assertEqual(response.context_data['total_editors'], 3)

    def test_program_detail_date_form(self):
        """
        Test that the date limiting form works on the program detail page.
        """
        factory = RequestFactory()

        data = {
            'start_date': '2019-01-01',
            'end_date': '2019-02-01'
        }

        request = factory.get(self.url1, data)
        response = ProgramDetailView.as_view()(request,
                                               pk=self.program1.pk)

        self.assertEqual(response.context_data['total_added'], 2)
        self.assertEqual(response.context_data['total_removed'], 0)

    def test_program_detail_user_list_form(self):
        """
        Test that the date limiting form works on the program detail page.
        """
        factory = RequestFactory()

        data = {
            'limit_to_user_list': True
        }

        request = factory.get(self.url1, data)
        response = ProgramDetailView.as_view()(request,
                                               pk=self.program1.pk)

        self.assertEqual(response.context_data['total_added'], 1)
        self.assertEqual(response.context_data['total_removed'], 0)


class OrganisationListTest(TestCase):

    def setUp(self):
        self.program = ProgramFactory()
        self.organisation_one = OrganisationFactory(program=self.program)
        self.organisation_two = OrganisationFactory(program=self.program)

    def test_organisation_list_view(self):
        """
        Test that we can simply load the organisation list page successfully
        """
        factory = RequestFactory()

        request = factory.get(reverse('programs:organisation-list'))
        response = OrganisationListView.as_view()(request)

        self.assertEqual(response.status_code, 200)

    def test_organisation_list_contents(self):
        """
        Test that the organisation list page contains the programs we expect.
        """

        factory = RequestFactory()

        request = factory.get(reverse('programs:organisation-list'))
        response = OrganisationListView.as_view()(request)

        self.assertContains(response, self.organisation_one.name)
        self.assertContains(response, self.organisation_two.name)


class OrganisationDetailTest(TestCase):
    """
    The same tests as above, at least for now.
    """

    def setUp(self):
        self.program1 = ProgramFactory()
        self.organisation1 = OrganisationFactory(program=self.program1)
        self.url1 = reverse('programs:organisation-detail',
                            kwargs={'pk': self.organisation1.pk})

        self.collection1 = CollectionFactory(organisation=self.organisation1)
        self.collection1_key = self.collection1.name

        urlpattern1 = URLPatternFactory(collection=self.collection1)

        self.linkevent1 = LinkEventFactory(
            link=urlpattern1.url + "/test",
            change=LinkEvent.ADDED,
            username='Jim',
            timestamp=datetime(2019, 1, 15))
        self.linkevent1.url.add(urlpattern1)
        self.linkevent1.save()

        self.linkevent2 = LinkEventFactory(
            link=urlpattern1.url + "/test",
            change=LinkEvent.ADDED,
            username='Jim',
            timestamp=datetime(2019, 1, 10))
        self.linkevent2.url.add(urlpattern1)
        self.linkevent2.save()

        self.linkevent3 = LinkEventFactory(
            link=urlpattern1.url + "/test",
            change=LinkEvent.REMOVED,
            username='Bob',
            timestamp=datetime(2017, 5, 5))
        self.linkevent3.url.add(urlpattern1)
        self.linkevent3.save()

        self.linkevent4 = LinkEventFactory(
            link=urlpattern1.url + "/test",
            change=LinkEvent.ADDED,
            username='Mary',
            timestamp=datetime(2019, 3, 1),
            on_user_list=True)
        self.linkevent4.url.add(urlpattern1)
        self.linkevent4.save()

    def test_organisation_detail_view(self):
        """
        Test that we can simply load a organisation detail page successfully
        """
        factory = RequestFactory()

        request = factory.get(self.url1)
        response = OrganisationDetailView.as_view()(request,
                                                    pk=self.organisation1.pk)

        self.assertEqual(response.status_code, 200)

    def test_organisation_detail_links_added(self):
        """
        Test that we're counting the correct total number of added links
        for this program.
        """
        factory = RequestFactory()

        request = factory.get(self.url1)
        response = OrganisationDetailView.as_view()(request,
                                                    pk=self.organisation1.pk)

        self.assertEqual(response.context_data['collections'][self.collection1_key]['total_added'], 3)

    def test_organisation_detail_links_removed(self):
        """
        Test that we're counting the correct total number of removed links
        for this organisation.
        """
        factory = RequestFactory()

        request = factory.get(self.url1)
        response = OrganisationDetailView.as_view()(request,
                                                    pk=self.organisation1.pk)

        self.assertEqual(response.context_data['collections'][self.collection1_key]['total_removed'], 1)

    def test_organisation_detail_total_editors(self):
        """
        Test that we're counting the correct total number of editors
        for this organisation.
        """
        factory = RequestFactory()

        request = factory.get(self.url1)
        response = OrganisationDetailView.as_view()(request,
                                                    pk=self.organisation1.pk)

        self.assertEqual(response.context_data['collections'][self.collection1_key]['total_editors'], 3)

    def test_organisation_detail_date_form(self):
        """
        Test that the date limiting form works on the organisation detail page.
        """
        factory = RequestFactory()

        data = {
            'start_date': '2019-01-01',
            'end_date': '2019-02-01'
        }

        request = factory.get(self.url1, data)
        response = OrganisationDetailView.as_view()(request,
                                                    pk=self.organisation1.pk)

        self.assertEqual(response.context_data['collections'][self.collection1_key]['total_added'], 2)
        self.assertEqual(response.context_data['collections'][self.collection1_key]['total_removed'], 0)

    def test_organisation_detail_user_list_form(self):
        """
        Test that the date limiting form works on the organisation detail page.
        """
        factory = RequestFactory()

        data = {
            'limit_to_user_list': True
        }

        request = factory.get(self.url1, data)
        response = OrganisationDetailView.as_view()(request,
                                                    pk=self.organisation1.pk)

        self.assertEqual(response.context_data['collections'][self.collection1_key]['total_added'], 1)
        self.assertEqual(response.context_data['collections'][self.collection1_key]['total_removed'], 0)
