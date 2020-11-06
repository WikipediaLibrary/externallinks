import factory
import random
import datetime

from .models import LinkAggregate
from extlinks.organisations.factories import CollectionFactory, OrganisationFactory


class LinkAggregateFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = LinkAggregate
        strategy = factory.CREATE_STRATEGY

    organisation = factory.SubFactory(OrganisationFactory)
    collection = factory.SubFactory(CollectionFactory)
    full_date = factory.Faker(
        "date_between_dates",
        date_start=datetime.date(2017, 1, 1),
        date_end=datetime.date(2020, 10, 31),
    )
    total_links_added = random.randint(0, 100)
    total_links_removed = random.randint(0, 80)
