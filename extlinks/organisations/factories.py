import factory

from .models import Organisation, Collection


class OrganisationFactory(factory.django.DjangoModelFactory):

    class Meta:
        model = Organisation
        strategy = factory.CREATE_STRATEGY

    name = factory.Faker('company')


class CollectionFactory(factory.django.DjangoModelFactory):

    class Meta:
        model = Collection
        strategy = factory.CREATE_STRATEGY

    name = factory.Faker('word')
