import factory

from .models import Program, Organisation, Collection


class ProgramFactory(factory.django.DjangoModelFactory):

    class Meta:
        model = Program
        strategy = factory.CREATE_STRATEGY

    name = factory.Faker('company')
    description = factory.Faker('text', max_nb_chars=200)


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
