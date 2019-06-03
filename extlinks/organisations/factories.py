import factory

from .models import Organisation, Collection


class OrganisationFactory(factory.django.DjangoModelFactory):

    class Meta:
        model = Organisation
        strategy = factory.CREATE_STRATEGY

    name = factory.Faker('company')

    @factory.post_generation
    def program(self, create, extracted, **kwargs):
        if not create:
            return
        if extracted:
            for program in extracted:
                self.program.add(program)


class CollectionFactory(factory.django.DjangoModelFactory):

    class Meta:
        model = Collection
        strategy = factory.CREATE_STRATEGY

    name = factory.Faker('word')
