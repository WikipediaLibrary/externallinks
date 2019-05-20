import factory

from .models import Program


class ProgramFactory(factory.django.DjangoModelFactory):

    class Meta:
        model = Program
        strategy = factory.CREATE_STRATEGY

    name = factory.Faker('company')
    description = factory.Faker('text', max_nb_chars=200)
