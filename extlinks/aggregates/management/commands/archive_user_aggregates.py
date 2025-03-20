from typing import Type
from django.db import models
from extlinks.aggregates.management.helpers import AggregateArchiveCommand
from extlinks.aggregates.models import UserAggregate


class Command(AggregateArchiveCommand):
    """
    This command archives data from the 'aggregates_useraggregate' table.
    """

    help = "Dump & delete or load data from the UserAggregate table"
    name = "UserAggregate"

    def get_model(self) -> Type[models.Model]:
        return UserAggregate
