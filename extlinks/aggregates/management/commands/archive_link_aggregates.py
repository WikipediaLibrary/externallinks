from typing import Type
from django.db import models
from extlinks.aggregates.management.helpers import AggregateArchiveCommand
from extlinks.aggregates.models import LinkAggregate


class Command(AggregateArchiveCommand):
    """
    This command archives data from the 'aggregates_linkaggregate' table.
    """

    help = "Dump & delete or load data from the LinkAggregate table"
    name = "LinkAggregate"

    def get_model(self) -> Type[models.Model]:
        return LinkAggregate
