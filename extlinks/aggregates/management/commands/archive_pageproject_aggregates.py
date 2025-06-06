from typing import Type
from django.db import models
from extlinks.aggregates.management.helpers import AggregateArchiveCommand
from extlinks.aggregates.models import PageProjectAggregate


class Command(AggregateArchiveCommand):
    """
    This command archives data from the 'aggregates_pageprojectaggregate' table.
    """

    help = "Dump & delete or load data from the PageProjectAggregate table"
    name = "PageProjectAggregate"

    def get_model(self) -> Type[models.Model]:
        return PageProjectAggregate
