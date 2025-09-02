import calendar, datetime, logging

from typing import Any, Dict, List, Optional, Tuple

from dateutil.relativedelta import relativedelta
from django.core.management.base import CommandError, CommandParser
from django.db.models.aggregates import Sum
from django.db.models.expressions import F

from extlinks.aggregates.models import LinkAggregate, ProgramTopOrganisationsTotal
from extlinks.common.management.commands import BaseCommand
from extlinks.programs.models import Program

logger = logging.getLogger("django")

CHUNK_SIZE = 10_000


class Command(BaseCommand):
    """
    Create top organisation totals for the given month(s).
    """

    help = "Generate top organisations totals for all programs"

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "-d",
            "--date",
            nargs="?",
            type=lambda arg: datetime.datetime.strptime(arg, "%Y-%m").date(),
            help="A date formatted as YYYY-MM to begin creating totals from.",
            required=False,
        )

    def handle(self, *args, **options):
        date = options["date"]

        # Pick the earliest possible date if one is not provided on the CLI.
        if not date:
            oldest = LinkAggregate.objects.order_by("full_date").first()
            if not oldest:
                raise CommandError(
                    "There are not LinkAggregate records to create "
                    "ProgramTopOrganisationsTotals from. Stopping..."
                )

            date = datetime.date(oldest.year, oldest.month, 1)

        for program in Program.objects.all():
            self.calculate_totals(program, date)

    def calculate_totals(self, program: Program, start: datetime.date):
        """
        Calculate totals for the given program starting at the given date.

        Parameters
        ----------
        program : Program
            The program to calculate totals for.

        start : datetime.date
            The date to start calculating totals for (inclusive).
        """

        organisations = list(program.organisation_set.all())

        while True:
            # Stop generating totals once we pass the current month.
            now = datetime.datetime.now(datetime.timezone.utc).date()
            if start.year > now.year or (
                start.year == now.year and start.month > now.month
            ):
                break

            # Calculate totals for the target month grouped by the organisation
            # and whether those totals are user list totals or not.
            top_organisations_totals = list(
                LinkAggregate.objects.filter(
                    full_date__gte=start,
                    full_date__lt=start + relativedelta(months=1),
                    organisation__in=organisations,
                )
                .values("organisation__pk", "on_user_list")
                .annotate(
                    organisation_id=F("organisation_id"),
                    full_date=F("full_date"),
                    total_links_added=Sum("total_links_added"),
                    total_links_removed=Sum("total_links_removed"),
                )
                .all()
            )

            new_totals = []
            existing_totals = []

            # Iterate through the caclulated totals and batch total INSERTs and
            # UPDATEs flushing them whenever we reach CHUNK_SIZE.
            for total in top_organisations_totals:
                created, updated = self.upsert_total(program, total)
                if created:
                    new_totals.append(created)
                elif updated:
                    existing_totals.append(updated)

                # Save whenever we exceed the maximum allowed chunk size.
                if len(new_totals) + len(existing_totals) >= CHUNK_SIZE:
                    self.bulk_save_totals(program, start, new_totals, existing_totals)
                    new_totals = []
                    existing_totals = []

            # Flush the remaining totals that didn't exceed CHUNK_SIZE.
            if len(new_totals) + len(existing_totals) > 0:
                self.bulk_save_totals(program, start, new_totals, existing_totals)
                new_totals = []
                existing_totals = []

            # Process the next month.
            start += relativedelta(months=1)

    def upsert_total(
        self, program: Program, total: Dict[str, Any]
    ) -> Tuple[
        Optional[ProgramTopOrganisationsTotal], Optional[ProgramTopOrganisationsTotal]
    ]:
        """
        Update an existing organisation total for a month if one exists, or
        create a new one if one doesn't exist.

        Parameters
        ----------
        program : Program
            The program to update (or create) the total for.

        total : Dict[str, Any]
            The total to update or create.

        Returns
        -------
        Tuple[Optional[ProgramTopOrganisationsTotal], Optional[ProgramTopOrganisationsTotal]]
            The created and updated totals. Only one of the two returned values
            in the tuple will contain a non-null value.
        """

        created = None
        updated = None

        # Get the last day of the month and use that in the record. We only
        # want to key these records by year and month and not day. This is done
        # so in-case monthly aggregates haven't run yet so that we don't leave
        # duplicate data in the totals tables.
        _, last_day = calendar.monthrange(
            total["full_date"].year, total["full_date"].month
        )
        full_date = datetime.date(
            total["full_date"].year, total["full_date"].month, last_day
        )

        existing_total = ProgramTopOrganisationsTotal.objects.filter(
            program=program,
            full_date=full_date,
            organisation_id=total["organisation_id"],
            on_user_list=total["on_user_list"],
        ).first()

        if existing_total:
            existing_total.total_links_added = total["total_links_added"]
            existing_total.total_links_removed = total["total_links_removed"]
            updated = existing_total
        else:
            new_total = ProgramTopOrganisationsTotal(
                program=program,
                organisation_id=total["organisation_id"],
                full_date=full_date,
                on_user_list=total["on_user_list"],
                total_links_added=total["total_links_added"],
                total_links_removed=total["total_links_removed"],
            )
            created = new_total

        return created, updated

    def bulk_save_totals(
        self,
        program: Program,
        date: datetime.date,
        new_totals: List[ProgramTopOrganisationsTotal],
        existing_totals: List[ProgramTopOrganisationsTotal],
    ):
        """
        Bulk save totals to the database all at once to reduce round trips.

        Parameters
        ----------
        program : Program
            The program to the totals belong to.

        date : datetime.date
            The date the totals belong to.

        new_totals : List[ProgramTopOrganisationsTotal]
            New totals that don't already exist in the database to save.

        existing_totals : List[ProgramTopOrganisationsTotal]
            Existing totals that already exist in the database to update.
        """

        logger.info(
            "Saving %d totals for '%s' to the database (%04d-%02d, %d INSERTS, %d UPDATES)",
            len(new_totals) + len(existing_totals),
            program.name,
            date.year,
            date.month,
            len(new_totals),
            len(existing_totals),
        )

        if len(new_totals) > 0:
            ProgramTopOrganisationsTotal.objects.bulk_create(new_totals)

        if len(existing_totals) > 0:
            ProgramTopOrganisationsTotal.objects.bulk_update(
                existing_totals,
                ["total_links_added", "total_links_removed"],
            )
