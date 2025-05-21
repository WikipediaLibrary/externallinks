import gzip
import json
import os

from typing import Union

from django.core.serializers import serialize

from extlinks.aggregates.models import (
    LinkAggregate,
    PageProjectAggregate,
    UserAggregate,
)
from extlinks.aggregates.management.helpers.aggregate_archive_command import (
    AggregateArchiveCommand,
)


def decode_archive(filename: str):
    """
    Loads and decompresses the given archive file.
    """

    with gzip.open(filename, "rt", encoding="utf-8") as archive:
        return json.loads(archive.read())


def validate_aggregate_archive(
    aggregate_type: str,
    aggregate: Union[LinkAggregate, UserAggregate, PageProjectAggregate],
    output_dir: str,
) -> bool:
    """
    Validates that the gvien aggregate has a matching archive file.
    """

    on_user_list = "1" if aggregate.on_user_list else "0"
    filename = f"aggregates_{aggregate_type}_{aggregate.organisation.id}_{aggregate.collection.id}_{aggregate.full_date}_{on_user_list}.json.gz"
    archive_path = os.path.join(output_dir, filename)

    if not os.path.isfile(archive_path):
        return False

    archive_json = decode_archive(archive_path)
    link_aggregate_json = json.loads(serialize("json", [aggregate]))

    return link_aggregate_json == archive_json


def validate_link_aggregate_archive(
    link_aggregate: LinkAggregate, output_dir: str
) -> bool:
    """
    Validates that the given LinkAggregate has a matching archive file.
    """

    return validate_aggregate_archive("linkaggregate", link_aggregate, output_dir)


def validate_user_aggregate_archive(
    user_aggregate: UserAggregate, output_dir: str
) -> bool:
    """
    Validates that the given UserAggregate has a matching archive file.
    """

    return validate_aggregate_archive("useraggregate", user_aggregate, output_dir)


def validate_pageproject_aggregate_archive(
    pageproject_aggregate: PageProjectAggregate, output_dir: str
) -> bool:
    """
    Validates that the given PageProjectAggregate has a matching archive file.
    """

    return validate_aggregate_archive(
        "pageprojectaggregate", pageproject_aggregate, output_dir
    )


__all__ = ["AggregateArchiveCommand"]
