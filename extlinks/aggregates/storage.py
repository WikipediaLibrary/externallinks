import datetime
import gzip
import itertools
import json
import logging
import os
import re

from typing import Callable, Dict, Hashable, Iterable, List, Optional, Set

from django.core.cache import cache
from django.db.models import Q

from extlinks.common.helpers import extract_queryset_filter
from extlinks.common.swift import (
    batch_download_files,
    get_object_list,
    swift_connection,
)

logger = logging.getLogger("django")

DEFAULT_EXPIRATION_SECS = 60 * 60


def get_archive_list(prefix: str, expiration=DEFAULT_EXPIRATION_SECS) -> List[Dict]:
    """
    Gets a list of all available archives in object storage.
    """

    key = f"{prefix}_archive_list"

    # Retrieves the list from cache if possible.
    archives = cache.get(key)
    if archives:
        return json.loads(archives)

    # Download and cache the archive list if one wasn't available in the cache.
    archives = get_object_list(
        swift_connection(), os.environ["ARCHIVE_CONTAINER"], f"{prefix}_"
    )
    cache.set(key, json.dumps(archives), expiration)

    return archives


def get_archives(
    archives: Iterable[str], expiration=DEFAULT_EXPIRATION_SECS
) -> Dict[str, bytes]:
    """
    Retrieves the requested archives from objects storage or cache.
    """

    # Retrieve as many of the archives from cache as possible.
    archives = list(archives)
    result = cache.get_many(archives)

    # Identify missing archives that were not available in cache.
    missing = set()
    for archive in archives:
        if archive not in result:
            missing.add(archive)

    # Download and cache missing archives.
    if len(missing) > 0:
        downloaded_archives = batch_download_files(
            swift_connection(), os.environ["ARCHIVE_CONTAINER"], archives
        )
        cache.set_many(downloaded_archives, expiration)
        result |= downloaded_archives

    return result


def decode_archive(archive: bytes) -> List[Dict]:
    """
    Decodes a gzipped archive into a list of dictionaries (row records).
    """

    return json.loads(gzip.decompress(archive))


def download_aggregates(
    prefix: str,
    queryset_filter: Q,
    from_date: Optional[datetime.date] = None,
    to_date: Optional[datetime.date] = None,
) -> List[Dict]:
    """
    Find and download archives needed to augment aggregate results from the DB.

    This function tries its best to apply the passed in Django queryset to the
    records it returns. This function supports filtering by collection, user
    list, and date ranges.
    """

    extracted_filters = extract_queryset_filter(queryset_filter)
    collection_id = extracted_filters["collection"].pk
    on_user_list = extracted_filters.get("on_user_list", False)

    if from_date is None:
        from_date = extracted_filters.get("full_date__gte")
        if isinstance(from_date, str):
            from_date = datetime.datetime.strptime(from_date, "%Y-%m-%d").date()

    if to_date is None:
        to_date = extracted_filters.get("full_date__lte")
        if isinstance(to_date, str):
            to_date = datetime.datetime.strptime(to_date, "%Y-%m-%d").date()

    # We're only returning objects that match the following pattern. The
    # archive filenames use the following naming convention:
    #
    # {prefix}_{organisation}_{collection}_{full_date}_{on_user_list}.json.gz
    pattern = (
        rf"^{prefix}_([0-9]+)_([0-9]+)_([0-9]+-[0-9]{{2}}-[0-9]{{2}})_([01])\.json\.gz$"
    )

    # Identify archives that need to be downloaded from object storage
    # because they are not available in the database.
    archives = []
    for archive in get_archive_list(prefix):
        details = re.search(pattern, archive["name"])
        if not details:
            continue

        archive_collection_id = int(details.group(2))
        archive_date = datetime.datetime.strptime(details.group(3), "%Y-%m-%d").date()
        archive_on_user_list = bool(int(details.group(4)))

        # Filter out archives that don't match the queryset filter.
        if (
            (archive_collection_id != collection_id)
            or (on_user_list != archive_on_user_list)
            or (to_date and archive_date > to_date)
            or (from_date and archive_date < from_date)
        ):
            continue

        archives.append(archive)

    # Bail out if there's nothing to download.
    if len(archives) == 0:
        return []

    # Download and decompress the archives from object storage.
    unflattened_records = (
        (record["fields"] for record in decode_archive(contents))
        for contents in get_archives(archive["name"] for archive in archives).values()
    )

    # Each archive has its own records and are grouped together in a
    # two-dimensional array. Merge them all together.
    return list(itertools.chain(*unflattened_records))


def calculate_totals(
    records: Iterable[Dict],
    group_by: Optional[Callable[[Dict], Hashable]] = None,
) -> List[Dict]:
    """
    Caclulate the totals of the passed in records.
    """

    totals = {}

    for record in records:
        key = group_by(record) if group_by else "_default"

        if key in totals:
            totals[key]["total_links_added"] += record["total_links_added"]
            totals[key]["total_links_removed"] += record["total_links_removed"]
            totals[key]["links_diff"] += (
                record["total_links_added"] - record["total_links_removed"]
            )
        else:
            totals[key] = record.copy()
            totals[key]["links_diff"] = (
                record["total_links_added"] - record["total_links_removed"]
            )

    return list(totals.values())


def find_unique(
    records: Iterable[Dict],
    group_by: Callable[[Dict], Hashable],
) -> Set[Hashable]:
    """
    Find all distinct values in the given records.
    """

    values = set()

    for record in records:
        values.add(group_by(record))

    return values
