import concurrent.futures
import logging
import os
import swiftclient

from typing import Dict, Iterable, Iterator, List, Optional, Tuple, cast

logger = logging.getLogger("django")

MAX_WORKERS = 10
OBJECT_LIMIT = 1000


def swift_connection() -> swiftclient.Connection:
    """
    Creates a swiftclient Connection configured using environment variables.

    This method works with v1 username & password authentication and v3
    application credentials authentication.
    """

    try:
        return swiftclient.Connection(
            auth_version=os.environ.get("OPENSTACK_AUTH_VERSION", "3"),
            authurl=os.environ["OPENSTACK_AUTH_URL"],
            key=os.environ.get("SWIFT_KEY"),
            user=os.environ.get("SWIFT_USERNAME"),
            os_options={
                "application_credential_id": os.environ.get(
                    "SWIFT_APPLICATION_CREDENTIAL_ID"
                ),
                "application_credential_secret": os.environ.get(
                    "SWIFT_APPLICATION_CREDENTIAL_SECRET"
                ),
            },
        )
    except KeyError:
        raise RuntimeError(
            "The 'OPENSTACK_AUTH_URL' and other appropriate credential "
            "environment variables must be defined to use the Swift client"
        )


def get_object_list(
    conn: swiftclient.Connection,
    container: str,
    prefix: Optional[str] = None,
) -> List[Dict]:
    """
    Gets a list of all objects in a container matching an optional prefix.
    """

    objects = []
    marker = None

    while True:
        _, objects_page = conn.get_container(
            container,
            prefix=prefix,
            marker=marker,
            limit=OBJECT_LIMIT,
        )
        if not objects_page:
            break

        objects.extend(objects_page)
        marker = objects_page[-1]["name"]

        if len(objects_page) < OBJECT_LIMIT:
            break

    return objects


def upload_file(conn: swiftclient.Connection, container: str, path: str) -> str:
    """
    Uploads a file on the local filesystem to the provided Swift container.
    """

    object_name = os.path.basename(path)

    with open(path, "rb") as f:
        conn.put_object(
            container,
            object_name,
            contents=f,
            content_type="application/octet-stream",
        )

    return object_name


def download_file(conn: swiftclient.Connection, container: str, object: str) -> bytes:
    """
    Downloads a file from object storage.
    """

    _, response = conn.get_object(container, object)

    return cast(bytes, response)


def batch_upload_files(
    conn: swiftclient.Connection,
    container: str,
    files: Iterator[str],
    max_workers=MAX_WORKERS,
) -> Tuple[List[str], List[str]]:
    """
    Uploads a batch of multiple files to the given Swift container.
    """

    successful = []
    failed = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(upload_file, conn, container, f): f for f in files}

        for future in concurrent.futures.as_completed(futures):
            path = futures[future]

            try:
                object_name = future.result()
                logger.info(f"Successfully uploaded '%s' as '%s'", path, object_name)
                successful.append(path)
            except Exception as exc:
                logger.error(f"Upload failed for '%s': %s", path, exc)
                failed.append(path)

    return successful, failed


def batch_download_files(
    conn: swiftclient.Connection,
    container: str,
    objects: Iterable[str],
    max_workers=MAX_WORKERS,
) -> Dict[str, bytes]:
    """
    Downloads a batch of multiple files from the given Swift container.
    """

    result: Dict[str, bytes] = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(download_file, conn, container, o): o for o in objects
        }

        for future in concurrent.futures.as_completed(futures):
            name = futures[future]

            try:
                value = future.result()
                result[name] = value
            except Exception as exc:
                logging.error(
                    "Unable to download '%s' from the '%s' container: %s",
                    name,
                    container,
                    exc,
                )

    return result
