import concurrent.futures
import logging
import os
import swiftclient

from typing import Iterable, List, Tuple

logger = logging.getLogger("django")


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


def upload_file(conn: swiftclient.Connection, container: str, path: str):
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


def batch_upload_files(
    conn: swiftclient.Connection,
    container: str,
    files: Iterable[str],
    max_workers=10,
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
