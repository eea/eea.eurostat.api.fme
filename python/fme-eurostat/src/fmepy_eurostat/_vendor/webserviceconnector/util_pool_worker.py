"""
Utility functions for handling a pool of worker threads.

Includes method specific functions for downloading and uploading.
"""

from __future__ import absolute_import, division, unicode_literals, print_function
import collections
import errno
import os
from multiprocessing.pool import ThreadPool

from ..webserviceconnector.util_folders import strip_prefix

PoolWorkerResult = collections.namedtuple("PoolWorkerResult", ["success", "reference", "error"])


def map_to_new_pool(source_generator, worker, handler, num_threads):
    """
    Creates a ThreadPool with configured settings and applies `worker` to items from
    `source_generator`. Results are handled by `handler` as they become ready. This keeps memory
    overhead fairly low. Handling is guaranteed to occur in input order.

    :param Iterable[Source] source_generator: Generator which produces arguments to **worker**.
    :type worker: (Iterable[Source]) -> Iterable[Result]
    :param worker: Function that is run by threads.
    :type handler: (Iterable[Result]) -> None
    :param handler: Callback that handles results.
    :rtype: None
    """
    pool = ThreadPool(num_threads)
    # chunksize defaults to 1. Thought this would be a problem, but threads are lightweight enough
    # that this doesn't have a significant impact even with small files, and larger chunksizes are
    # *very* bad for large files
    results = pool.imap(worker, source_generator)
    handler(results)
    pool.close()
    pool.join()


def generate_download_worker(connector, folder_id, target_folder, feature, config, delim):
    """Generates a worker for downloading.

    :param FMEWebFilesystemConnectorTransformer connector: FME Web Filesystem Connector.
    :param str folder_id: Relative folder path.
    :param str target_folder: Absolute folder path on disk.
    :param fmeobjects.FMEFeature feature: The feature requesting the operation.
    :param DownloadOperationConfig config: Configuration attributes.
    :param str delim: The delimiter.
    :rtype: (Tuple[str]) -> PoolWorkerResult
    """
    def download_worker(entry):
        # type: (IContainerItem) -> PoolWorkerResult
        if entry.is_container:
            return PoolWorkerResult(success=False, reference=entry.id, error=None)

        relative_target = strip_prefix(entry.id, folder_id, delim)
        download_target = os.path.dirname(
            os.path.join(os.path.normpath(target_folder), os.path.normpath(relative_target))
        )
        if not os.path.exists(download_target):
            # https://stackoverflow.com/questions/600268/mkdir-p-functionality-in-python
            try:
                os.makedirs(download_target)
            except OSError as e:
                if not (e.errno == errno.EEXIST and os.path.isdir(download_target)):
                    raise

        try:
            connector.download_file(feature, config, entry, download_target, output=False)
            return PoolWorkerResult(success=True, reference=entry.id, error=None)
        except Exception as e:
            return PoolWorkerResult(success=False, reference=entry.id, error=e)

    return download_worker


def generate_upload_worker(driver, config):
    """Generates a worker for uploading.

    :param FMEWebFilesystemDriver driver: The Web Filesystem driver.
    :param UploadOperationConfig config: Configuration attributes.
    :rtype: (Tuple[str]) -> UploadWorkerResult
    """

    def upload_worker(args):
        remote_key, local_file = args

        if not config.overwrite_destination:
            if driver.get_item_info(item_id=remote_key, blob_container=config.blob_container):
                return PoolWorkerResult(reference=local_file, success=False, error="FILE_EXISTS")

        try:
            driver.upload_file(
                item_id=remote_key, source_file=local_file, blob_container=config.blob_container
            )
            return PoolWorkerResult(reference=local_file, success=True, error=None)
        except Exception as e:
            return PoolWorkerResult(reference=local_file, success=False, error=e)

    return upload_worker
