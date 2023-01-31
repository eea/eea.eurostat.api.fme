# coding: utf-8

"""
driver.py

Shared code between the Google Cloud Storage WebFS and the GoogleCloudStorageConnector.
"""
from fmegeneral.fmelog import get_configured_logger
from fmemodules import WSCException

from fmepy_eurostat._vendor.webserviceconnector.fmewebfs import (
    FMEWebFilesystemDriver,
    ContainerItem,
    ContainerContentResponse,
    ContinuationInfo,
)

#from fmepy_google_cloud_storage.api import GoogleCloudStorageApi, GoogleAPIErrorWrapper
from fmepy_eurostat.constants import (
    DELIM, LOG_NAME, NUM_THREADS, MUST_USE_LITERAL_9900062, PACKAGE_KEYWORD, PARAM_PROJECT_ID, PARAM_BUCKET
)
from fmepy_eurostat._vendor.webserviceconnector.util_pool_worker import (
    PoolWorkerResult,
    map_to_new_pool,
)


def block_non_literal_args(func):
    def decorated(*args, **kwargs):
        project_id = kwargs.get(PARAM_PROJECT_ID, None)
        bucket = kwargs.get(PARAM_BUCKET, kwargs.get("bucket_name", None))
        for value in (project_id, bucket):
            try:
                if "FME_CONDITIONAL" in value or "@Value" in value:
                    raise WSCException(MUST_USE_LITERAL_9900062, [LOG_NAME])
            except TypeError:
                pass
        return func(*args, **kwargs)

    return decorated


class EurostatFilesystemDriver(FMEWebFilesystemDriver):
    def __init__(self):
        super(EurostatFilesystemDriver, self).__init__()
        self._session = None
        #self._api_internal = None  # type: GoogleCloudStorageApi
        #self._api_factory = lambda: GoogleCloudStorageApi()
        self._log = get_configured_logger(self.keyword)

    @property
    def name(self):
        return LOG_NAME

    @property
    def keyword(self):
        return PACKAGE_KEYWORD

    @property
    def api(self):
        if self._api_factory:
            self._api_internal = self._api_factory()
            self._api_factory = None
        return self._api_internal

    def set_api(self, api=None, apifunc=None):
        if not api and not apifunc:
            return
        self._api_internal = api
        self._api_factory = apifunc

    @block_non_literal_args
    def get_container_contents(
        self,
        container_id,
        query=None,
        page_size=None,
        bucket_name=None,
        include_subfolders=False,
        **kwargs
    ):
        """
         Get a directory listing, returned in the form expected by FME Workbench.
         The caller is responsible for proceeding through pagination, if applicable.

         :param container_id: Identifier for the container (e.g. folder) for which contents are being listed.
         :param query: Query or filter string for the request.
             This is an arbitrary string specific to the underlying Web Filesystem.
         :param int page_size: Requested maximum number of items to return per page.
         :param str|unicode bucket_name: the bucket name on Google Cloud Storage, different from the
             FMe webfs "container", which is a prefix
         :param bool include_subfolders: (optional) whether to list recursively
         :param dict kwargs:
             - mode: if set to "bucket", will list buckets instead of blobs
             - next_marker: indicates there is another page of results
         :returns: An dict-like object representing the directory listing.
             This object is in the form expected by FME Workbench, and represents one page of results.
             It may contain info needed to proceed to the next page.
        :rtype: ContainerContentResponse
        """
        self._log.info('get_container_contents container_id: %s, query: %s, page_size: %s, bucket_name: %s, include_subfolders: %s, kwargs: %s'
            , container_id
            , query
            , page_size
            , bucket_name
            , include_subfolders
            , str(kwargs)
        )
        mode = kwargs.get("mode", None)
        next_marker = kwargs.get("next_marker", None)

        if mode == "CategoryScheme":
            return self.api.list_category_schemes()
        elif mode == "Category":
            parent = container_id or kwargs.get('__eurostat_category_scheme')
            return self.api.list_categories(parent)

        else:  # blob
            raise Exception('not implemented')
            if not bucket_name:
                bucket_name = kwargs.get(PARAM_BUCKET, None)

            blob_gen = self.api.list_blobs(
                bucket_name,
                container_id,
                include_subfolders=include_subfolders,
                page_size=page_size,
                next_marker=next_marker,
            )
            if page_size:
                response = ContainerContentResponse(list(blob_gen))
                if blob_gen.next_marker is not None:
                    response.continuation = ContinuationInfo(
                        {
                            "next_marker": blob_gen.next_marker,
                            "LIMIT": page_size,
                            "_FME_BUCKET": bucket_name,
                            "include_subfolders": include_subfolders,
                        }
                    )
            else:
                response = ContainerContentResponse(blob_gen)
        return response

    @block_non_literal_args
    def download_file(self, item_id, dest_file, **kwargs):
        """
        Download a single file.

        :param file_id: identifier for the file to download.
        :param dest_file: file-like object to write into.
        :param kwargs:
            - _FME_BUCKET: bucket name
        """
        bucket_name = kwargs[PARAM_BUCKET]
        self.api.download_blob(bucket_name, item_id, dest_file)

    @block_non_literal_args
    def upload_file(
        self, item_id, source_file, blob_container=None, metadata=None, acl=None, **kwargs
    ):
        """
        Upload the given file to the remote path.

        :param str|unicode item_id: remote destination path
        :param str|unicode|io.IOBase source_file: local path or file-like object to upload from
        :param str|unicode blob_container: the blob container or share to upload to
        :param dict metadata:  (optional) user-supplied metadata to set on the uploaded file
        :param kwargs:
            - _FME_BUCKET: bucket name. `blob_container` takes precedence
        :return:
        """
        if not blob_container:
            blob_container = kwargs[PARAM_BUCKET]
        self.api.upload_blob(
            blob_container, item_id, source_file, metadata=metadata, acl=acl, **kwargs
        )

    @block_non_literal_args
    def get_item_info(self, item_id, blob_container=None, explicit_folder=False, **kwargs):
        """
        :param String item_id: id of blob
        :param String blob_container: Google Cloud Storage bucket name
        :param Bool explicit_folder: if true, do not create a BlobPrefix if blob not found
        :param kwargs:
            - _FME_BUCKET: bucket name. `blob_container` takes precedence
        :return: GoogleCloudStorageContainerItem
        """
        self._log.info(
            'item_id: %s, blob_container: %s, explicit_folder: %s, kwargs: %s'
            ,  item_id, blob_container, explicit_folder, kwargs
        )
        mode = kwargs.get("mode", None)
        if 'Category' == mode:
            container_id = kwargs.get('CONTAINER_ID')
            return self.api.info(container_id)

        if not blob_container:
            blob_container = kwargs[PARAM_BUCKET]
        if item_id == DELIM:
            item_id = ""  # root directory
        return self.api.info(blob_container, item_id, explicit_folder)

    @block_non_literal_args
    def delete_item(self, item_id, bucket_name=None, **kwargs):
        """
        Delete the given item from the remote location.

        :param str|unicode item_id: the remote path to delete. Note that neither API supports "bulk"
            deletion, so this only applies to individual files/blobs
        :param str|unicode bucket_name: bucket to delete from
        :param kwargs:
            - _FME_BUCKET: bucket name. `bucket_name` takes precedence
        """
        if not bucket_name:
            bucket_name = kwargs[PARAM_BUCKET]

        if not item_id:
            return

        info = self.get_item_info(blob_container=bucket_name, item_id=item_id)

        if not info:
            # Item does not exist, so confirm that it is "deleted"
            return

        item_id = info.id

        if not info.is_container:
            self.api.delete(bucket_name=bucket_name, remote_path=item_id)
            return

        item_source = (
            item
            for item in self.get_container_contents(
                bucket_name=bucket_name, container_id=item_id, include_subfolders=True
            ).contents
            if not item.is_container
        )

        def worker(delete_item):
            try:
                self.api.delete(bucket_name=bucket_name, remote_path=delete_item.id)
            except Exception as e:
                return PoolWorkerResult(reference=delete_item.id, success=False, error=e)

            return PoolWorkerResult(reference=delete_item.id, success=True, error=None)

        def handler(results):
            for result in results:
                if not result.success:
                    if self._log:
                        self._log.warn(
                            "Could not delete '%s': '%s'" % (result.reference, result.error)
                        )
                else:
                    self._log.info("Deleted '%s'" % result.reference)

        map_to_new_pool(item_source, worker, handler, NUM_THREADS)
