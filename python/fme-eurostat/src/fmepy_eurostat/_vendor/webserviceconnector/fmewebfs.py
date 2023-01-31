"""
FME Web Filesystem
==================

Base classes, wrappers, and utilities for FME's Web Filesystem integration.

FME's Web Filesystem integration includes direct access by FME Workbench,
and access through Web Connector Transformers.
"""

from __future__ import absolute_import, division, unicode_literals, print_function

import abc
import os
from collections import deque
from functools import wraps
from io import BytesIO
from itertools import chain
from tempfile import SpooledTemporaryFile

import six
import fme
from fmeobjects import FMEFeature

from fmegeneral.fmeutil import getSystemLocale
from requests import Session, HTTPError

from fmegeneral.fmelog import get_configured_logger
from fmegeneral.plugins import FMEEnhancedTransformer
from ..webserviceconnector.config import (
    ListOperationConfig,
    DownloadOperationConfig,
    UploadOperationConfig,
    DeleteOperationConfig,
)
from ..webserviceconnector.errors import WebFilesystemHTTPError
from ..webserviceconnector.util import (
    get_attribute_from_feature,
    set_attributes_on_feature,
    sanitize_fs_name,
    mkdir_p,
)


class IContainerItem(dict):
    """
    Interface for a 'container item', which can be a container itself (a folder-like item), or a file-like item.

    Container items always have a name and ID. The name is typically something human-readable, like a filename.
    The ID varies based on the implementation. It could be the UUID of the item, its absolute path, or anything else.
    """

    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def is_container(self):
        pass

    @abc.abstractproperty
    def id(self):
        pass

    @abc.abstractproperty
    def name(self):
        pass

    @property
    def encoding(self):
        """Optional, thus default implementation of binary."""
        return None


class ContainerItem(IContainerItem):
    """
    Information about a single item from a Web Filesystem.
    It may be a file or container (folder).

    Web Filesystem implementations return these to the FME Workbench UI.
    This class is basically a dict struct that facilitates interoperation with the FME Workbench UI,
    and an abstraction for the underlying dict keys that are needed.
    """

    def __init__(self, is_container, item_id, name, icon=None, **kwargs):
        """
        :param bool is_container: If true, then this item represents a container, such as a folder.
        :param str item_id: Identifier for the item.
            What this means is up to each Web Filesystem implementation.
            It may be a GUID, or path to the item, or both.
            It's up to the underlying Web Filesystem implementation to handle possible values.
        :param str name: User-visible name of the item.
        :param str icon: Optional icon to show for this item.
            Either a precompiled resource path, or path relative to FME_HOME.
        :param kwargs: Any additional data to include.
        """
        super(ContainerItem, self).__init__(
            IS_CONTAINER=is_container, ID=item_id, NAME=name, **kwargs
        )
        if icon:
            # Workbench crashes if this key is provided with None value.
            self["ICON"] = icon

    @property
    def is_container(self):
        """

        :return: If item represents a container.
        """
        return self["IS_CONTAINER"]

    @property
    def id(self):
        """

        :return: ID of the item.
        :rtype: str
        """
        return self["ID"]

    @property
    def name(self):
        """

        :return: Name of the item.
        :rtype: str
        """
        return self["NAME"]


class ContinuationInfo(dict):
    """
    Information needed for pagination through multi-page results,
    in the structure expected by the FME Workbench UI.
    """

    def __init__(self, args):
        """
        :param dict args: Data needed by the Web Filesystem to request the next page.
        """
        super(ContinuationInfo, self).__init__(ARGS=args)

    @property
    def args(self):
        """

        :return: Data needed by the Web Filesystem to request the next page.
        :rtype: dict
        """
        return self["ARGS"]


class ContainerContentResponse(dict):
    """
    A response to a request for a container contents list.

    Web Filesystem implementations return these to the FME Workbench UI.
    """

    def __init__(self, contents, continuation_info=None, **kwargs):
        """
        :param list[ContainerItem] contents: Container contents. Must be a list.
        :param ContinuationInfo continuation_info:
            Arguments needed to obtain the next page of results, if applicable.
            These arguments are to be included with the next container content request,
            to be consumed by the Web Filesystem implementation.
        :param kwargs: Any additional data to include.
        """
        super(ContainerContentResponse, self).__init__(CONTENTS=contents, **kwargs)
        if continuation_info:
            self["CONTINUE"] = continuation_info

    @property
    def continuation(self):
        """

        :return: Arguments needed to obtain the next page of results, if applicable.
        :rtype: ContinuationInfo
        """
        return self.get("CONTINUE", {})

    @continuation.setter
    def continuation(self, args):
        """
        :param ContinuationInfo args: Arguments needed to continue to the next page of results.
        """
        self["CONTINUE"] = args

    @property
    def contents(self):
        """

        :return: Container contents.
        :rtype: list[ContainerItem]
        """
        return self["CONTENTS"]


class FMEWebFilesystemDriver(object):
    """
    A Web Filesystem driver contains the implementation for common and generic interactions with the remote server.
    Web Filesystem drivers implement this abstract class.
    """

    __metaclass__ = abc.ABCMeta

    def __init__(self):
        super(FMEWebFilesystemDriver, self).__init__()
        self._session = None

    @abc.abstractproperty
    def name(self):
        """The human-readable name of this Web Filesystem."""
        pass

    @abc.abstractproperty
    def keyword(self):
        """A unique alphanumeric identifier for this Web Filesystem."""
        pass

    @property
    def session(self):
        """
        The Requests Session that this driver is using for HTTP.
        If one hasn't been set, then one is created, stored, and returned.

        :rtype: requests.Session
        """
        if not self._session:
            self._session = Session()
        return self._session

    @session.setter
    def session(self, new_session):
        self._session = new_session

    @abc.abstractmethod
    def get_container_contents(self, container_id, query=None, page_size=0, **kwargs):
        """
        Get a directory listing, returned in the form expected by FME Workbench.
        The caller is responsible for proceeding through pagination, if applicable.

        :param str container_id: Identifier for the container (e.g. folder) for which contents are being listed.
        :param str query: Query or filter string for the request.
            This is an arbitrary string specific to the underlying Web Filesystem.
        :param int page_size: Requested maximum number of items to return per page.
        :returns: An dict-like object representing the directory listing.
            This object is in the form expected by FME Workbench, and represents one page of results.
            It may contain info needed to proceed to the next page.
        :rtype: ContainerContentResponse
        """
        pass

    def walk(self, top_container_id, query=None, page_size=0, **kwargs):
        """
        Directory walk, with similar semantics to :func:`os.walk`.

        Unlike :meth:`get_container_contents`, this method transparently handles server-side response pagination
        instead of returning pagination arguments for the caller to progress through pagination.

        :param str top_container_id: Identifier for the container to start walking from.
        :param str query: Query or filter string for the request.
            This is an arbitrary string specific to the underlying Web Filesystem.
        :param int page_size: Requested maximum number of items to return per page.
        :return: A 3-tuple generator, yielding dirpath, dirs, and files.
            - dirpath: list of container-type ContainerItem that represent the current remote path
            - dirs: list of container-type ContainerItem that are containers at the current path, i.e. subfolders
            - files: list of file-type ContainerItem that are files at the current path
        :rtype: list[ContainerItem], list[ContainerItem], list[ContainerItem]
        """
        top_container_info = self.get_item_info(top_container_id)
        if not top_container_info or not top_container_info.is_container:
            raise ValueError("Cannot walk non-folder")

        # A queue of sub(directories) and their ancestry.
        folders_to_visit = deque()
        folders_to_visit.append([top_container_info])

        while folders_to_visit:
            current_ancestry = folders_to_visit.popleft()

            # Prepare request for directory listing of the current leaf directory.
            parent_folder_id = current_ancestry[-1].id

            extra_kwargs = dict(kwargs)  # Copy for safe mutation during pagination.
            while True:
                resp = self.get_container_contents(
                    parent_folder_id, query=query, page_size=page_size, **extra_kwargs
                )

                dirpath = current_ancestry
                folders = list(filter(lambda item: item.is_container, resp.contents))
                files = list(filter(lambda item: not item.is_container, resp.contents))

                yield dirpath, folders, files

                for folder in folders:
                    folders_to_visit.append(current_ancestry + [folder])

                if not resp.continuation:
                    break
                extra_kwargs.update(resp.continuation.args)

    @abc.abstractmethod
    def download_file(self, file_id, dest_file, **kwargs):
        """
        Download a single file.

        :param str file_id: Identifier for the file to download.
        :param io.BytesIO dest_file: File-like object to write into.
        """
        pass

    @abc.abstractmethod
    def delete_item(self, item_id, **kwargs):
        """
        Delete an item.

        :param str item_id: Identifier for the item to delete.
            It may be a file or a folder. It might not exist.
        """
        pass

    @abc.abstractmethod
    def get_item_info(self, item_id, **kwargs):
        """
        Get the metadata for a single file or container.

        :param str item_id: Identifier for the item.
        :return: Metadata about the item.
        :rtype: IContainerItem
        """
        pass


class IFMEWebFilesystem(object):
    """
    This is the interface expected by the FME Workbench UI.
    It's used by the `WEB_SELECT` GUI type and the reader dataset field's remote file feature.
    """

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def getContainerContents(self, args):
        """
        Called by the `WEB_SELECT` GUI type to obtain a listing of items from a container.
        What this means will vary depending on the terminology and concepts of the underlying Web Filesystem.
        Typically, a container is a folder, and items are subfolders and files.

        :type args: dict
        :param args: Contains these keys:

            - `CONTAINER_ID`: Identifier for the container for which to list items.
            - `QUERY`: Optional. Query/search/filter string. This is a user-specified freeform value.
              The underlying Web Filesystem implementation is responsible for understanding it.
            - LIMIT: Optional. Integer for the maximum number of items to return per page.
            - Any other arbitrary key-value pairs specified by the Web Filesystem's GUI implementation.

        :returns: A dict with keys:

            - `CONTENT`: A list. Each element is a dict with these keys:

                - `IS_CONTAINER`: Boolean for whether this item represents a type of container, e.g. a folder.
                - `ID`: Identifier for the item.
                - `NAME`: Human-readable name of the item.
                - `ICON`: Optional. Path to the item's icon relative to ``FME_HOME``, or a precompiled resource.

            - `CONTINUE`: Optional. A dict with key `ARGS`. The value of `ARGS` is a dict of arbitrary data
              that will be included with the next pagination call to this method.

        If `CONTINUE` is not present, then there are no more pages in the response.

        :rtype: dict
        """
        pass

    @abc.abstractmethod
    def downloadFile(self, args):
        """
        Called by Workbench to download a single file.
        Used in the context of the reader dataset value referring to a file on a Web Filesystem.

        :type args: dict
        :param args: Contains these keys:

            - `FILE_ID`: Identifier for the file to download.
            - `TARGET_FOLDER`: Local filesystem folder path to write file to.
            - `FILENAME`: Optional. Name of destination file to write to.

        :rtype: None
        """
        pass

    @abc.abstractmethod
    def downloadFolder(self, args):
        """
        Called by Workbench to download a folder, and optionally all its subfolders.
        Used in the context of the reader dataset value referring to a folder on a Web Filesystem.

        :type args: dict
        :param args: Contains these keys:

            - `CONTAINER_ID`: Identifier for the folder (container) to download.
            - `TARGET_FOLDER`: Local filesystem folder path to write to.
            - `EXCLUDE_SUB_FOLDERS`: Optional. If this key is present, then subfolders and their contents are not to be downloaded.

        :rtype: None
        """
        pass


def prettify_http_errors(method):
    """
    A decorator for methods on :class:`FMEWebFilesystem` to catch exceptions from :mod:`requests`
    and re-raise one that's more Workbench-friendly.
    """

    @wraps(method)  # see https://github.com/sphinx-doc/sphinx/issues/3783
    def inner(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except HTTPError as e:
            six.raise_from(WebFilesystemHTTPError(self._driver.name, e), e)
            raise
        # TODO: Connectivity, timeouts, SSL, proxy...

    return inner


class FMEWebFilesystem(IFMEWebFilesystem):
    """
    This class implements the interface expected by the FME Workbench UI.
    Through the use of a :class:`FMEWebFilesystemDriver`, generic functionality is provided, including:

        * Informational logging prior to making a request through the driver.
        * Informative generic logging of HTTP error codes, if the driver raised :class:`requests.HTTPError`
          through :meth:`requests.Response.raise_for_status`.
    """

    def __init__(self, driver):
        """
        :param FMEWebFilesystemDriver driver: An instance of :class:`FMEWebFilesystemDriver`.
        """
        self._driver = driver
        self._log = get_configured_logger(self._driver.keyword)

    @prettify_http_errors
    def getContainerContents(self, args):
        """
        See :meth:`IFMEWebFilesystem.getContainerContents`.

        :param args: See :meth:`IFMEWebFilesystem.getContainerContents`.
        :rtype: ContainerContentResponse
        """
        container_id = args["CONTAINER_ID"]
        query, page_size = args.get("QUERY"), args.get("LIMIT")

        # If not requesting the root or search result, then try to resolve and validate the container ID.
        if container_id and container_id not in ["/", "Search Results"]:
            self._log.info("%s: Looking for item with ID '%s'", self._driver.name, container_id)
            container_info = self._driver.get_item_info(container_id, **args)

            if not container_info:
                raise ValueError("Could not find item with ID '%s'", container_id)
            if not container_info.is_container:
                raise ValueError(
                    "It is not a valid operation to get a list of items in '%s' (ID '%s')",
                    container_info.name,
                    container_info.id,
                )
            self._log.info(
                "%s: Item with ID '%s' is named '%s'",
                self._driver.name,
                container_info.id,
                container_info.name,
            )

        self._log.info("%s: Getting contents of '%s'", self._driver.name, container_id)
        results = self._driver.get_container_contents(
            container_id, query=query, page_size=page_size, **args
        )

        # If the driver specified arguments for the next page,
        # we need to include CONTAINER_ID, as Workbench needs it.
        if results.continuation:
            results.continuation.args["CONTAINER_ID"] = container_id

        return results

    @prettify_http_errors
    def downloadFile(self, args):
        file_id, target_folder, filename = (
            args["FILE_ID"],
            args["TARGET_FOLDER"],
            args.get("FILENAME"),
        )

        if not os.path.exists(target_folder):
            os.makedirs(target_folder)
        elif os.path.isfile(target_folder):
            raise ValueError("Destination folder is actually a file")

        # If Workbench provided no filename, then use the remote filename.
        if not filename:
            filename = self._driver.get_item_info(file_id, **args).name

        dest_path = os.path.join(target_folder, sanitize_fs_name(filename))

        self._log.info("%s: Downloading file '%s' to '%s'", self._driver.name, file_id, dest_path)

        with open(dest_path, "wb") as dest_file:
            return self._driver.download_file(file_id, dest_file=dest_file, **args)

    @prettify_http_errors
    def downloadFolder(self, args):
        container_id, connection = args["CONTAINER_ID"], args.get("CONNECTION")
        query, page_size = args.get("QUERY"), args.get("LIMIT")
        root_target_folder, exclude_subfolders = (
            args["TARGET_FOLDER"],
            "EXCLUDE_SUB_FOLDERS" in args,
        )
        include_subfolders = not exclude_subfolders

        # If not requesting the root, then try to resolve and validate the container ID.
        if container_id and container_id != "/":
            self._log.info("%s: Looking for item with ID '%s'", self._driver.name, container_id)
            folder_info = self._driver.get_item_info(container_id, **args)
            if not folder_info.is_container:
                raise ValueError("'%s' (ID '%s') is not a folder", folder_info.name, folder_info.id)

        self._log.info(
            "%s: Downloading folder '%s' to '%s'",
            self._driver.name,
            container_id,
            root_target_folder,
        )

        for parents, folders, files in self._driver.walk(
            container_id, query=query, page_size=page_size, **args
        ):
            dest_dir = os.path.join(
                root_target_folder, *map(lambda folder: sanitize_fs_name(folder.name), parents[1:])
            )

            if not os.path.exists(dest_dir):
                os.makedirs(dest_dir)

            for item in files:
                dest_path = os.path.join(dest_dir, sanitize_fs_name(item.name))
                self._log.info(
                    "%s: Downloading file '%s' (ID '%s') to '%s'",
                    self._driver.name,
                    item.name,
                    item.id,
                    dest_path,
                )
                with open(dest_path, "wb") as f:
                    self._driver.download_file(item.id, f, **args)

            # If we're not supposed to download any subfolders,
            # then tell the walk operation to not proceed to subfolders.
            if not include_subfolders:
                folders[:] = []


def get_encoding_from_requested_source(encoding, config, file_info):
    """
    Given a download config and file metadata, get encoding from the location the user requested:

    - auto-detected from response headers
    - use the system encoding
    - keep as binary
    - use a specified encoding

    :param six.text_type encoding: an override encoding. set to None to actually do detection
    :param DownloadOperationConfig config: config from the transformer
    :param IContainerItem file_info: file metadata
    :rtype: six.text_type
    :return: The detected encoding.
    """
    if config.encoding == "auto-detect":
        encoding = file_info.encoding
    elif config.encoding == "fme-system":
        encoding = getSystemLocale()
    else:
        encoding = config.encoding

    if not encoding:
        encoding = "utf-8"

    # Ref:
    # https://docs.python.org/3/library/codecs.html
    # https://docops.ca.com/ca-xcom-data-transport-for-windows-/11-6/en/reference/character-sets-for-unicode-transfer
    fixes = {
        "fme-binary": None,
        "x-mac-greek": "mac_greek",
        "x-mac-cyrillic": "mac_cyrillic",
        "x-mac-centraleurroman": "maccentraleurope",
        "ksc_5601": "ksc5601",
        "ibm-950_p110-1999": "cp950",
        "ibm-737_p100-1997": "cp737",
        "ibm-5471_p100-2006": "big5hkscs",
        # This is not 100% accurate but is ignored in practice
        # https://en.wikipedia.org/wiki/ISO/IEC_8859-11
        "windows-874-2000": "cp874",
    }
    encoding = fixes.get(encoding, encoding)

    return encoding


class FMEWebFilesystemConnectorTransformer(FMEEnhancedTransformer):
    """
    This is an abstract class intended to be the foundation for "Web Connector" transformers,
    i.e. transformers that expose CRUD operations for a Web Filesystem.

    This class requires the use of an :class:`FMEWebFilesystemDriver`.

    It also expects certain generic internal configuration attributes
    to be set on incoming features by the transformer's GUI definition files.
    These requirements are expected by the various `get_[operation]_config()` methods
    and the :class:`config.ConfigFromFeature` subclasses they return.
    Override these methods and extend these classes to pick up extra configuration supported by your Web Filesystem.

    Many operations are already implemented generically, including listing, download, and deletion.
    """

    __metaclass__ = abc.ABCMeta

    def __init__(self):
        super(FMEWebFilesystemConnectorTransformer, self).__init__()
        self._driver = None
        self._transformer_name = "Transformer"
        # Check if the base class has defined a logger for use.
        # Create our own if there isn't one.
        try:
            self._log
        except AttributeError:
            try:
                debug = fme.macroValues.get("FME_DEBUG", False)
            except AttributeError:
                debug = False
            self._log = get_configured_logger(self.driver.keyword, debug)

    @property
    def keyword(self):
        if not self._keyword:
            # Make sure _driver exists to prevent unhandled exception.
            try:
                self._driver
            except AttributeError:
                self._driver = None
            self._keyword = self.driver.keyword
        return self._keyword

    @abc.abstractproperty
    def driver(self):
        """
        The web filesystem driver for this connector.

        :rtype: FMEWebFilesystemDriver
        """

    def setup(self, first_feature):
        """Operations for the first call of input.

        :param FMEFeature first_feature: The first feature.
        """
        super(FMEWebFilesystemConnectorTransformer, self).setup(first_feature)
        self._transformer_name = first_feature.getFeatureType()

    def pre_input(self, feature):
        """
        Sanitize inputs to make sure features aren't rejected in error.

        :param FMEFeature feature: The feature that's requesting this operation.
        """
        super(FMEWebFilesystemConnectorTransformer, self).pre_input(feature)
        feature.removeAttribute("fme_rejection_code")

    def handle_operation(self, operation_keyword, feature):
        """
        Redirect an input feature to an operation handler method.
        The default implementation recognizes upload, download, delete, and list.
        These operations are redirected to the corresponding abstract method on this class.

        Override this method in order to add support for other operation keywords.

        :param str operation_keyword: A string like upload/download/list/delete. Case-insensitive.
        :param FMEFeature feature: The feature that's requesting this operation.
        :return: True if the operation was recognized and forwarded to a corresponding handler method.
        :rtype: bool
        """
        operation_keyword = operation_keyword.lower()

        if operation_keyword == "upload":
            self.handle_upload(feature, self.get_upload_config(feature))
            return True
        if operation_keyword == "download":
            self.handle_download(feature, self.get_download_config(feature))
            return True
        if operation_keyword == "delete":
            self.handle_delete(feature, self.get_delete_config(feature))
            return True
        if operation_keyword == "list":
            self.handle_list(feature, self.get_list_config(feature))
            return True

        return False

    def get_upload_config(self, feature):
        """
        Get config for an upload operation.
        For implementations that have extra config,
        override this method to return a subclass of :class:`config.UploadOperationConfig`.

        :param FMEFeature feature: Feature from which to get config.
        :rtype: config.UploadOperationConfig
        """
        return UploadOperationConfig(feature)

    def handle_upload(self, feature, config):
        """
        Handle an upload operation.

        :param FMEFeature feature: The feature requesting the operation.
        :param UploadOperationConfig config: Configuration from :meth:`get_upload_config`.
        """
        if config.source_is_file:
            if os.path.isdir(config.source_file):
                self.reject_feature(feature, "INVALID_FILE", "Cannot upload folder as file")
                return
            if not os.path.exists(config.source_file):
                self.reject_feature(feature, "INVALID_FILE", "Source file does not exist")
                return
            with open(config.source_file, "rb") as f:
                try:
                    self.upload_file(
                        feature,
                        config,
                        f,
                        os.path.basename(config.source_file),
                        config.remote_destination,
                    )
                except Exception as e:
                    self.reject_feature(feature, "UPLOAD_ERROR", six.text_type(e))
            return

        if config.source_is_attribute:
            if not config.filename:
                self.reject_feature(feature, "INVALID_FILE", "Missing or empty filename attribute")
                return
            if not config.data_attribute or feature.getAttribute(config.data_attribute) in (
                None,
                "",
            ):
                self.reject_feature(feature, "INVALID_FILE", "Missing or empty data attribute")
                return

            data = feature.getAttribute(config.data_attribute)

            try:
                float(data)
                data = six.text_type(data)
            except ValueError:
                pass
            except Exception as e:
                self.reject_feature(
                    feature, "INVALID_FILE", "Attribute contents could not be converted to string"
                )
                return

            if isinstance(data, six.text_type):
                data = data.encode("utf8")

            try:
                data_bytes = BytesIO(data)
            except Exception as e:
                self.reject_feature(
                    feature, "INVALID_FILE", "Attribute contents could not be read for upload"
                )
                return

            self.upload_file(
                feature, config, data_bytes, config.filename, config.remote_destination
            )
            return

        if config.source_is_folder:
            if os.path.isfile(config.source_folder):
                self.reject_feature(feature, "INVALID_FILE", "Cannot upload file as folder")
                return
            if not os.path.exists(config.source_folder):
                self.reject_feature(feature, "INVALID_FILE", "Source folder does not exist")
                return
            self.upload_folder(
                feature,
                config,
                config.source_folder,
                config.include_subfolders,
                config.remote_destination,
            )
            return

    @abc.abstractmethod
    def upload_file(self, feature, config, file_handle, filename, destination):
        """
        Upload the data from a file-like object.

        It's the implementation's responsibility to decide what to do in the case of conflicts.

        :param FMEFeature feature: The feature requesting the operation.
        :param UploadOperationConfig config: Configuration from :meth:`get_upload_config`.
        :param file_handle: File-like object for the data to upload.
            This may be an local filesystem file, or an attribute on the feature.
        :param str filename: Destination filename.
        :param destination: It's up to the implementation to determine how to interpret this value.
        """
        pass

    @abc.abstractmethod
    def upload_folder(self, feature, config, folder, include_subfolders, destination):
        """
        Upload the files in a local folder.

        It's the implementation's responsibility to decide what to do in the case of conflicts.

        :param FMEFeature feature: The feature requesting the operation.
        :param UploadOperationConfig config: Configuration from :meth:`get_upload_config`.
        :param str folder: Local filesystem path. Guaranteed to exist and be a folder.
        :param bool include_subfolders: If true, then subfolders are to be uploaded.
        :param destination: It's up to the implementation to determine how to interpret this value.
        """
        pass

    def get_download_config(self, feature):
        """
        Get config for a download operation.
        For implementations that have extra config,
        override this method to return a subclass of :class:`config.DownloadOperationConfig`.

        :param FMEFeature feature: Feature from which to get config.
        :rtype: config.DownloadOperationConfig
        """
        return DownloadOperationConfig(feature)

    def handle_download(self, feature, config):
        """
        Handle a download operation.

        :param FMEFeature feature: The feature requesting the operation.
        :param DownloadOperationConfig config: Configuration from :meth:`get_download_config`.
        """
        object_info = None
        # "/" can be a valid value for config.object_id. (See FMEENGINE-57648.)
        if config.object_id:
            object_info = self.driver.get_item_info(config.object_id, **config)
        if config.target_is_file:
            if not object_info:
                self.reject_feature(feature, "INVALID_FILE", "file not found")
                return
            self.download_file(feature, config, object_info, config.target_folder)
            return
        if config.target_is_folder:
            self.download_folder(
                feature, config, object_info, config.target_folder, config.include_subfolders
            )
            return
        if config.target_is_attribute:
            if not object_info:
                self.reject_feature(feature, "INVALID_FILE", "file not found")
                return
            self.download_attribute(feature, config, object_info, config.target_attribute)
            return

        raise ValueError("Download destination is not file, folder, or attribute")

    def download_file(self, feature, config, file_info, target_folder, filename=None, output=True):
        """
        Handle a file download operation.

        :param FMEFeature feature: The feature requesting the operation.
        :param DownloadOperationConfig config: Configuration from :meth:`get_download_config`.
        :param IContainerItem file_info: Metadata about the file to download.
            If this represents a container, then the requester feature is rejected.
        :param str target_folder: Path to local destination folder.
        :param str filename: Local destination filename, which overrides the remote name.
        :param bool output: If true, emit a feature for the downloaded file.
            Output rejected feature on errors.
            If false, log errors and do not output rejected feature on errors.
        """

        if file_info.is_container:
            self._handle_rejection(
                feature,
                "INVALID_FILE",
                "cannot download folder '%0' as file".format(file_info.name),
                output,
            )
            return

        if not filename:
            filename = file_info.name
        dest_path = os.path.join(target_folder, filename)

        self._log.info(
            "Downloading '%s' (ID '%s') to '%s'", file_info.name, file_info.id, dest_path
        )

        if os.path.exists(dest_path):
            if config.overwrite_target:
                self._log.info("Overwriting file '%s'", dest_path)
                try:
                    os.remove(dest_path)
                except OSError:
                    self._handle_rejection(
                        feature,
                        "INVALID_FILE",
                        "could not overwrite '{0}'".format(dest_path),
                        output,
                    )
                    return
            else:
                self._handle_rejection(
                    feature, "INVALID_FILE", "destination exists: '{0}'".format(dest_path), output
                )
                return

        dest_dir = os.path.dirname(dest_path)
        try:
            mkdir_p(dest_dir)
        except OSError:
            self._handle_rejection(
                feature, "INVALID_FOLDER", "could not create folder '{0}'".format(dest_dir), output
            )
            return

        try:
            with open(dest_path, "wb") as f:
                self.driver.download_file(file_info.id, f, **config)
        except OSError as e:
            self._handle_rejection(
                feature, "INVALID_FILE", "could not write to file: '{0}'".format(str(e)), output
            )
            return

        if output:
            self.pyoutput(feature)

    def download_folder(self, feature, config, folder_info, target_folder, include_subfolders):
        """
        Handle a folder download operation.

        :param FMEFeature feature: The feature requesting the operation.
        :param DownloadOperationConfig config: Configuration from :meth:`get_download_config`.
        :param IContainerItem folder_info: Metadata about the folder to download.
            If this does not represent a container, then the requester feature is rejected.
            If this is falsey, then this is a request to download the root.
        :param str target_folder: Path to local destination folder.
        :param bool include_subfolders: If true, then subfolders are also downloaded.
        """
        if folder_info and not folder_info.is_container:
            self.reject_feature(feature, "INVALID_FILE", "cannot download file as folder")
            return

        target_folder = os.path.join(target_folder, folder_info.name)
        if not os.path.exists(target_folder):
            os.makedirs(target_folder)

        self._log.info("Downloading %s to %s", folder_info.id, target_folder)

        folder_id = folder_info.id if folder_info else ""

        for parents, folders, files in self.driver.walk(folder_id, **config):
            dest_dir = os.path.join(target_folder, *map(lambda folder: folder.name, parents[1:]))

            if not os.path.exists(dest_dir):
                os.makedirs(dest_dir)

            for item in files:
                self.download_file(feature, config, item, dest_dir, output=False)

            # If we're not supposed to download any subfolders,
            # then tell the walk operation to not proceed to subfolders.
            if not include_subfolders:
                folders[:] = []

        # Just pass through input feature, stripped of config attributes.
        self.pyoutput(feature)

    def download_attribute(self, feature, config, file_info, attribute_name, encoding="utf8"):
        """
        Handle a download-file-to-attribute operation.

        :param FMEFeature feature: The feature requesting the operation.
        :param DownloadOperationConfig config: Configuration from :meth:`get_download_config`.
        :param IContainerItem file_info: Metadata about the file to download.
            If this represents a container, then the requestor feature is rejected.
        :param str attribute_name: Destination attribute. It will be set on the requester feature, which is then outputted.
        :param str encoding: Interpret data using as this encoding.
            If decoding fails, a warning is logged, and the attribute value is represented as binary.
            Use `None` to skip the decoding attempt and treat the data as binary.
        """
        if file_info.is_container:
            self.reject_feature(feature, "INVALID_FILE", "cannot download folder as attribute")
            return

        self._log.info(
            "Downloading '%s' (ID '%s') to attribute '%s'",
            file_info.name,
            file_info.id,
            attribute_name,
        )

        with SpooledTemporaryFile() as f:
            self.driver.download_file(file_info.id, f, **config)
            f.seek(0)
            attr_value = f.read()
            if encoding:
                try:
                    attr_value = attr_value.decode(encoding)
                except UnicodeDecodeError:
                    self._log.warn(
                        "Could not decode contents of '%s' as '%s'", file_info.name, encoding
                    )
                    attr_value = bytearray(attr_value)
            else:
                # Binary data needs to be bytearray when handed to fmeobjects in PY27.
                attr_value = bytearray(attr_value)

            feature.setAttribute(attribute_name, attr_value)

        self.pyoutput(feature)

    def get_delete_config(self, feature):
        """
        Get config for an delete operation.
        For implementations that have extra config,
        override this method to return a subclass of :class:`config.DeleteOperationConfig`.

        :param FMEFeature feature: Feature from which to get config.
        :rtype: config.DeleteOperationConfig
        """
        return DeleteOperationConfig(feature)

    def handle_delete(self, feature, config):
        """
        Handle a request to delete an object, which could be a file or folder.

        :param FMEFeature feature: The feature requesting the operation.
        :param DeleteOperationConfig config: Configuration from :meth:`get_delete_config`.
        """
        self._log.info("Deleting item with ID '%s'", config.object_id)
        self.driver.delete_item(config.object_id, **config)
        self.pyoutput(feature)

    def get_list_config(self, feature):
        """
        Get config for a container listing operation.
        For implementations that have extra config,
        override this method to return a subclass of :class:`config.ListOperationConfig`.

        :param FMEFeature feature: Feature from which to get config.
        :rtype: config.ListOperationConfig
        """
        return ListOperationConfig(feature)

    def handle_list(self, feature, config):
        """
        Handle a request to list the contents of a container (folder).

        :param FMEFeature feature: The feature requesting the operation.
        :param ListOperationConfig config: Configuration from :meth:`get_list_config`.
        """
        container_info = self.driver.get_item_info(config.container_id)
        if not container_info or not container_info.is_container:
            self.reject_feature(feature, "INVALID_FOLDER", "Cannot list non-folder")
            return

        for parents, folders, files in self.driver.walk(config.container_id, **config):
            for entry in chain(folders, files):
                output_feature = FMEFeature(feature)
                set_attributes_on_feature(output_feature, config.build_attributes(entry, parents))
                self.pyoutput(output_feature)

            if not config.include_subfolders:
                folders[:] = []

    def receive_feature(self, feature):
        """
        Requester features enter the transformer through this method.

        :param FMEFeature feature: The requester feature.
        """
        super(FMEWebFilesystemConnectorTransformer, self).receive_feature(feature)
        operation = get_attribute_from_feature(feature, "OPERATION_TYPE", pop=True)
        handled = self.handle_operation(operation, feature)
        if not handled:
            self.reject_feature(
                feature, "INVALID_PARAMETER", "Unrecognized operation '{}'".format(operation)
            )  # FIXME
            return

    def _handle_rejection(self, feature, rejection_code, rejection_message, output=True):
        """
        Helper for handling errors.  Has the option for logging errors instead of outputting
        a rejected feature.

        :param FMEFeature feature: The feature to pass through to rejection
        :param str rejection_code: Rejection code
        :param str rejection_message: More detailed rejection message
        :param Boolean output: If true, output the error feature on the rejection port
            If false, output the error message in the log
        """
        if output:
            self.reject_feature(feature, rejection_code, rejection_message)
        else:
            self._log.error(rejection_message)
