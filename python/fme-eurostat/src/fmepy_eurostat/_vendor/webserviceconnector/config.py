from __future__ import absolute_import, division, unicode_literals

from fmegeneral.fmeutil import aggressive_normpath, choiceToBool
from ..webserviceconnector.util import get_attribute_from_feature


def _aggressive_normpath_wrapper(path):
    """

    :param str path: The input path.
    :return: The normalized path or input value unchanged.
    """
    try:
        return _strip_double_quotes(aggressive_normpath(path))
    except TypeError:
        # aggressive_normpath expects a path
        # If it's given something that couldn't be path, we end up here
        return path


def _strip_double_quotes(string):
    """

    :param str string:
    :return: string stripped of double quotes or unchanged string
    """
    try:
        return string if not string.startswith('"') else string.strip('"')
    except AttributeError:
        return string


class ConfigFromFeature(dict):
    """
    Represents internal configuration attributes taken from an input feature of a transformer.

    Internal configuration attributes are ones that users should never see:
    they're typically set by the UI, and the transformer implementation is responsible for
    consuming and removing them before they're passed out as output features.

    This class is a dict. This allows it to be used generically and with `**kwargs`.
    """

    def __init__(self, feature):
        """
        Override this to define and set member variables.
        This superclass constructor must be called.

        :param FMEFeature feature: The feature from which to pop config attributes.
        """
        super(ConfigFromFeature, self).__init__()
        self.feature = feature

    def pop(self, attr_name, decode=True, missing_value=None, process_func=None):
        """
        Pop an attribute off of the feature and return its value.
        This dict instance is also updated to store the value.

        :param str attr_name: Attribute name to pop.
            This name is also the key for the value in this dict.
        :param bool decode: If true, the value will be FME-decoded first.
        :param missing_value: Value to return if the attribute isn't on the feature.
        :param function process_func: Function that takes a single argument, the decoded (if applicable) value,
            and returns the value that should actually be stored and returned.
        :return: The attribute value.
        """
        value = get_attribute_from_feature(
            self.feature, attr_name, pop=True, decode=decode, missing_value=missing_value)
        if process_func:
            value = process_func(value)
        self[attr_name] = value
        return value


class ListOperationConfig(ConfigFromFeature):
    """Common configuration attributes for container listing operations."""
    def __init__(self, feature):
        super(ListOperationConfig, self).__init__(feature)

        self.container_id = self.pop('_FME_FILES')
        self.name = self.pop('_FME_OBJECT_NAME_ATTRIBUTE')
        self.path = self.pop('_FME_OBJECT_PATH_ATTRIBUTE')
        self.id = self.pop('_FME_OBJECT_ID_ATTRIBUTE')
        self.type = self.pop('_FME_OBJECT_TYPE_ATTRIBUTE')
        self.modified = self.pop('_FME_OBJECT_MODIFIED_ATTRIBUTE')
        self.size = self.pop('_FME_OBJECT_SIZE_ATTRIBUTE')
        self.output_list = self.pop('OUTPUT_LIST')
        self.relative_path = self.pop('_FME_OBJECT_RELATIVE_PATH_ATTRIBUTE')

        # The transformers' generic listing operation includes all subfolders.
        # This can be configurable, but in practice, transformers don't expose it.
        # This is unlike the Web Filesystem's listing operation, which is always only for direct descendants.
        self.include_subfolders = self.pop('_FME_INCLUDE_SUBFOLDERS', process_func=choiceToBool, missing_value='yes')

    def build_attributes(self, item, parents):
        """
        Given the info for one item,
        get a dict that maps attribute name to value, containing all relevant metadata.
        These attributes are to be applied to the output feature.

        :param IContainerItem item: The item for which to build an attributes dict.
        :param list parents: Container items that represent ancestry of this item.
        :return: A mapping of attribute name to its value.
            This base implementation returns item name, ID, and type (file/folder).
        :rtype: dict
        """
        return {
            self.name: item.name,
            self.id: item.id,
            self.type: 'Folder' if item.is_container else 'File',
        }


class DownloadOperationConfig(ConfigFromFeature):
    """Common configuration attributes for download operations."""
    def __init__(self, feature):
        super(DownloadOperationConfig, self).__init__(feature)

        self.target_type = self.pop('_FME_DATA_TARGET')
        self.object_id = self.pop('_FME_FILES')
        self.target_folder = self.pop('_FME_TARGET_FOLDER', process_func=_aggressive_normpath_wrapper)
        self.include_subfolders = self.pop('_FME_INCLUDE_SUBFOLDERS', process_func=choiceToBool)
        self.target_attribute = self.pop('_FME_TARGET_ATTRIBUTE')
        self.encoding = None

    @property
    def target_is_file(self):
        """If true, the user claims the target is a file."""
        return self.target_type == 'File'

    @property
    def target_is_attribute(self):
        """If true, the target is an attribute on an FMEFeature."""
        return self.target_type == 'Attribute'

    @property
    def target_is_folder(self):
        """If true, the user claims the target is a folder."""
        return self.target_type == 'Folder'

    @property
    def overwrite_target(self):
        """If true, the local target file/folder should be overwritten if it exists."""
        return False


class UploadOperationConfig(ConfigFromFeature):
    """Common configuration attributes for upload operations."""
    def __init__(self, feature):
        super(UploadOperationConfig, self).__init__(feature)

        self.source_type = self.pop('_FME_SOURCE_TYPE')
        self.remote_destination = self.pop('_FME_UPLOAD_DESTINATION_PATH')
        self.include_subfolders = self.pop('_FME_INCLUDE_SUBFOLDERS', process_func=choiceToBool)
        self.source_file = self.pop('_FME_SOURCE_FILE', process_func=_aggressive_normpath_wrapper)
        self.source_folder = self.pop('_FME_SOURCE_FOLDER', process_func=_aggressive_normpath_wrapper)
        self.filename = self.pop('_FME_UPLOAD_ATTR_FILENAME', process_func=_strip_double_quotes)
        self.data_attribute = self.pop('_FME_DATA_TO_UPLOAD')
        self.object_id_attr = self.pop('_FME_OBJECT_ID_ATTRIBUTE')

    @property
    def source_is_file(self):
        """If true, the user claims the source is a file."""
        return self.source_type == 'File'

    @property
    def source_is_attribute(self):
        """If true, the source is an attribute on an FMEFeature."""
        return self.source_type == 'Attribute'

    @property
    def source_is_folder(self):
        """If true, the user claims the source is a folder."""
        return self.source_type == 'Folder'

    @property
    def overwrite_destination(self):
        """If true, the remote destination file/folder should be overwritten if it exists."""
        return False

    def build_attributes(self, item):
        """
        Given the info for one item,
        get a dict that maps attribute name to value, containing all relevant metadata.
        These attributes are to be applied to the output feature.

        :param IContainerItem item: The item for which to build an attributes dict.
        :return: A mapping of attribute name to its value.
        :rtype: dict
        """
        return {
            self.object_id_attr: item.id,
        }


class DeleteOperationConfig(ConfigFromFeature):
    """Common configuration attributes for delete operations."""
    def __init__(self, feature):
        super(DeleteOperationConfig, self).__init__(feature)

        self.object_id = self.pop('_FME_FILEID')
