# coding: utf-8
"""
Utility functions for working with folders/prefixes.
"""

from __future__ import absolute_import, division, unicode_literals, print_function
import os
from itertools import takewhile
from ..webserviceconnector.fmewebfs import IContainerItem


class FolderFromPathBuilder(object):
    """Given a list of full paths, returns the common folders.

    Given a list of full paths, such as

    ``a/b/file0``,
    ``a/b/c/file1``,
    ``a/b/d/file2``,
    ``b/b/file5``,
    ``b/b/x/file3``,
    ``b/b/y/file4``.

    one by one via feed_path, return the common folders:

    ``a/``,
    ``a/b/``,
    ``a/b/c/``,
    ``a/b/d/``,
    ``b/``,
    ``b/b/``,
    ``b/b/x/``,
    ``b/b/y/``.

    This assumes that the paths are sorted lexicographically.
    """

    def __init__(self, root_prefix="", delimiter="/"):
        """
        :param six.text_type root_prefix: a root prefix to exclude
        :param six.text_type delimiter: the path separator
        """
        self._prefix = root_prefix
        self._delim = delimiter
        self._prev_folders = set()

    def feed_path(self, path):
        """Add a path to the builder, and output any newly-identified folders.

        :param six.text_type path: The full path to a file.
        :return: Newly identified folders.
        :rtype: list[six.text_type]
        """
        path = path[len(self._prefix) :]
        parts = path.split(self._delim)

        result = []

        for index in range(1, len(parts)):
            folder = self._prefix + self._delim.join(parts[:index]) + self._delim
            result.append(folder)

        prev_folders = self._prev_folders
        self._prev_folders = set(result)

        return [r for r in result if r not in prev_folders]

    @staticmethod
    def _common_prefix(p1, p2):
        """Static method."""
        return "".join(x for x, y in takewhile(lambda pair: pair[0] == pair[1], zip(p1, p2)))


def file_only(func):
    """
    Decorator for :class:`fmewebfs.IContainerItem` properties that only apply to files, not folders. If the
    ContainerItem is a folder, the property returns `None`
    """

    def decorated(self, *args, **kwargs):
        if self.is_container:
            return None
        else:
            return func(self, *args, **kwargs)

    return decorated


def strip_prefix(string, prefix, delim=None):
    """
    Checks to see if the given prefix is present. If yes, removes it.

    :param six.text_type string: The input string.
    :param six.text_type prefix: The prefix to remove.
    :param six.text_type delim: Delimiter to remove from beginning of string.
    :return: The potentially modified string.
    :rtype: str
    """
    if prefix and string.startswith(prefix):
        string = string[len(prefix) :]
    if delim:
        string = string.lstrip(delim)
    return string


def append_folder_to_prefix(destination_prefix, folder_path, delim):
    """
    Appends a folder to a prefix.

    :param str destination_prefix: The prefix.
    :param str folder_path: The folder path.
    :param str delim: The delimiter.
    :return: The folder name.
    :rtype: str
    """
    return delim.join(
        [destination_prefix.strip(delim), os.path.basename(folder_path.rstrip(delim))]
    ).strip(delim)


def create_target_folder(target_folder, folder_name):
    """
    Creates a local folder.

    :param str target_folder: Path to local destination folder.
    :param str folder_name: Sub-folder to create.
    :return: The folder path.
    :rtype: str
    """
    target_folder = os.path.join(target_folder, folder_name)
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)
    return target_folder


def get_folder_id(folder_info, delim, prefix_root=False):
    """
    Makes a folder id (prefix) by adding a delim if needed to a folder name.

    :param IContainerItem folder_info: Folder information.
    :param bool prefix_root: If true, root is '', otherwise root is '/'.
    :return: The folder prefix.
    :rtype: str
    """
    if folder_info:
        folder_id = folder_info.id
        if prefix_root and (not folder_id or folder_id == delim):
            return ""
        if not folder_id.endswith(delim):
            folder_id += delim
    else:
        folder_id = ""
    return folder_id
