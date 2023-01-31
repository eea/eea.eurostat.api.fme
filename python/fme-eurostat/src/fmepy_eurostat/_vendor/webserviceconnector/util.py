"""
Miscellaneous utilities for FME Web Filesystem and Web Connectors.

These may be more suitable in ``fmegeneral.fmeutil``,
but are currently kept here to avoid potential merge conflicts.
"""

from __future__ import absolute_import, division, unicode_literals, print_function
import abc
import errno
import os
import re
from fmeobjects import FME_ATTR_UNDEFINED, FMESession

import six
from pypac import pac_context_for_url


def set_attributes_on_feature(feature, attrs):
    """
    Helper to set a bunch of attributes onto a feature.
    Handles null values. Skips falsey attribute names.

    :param FMEFeature feature: Feature to set attributes on.
    :param dict attrs: Mapping of attribute name to value.
    """
    for name, value in attrs.items():
        if not name:
            continue
        if value is None:
            feature.setAttributeNullWithType(name, FME_ATTR_UNDEFINED)
        else:
            feature.setAttribute(name, value)


def pop_attributes_off_feature(feature, attributes=None, startswith=None, missing_value=None):
    """
    Get attributes from a feature, and remove them from the feature after doing so.

    :param FMEFeature feature: Feature to pop attributes from.
    :param list attributes: Names of attributes to pop. The names in this list must not be repeated.
    :param str startswith: Pop attributes starting with this string.
    :param str missing_value: Value to use if the attribute is missing.
    :return: Dictionary of attribute names to their values.
    :rtype: dict
    """
    results = {}

    if attributes:
        for name in attributes:
            results[name] = get_attribute_from_feature(feature, name, missing_value=missing_value)
            feature.removeAttribute(name)

    if startswith:
        all_attr_names = feature.getAllAttributeNames()
        for name in all_attr_names:
            if name.startswith(startswith) and name not in attributes:
                results[name] = get_attribute_from_feature(feature, name, missing_value=missing_value)
                feature.removeAttribute(name)

    return results


def get_attribute_from_feature(feature, name, missing_value=None, decode=False, pop=False):
    """
    Get an attribute from a feature, with some optional behaviours.

    :param FMEFeature feature: Feature to get the attribute from.
    :param str name: Attribute name.
    :param str missing_value: Value to return if the attribute is missing.
    :param bool decode: If true, and the value of the attribute is a string,
        then return the FME-decoded value.
    :param bool pop: If true, then the attribute is also removed from the feature.
    :return: The attribute value.
    """
    is_null, is_missing, _ = feature.getAttributeNullMissingAndType(name)
    if is_missing:
        return missing_value
    if is_null:
        return None

    value = feature.getAttribute(name)
    if pop:
        feature.removeAttribute(name)
    if decode and isinstance(value, six.string_types):
        return FMESession().decodeFromFMEParsableText(value)
    return value


def sanitize_fs_name(name, replacement='_'):
    """
    Remove the most obvious disallowed characters from a name intended to be used as a file or folder name.
    This is a simple implementation and isn't foolproof.

    :param str name: File or folder name to sanitize.
    :param str replacement: Replace invalid characters with this value.
    :return: Sanitized name.
    """
    name = re.sub(r'/\'\?%\*:\|"<>', replacement, name)
    return name


def mkdir_p(dirpath):
    """
    Create a nested directory, similar to ``mkdir -p``

    :param str dirpath: The directory path.
    """
    if not os.path.exists(dirpath):
        # https://stackoverflow.com/questions/600268/mkdir-p-functionality-in-python
        try:
            os.makedirs(dirpath)
        except OSError as e:
            if not (e.errno == errno.EEXIST and os.path.isdir(dirpath)):
                raise


class ItemGen(object):
    """
    Base class for item-listing iterators. Shared features include the BlobService and the
    continuation marker. We really want to keep using a generator like the Azure SDK does in order
    to keep memory usage low when listing. However, we also need to be able to return a
    next_marker, thus the complication of a full iterator class rather than generators.

    Override :meth:`_generate` to implement.
    """

    def __init__(self, service, next_marker=None):
        self._service = service
        self._generator = None
        self.next_marker = next_marker

    def __iter__(self):
        return self

    def __next__(self):
        if not self._generator:
            self._generator = self._generate()
        return next(self._generator)

    def next(self):
        """Get the next_marker."""
        return self.__next__()

    @abc.abstractmethod
    def _generate(self):
        """
        Abstract method - override to produce the correct type of items.

        :return: An iterable of container items.
        :rtype: Iterable[ContainerItem]
        """
        pass


def add_pac_context(url):
    """
    Decorator which calls the decorated function using the PAC config for the given URL.

    :param six.text_type url: The URL for which to get the proxy configuration.
    """

    def decorator(func):
        def decorated(self, *args, **kwargs):
            # If it's a bound method, and self has use_proxy set to a falsey value,
            # just call the function as-is
            if hasattr(self, "use_proxy"):
                if not self.use_proxy:
                    return func(self, *args, **kwargs)
            # But if it's an unbound function, or use_proxy is not set, or it's truthy,
            # wrap the function in pac_context_for_url
            with pac_context_for_url(url):
                return func(self, *args, **kwargs)

        return decorated

    return decorator
