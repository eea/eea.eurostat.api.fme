from __future__ import absolute_import, division, unicode_literals

import re

import six

from ..webserviceconnector.webserviceconnector import WSCException


class WebFilesystemError(WSCException):
    """The general exception class for all Web Service Connector errors."""
    def __init__(self, message, params=None, details=''):
        """
        This exception, its subclasses, and its superclass are intended to be uncaught in Python.
        When FME Workbench invokes a Web Filesystem, exceptions of this type that are uncaught
        are converted into an error dialog and log message.

        :param str message: FMS message number, or string with ``%`` substitutions.
        :param list params: Message arguments.
        :param str details: Text containing details about why the exception was thrown.
            For example, the HTTP response content.
        """
        if params is None:
            params = []
        if not isinstance(message, int):
            message = re.sub(r'%(\d+)', r'{\1}', message)
            message = message.format(*params)

        super(WebFilesystemError, self).__init__(
            message_number=message if isinstance(message, int) else -1,
            message_parameters=params,
            message=message if isinstance(message, six.string_types) else '',
            details=details,
        )



class NotFoundError(WebFilesystemError):
    """Generic exception for non-existent items."""
    def __init__(self, prefix, item_id, details=None):
        """

        :param str prefix: The prefix string.
        :param str item_id: The id of the item.
        :param str details: Text containing details about why the exception was thrown.
        """
        super(NotFoundError, self).__init__(
            "%0: Could not find item '%1'", params=[prefix, item_id],
            details=details
        )


class WebFilesystemHTTPError(WebFilesystemError):
    """Generic exception for all HTTP error codes (>=400)."""
    def __init__(self, service_name, http_error, details=None):
        """

        :param str service_name: Name of the web filesystem.
        :param requests.HTTPError http_error: Represents an HTTP error.
        :param str details: Custom error details.
        """
        response = http_error.response
        super(WebFilesystemHTTPError, self).__init__(
            "%0: HTTP error %1 (%2) for URL %3",
            params=[service_name, response.status_code, response.reason, http_error.request.url],
            details=details if details else response.text
        )
