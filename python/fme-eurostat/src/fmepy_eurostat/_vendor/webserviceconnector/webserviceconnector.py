from __future__ import print_function, unicode_literals, absolute_import, division

import os
import abc
from fmewebservices import FMENamedConnectionManager, FMEOAuthV2Connection, FMETokenConnection

import requests
from fmegeneral.fmehttp import FMERequestsSession
from fmegeneral.fmelog import get_configured_logger

import six

# Since this code is invoked from GUI code without access to fme objects we'll raise exceptions prefaced with special
# code that will be picked up by the GUI to display appropriate localized messages

# Generic error codes
from fmegeneral.webservices import FMETokenConnectionWrapper

# NOTE: see _handleErrorCodes
kServiceName = 'web service'
kLogMsgPrefix = 'Retrieval Error'
# Log message numbers
kConnector_InvalidWriteToFile_932303 = 932303
kConnector_InvalidInput_932308 = 932308
kConnector_InvalidConnection_932315 = 932315
kConnector_InvalidRequest_932316 = 932316
kConnector_ConnectionManager_932324 = 932324
kConnector_ConnectionManagerAuth_932325 = 932325
kConnector_BadRequest_932327 = 932327
kConnector_DNE_932328 = 932328
kConnector_ServerError_932329 = 932329
kConnector_Auth_932330 = 932330
kConnector_Permissions_932331 = 932331
kConnector_DuplicateError_932342 = 932342
kConnector_TimeoutError_932343 = 932343
kConnector_UnicodeConvert_932349 = 932349


# Exception Classes for Web Service Connectors
class WSCException(Exception):
    def __init__(self,
                 message_number=-1,
                 message_parameters=None,
                 details='',
                 message=''):
        """The general exception class for all Web Service Connector errors.

        Do not move the definition of this class.
        FME Workbench expects it to be in webserviceconnector.webserviceconnector.

        :param int message_number: The message number specified in the .fms file
        :param list message_parameters: The parameters needed to format the message in the .fms file
        :param str|unicode details: More details about why the exception was thrown. For example, the HTTP
           response content.
        :param str|unicode message: Free-form error message. Mutually exclusive with message_number and
           message_parameters
        """
        self.message_number = message_number
        if not message_parameters:
            self.message_parameters = []
        else:
            self.message_parameters = [str(x) for x in message_parameters]
        self.details = details
        self.message = message
        super(WSCException, self).__init__(self.message)


class WSCIOException(WSCException):
    def __init__(self, message_parameters=None, details='', message=''):
        """The exception for any web service related IO errors.

        :param list message_parameters: The parameters needed to format the message in the .fms file
        :param str|unicode details: More details about why the exception was thrown. For example, the HTTP
           response content.
        :param str|unicode message: Free-form error message. Mutually exclusive with message_number and
           message_parameters
        """
        super(WSCIOException, self).__init__(
            kConnector_InvalidWriteToFile_932303, message_parameters, details,
            message)


class WSCInvalidArgsException(WSCException):
    def __init__(self, message_parameters=None, details='', message=''):
        """The exception for any web service related arugment, key, value, or
        index errors.

        :param list message_parameters: The parameters needed to format the message in the .fms file
        :param str|unicode details: More details about why the exception was thrown. For example, the HTTP
           response content.
        :param str|unicode message: Free-form error message. Mutually exclusive with message_number and
           message_parameters
        """
        super(WSCInvalidArgsException, self).__init__(
            kConnector_InvalidInput_932308, message_parameters, details,
            message)


class WSCConnectionRetrievalException(WSCException):
    def __init__(self, message_parameters=None, details='', message=''):
        """The exception for Web Connection Manager connection retrieval
        errors.

        :param list message_parameters: The parameters needed to format the message in the .fms file
        :param str|unicode details: More details about why the exception was thrown. For example, the HTTP
           response content.
        :param str|unicode message: Free-form error message. Mutually exclusive with message_number and
           message_parameters
        """
        super(WSCConnectionRetrievalException, self).__init__(
            kConnector_InvalidConnection_932315, message_parameters, details,
            message)


class WSCRequestsException(WSCException):
    def __init__(self, message_parameters=None, details='', message=''):
        """The wrapper exception for anything thrown by the requests library.

        :param list message_parameters: The parameters needed to format the message in the .fms file
        :param str|unicode details: More details about why the exception was thrown. For example, the HTTP
           response content.
        :param str|unicode message: Free-form error message. Mutually exclusive with message_number and
           message_parameters
        """
        super(WSCRequestsException, self).__init__(
            kConnector_InvalidRequest_932316, message_parameters, details,
            message)


class WSCConnectionManagerException(WSCException):
    def __init__(self, message_parameters=None, details='', message=''):
        """The exception for Web Connection Manager access errors.

        :param list message_parameters: The parameters needed to format the message in the .fms file
        :param str|unicode details: More details about why the exception was thrown. For example, the HTTP
           response content.
        :param str|unicode message: Free-form error message. Mutually exclusive with message_number and
           message_parameters
        """
        super(WSCConnectionManagerException, self).__init__(
            kConnector_ConnectionManager_932324, message_parameters, details,
            message)


class WSCAuthRetrievalException(WSCException):
    def __init__(self, message_parameters=None, details='', message=''):
        """The exception for Web Connection Manager authentication retrieval
        errors.

        :param list message_parameters: The parameters needed to format the message in the .fms file
        :param str|unicode details: More details about why the exception was thrown. For example, the HTTP
           response content.
        :param str|unicode message: Free-form error message. Mutually exclusive with message_number and
           message_parameters
        """
        super(WSCAuthRetrievalException, self).__init__(
            kConnector_ConnectionManagerAuth_932325, message_parameters,
            details, message)


class WSCHttpRequestException(WSCException):
    def __init__(self, message_parameters=None, details='', message=''):
        """The exception for http authentication errors.

        :param list message_parameters: The parameters needed to format the message in the .fms file
        :param str|unicode details: More details about why the exception was thrown. For example, the HTTP
           response content.
        :param str|unicode message: Free-form error message. Mutually exclusive with message_number and
           message_parameters
        """
        super(WSCHttpRequestException, self).__init__(
            kConnector_BadRequest_932327, message_parameters, details, message)


class WSCHttpDNEException(WSCException):
    def __init__(self, message_parameters=None, details='', message=''):
        """The exception for http request errors, status code 404.

        :param list message_parameters: The parameters needed to format the message in the .fms file
        :param str|unicode details: More details about why the exception was thrown. For example, the HTTP
           response content.
        :param str|unicode message: Free-form error message. Mutually exclusive with message_number and
           message_parameters
        """
        super(WSCHttpDNEException, self).__init__(
            kConnector_DNE_932328, message_parameters, details, message)


class WSCHttpServerException(WSCException):
    def __init__(self, message_parameters=None, details='', message=''):
        """The exception for http server side errors, status code 500+

        :param list message_parameters: The parameters needed to format the message in the .fms file
        :param str|unicode details: More details about why the exception was thrown. For example, the HTTP
           response content.
        :param str|unicode message: Free-form error message. Mutually exclusive with message_number and
           message_parameters
        """
        super(WSCHttpServerException, self).__init__(
            kConnector_ServerError_932329, message_parameters, details,
            message)


class WSCHttpAuthException(WSCException):
    def __init__(self, message_parameters=None, details='', message=''):
        """The exception for http authentication errors.

        :param list message_parameters: The parameters needed to format the message in the .fms file
        :param str|unicode details: More details about why the exception was thrown. For example, the HTTP
           response content.
        :param str|unicode message: Free-form error message. Mutually exclusive with message_number and
           message_parameters
        """
        super(WSCHttpAuthException, self).__init__(
            kConnector_Auth_932330, message_parameters, details, message)


class WSCHttpPermissionsException(WSCException):
    def __init__(self, message_parameters=None, details='', message=''):
        """The exception for http permissions errors, status code 401.

        :param list message_parameters: The parameters needed to format the message in the .fms file
        :param str|unicode details: More details about why the exception was thrown. For example, the HTTP
           response content.
        :param str|unicode message: Free-form error message. Mutually exclusive with message_number and
           message_parameters
        """
        super(WSCHttpPermissionsException, self).__init__(
            kConnector_Permissions_932331, message_parameters, details,
            message)


class WSCHttpConflictException(WSCException):
    def __init__(self, message_parameters=None, details='', message=''):
        """The exception for http conflict errors, status code 409.

        :param list message_parameters: The parameters needed to format the message in the .fms file
        :param str|unicode details: More details about why the exception was thrown. For example, the HTTP
           response content.
        :param str|unicode message: Free-form error message. Mutually exclusive with message_number and
           message_parameters
        """
        super(WSCHttpConflictException, self).__init__(
            kConnector_DuplicateError_932342, message_parameters, details,
            message)


class WSCTimeoutException(WSCException):
    def __init__(self, message_parameters=None, details='', message=''):
        """The wrapper exception for timeouts thrown by the requests library.

        :param list message_parameters: The parameters needed to format the message in the .fms file
        :param str|unicode details: More details about why the exception was thrown. For example, the HTTP
           response content.
        :param str|unicode message: Free-form error message. Mutually exclusive with message_number and
           message_parameters
        """
        super(WSCTimeoutException, self).__init__(
            kConnector_TimeoutError_932343, message_parameters, details,
            message)


class WSCUnexpectedResponseException(WSCException):
    def __init__(self, message_parameters=None, details='', message=''):
        """The exception for an unexpected response from the http server.

        :param list message_parameters: The parameters needed to format the message in the .fms file
        :param str|unicode details: More details about why the exception was thrown. For example, the HTTP
           response content.
        :param str|unicode message: Free-form error message. Mutually exclusive with message_number and
           message_parameters
        """
        super(WSCUnexpectedResponseException, self).__init__(
            kConnector_UnicodeConvert_932349, message_parameters, details,
            message)


class WebServiceConnector(object):
    def __init__(self, args):
        """Abstract class used to power browser for selecting files from a web
        service. Retrieves the appropriate request header using a connection
        name from the connection manager.

        :param dict args: A JSON Dictionary where information from a service can be passed.  Currently only
           connection name is retrieved from 'CONNECTION' key
        :raises WSCException: invalid arguments supplied
        """
        try:
            # Initialize header object
            self._headers = {}
            # Grab the connection name (supplied from workbench) from the input args
            self._connectionName = args['CONNECTION']
            # Grab the header from the connection manager
            self.refreshHeader()
            # Set the content type to json
            self._headers['Content-Type'] = 'application/json'
            # Keep a session object to improve performance
            self._session = FMERequestsSession(
                self._connectionName,
                log=get_configured_logger('webservice_connector_log'))
        except KeyError as ke:
            raise WSCInvalidArgsException([kLogMsgPrefix, six.text_type(ke)])

    @abc.abstractmethod
    def get_root_container_id(self):
        pass

    def normalize_container_id(self, container_id):
        """
        If the container ID is falsey, or just `/`, then it should be the
        root ID specified by each webservice.
        """
        if not container_id or container_id == '/':
            return self.get_root_container_id()
        return container_id

# --------------------------------------------------------------------------------------------------------------------
# The Following 4 methods: getContainerContents(), getFile(), getItemDetails(), getItemName() are required
# for all python connectors
# --------------------------------------------------------------------------------------------------------------------

    def getContainerContents(self, args):
        """A method for returning contents of a web service. These contents can
        be items (files) or Folders (containers) or Resources (e.g. Amazon S3
        buckets or AutodeskA360 Hubs/Projects)

        :param dict args: a dict of key value pairs.
        :return: A list of dicts, each dict being a single content structure
        """
        raise NotImplementedError

    def getFile(self, file_id):
        """A method for retrieving a file with the specified id.

        :param str file_id: the id of the file to be downloaded
        """
        raise NotImplementedError

    def getItemDetails(self, item_id, is_folder=False):
        """A method for obtaining the metadata response for a given file or
        folder.

        :param str item_id: the Id of the item
        :param is_folder: boolean for if the item is a folder
        :return: a dict containing the meta data entries will be unique to each web service
        """
        raise NotImplementedError

    def getItemName(self, file_id, is_folder=False):
        """A method to return the name of a file or folder given its id.

        :param str file_id: the Id of the item
        :param is_folder: boolean for if the item is a fold
        :return: the name of the file
        """
        raise NotImplementedError

    # --------------------------------------------------------------------------------------------------------------------
    # The Following methods do not need to be reimplemented in derived connectors
    # --------------------------------------------------------------------------------------------------------------------

    def downloadFile(self, args):
        """A method for downloading a file with a specified ID to a location on
        disk.

        :param dict args: A dict of key-value pairs must contain key 'FILE_ID' an 'TARGET_FOLDER'
           for file id and location to be written on disk respectively.  May optionally contain
           a 'FILENAME' key if this is specified the file will be written to disk with this name
        :raises WSCException: invalid arguments supplied or unable to write to file
        """
        targetPath = ""
        try:
            fileId = args['FILE_ID']
            targetFolder = args['TARGET_FOLDER']

            if not 'FILENAME' in args:
                fileName = self.getItemName(fileId, False)
            else:
                fileName = args['FILENAME']

            targetPath = os.path.join(targetFolder, fileName)

            if not os.path.exists(targetFolder):
                self._makeNewDirectory(targetFolder)

            # Writing the content to that specified location
            with open(targetPath, "wb") as f:
                f.write(self.getFile(fileId))
        except IOError:
            raise WSCIOException([kLogMsgPrefix, targetPath])
        except KeyError as ke:
            raise WSCInvalidArgsException([kLogMsgPrefix, six.text_type(ke)])

    def downloadFolder(self, args):
        """A method for downloading a file with a specified ID to a location on
        disk.

        :param dict args: A dict of key-value pairs must contain key 'CONTAINER_ID' an 'TARGET_FOLDER'
           for file id and location to be written on disk respectively.  May optionally contain
           a 'EXCLUDE_SUB_FOLDERS' key if sub folders are not to be downloaded
        :raises WSCException: invalid arguments supplied
        """
        try:
            path = args['TARGET_FOLDER']
            includeSubFolders = True
            if 'EXCLUDE_SUB_FOLDERS' in args:
                includeSubFolders = False

            resp = self.getContainerContents(args)

            # Make the directory
            self._makeNewDirectory(path)

            #download the innerfiles
            self._downloadFolderContents(resp, path, includeSubFolders)

            #check for additional pages
            while 'CONTINUE' in resp:
                resp = self.getContainerContents(resp['CONTINUE']['ARGS'])
                self._downloadFolderContents(resp, path, includeSubFolders)
        except KeyError as ke:
            raise WSCInvalidArgsException([kLogMsgPrefix, six.text_type(ke)])

    def refreshHeader(self, force_refresh=False):
        """Method that resets the header used to make requests, using the
        Connection Manager.

        :param bool force_refresh: if this is true a new token will be retrieved even if the original
           token is still valid according to its expiry.
        :raises WSCException: unable to access Web Connection Manager or
           unable to retrieve auth from Web Connection Manager or
           unable to retrieve connection from Web Connection Manager
        """
        ncMan = FMENamedConnectionManager()
        if ncMan is None:
            raise WSCConnectionManagerException([kLogMsgPrefix])
        conn = ncMan.getNamedConnection(self._connectionName)
        if isinstance(conn, (FMEOAuthV2Connection, FMETokenConnection)):
            # This is the current mechanism to ensure we get a new token even
            # if the existing token may not be expired
            wrapped = FMETokenConnectionWrapper(conn)
            if force_refresh:
                wrapped.set_suspect_expired()
            try:
                header, authcode = wrapped.get_authorization_header()
            except:
                raise WSCAuthRetrievalException([kLogMsgPrefix, kServiceName])
            # Save the new value to the header if the header has a value.
            if header:
                self._headers[header] = authcode
        else:
            raise WSCConnectionRetrievalException([kLogMsgPrefix])

    def _makeRequest(self, request_method, url, **kwargs):
        """Method that makes a request using the FME Requests session Object,
        defaults to a 60s timeout. Uses a header with the authorization
        information supplied to the constructor.  Returns response or raises
        exception in case of http error.

        :param request_method: the Requests method to call
        :param str url: the url
        :param kwargs: additional arguments
        :raises WSCException: failed or unexpected response from the server or
           unable to retrieve contents within the specified timeout or
           ambiguous exception that occurred while handling your request
        :return: The content of the response
        """
        try:
            if 'timeout' not in kwargs:
                kwargs['timeout'] = 300
            try:
                response = request_method(
                    self._session, url, headers=self._headers, **kwargs)
                # If an invalid auth error retrieved refresh the header and attempt again
                if response.status_code not in range(200, 205) and \
                   response.status_code != 308:
                    if response.status_code == 401:
                        self.refreshHeader(True)
                        #if we hit any http error raise an exception and retry below
                        raise WSCUnexpectedResponseException(
                            [kLogMsgPrefix, kServiceName])
            except (requests.exceptions.Timeout,
                    requests.exceptions.RequestException, WSCException):
                response = request_method(
                    self._session, url, headers=self._headers, **kwargs)
        #Catch any exceptions requests may throw
        except requests.exceptions.Timeout:
            raise WSCTimeoutException([kLogMsgPrefix, kServiceName])
        except requests.exceptions.RequestException:
            raise WSCRequestsException([kLogMsgPrefix, kServiceName])
        # if response is not returned within the okay code range, pass to error handler
        if response.status_code not in range(200, 205) and \
           response.status_code != 308:
            self._handleErrorCodes(response)
        else:
            return response

    def _makeNewDirectory(self, new_path):
        """Method that creates a directory when downloading folders or files.

        :param str new_path: the new directory you wish to create in the file system
        :raises WSCException: invalid arguments supplied
        """
        try:
            if not os.path.exists(new_path):
                os.makedirs(new_path)
        except:
            raise WSCIOException([kLogMsgPrefix, new_path])

    def _downloadFolderContents(self, resp, path, include_sub_folders):
        """Helper method that takes a list response and downloads the
        appropriate folders.

        :param dict resp: the response object from the listFilesinFolders method
        :param str path: the path to download the folder to
        :param bool include_sub_folders: whether to include subfolders
        """

        #Download the files in the response
        items = [x for x in resp['CONTENTS'] if not x['IS_CONTAINER']]
        for item in items:
            self.downloadFile({
                'FILE_ID': item['ID'],
                'TARGET_FOLDER': path,
                'FILENAME': item['NAME']
            })
        #Download the folders in the response
        if include_sub_folders:
            containers = [x for x in resp['CONTENTS'] if x['IS_CONTAINER']]
            for container in containers:
                data = {
                    'CONTAINER_ID': container['ID'],
                    'TARGET_FOLDER': os.path.join(path, container['NAME'])
                }
                self.downloadFolder(data)


# --------------------------------------------------------------------------------------------------------------------
# The Following methods are convenience wrappers of request methods
# --------------------------------------------------------------------------------------------------------------------

    def get(self, url, **kwargs):
        return self._makeRequest(FMERequestsSession.get, url, **kwargs)

    def put(self, url, **kwargs):
        return self._makeRequest(FMERequestsSession.put, url, **kwargs)

    def patch(self, url, **kwargs):
        return self._makeRequest(FMERequestsSession.patch, url, **kwargs)

    def post(self, url, **kwargs):
        return self._makeRequest(FMERequestsSession.post, url, **kwargs)

    def delete(self, url, **kwargs):
        return self._makeRequest(FMERequestsSession.delete, url, **kwargs)

    def addToHeader(self, header_key, header_val):
        """Method that allows users to add additional key-value pairs to the
        header. All values stored in the header should be encodable in latin-1.

        :param str header_key: the key to be added to the header
        :param str header_val: the corresponding value to be added to the header
        """
        try:
            header_val.encode('latin-1')
            self._headers[header_key] = header_val
        except UnicodeEncodeError:
            error_message = "Request header \"{header}:{value}\" contains characters not encodable in latin-1"
            raise WSCHttpRequestException(
                message_parameters=[kLogMsgPrefix, kServiceName, self._connectionName],
                details=error_message.format(header=header_key, value=header_val)
            )


    def removeFromHeader(self, header_key):
        """Method that allows users to remove a specific key value from header.

        :param str header_key: the key to be added to the header
        """
        self._headers.pop(header_key, None)

    def _handleErrorCodes(self, response):
        """Private method that raises the appropriate exception based on the
        status of the response.

        :param fmegeneral.fmehttp.FMERequestsSession response: the response of the HTTP call
        :raises WSCException: http authentication errors or http request errors (status code 400)
           http permissions errors (status code 401) or http does not exist errors (status code 404) or
           http conflict errors (status code 409) or http server side errors (status code 500+)
        """
        #Look for some type of error details that may help the user in either the body or the response.reason
        if not response.text:
            content = response.reason
        else:
            content = response.text
        # NOTE: won't be able to specify each individual service name when raising WSCException here, but that should
        # be fine as the GUI's error pop-up specifies the service name.
        if response.status_code == 401:
            raise WSCHttpAuthException(
                [kLogMsgPrefix, kServiceName, response.url], content)
        elif response.status_code == 403:
            raise WSCHttpPermissionsException(
                [kLogMsgPrefix, kServiceName, response.url], content)
        elif response.status_code == 404:
            raise WSCHttpDNEException(
                [kLogMsgPrefix, kServiceName, response.url], content)
        elif response.status_code == 409:
            raise WSCHttpConflictException(
                [kLogMsgPrefix, kServiceName, response.url], content)
        elif 400 <= response.status_code < 500 and response.status_code != 429:
            raise WSCHttpRequestException(
                [kLogMsgPrefix, kServiceName, response.url], content)
        elif response.status_code == 500 or response.status_code == 429 or response.status_code == 503:
            raise WSCHttpServerException(
                [kLogMsgPrefix, kServiceName, response.url], content)
