# System imports

from __future__ import print_function, unicode_literals, absolute_import, division

import mimetypes
import os
from fmeobjects import FMESession, FME_WARN, FME_INFORM, FMEFeature, FMEException
from fmegeneral.fmeutil import isoTimestampToFMEFormat
import sys

import six
from fmegeneral.fmeutil import Logger, systemToUnicode
from ..webserviceconnector.webserviceconnector import WSCException, WSCHttpDNEException, WSCUnexpectedResponseException, \
   WSCIOException, WSCHttpConflictException, WSCInvalidArgsException, WSCConnectionManagerException, \
   WSCConnectionRetrievalException, WSCAuthRetrievalException, WSCHttpPermissionsException, WSCHttpRequestException, \
   WSCHttpAuthException, WSCHttpServerException, WSCTimeoutException, WSCRequestsException, \
   kConnector_InvalidWriteToFile_932303, kConnector_ServerError_932329, \
   kConnector_InvalidInput_932308, kConnector_InvalidConnection_932315, kConnector_InvalidRequest_932316, \
   kConnector_ConnectionManager_932324, kConnector_ConnectionManagerAuth_932325, kConnector_TimeoutError_932343, \
   kConnector_UnicodeConvert_932349, kConnector_Auth_932330, kConnector_BadRequest_932327, \
   kConnector_Permissions_932331, kConnector_DNE_932328, kConnector_DuplicateError_932342

from fmehdfs.fmehdfsDefs import HdfsUtilException

# --- Constant Definitions for the attributes we use internally

kConnector_Dest = 'DEST'
kConnector_OperationType = 'OPERATION_TYPE'
kConnector_OutputAttribute = 'OUTPUT_ATTR'
kConnector_Source = 'SOURCE'

# -- Upload

kConnector_DataToUpload = '_FME_DATA_TO_UPLOAD'
kConnector_IncludeSubFolders = '_FME_INCLUDE_SUBFOLDERS'
kConnector_SourceFile = '_FME_SOURCE_FILE'
kConnector_SourceFolder = '_FME_SOURCE_FOLDER'
kConnector_SourceType = '_FME_SOURCE_TYPE'
kConnector_UploadAttributeFileName = '_FME_UPLOAD_ATTR_FILENAME'
kConnector_UploadDestinationPath = '_FME_UPLOAD_DESTINATION_PATH'
kConnector_UrlAttribute = '_FME_OBJECT_URL_ATTRIBUTE'
kConnector_DirectUrlAttribute = '_FME_OBJECT_DIRECT_URL_ATTRIBUTE'

# -- Download

kConnector_Files = '_FME_FILES'
kConnector_DataTarget = '_FME_DATA_TARGET'
kConnector_TargetFolder = '_FME_TARGET_FOLDER'
kConnector_TargetAttribute = '_FME_TARGET_ATTRIBUTE'
kConnector_DownloadPathAttribute = '_FME_DOWNLOAD_PATH_ATTRIBUTE'

# -- Delete and list

kConnector_Path = 'PATH'
kConnector_FileId = '_FME_FILEID'

# -- List

kConnector_NameAttribute = '_FME_OBJECT_NAME_ATTRIBUTE'
kConnector_PathAttribute = '_FME_OBJECT_PATH_ATTRIBUTE'
kConnector_IdAttribute = '_FME_OBJECT_ID_ATTRIBUTE'
kConnector_TypeAttribute = '_FME_OBJECT_TYPE_ATTRIBUTE'
kConnector_Modified_Attribute = '_FME_OBJECT_MODIFIED_ATTRIBUTE'
kConnector_SizeAttribute = '_FME_OBJECT_SIZE_ATTRIBUTE'
kConnector_Output_List = 'OUTPUT_LIST'
kConnector_RelativePathAttribute = '_FME_OBJECT_RELATIVE_PATH_ATTRIBUTE'

# -- Create
kConnector_ImageUrlAttribute = '_FME_IMAGE_URL'
kConnector_DescriptionAttribute = '_FME_NOTE'
kConnector_OutLinkAttribute = '_FME_OUT_LINK'
kConnector_CreateAttributeName = '_FME_UPLOAD_ATTR_NAME'

# -- Edit
kConnector_NewDescriptionAttribute = '_FME_NEW_DESCRIPTION'
kConnector_NewLinkAttribute = '_FME_NEW_LINK'
kConnector_NewNameAttribute = '_FME_NEW_NAME'

# --- Constant Definitions for error handling

kFMERejectionCode = 'fme_rejection_code'
kFMERejectionMsg = 'fme_rejection_message'
kFMEGeneralError = 'ERROR_DURING_PROCESSING'
kFileUplaod = 'File Upload'
kFileDownload = 'File Download'
kFileDelete = 'File Delete'
kFileList = 'File List'
kConnector_InvalidFileDownload_93300 = 932300
kConnector_FileDownloadToFolder_932301 = 932301
kConnector_FileDownloadToAttribute_932302 = 932302
kConnector_InvalidReadFromFile_932304 = 932304
kConnector_InvalidMediaUpload_932305 = 932305
kConnector_InvalidPathIOError_932306 = 932306
kConnector_DuplicateUpload_932307 = 932307
kConnector_InvalidMetadataList_932309 = 932309
kConnector_InvalidFileDelete_932310 = 932310
kConnector_InvalidFolderUpload_932311 = 932311
kConnector_FolderRecursionSuccess_932312 = 932312
kConnector_InvalidMediaUploadAttr_932313 = 932313
kConnector_InvalidOperationType_932314 = 932314
kConnector_Delete_932317 = 932317
kConnector_List_932318 = 932318
kConnector_Overwite_932319 = 932319
kConnector_InvalidFolderDownload_932320 = 932320
kConnector_UploadingFile_932321 = 932321
kConnector_CreatingFolder_932322 = 932322
kConnector_UploadingAttribute_932323 = 932323
kConnector_UnexpectedResponse_932326 = 932326
kConnector_Response_932332 = 932332
kConnector_mimeType_932333 = 932333
kConnector_nameRetrieval_932334 = 932334
kConnector_ListFailed_932335 = 932335
kConnector_FileUploadFailed_932336 = 932336
kConnector_FolderUploadFailed_932337 = 932337
kConnector_LinkRetrievalFailed_932338 = 932338
kConnector_MetaDataUploadFailed_932339 = 932339
kConnector_DateConversionError_932340 = 932340
kConnector_EmptyFolder_932344 = 932344
kConnector_FileDownloadFailed_932345 = 932345
kConnector_FolderDownloadFailed_932346 = 932346
kConnector_NothingToDelete_932347 = 932347
kConnector_LocalFileDNE_932348 = 932348

# --- Constant Definitions for parameters passed in from the FMX file

kConnectorUpload = 'Upload'
kConnectorDelete = 'Delete'
kConnectorDownload = 'Download'
kConnectorList = 'List'
kConnectorNotifs = 'Configure Push Notifications'
kConnectorCreate = 'Create'
kConnectorEdit = 'Edit'


# --- Creating an exception class to handle exceptions thrown in functions
class FunctionRejection(Exception):
    def __init__(self, messageNum, messageArgs):
        self.messageNum = messageNum
        self.messageArgs = messageArgs
        Exception.__init__(self)


class WebserviceConnectorFactory(object):
    def __init__(self):
        self._logger = Logger()
        self._session = FMESession()
        self._namedConnKey = None
        self._serviceName = None
        self._transformerName = None
        self._webServiceConnector = None

        # Upload Only
        self._sharableLinkAttribute = None
        self._downloadLinkAttribute = None
        self._newId = None
        self._uploadDestId = None

        # Upload and Download
        self._includeSubFolders = None
        self._imageUrlAttribute = None
        self._noteAttribute = None

        # List Only
        self._nameAttribute = None
        self._idAttribute = None
        self._pathAttribute = None
        self._typeAttribute = None
        self._lastModifiedAttribute = None
        self._sizeAttribute = None
        self._relativePathAttribute = None

    # --------------------------------------------------------------------------------------------------------------------
    # The Following 3 methods: input(), pyoutput(), and close() are required for all python transformers
    # --------------------------------------------------------------------------------------------------------------------

    def input(self, feature):
        """Do the real work.  If any problem happens, catch it and assign
        appropriate exception attributes.

        :param FMEFeature feature: the FMEFeature input to the transformer to add and remove attributes from
        """

        try:
            # Sanitize inputs (FMEENGINE-57074)
            feature.removeAttribute("fme_rejection_code")

            operationType = self.decodeValue(
                feature.getAttribute(kConnector_OperationType))
            # Grabbing Necessary param attributes
            namedConn = self.decodeValue(
                feature.getAttribute(self._namedConnKey))

            self.initializeWebService({'CONNECTION': namedConn}, feature)

            outputOriginalFeature = True
            # Setting up cases for operations
            if operationType == kConnectorDelete:
                self._doDelete(feature)
            elif operationType == kConnectorUpload:
                self._doUpload(feature)
            elif operationType == kConnectorDownload:
                self._doDownload(feature)
            # In list mode we clone features and only want to output the original in the event of a failure
            elif operationType == kConnectorList:
                outputOriginalFeature = False
                self._doList(feature)
            # This operation is currently only supported for Google Drive
            elif operationType == kConnectorNotifs:
                # Note that this is an unexposed function currently only implemented for google drive
                self._doPushNotifications(feature)
            elif operationType == kConnectorCreate:
                self._doCreate(feature)
            elif operationType == kConnectorEdit:
                self._doEdit(feature)
            # if operation is not one of the above we should go out rejected
            else:
                raise FunctionRejection(kConnector_InvalidInput_932308,
                                        [self._transformerName])

            # Remove attributes that should stay internal here
            feature.removeAttribute(kConnector_OperationType)
            feature.removeAttribute(self._namedConnKey)
            # Return the feature to FME.  A TestFactory after the PythonFactory will route it properly in the case of rejection
            if outputOriginalFeature:
                self.pyoutput(feature)
        except Exception as e:
            self._handleException(e, feature)

    def close(self):
        pass

    def has_support_for(self, support_type):
        """
        Return whether this transformer supports a certain type. Currently,
        the only supported type is fmeobjects.FME_SUPPORT_FEATURE_TABLE_SHIM.

        This method was added in FME 2022.0.
        Refer to :func:`fmegeneral.plugins.FMETransformer.has_support_for` for more details.

        :type support_type: int
        :returns: True if the passed in support type is supported.
        :rtype: bool
        """
        # Usages of this class will probably support FME_SUPPORT_FEATURE_TABLE_SHIM,
        # but this should be verified and explicitly enabled in the implementation.
        return False

    def pyoutput(self, feature):
        # Stub. Implementation is injected at runtime.
        # This method returns a feature to FME.
        pass

    # --------------------------------------------------------------------------------------------------------------------
    # The following methods: intializeWebService(), createFolder(), uploadData(), deleteUrl(),
    # setMetadataAttributes() must be reimplemented in any derived class
    # getUploadMetadataAttributes(), getMetadataAttributes(), and _doPushNotifications
    #  and may optionally be implemented in a derived class
    # --------------------------------------------------------------------------------------------------------------------

    def initializeWebService(self, args, feature):
        """Method that creates an instance of a WebServiceConnector class and
        stores this in self._webServiceConnector.

        :param dict args: a dictionary with a 'CONNECTION' key to supply the connection name to the WebServiceConnector
           constructor
        :param FMEFeature feature: the FMEFeature to get attributes that specific web services may need to initialize
           their WebServiceConnector
        """

        raise NotImplementedError

    def createFolder(self, feature, folder_name, base_folder_id):
        """Method that creates a folder in a specified location in the web
        service.  Also sets attributes corresponding to the file's new
        webservice id an url on the feature.

        :param FMEFeature feature: the FMEFeature to add create folder attributes to
        :param str|unicode folder_name: The name to be used for the folder to be created
        :param str|unicode base_folder_id: The id of the parent folder
        :return: the ID of the newly created folder
        """

        raise NotImplementedError

    def uploadData(self, feature, data, file_name, base_folder_id):
        """Method that uploads the bytes of a file to a specified location in
        the webservice.  Also sets attributes corresponding to the file's new
        webservice id an url on the feature.

        :param FMEFeature feature:  the FMEFeature to add upload data attributes to
        :param bytes data: file handle of the file or bytes/bytearray to be written to the webservice
        :param str|unicode file_name: the name of the file to be written to the webservice
        :param str|unicode base_folder_id: the id of the folder to upload the file into
        """
        raise NotImplementedError

    def deleteUrl(self, item_id):
        """Method that returns the url to be used in the delete request.

        :param str|unicode item_id: the id of the object to be deleted
        """
        raise NotImplementedError

    def setMetadataAttributes(self, feature, get_resp, is_folder):
        """Method that sets metadata attributes on features based on a get
        metadata response object. Any key errors thrown will be caught in
        createListFeatures()

        :param FMEFeature feature: the FMEFeature to set attributes for
        :param dict get_resp: The response object from a get metadata request.  Structure will be unique per webservice
        :param bool is_folder: Boolean for whether metadata object is a folder
        """
        raise NotImplementedError

    def getMetadataAttributes(self, feature):
        """Method that retrieves the name of metadata attributes supplied in
        the interface used for List operation.

        :param FMEFeature feature: the FMEFeature to get attributes from
        """
        self._nameAttribute = self.decodeValue(
            feature.getAttribute(kConnector_NameAttribute))
        self._idAttribute = self.decodeValue(
            feature.getAttribute(kConnector_IdAttribute))
        self._pathAttribute = self.decodeValue(
            feature.getAttribute(kConnector_PathAttribute))
        self._typeAttribute = self.decodeValue(
            feature.getAttribute(kConnector_TypeAttribute))
        self._lastModifiedAttribute = self.decodeValue(
            feature.getAttribute(kConnector_Modified_Attribute))
        self._sizeAttribute = self.decodeValue(
            feature.getAttribute(kConnector_SizeAttribute))
        self._relativePathAttribute = self.decodeValue(
            feature.getAttribute(kConnector_RelativePathAttribute))
        self._sharableLinkAttribute = self.decodeValue(
            feature.getAttribute(kConnector_UrlAttribute))
        self._downloadLinkAttribute = self.decodeValue(
            feature.getAttribute(kConnector_DirectUrlAttribute))

    def getUploadMetadataAttributes(self, feature):
        """Method that retrieves the name of metadata attributes supplied in
        the interface used for Upload operation if reimplemented setting of
        self._newId is required.  Additional attributes are optional.

        :param FMEFeature feature: the FMEFeature to get attributes from
        """
        self._newId = self.decodeValue(
            feature.getAttribute(kConnector_IdAttribute))
        self._sharableLinkAttribute = self.decodeValue(
            feature.getAttribute(kConnector_UrlAttribute))
        self._downloadLinkAttribute = self.decodeValue(
            feature.getAttribute(kConnector_DirectUrlAttribute))

    def getCreateMetadataAttributes(self, feature):
        """Method that retrieves the name of metadata attributes supplied in
        the interface used for Create operation if reimplemented setting of
        self._newId is required.  Additional attributes are optional.

        :param FMEFeature feature: the FMEFeature to get attributes from
        """
        self._newId = self.decodeValue(
            feature.getAttribute(kConnector_IdAttribute))

    def getEditMetadataAttributes(self, feature):
        """Method that retrieves the name of metadata attributes supplied in
        the interface used for Edit operation if reimplemented setting of
        self._newId is required.  Additional attributes are optional.

        :param FMEFeature feature: the FMEFeature to get attributes from
        """
        self._newId = self.decodeValue(
            feature.getAttribute(kConnector_IdAttribute))

    def _doPushNotifications(self, feature):
        """Method that configures push notifications.  This mode is currently
        unexposed and only implemented for google drive.

        :param FMEFeature feature: the FMEFeature to add do push notification attributes to
        """

        pass

    def _createItem(self, feature):
        """Method that creates a new item.  This mode is currently unexposed
        and only implemented for pinterest.

        :param FMEFeature feature: the MEFeature to add create item attributes to
        """

        pass

    def _editItem(self, feature):
        """Method that edits an existing item.  This mode is currently
        unexposed and only implemented for pinterest.

        :param FMEFeature feature: the FMEFeature to add edit items attributes to
        """

        pass

    # --------------------------------------------------------------------------------------------------------------------
    # The following methods: getMimeType() and decodeValue() and getItemId() are public utility methods used multiple operations
    # --------------------------------------------------------------------------------------------------------------------

    def getMimeType(self, file_name):
        """Determines a mimetype given a filename.

        :param str|unicode file_name: name of the file including extension
        :return: mimeType string
        """
        mimetypes.init()

        try:
            mime_type = mimetypes.guess_type(file_name)[0]
            if mime_type is None:
                # no mimetype could be determined.
                raise ValueError

            return mime_type
        except Exception:
            raise FunctionRejection(kConnector_mimeType_932333,
                                    [self._transformerName, file_name])

    def decodeValue(self, val):
        """Method that takes any text related input, or things that could
        possibly be international characters, and decode it from WWJD to
        system.

        :param str val: Value that needs to be decoded
        :return: A system value
        """
        if isinstance(val, six.string_types):
            return self._session.decodeFromFMEParsableText(val)
        else:
            return six.text_type(val)

    def getItemId(self,
                  item_name,
                  base_folder_id,
                  is_folder,
                  case_sensitive=True):
        """Obtains an Items Id from its name and baseFolderID.

        :param str|unicode item_name: name of the item
        :param str|unicode base_folder_id: id of folder in the webservice to look for the item in
        :param bool is_folder: whether the item is a folder
        :param bool case_sensitive: whether to match name case sensitive
        :return: item ID if Found, None if no ID found
        """

        # assemble json object expected by the connector
        data = {'CONTAINER_ID': base_folder_id}
        resp = self._webServiceConnector.getContainerContents(data)

        id = self._findId(item_name, resp, is_folder, case_sensitive)
        if id is not None:
            return id

        # Check for additional pages
        while 'CONTINUE' in resp and 'ARGS' in resp['CONTINUE']:
            resp = self._webServiceConnector.getContainerContents(
                resp['CONTINUE']['ARGS'])
            id = self._findId(item_name, resp, is_folder, case_sensitive)
            if id is not None:
                return id
        return None

    # --------------------------------------------------------------------------------------------------------------------
    # The following private methods are used in the upload operation
    # --------------------------------------------------------------------------------------------------------------------

    def _doUpload(self, feature):
        """Method that handles upload operations.

        :param FMEFeature feature: the FMEFeature to add upload attributes to
        """
        self.getUploadMetadataAttributes(feature)
        sourceType = self.decodeValue(
            feature.getAttribute(kConnector_SourceType))
        self._uploadDestId = self.decodeValue(
            feature.getAttribute(kConnector_UploadDestinationPath))

        # If the folder parameter is set then we need to make a GET request to the folder specified, grab its ID
        if sourceType == "File":
            fileToUpload = systemToUnicode(
                self.decodeValue(feature.getAttribute(kConnector_SourceFile)))
            self._uploadFile(feature, fileToUpload, self._uploadDestId)

        elif sourceType == "Folder":
            self._includeSubFolders = (str(
                feature.getAttribute(kConnector_IncludeSubFolders)).lower() ==
                                       "yes")
            folderToUpload = systemToUnicode(
                self.decodeValue(
                    feature.getAttribute(kConnector_SourceFolder)))
            self._uploadFolder(feature, folderToUpload, self._uploadDestId)

        elif sourceType == "Attribute":
            fileName = systemToUnicode(
                self.decodeValue(
                    feature.getAttribute(kConnector_UploadAttributeFileName)))
            attributeToUpload = self.decodeValue(
                feature.getAttribute(kConnector_DataToUpload))
            self._uploadAttribute(feature, attributeToUpload, fileName,
                                  self._uploadDestId)
        else:
            raise FunctionRejection(kConnector_InvalidInput_932308,
                                    [self._transformerName])

        # Remove attributes that should stay internal here
        self._removeUploadAttributes(feature)

    def _uploadFile(self, feature, file_to_upload, base_folder_id):
        """Method that handles uploading a file from OS to a web service.

        :param FMEFeature feature: the FMEFeature to add upload file attributes to
        :param str|unicode file_to_upload: the full path to the file to be uploaded
        :param str|unicode base_folder_id: the folder to be used to upload to on the webservice
        """
        # Handle long filenames on windows see http://stackoverflow.com/questions/14075465/copy-a-file-with-a-too-long-path-to-another-directory-in-python
        if len(file_to_upload) > 255 and sys.platform == 'win32':
            file_to_upload = six.text_type('\\\\?\\') + os.path.abspath(
                six.text_type(file_to_upload)).replace('/', '\\')
        fileName = os.path.basename(file_to_upload)
        self._logger.logMessage(
            kConnector_UploadingFile_932321,
            [self._transformerName, fileName, self._serviceName], FME_INFORM)

        try:
            with open(file_to_upload, 'rb') as f:
                self.uploadData(feature, f, fileName, base_folder_id)
        except IOError:
            raise WSCIOException([self._transformerName, file_to_upload])
        except WSCHttpConflictException:
            # Notify the user that they are trying to upload a duplicate file
            self._logger.logMessage(
                kConnector_DuplicateUpload_932307,
                [self._transformerName, fileName, self._serviceName], FME_WARN)
            raise
        except WSCUnexpectedResponseException:
            # Log a message here with the details of the file and raise up to deal with the http error in the base class
            self._logger.logMessage(
                kConnector_FileUploadFailed_932336,
                [self._transformerName, fileName, self._serviceName])
            raise

    def _uploadFolder(self, feature, folder_to_upload, base_folder_id):
        """Method that uploads a folder to a web service. This method is
        recursive and will call itself on subfolders if
        self._includesSubFolders is True.

        :param FMEFeature feature: the FMEFeature to add upload folder attributes to
        :param str|unicode folder_to_upload: the full path of the folder to upload
        :param str|unicode base_folder_id: the id of the folder on the webservice to upload to
        """
        # Get the folder's name from the path
        folderName = os.path.basename(os.path.normpath(folder_to_upload))

        # Create the folder object on the web service
        self._logger.logMessage(
            kConnector_CreatingFolder_932322,
            [self._transformerName, folderName, self._serviceName], FME_INFORM)
        try:
            folderID = self.createFolder(feature, folderName, base_folder_id)
        except WSCHttpConflictException:
            # Notify the user that they are trying to create a duplicate folder
            self._logger.logMessage(
                kConnector_DuplicateUpload_932307,
                [self._transformerName, folderName, self._serviceName],
                FME_WARN)
            raise
        except WSCUnexpectedResponseException:
            # Log details and reraise exception
            self._logger.logMessage(
                kConnector_FolderUploadFailed_932337,
                [self._transformerName, folderName, self._serviceName],
                FME_WARN)
            raise

        # If this is the highest level folder we'll want to set the id attribute here
        if base_folder_id == self._uploadDestId:
            feature.setAttribute(self._newId, folderID)

        # Go through all items 1 level down in the folder, upload files and subfolders if applicable
        try:
            for item in os.listdir(folder_to_upload):
                if not os.path.isdir(os.path.join(folder_to_upload, item)):
                    # If we fail on any individual file, send a feature out the rejected port and continue on
                    try:
                        self._uploadFile(feature, os.path.join(folder_to_upload, item), folderID)
                    except Exception as e:
                        feat = FMEFeature(feature)
                        self._handleException(e, feat)
                elif self._includeSubFolders:
                    self._uploadFolder(feature, os.path.join(folder_to_upload, item), folderID)

        except OSError:
            raise FunctionRejection(kConnector_LocalFileDNE_932348,
                                    [self._transformerName, folder_to_upload])

    def _uploadAttribute(self, feature, attribute_to_upload, file_name,
                         base_folder_id):
        """Method that uploads data from a user specified attribute to a file
        on a web service.

        :param FMEFeature feature: the FMEFeature to add upload attributes to
        :param str|unicode attribute_to_upload: The name of the attribute whose contents is to be uploaded
        :param str|unicode file_name: The name to be given to the resulting file on the webservice
        :param str|unicode base_folder_id: The id of the folder on the web service to upload the file into
        """
        # ensure attribute is converted to a string type
        attributeContents = feature.getAttribute(attribute_to_upload)
        # Check if attribute is binary and if it is upload raw
        if not isinstance(attributeContents, (bytearray, bytes)):
            # If we are not binary we want to convert to a string type, allows us to write a float etc to a file without error
            try:
                if not isinstance(attributeContents, six.string_types):
                    attributeContents = six.text_type(attributeContents)
                # Make sure text is utf-8 encoded (this is the best we can do without exposing an encoding option)
                attributeContents = attributeContents.encode('utf-8')
            except UnicodeEncodeError:
                raise FunctionRejection(
                    kConnector_UnicodeConvert_932349,
                    [self._transformerName, attribute_to_upload])

        self._logger.logMessage(
            kConnector_UploadingAttribute_932323,
            [self._transformerName, attribute_to_upload, self._serviceName],
            FME_INFORM)
        self.uploadData(feature, attributeContents, file_name, base_folder_id)

    def _removeUploadAttributes(self, feature):
        """Method that generalizes the process of removing attributes from the
        upload method.

        :param FMEFeature feature: the feature that got the attributes to do the logic in the method
        """
        feature.removeAttribute(kConnector_OperationType)
        feature.removeAttribute(kConnector_UploadDestinationPath)
        feature.removeAttribute(kConnector_Dest)
        feature.removeAttribute(kConnector_OutputAttribute)
        feature.removeAttribute(kConnector_Source)
        feature.removeAttribute(kConnector_SourceType)
        feature.removeAttribute(kConnector_IdAttribute)
        feature.removeAttribute(kConnector_SourceFile)
        feature.removeAttribute(kConnector_SourceFolder)
        feature.removeAttribute(kConnector_IncludeSubFolders)
        feature.removeAttribute(kConnector_DataToUpload)
        feature.removeAttribute(kConnector_UploadAttributeFileName)
        feature.removeAttribute(kConnector_UrlAttribute)
        feature.removeAttribute(kConnector_DirectUrlAttribute)

    def _findId(self, item_name, resp, is_folder, case_sensitive):
        """Method that searches the get files in folder response for a name
        match and returns the id if found.

        :param str|unicode item_name: name of the item to find the id from
        :param dict resp: response from the contents received
        :param bool is_folder: whether the item is a folder
        :param bool case_sensitive: whether to match name case sensitive
        :return:
        """
        if not is_folder:
            items = [x for x in resp['CONTENTS'] if not x['IS_CONTAINER']]
            for item in items:
                if item['NAME'] == item_name or (
                    (not case_sensitive)
                        and item['NAME'].lower() == item_name.lower()):
                    return item['ID']

        if is_folder:
            containers = [x for x in resp['CONTENTS'] if x['IS_CONTAINER']]
            for container in containers:
                if container['NAME'] == item_name or (
                    (not case_sensitive)
                        and container['NAME'].lower() == item_name.lower()):
                    return container['ID']
        return None

    # --------------------------------------------------------------------------------------------------------------------
    # The following private methods are used in the download operation
    # --------------------------------------------------------------------------------------------------------------------

    def _doDownload(self, feature):
        """Method that handles the download operation.

        :param FMEFeature feature: the FMEFeature to add download attributes to
        """

        # grab the ID of the item to be downloaded
        itemID = self.decodeValue(feature.getAttribute(kConnector_Files))
        # grab the OS destination folder
        targetFolder = self.decodeValue(
            feature.getAttribute(kConnector_TargetFolder))
        # grab whether to treat item as file or folder or attribute contents
        targetType = self.decodeValue(
            feature.getAttribute(kConnector_DataTarget))

        itemName = itemID
        try:
            # try to get the item name, if object ID and Download As parameter don't line up there is the potential to get
            # an exception here. If this is a case the outer try will catch and log
            try:
                itemName = self._webServiceConnector.getItemName(
                    itemID, targetType == "Folder")
            except WSCInvalidArgsException:
                raise FunctionRejection(
                    kConnector_nameRetrieval_932334,
                    [self._transformerName, itemID, self._serviceName])

            # Build the download url based off of the specified file id
            if targetType == "File":
                self._downloadFile(itemID, targetFolder, itemName)

                downloadPathAttribute = feature.getAttribute(kConnector_DownloadPathAttribute)
                if downloadPathAttribute is not None:
                    feature.setAttribute(
                        downloadPathAttribute, os.path.join(targetFolder, itemName)
                    )

            elif targetType == "Attribute":
                # Python requests library tries to guess at what the encoding is via the contents and always seems to send
                #  back a very non-standard encoding type. So we will default to the contents of the file, and let the user
                #  encode the attribute with the 'AttributeEncoder' transformer

                rawContents = self._webServiceConnector.getFile(itemID)
                # TODO: This handling of file content makes assumptions that may not be true and is therefore limiting.
                # Attempt to convert the contents to a unicode, if this suceeds we'll set a unicode string on the feature
                # so attribute contents is human readable
                # if this fails assume we have binary and convert to the appropriate binary type
                if six.PY2:
                    try:
                        decodedContents = unicode(
                            rawContents, encoding='utf-8')
                    except UnicodeError:
                        decodedContents = bytearray(rawContents)
                else:
                    try:
                        decodedContents = rawContents.decode('utf-8')
                    except UnicodeDecodeError:
                        decodedContents = rawContents

                targetAttribute = self.decodeValue(
                    feature.getAttribute(kConnector_TargetAttribute))

                self._logger.logMessage(
                    kConnector_FileDownloadToAttribute_932302,
                    [self._serviceName, itemName, targetAttribute], FME_INFORM)

                # Set the file as an attribute
                feature.setAttribute(targetAttribute, decodedContents)

            elif targetType == "Folder":
                # Check whether the user wants to download sub folders or not
                self._includeSubFolders = (str(
                    feature.getAttribute(kConnector_IncludeSubFolders))
                                           .lower() == "yes")
                # figure out the OS path based on the filename
                folderPath = os.path.join(targetFolder, itemName)
                self._downloadFolder(feature, itemID, folderPath)

                downloadPathAttribute = feature.getAttribute(kConnector_DownloadPathAttribute)
                if downloadPathAttribute is not None:
                    feature.setAttribute(downloadPathAttribute, folderPath)
                # Remove attributes that should stay internal here
            else:
                raise FunctionRejection(kConnector_InvalidInput_932308,
                                        [self._transformerName])
        except:
            # Log message about download failing that prompts the user to check that they have selected the correctDo
            # type of object for the download method.
            if targetType == "Folder":
                self._logger.logMessage(kConnector_FolderDownloadFailed_932346,
                                        [self._transformerName, itemName],
                                        FME_WARN)
            else:
                self._logger.logMessage(kConnector_FileDownloadFailed_932345,
                                        [self._transformerName, itemName],
                                        FME_WARN)
            raise

        self._removeDownloadAttributes(feature)

    def _downloadFile(self, file_id, target_folder, file_name):
        """Method to download a file to a target location on the OS.

        :param str|unicode file_id: the id of the file to be downloaded
        :param str|unicode target_folder: the target path on the os
        :param str|unicode file_name: the name of the file
        """

        # Assemble the dict expected by the webserviceConnector
        args = {
            'FILE_ID': file_id,
            'TARGET_FOLDER': target_folder,
            'FILENAME': file_name
        }

        self._logger.logMessage(
            kConnector_FileDownloadToFolder_932301,
            [self._transformerName, file_name, target_folder], FME_INFORM)
        self._webServiceConnector.downloadFile(args)

    def _downloadFolder(self, feature, folder_id, path):
        """Method to download a folder from the webservice to the OS.

        :param FMEFeature feature: FMEFeature to add download folder attributes to
        :param str|unicode folder_id: the Id of the folder to be downloaded
        :param str|unicode path: the os location to download the folder to
        """
        data = {'CONTAINER_ID': folder_id, 'TARGET_FOLDER': path}
        if not self._includeSubFolders:
            data['EXCLUDE_SUB_FOLDERS'] = 'YES'
        try:
            self._webServiceConnector.downloadFolder(data)
        except WSCIOException:
            # Catch errors related to making new directories
            raise FunctionRejection(kConnector_InvalidFolderDownload_932320,
                                    [self._transformerName])

    def _removeDownloadAttributes(self, feature):
        """Method that generalizes the process of removing attributes from the
        download method.

        :param FMEFeature feature: the feature that got the attributes to do the logic in the method
        """
        feature.removeAttribute(kConnector_OperationType)
        feature.removeAttribute(kConnector_Source)
        feature.removeAttribute(kConnector_Files)
        feature.removeAttribute(kConnector_Dest)
        feature.removeAttribute(kConnector_DataTarget)
        feature.removeAttribute(kConnector_TargetFolder)
        feature.removeAttribute(kConnector_TargetAttribute)
        feature.removeAttribute(kConnector_DownloadPathAttribute)
        feature.removeAttribute(kConnector_IncludeSubFolders)

    # --------------------------------------------------------------------------------------------------------------------
    # The following private methods are used in the list operation
    # --------------------------------------------------------------------------------------------------------------------

    def _doList(self, feature):
        """Method to handle the list operation.

        :param FMEFeature feature: the FMEFeature to add list attributes to
        """
        self.getMetadataAttributes(feature)
        folderId = self.decodeValue(feature.getAttribute(kConnector_Files))

        self._logger.logMessage(kConnector_List_932318,
                                [self._transformerName], FME_INFORM)

        self._listFolder(feature, folderId)

    def _listFolder(self,
                    feature,
                    folder_id,
                    continue_args=None,
                    folder_name=""):
        """Method that lists the contents of a folder from a webservice.
        Supports recursive pagination using continuation arguments, and
        recursive subfolder traversal. Subfolder traversal toggled via option
        attribute on feature.

        :param FMEFeature feature: the FMEFeature to add list folder attributes to
        :param str|unicode folder_id: the Id of the folder to be listed
        :param dict continue_args: a dict of args to be supplied to the webserviceconnectors getContainerContents method
        :param str|unicode folder_name: name of the folder to list
        :returns: Nothing. FMEFeatures are emitted via pyoutput().
        """
        try:
            if continue_args:
                resp = self._webServiceConnector.getContainerContents(
                    continue_args)
            else:
                # assemble json object expected by the connector
                data = {'CONTAINER_ID': folder_id}
                resp = self._webServiceConnector.getContainerContents(data)
        except:
            # Log details now and repass the exception up
            self._logger.logMessage(kConnector_ListFailed_932335,
                                    [self._transformerName, folder_id],
                                    FME_WARN)
            raise

        # If name is empty we are at the base folder and we should try to get the name to start the path
        if not folder_name:
            folder_name = self._webServiceConnector.getItemName(
                folder_id, True).split(' [')[0]

        # Add the metadata to the features
        items = [x for x in resp['CONTENTS'] if not x['IS_CONTAINER']]
        self._createListFeatures(feature, items, False, folder_name)

        # PR81665: "Include Subfolders" setting introduced. If setting not present, assume 'yes'.
        includeSubfolderSetting = feature.getAttribute(
            kConnector_IncludeSubFolders)
        self._includeSubFolders = not includeSubfolderSetting or str(
            includeSubfolderSetting).lower() == 'yes'
        if self._includeSubFolders:
            containers = [x for x in resp['CONTENTS'] if x['IS_CONTAINER']]
            self._createListFeatures(feature, containers, True, folder_name)
            for container in containers:
                self._listFolder(
                    feature, container['ID'], None,
                    folder_name + '/' + container['NAME'].split(' [')[0])

        # Check for additional pages
        if 'CONTINUE' in resp and 'ARGS' in resp['CONTINUE']:
            self._listFolder(feature, folder_id, resp['CONTINUE']['ARGS'],
                             folder_name)

    def _createListFeatures(self,
                            feature,
                            items,
                            is_folder=False,
                            folder_name=""):
        """Method that adds metadata and outputs a feature for each item.

        :param list[dict] items: a list of dicts with an 'ID' key
        :param FMEFeature feature: FMEFeature
        :param bool is_folder: a boolean indicating if an item is a folder
        :param str|unicode folder_name: name of the folder
        """
        for item in items:
            # Make a copy of the input feature
            feat = FMEFeature(feature)
            try:
                getDetails = self._webServiceConnector.getItemDetails(
                    item['ID'], is_folder)
                if getDetails is None:
                    continue
            except:
                # Log details and reraise original exception
                self._logger.logMessage(kConnector_InvalidMetadataList_932309,
                                        [self._transformerName, item['ID']],
                                        FME_WARN)
                raise
            # Setting the output attributes
            try:
                self.setMetadataAttributes(feat, getDetails, is_folder)
                # Set the relative path attribute, the way this is calculated is not service specific

                # This seems questionable - why does the relative path include the listed folder?
                # 2019-02-20 <dvn>
                path = folder_name + '/' + item['NAME'].split(' [')[0]

                feat.setAttribute(self._relativePathAttribute, path)
            except KeyError:
                raise WSCUnexpectedResponseException(
                    [self._transformerName, self._serviceName, path],
                    str(getDetails))

            # Remove attributes that should stay internal here
            self._removeListAttributes(feat)
            self.pyoutput(feat)

    def _setModifiedDate(self, feature, date_string):
        """This method converts an iso date to fme form and set the correct
        attribute on the feature.

        :param FMEFeature feature: FMEFeature
        :param str date_string: A string with the date in ISO format
        """

        # try to convert date to fme form
        fmedate = isoTimestampToFMEFormat(date_string)
        if fmedate is not None:
            feature.setAttribute(self._lastModifiedAttribute, fmedate)
        else:
            # Date parsing failed warn and backoff to original value
            self._logger.logMessage(kConnector_DateConversionError_932340,
                                    [self._transformerName, date_string],
                                    FME_WARN)
            feature.setAttribute(self._lastModifiedAttribute, date_string)

    def _removeListAttributes(self, feature):
        """Method that generalizes the process of removing attributes from the
        list method.

        :param FMEFeature feature: the feature that got the attributes to do the logic in the method
        """
        feature.removeAttribute(kConnector_OperationType)
        feature.removeAttribute(kConnector_Output_List)
        feature.removeAttribute(kConnector_Path)
        feature.removeAttribute(kConnector_Files)
        feature.removeAttribute(kConnector_NameAttribute)
        feature.removeAttribute(kConnector_PathAttribute)
        feature.removeAttribute(kConnector_TypeAttribute)
        feature.removeAttribute(kConnector_Modified_Attribute)
        feature.removeAttribute(kConnector_SizeAttribute)
        feature.removeAttribute(kConnector_IdAttribute)
        feature.removeAttribute(kConnector_UrlAttribute)
        feature.removeAttribute(kConnector_DirectUrlAttribute)
        feature.removeAttribute(kConnector_SourceType)
        feature.removeAttribute(kConnector_RelativePathAttribute)
        feature.removeAttribute(kConnector_IncludeSubFolders)
        feature.removeAttribute(self._namedConnKey)

    # --------------------------------------------------------------------------------------------------------------------
    # The following private methods are used in the delete operation
    # --------------------------------------------------------------------------------------------------------------------

    def _doDelete(self, feature):
        """Method that implements the delete operation.

        :param FMEFeature feature: FMEFeature
        """
        fileToDelete = self.decodeValue(
            feature.getAttribute(kConnector_FileId))

        self._logger.logMessage(
            kConnector_Delete_932317,
            [self._transformerName, self._serviceName, fileToDelete],
            FME_INFORM)
        # Based off the file ID, make a request to delete the specified file
        try:
            self._webServiceConnector.delete(self.deleteUrl(fileToDelete))
        except WSCHttpDNEException:
            # Log that no deleting occurred but still pass
            self._logger.logMessage(
                kConnector_NothingToDelete_932347,
                [self._transformerName, fileToDelete, self._serviceName],
                FME_INFORM)
        except Exception:
            # For any other exceptions we'll assume the item existed and something else went wrong so reraise to deal with
            # upstream
            self._logger.logMessage(
                kConnector_InvalidFileDelete_932310,
                [self._transformerName, fileToDelete, self._serviceName],
                FME_WARN)
            raise

        # Remove attributes that should stay internal here
        self._removeDeleteAttributes(feature)

    def _removeDeleteAttributes(self, feature):
        """Method that generalizes the process of removing attributes from the
        delete method.

        :param FMEFeature feature: the feature that got the attributes to do the logic in the method
        """
        feature.removeAttribute(kConnector_OperationType)
        feature.removeAttribute(kConnector_Path)
        feature.removeAttribute(kConnector_FileId)

    # --------------------------------------------------------------------------------------------------------------------
    # The following private methods are used in the create operation
    # --------------------------------------------------------------------------------------------------------------------

    def _doCreate(self, feature):
        """Method that implements the create operation. It will get the
        attributes required to create the new item, and remove them once the
        item has been created.

        :param FMEFeature feature: The feature to get the attributes from
        """
        self.getCreateMetadataAttributes(feature)
        self._createItem(feature)
        # Remove attributes that should stay internal here
        self._removeCreateAttributes(feature)

    def _removeCreateAttributes(self, feature):
        """Method that generalizes the process of removing attributes from the
        create method.

        :param FMEFeature feature: the feature that got the attributes to do the logic in the method
        """

        feature.removeAttribute(kConnector_OperationType)
        feature.removeAttribute(kConnector_UploadDestinationPath)
        feature.removeAttribute(kConnector_ImageUrlAttribute)
        feature.removeAttribute(kConnector_OutLinkAttribute)
        feature.removeAttribute(kConnector_DescriptionAttribute)
        feature.removeAttribute(kConnector_SourceType)
        feature.removeAttribute(kConnector_IdAttribute)
        feature.removeAttribute(kConnector_UrlAttribute)
        feature.removeAttribute(kConnector_DirectUrlAttribute)
        feature.removeAttribute(kConnector_CreateAttributeName)
        feature.removeAttribute(kConnector_OutputAttribute)
        feature.removeAttribute(kConnector_Source)
        feature.removeAttribute(kConnector_Files)

    # --------------------------------------------------------------------------------------------------------------------
    # The following private methods are used in the edit operation
    # --------------------------------------------------------------------------------------------------------------------

    def _doEdit(self, feature):
        """Method that implements the edit operation. It will get the
        attributes required to edit the existing item, and remove them once the
        item has been edited.

        :param FMEFeature feature: The feature to get the attributes from
        """
        self.getEditMetadataAttributes(feature)
        self._editItem(feature)
        # Remove attributes that should stay internal here
        self._removeEditAttributes(feature)

    def _removeEditAttributes(self, feature):
        """Method that generalizes the process of removing attributes from the
        edit method.

        :param FMEFeature feature: the feature that got the attributes to do the logic in the method
        """

        feature.removeAttribute(kConnector_OperationType)
        feature.removeAttribute(kConnector_UploadDestinationPath)
        feature.removeAttribute(kConnector_ImageUrlAttribute)
        feature.removeAttribute(kConnector_OutLinkAttribute)
        feature.removeAttribute(kConnector_DescriptionAttribute)
        feature.removeAttribute(kConnector_SourceType)
        feature.removeAttribute(kConnector_IdAttribute)
        feature.removeAttribute(kConnector_UrlAttribute)
        feature.removeAttribute(kConnector_DirectUrlAttribute)
        feature.removeAttribute(kConnector_CreateAttributeName)
        feature.removeAttribute(kConnector_OutputAttribute)
        feature.removeAttribute(kConnector_Source)
        feature.removeAttribute(kConnector_NewDescriptionAttribute)
        feature.removeAttribute(kConnector_NewLinkAttribute)
        feature.removeAttribute(kConnector_NewNameAttribute)
        feature.removeAttribute(kConnector_SourceFolder)
        feature.removeAttribute(kConnector_Files)

    # --------------------------------------------------------------------------------------------------------------------
    # The following is a private utility method for error handling
    # --------------------------------------------------------------------------------------------------------------------
    def _handleException(self, e, feature):
        """Method that handles all WSCException's.

        :param Exception e: the exception to be handled
        :param FMEFeature feature: the feature that got the attributes to do the logic in the method
        """
        # We have nested try blocks inner level catches errors and sets message params and adds additional log
        # messages if necessary.  Outer level logs and sends a feature out the rejected port

        # Since all exceptions will reach this method, remove attributes that should stay internal here
        feature.removeAttribute(kConnector_OperationType)
        feature.removeAttribute(self._namedConnKey)
        self._removeListAttributes(feature)
        self._removeDeleteAttributes(feature)
        self._removeUploadAttributes(feature)
        self._removeDownloadAttributes(feature)
        self._removeCreateAttributes(feature)
        self._removeEditAttributes(feature)

        namedConn = '<No Connection>'
        try:
            try:
                raise e
            # For connection errors we'll fail the translation as something is immediately wrong
            except (WSCConnectionManagerException,
                    WSCConnectionRetrievalException) as cme:
                raise FMEException(cme.message_number, [self._transformerName])
            except WSCAuthRetrievalException as are:
                raise FMEException(are.message_number,
                                   [self._transformerName, namedConn])
            except WSCIOException as ie:
                if len(ie.message_parameters) > 1:
                    path = ie.message_parameters[1]
                else:
                    path = ''
                raise FunctionRejection(ie.message_number,
                                        [self._transformerName, path])
            except (WSCHttpRequestException, WSCHttpDNEException,
                    WSCHttpAuthException, WSCHttpServerException,
                    WSCHttpPermissionsException, WSCHttpConflictException,
                    WSCUnexpectedResponseException) as re:
                self._logger.logMessage(
                    re.message_number,
                    [self._transformerName, self._serviceName], FME_WARN)
                if len(re.message_parameters) > 2:
                    response_url = re.message_parameters[2]
                else:
                    response_url = ''
                response_content = re.details
                raise FunctionRejection(
                    kConnector_Response_932332,
                    [self._transformerName, response_url, response_content])
            except (WSCTimeoutException, WSCRequestsException,
                    WSCInvalidArgsException) as ge:
                ge.message_parameters[0] = self._transformerName
                raise FunctionRejection(ge.message_number,
                                        ge.message_parameters)
            except HdfsUtilException as hue:
                raise FunctionRejection(hue.message_number,
                                        hue.message_parameters)

        except FunctionRejection as f:
            # Exception thrown by a util function message and args stored
            messageNum = f.messageNum
            messageArgs = f.messageArgs
            # If the exception was not caught above just output a generic exception
            self._logger.logMessage(messageNum, messageArgs, FME_WARN)
            # If there was a failure we will want to always output the feature to the rejected port
            feature.setAttribute(kFMERejectionCode, kFMEGeneralError)
            formattedMessage = self._logger.getMessage(messageNum, messageArgs)
            feature.setAttribute(kFMERejectionMsg, formattedMessage)
            self.pyoutput(feature)

        except WSCException as wse:
            # General Exception thrown when a webservice has a custom error message.
            message = wse.message
            details = wse.details
            rejection_message = None

            # Setting the free-form message field overrides the usage of the log message.
            if message:
                self._logger.logMessageString(message, FME_WARN)
                rejection_message = message
            else:
                self._logger.logMessage(wse.message_number, wse.message_parameters, FME_WARN)
                rejection_message = self._logger.getMessage(wse.message_number, wse.message_parameters)

            if details:
                self._logger.logMessageString(details, FME_WARN)

            feature.setAttribute(kFMERejectionMsg, rejection_message)
            feature.setAttribute(kFMERejectionCode, kFMEGeneralError)
            self.pyoutput(feature)
