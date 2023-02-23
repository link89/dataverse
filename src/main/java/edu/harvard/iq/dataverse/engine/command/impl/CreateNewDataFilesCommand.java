package edu.harvard.iq.dataverse.engine.command.impl;

import edu.harvard.iq.dataverse.DataFile;
import edu.harvard.iq.dataverse.DatasetVersion;
import edu.harvard.iq.dataverse.authorization.Permission;
import edu.harvard.iq.dataverse.datasetutility.FileExceedsMaxSizeException;
import edu.harvard.iq.dataverse.datasetutility.FileSizeChecker;
import static edu.harvard.iq.dataverse.datasetutility.FileSizeChecker.bytesToHumanReadable;
import edu.harvard.iq.dataverse.engine.command.AbstractCommand;
import edu.harvard.iq.dataverse.engine.command.CommandContext;
import edu.harvard.iq.dataverse.engine.command.DataverseRequest;
import edu.harvard.iq.dataverse.engine.command.RequiredPermissions;
import edu.harvard.iq.dataverse.engine.command.exception.CommandException;
import edu.harvard.iq.dataverse.engine.command.exception.CommandExecutionException;
import edu.harvard.iq.dataverse.ingest.IngestServiceShapefileHelper;
import edu.harvard.iq.dataverse.DataFileServiceBean.UserStorageQuota;
import edu.harvard.iq.dataverse.util.file.FileExceedsStorageQuotaException;
import edu.harvard.iq.dataverse.util.BundleUtil;
import edu.harvard.iq.dataverse.util.FileUtil;
import static edu.harvard.iq.dataverse.util.FileUtil.MIME_TYPE_UNDETERMINED_DEFAULT;
import static edu.harvard.iq.dataverse.util.FileUtil.createIngestFailureReport;
import static edu.harvard.iq.dataverse.util.FileUtil.determineFileType;
import static edu.harvard.iq.dataverse.util.FileUtil.determineFileTypeByNameAndExtension;
import static edu.harvard.iq.dataverse.util.FileUtil.getFilesTempDirectory;
import static edu.harvard.iq.dataverse.util.FileUtil.saveInputStreamInTempFile;
import static edu.harvard.iq.dataverse.util.FileUtil.useRecognizedType;
import edu.harvard.iq.dataverse.util.ShapefileHandler;
import edu.harvard.iq.dataverse.util.StringUtil;
import edu.harvard.iq.dataverse.util.file.BagItFileHandler;
import edu.harvard.iq.dataverse.util.file.BagItFileHandlerFactory;
import edu.harvard.iq.dataverse.util.file.CreateDataFileResult;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import javax.enterprise.inject.spi.CDI;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author landreev
 */
@RequiredPermissions( Permission.EditDataset )
public class CreateNewDataFilesCommand extends AbstractCommand<CreateDataFileResult> {
    private static final Logger logger = Logger.getLogger(CreateNewDataFilesCommand.class.getCanonicalName());
    
    private final DatasetVersion version;
    private final InputStream inputStream;
    private final String fileName;
    private final String suppliedContentType; 
    private final String newStorageIdentifier; 
    private final String newCheckSum; 
    private DataFile.ChecksumType newCheckSumType;

    public CreateNewDataFilesCommand(DataverseRequest aRequest, DatasetVersion version, InputStream inputStream, String fileName, String suppliedContentType, String newStorageIdentifier, String newCheckSum) {
        this(aRequest, version, inputStream, fileName, suppliedContentType, newStorageIdentifier, newCheckSum, null);
    }
    
    public CreateNewDataFilesCommand(DataverseRequest aRequest, DatasetVersion version, InputStream inputStream, String fileName, String suppliedContentType, String newStorageIdentifier, String newCheckSum, DataFile.ChecksumType newCheckSumType) {
        super(aRequest, version.getDataset());
        
        this.version = version;
        this.inputStream = inputStream;
        this.fileName = fileName;
        this.suppliedContentType = suppliedContentType; 
        this.newStorageIdentifier = newStorageIdentifier; 
        this.newCheckSum = newCheckSum; 
        this.newCheckSumType = newCheckSumType;
    }

    @Override
    public CreateDataFileResult execute(CommandContext ctxt) throws CommandException {
        List<DataFile> datafiles = new ArrayList<>();

        //When there is no checksum/checksumtype being sent (normal upload, needs to be calculated), set the type to the current default
        if(newCheckSumType == null) {
            newCheckSumType = ctxt.systemConfig().getFileFixityChecksumAlgorithm();
        }

        String warningMessage = null;

        // save the file, in the temporary location for now: 
        Path tempFile = null;

        Long fileSizeLimit = ctxt.systemConfig().getMaxFileUploadSizeForStore(version.getDataset().getEffectiveStorageDriverId());
        Long storageQuotaLimit = null; 
        
        if (ctxt.systemConfig().isStorageQuotasEnforced()) {
            //storageQuotaLimit = ctxt.files().getClass()...;
            UserStorageQuota quota = ctxt.files().getUserStorageQuota(super.getRequest().getAuthenticatedUser(), this.version.getDataset());
            if (quota != null) {
                storageQuotaLimit = quota.getRemainingQuotaInBytes();
            }
        }
        String finalType = null;
        
        if (newStorageIdentifier == null) {
            if (getFilesTempDirectory() != null) {
                try {
                    tempFile = Files.createTempFile(Paths.get(getFilesTempDirectory()), "tmp", "upload");
                    // "temporary" location is the key here; this is why we are not using
                    // the DataStore framework for this - the assumption is that
                    // temp files will always be stored on the local filesystem.
                    // -- L.A. Jul. 2014
                    logger.fine("Will attempt to save the file as: " + tempFile.toString());
                    Files.copy(inputStream, tempFile, StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException ioex) {
                    throw new CommandExecutionException("Failed to save the upload as a temp file (temp disk space?)", ioex, this);
                }

                // A file size check, before we do anything else:
                // (note that "no size limit set" = "unlimited")
                // (also note, that if this is a zip file, we'll be checking
                // the size limit for each of the individual unpacked files)
                Long fileSize = tempFile.toFile().length();
                if (fileSizeLimit != null && fileSize > fileSizeLimit) {
                    try {
                        tempFile.toFile().delete();
                    } catch (Exception ex) {
                        // ignore - but log a warning
                        logger.warning("Could not remove temp file " + tempFile.getFileName());
                    }
                    throw new CommandExecutionException(MessageFormat.format(BundleUtil.getStringFromBundle("file.addreplace.error.file_exceeds_limit"), bytesToHumanReadable(fileSize), bytesToHumanReadable(fileSizeLimit)), this);
                }

            } else {
                throw new CommandExecutionException("Temp directory is not configured.", this);
            }
            
            logger.fine("mime type supplied: " + suppliedContentType);
            
            // Let's try our own utilities (Jhove, etc.) to determine the file type
            // of the uploaded file. (We may already have a mime type supplied for this
            // file - maybe the type that the browser recognized on upload; or, if
            // it's a harvest, maybe the remote server has already given us the type
            // for this file... with our own type utility we may or may not do better
            // than the type supplied:
            // -- L.A.
            String recognizedType = null;

            try {
                recognizedType = determineFileType(tempFile.toFile(), fileName);
                logger.fine("File utility recognized the file as " + recognizedType);
                if (recognizedType != null && !recognizedType.equals("")) {
                    if (useRecognizedType(suppliedContentType, recognizedType)) {
                        finalType = recognizedType;
                    }
                }

            } catch (Exception ex) {
                logger.warning("Failed to run the file utility mime type check on file " + fileName);
            }

            if (finalType == null) {
                finalType = (suppliedContentType == null || suppliedContentType.equals(""))
                        ? MIME_TYPE_UNDETERMINED_DEFAULT
                        : suppliedContentType;
            }

            // A few special cases:
            // if this is a gzipped FITS file, we'll uncompress it, and ingest it as
            // a regular FITS file:
            if (finalType.equals("application/fits-gzipped")) {

                InputStream uncompressedIn = null;
                String finalFileName = fileName;
                // if the file name had the ".gz" extension, remove it,
                // since we are going to uncompress it:
                if (fileName != null && fileName.matches(".*\\.gz$")) {
                    finalFileName = fileName.replaceAll("\\.gz$", "");
                }

                DataFile datafile = null;
                try {
                    uncompressedIn = new GZIPInputStream(new FileInputStream(tempFile.toFile()));
                    File unZippedTempFile = saveInputStreamInTempFile(uncompressedIn, fileSizeLimit, storageQuotaLimit);
                    datafile = FileUtil.createSingleDataFile(version, unZippedTempFile, finalFileName, MIME_TYPE_UNDETERMINED_DEFAULT, ctxt.systemConfig().getFileFixityChecksumAlgorithm());
                } catch (IOException | FileExceedsMaxSizeException | FileExceedsStorageQuotaException ioex) {
                    // it looks like we simply skip the file silently, if its uncompressed size
                    // exceeds the limit. we should probably report this in detail instead.
                    datafile = null;
                } finally {
                    if (uncompressedIn != null) {
                        try {
                            uncompressedIn.close();
                        } catch (IOException e) {
                        }
                    }
                }

                // If we were able to produce an uncompressed file, we'll use it
                // to create and return a final DataFile; if not, we're not going
                // to do anything - and then a new DataFile will be created further
                // down, from the original, uncompressed file.
                if (datafile != null) {
                    // remove the compressed temp file:
                    try {
                        tempFile.toFile().delete();
                    } catch (SecurityException ex) {
                        // (this is very non-fatal)
                        logger.warning("Failed to delete temporary file " + tempFile.toString());
                    }

                    datafiles.add(datafile);
                    return CreateDataFileResult.success(fileName, finalType, datafiles);
                }

                // If it's a ZIP file, we are going to unpack it and create multiple
                // DataFile objects from its contents:
            } else if (finalType.equals("application/zip")) {

                ZipInputStream unZippedIn = null;
                ZipEntry zipEntry = null;

                int fileNumberLimit = ctxt.systemConfig().getZipUploadFilesLimit();

                try {
                    Charset charset = null;
                    /*
                	TODO: (?)
                	We may want to investigate somehow letting the user specify 
                	the charset for the filenames in the zip file...
                    - otherwise, ZipInputStream bails out if it encounteres a file 
                	name that's not valid in the current charest (i.e., UTF-8, in 
                    our case). It would be a bit trickier than what we're doing for 
                    SPSS tabular ingests - with the lang. encoding pulldown menu - 
                	because this encoding needs to be specified *before* we upload and
                    attempt to unzip the file. 
                	        -- L.A. 4.0 beta12
                	logger.info("default charset is "+Charset.defaultCharset().name());
                	if (Charset.isSupported("US-ASCII")) {
                    	logger.info("charset US-ASCII is supported.");
                    	charset = Charset.forName("US-ASCII");
                    	if (charset != null) {
                       	    logger.info("was able to obtain charset for US-ASCII");
                    	}
                    
                	 }
                     */

                    if (charset != null) {
                        unZippedIn = new ZipInputStream(new FileInputStream(tempFile.toFile()), charset);
                    } else {
                        unZippedIn = new ZipInputStream(new FileInputStream(tempFile.toFile()));
                    }

                    Long storageQuotaLimitForUnzippedFiles = storageQuotaLimit; 
                    while (true) {
                        try {
                            zipEntry = unZippedIn.getNextEntry();
                        } catch (IllegalArgumentException iaex) {
                            // Note:
                            // ZipInputStream documentation doesn't even mention that
                            // getNextEntry() throws an IllegalArgumentException!
                            // but that's what happens if the file name of the next
                            // entry is not valid in the current CharSet.
                            // -- L.A.
                            warningMessage = "Failed to unpack Zip file. (Unknown Character Set used in a file name?) Saving the file as is.";
                            logger.warning(warningMessage);
                            throw new IOException();
                        }

                        if (zipEntry == null) {
                            break;
                        }
                        // Note that some zip entries may be directories - we
                        // simply skip them:

                        if (!zipEntry.isDirectory()) {
                            if (datafiles.size() > fileNumberLimit) {
                                logger.warning("Zip upload - too many files.");
                                warningMessage = "The number of files in the zip archive is over the limit (" + fileNumberLimit
                                        + "); please upload a zip archive with fewer files, if you want them to be ingested "
                                        + "as individual DataFiles.";
                                throw new IOException();
                            }

                            String fileEntryName = zipEntry.getName();
                            logger.fine("ZipEntry, file: " + fileEntryName);

                            if (fileEntryName != null && !fileEntryName.equals("")) {

                                String shortName = fileEntryName.replaceFirst("^.*[\\/]", "");

                                // Check if it's a "fake" file - a zip archive entry
                                // created for a MacOS X filesystem element: (these
                                // start with "._")
                                if (!shortName.startsWith("._") && !shortName.startsWith(".DS_Store") && !"".equals(shortName)) {
                                    // OK, this seems like an OK file entry - we'll try
                                    // to read it and create a DataFile with it:

                                    File unZippedTempFile = saveInputStreamInTempFile(unZippedIn, fileSizeLimit, storageQuotaLimitForUnzippedFiles);
                                    DataFile datafile = FileUtil.createSingleDataFile(version, 
                                            unZippedTempFile, 
                                            null, 
                                            shortName,
                                            MIME_TYPE_UNDETERMINED_DEFAULT,
                                            ctxt.systemConfig().getFileFixityChecksumAlgorithm(), null, false);
                                    
                                    storageQuotaLimitForUnzippedFiles = storageQuotaLimitForUnzippedFiles - datafile.getFilesize();

                                    if (!fileEntryName.equals(shortName)) {
                                        // If the filename looks like a hierarchical folder name (i.e., contains slashes and backslashes),
                                        // we'll extract the directory name; then subject it to some "aggressive sanitizing" - strip all 
                                        // the leading, trailing and duplicate slashes; then replace all the characters that 
                                        // don't pass our validation rules.
                                        String directoryName = fileEntryName.replaceFirst("[\\\\/][\\\\/]*[^\\\\/]*$", "");
                                        directoryName = StringUtil.sanitizeFileDirectory(directoryName, true);
                                        // if (!"".equals(directoryName)) {
                                        if (!StringUtil.isEmpty(directoryName)) {
                                            logger.fine("setting the directory label to " + directoryName);
                                            datafile.getFileMetadata().setDirectoryLabel(directoryName);
                                        }
                                    }

                                    if (datafile != null) {
                                        // We have created this datafile with the mime type "unknown";
                                        // Now that we have it saved in a temporary location,
                                        // let's try and determine its real type:

                                        String tempFileName = getFilesTempDirectory() + "/" + datafile.getStorageIdentifier();

                                        try {
                                            recognizedType = determineFileType(new File(tempFileName), shortName);
                                            logger.fine("File utility recognized unzipped file as " + recognizedType);
                                            if (recognizedType != null && !recognizedType.equals("")) {
                                                datafile.setContentType(recognizedType);
                                            }
                                        } catch (Exception ex) {
                                            logger.warning("Failed to run the file utility mime type check on file " + fileName);
                                        }

                                        datafiles.add(datafile);
                                    }
                                }
                            }
                        }
                        unZippedIn.closeEntry();

                    }

                } catch (IOException ioex) {
                    // just clear the datafiles list and let
                    // ingest default to creating a single DataFile out
                    // of the unzipped file.
                    logger.warning("Unzipping failed; rolling back to saving the file as is.");
                    if (warningMessage == null) {
                        warningMessage = BundleUtil.getStringFromBundle("file.addreplace.warning.unzip.failed");
                    }

                    datafiles.clear();
                } catch (FileExceedsMaxSizeException femsx) {
                    logger.warning("One of the unzipped files exceeds the size limit; resorting to saving the file as is. " + femsx.getMessage());
                    warningMessage =  BundleUtil.getStringFromBundle("file.addreplace.warning.unzip.failed.size", Arrays.asList(FileSizeChecker.bytesToHumanReadable(fileSizeLimit)));
                    datafiles.clear();
                } catch (FileExceedsStorageQuotaException fesqx) {
                    logger.warning("One of the unzipped files exceeds the storage quota limit; resorting to saving the file as is. " + fesqx.getMessage());
                    warningMessage =  BundleUtil.getStringFromBundle("file.addreplace.warning.unzip.failed.quota", Arrays.asList(FileSizeChecker.bytesToHumanReadable(storageQuotaLimit)));
                    datafiles.clear();
                } finally {
                    if (unZippedIn != null) {
                        try {
                            unZippedIn.close();
                        } catch (Exception zEx) {
                        }
                    }
                }
                if (datafiles.size() > 0) {
                    // remove the uploaded zip file:
                    try {
                        Files.delete(tempFile);
                    } catch (IOException ioex) {
                        // do nothing - it's just a temp file.
                        logger.warning("Could not remove temp file " + tempFile.getFileName().toString());
                    }
                    // and return:
                    return CreateDataFileResult.success(fileName, finalType, datafiles);
                }

            } else if (finalType.equalsIgnoreCase(ShapefileHandler.SHAPEFILE_FILE_TYPE)) {
                // Shape files may have to be split into multiple files,
                // one zip archive per each complete set of shape files:

                // File rezipFolder = new File(this.getFilesTempDirectory());
                File rezipFolder = FileUtil.getShapefileUnzipTempDirectory();

                IngestServiceShapefileHelper shpIngestHelper;
                shpIngestHelper = new IngestServiceShapefileHelper(tempFile.toFile(), rezipFolder);

                boolean didProcessWork = shpIngestHelper.processFile();
                if (!(didProcessWork)) {
                    logger.severe("Processing of zipped shapefile failed.");
                    return CreateDataFileResult.error(fileName, finalType);
                }

                try {
                    Long storageQuotaLimitForRezippedFiles = storageQuotaLimit;
                    
                    for (File finalFile : shpIngestHelper.getFinalRezippedFiles()) {
                        FileInputStream finalFileInputStream = new FileInputStream(finalFile);
                        finalType = FileUtil.determineContentType(finalFile);
                        if (finalType == null) {
                            logger.warning("Content type is null; but should default to 'MIME_TYPE_UNDETERMINED_DEFAULT'");
                            continue;
                        }

                        File unZippedShapeTempFile = saveInputStreamInTempFile(finalFileInputStream, fileSizeLimit, storageQuotaLimitForRezippedFiles);
                        DataFile new_datafile = FileUtil.createSingleDataFile(version, unZippedShapeTempFile, finalFile.getName(), finalType, ctxt.systemConfig().getFileFixityChecksumAlgorithm());
                        
                        String directoryName = null;
                        String absolutePathName = finalFile.getParent();
                        if (absolutePathName != null) {
                            if (absolutePathName.length() > rezipFolder.toString().length()) {
                                // This file lives in a subfolder - we want to 
                                // preserve it in the FileMetadata:
                                directoryName = absolutePathName.substring(rezipFolder.toString().length() + 1);

                                if (!StringUtil.isEmpty(directoryName)) {
                                    new_datafile.getFileMetadata().setDirectoryLabel(directoryName);
                                }
                            }
                        }
                        if (new_datafile != null) {
                            datafiles.add(new_datafile);
                            // todo: can this new_datafile be null?
                            storageQuotaLimitForRezippedFiles = storageQuotaLimitForRezippedFiles - new_datafile.getFilesize();
                        } else {
                            logger.severe("Could not add part of rezipped shapefile. new_datafile was null: " + finalFile.getName());
                        }
                        try {
                            finalFileInputStream.close();
                        } catch (IOException ioex) {
                            // this one can be ignored
                        }

                    }
                } catch (FileExceedsMaxSizeException | FileExceedsStorageQuotaException femsx) {
                    logger.severe("One of the unzipped shape files exceeded the size limit, or the storage quota; giving up. " + femsx.getMessage());
                    datafiles.clear();
                    // (or should we throw an exception, instead of skipping it quietly?
                } catch (IOException ioex) {
                    throw new CommandExecutionException("Failed to process one of the components of the unpacked shape file", ioex, this);
                    // todo? - maybe try to provide a more detailed explanation, of which repackaged component, etc.?
                }

                // Delete the temp directory used for unzipping
                // The try-catch is due to error encountered in using NFS for stocking file,
                // cf. https://github.com/IQSS/dataverse/issues/5909
                try {
                    FileUtils.deleteDirectory(rezipFolder);
                } catch (IOException ioex) {
                    // do nothing - it's a temp folder.
                    logger.warning("Could not remove temp folder, error message : " + ioex.getMessage());
                }

                if (datafiles.size() > 0) {
                    // remove the uploaded zip file:
                    try {
                        Files.delete(tempFile);
                    } catch (IOException ioex) {
                        // ignore - it's just a temp file - but let's log a warning
                        logger.warning("Could not remove temp file " + tempFile.getFileName().toString());
                    } catch (SecurityException se) {
                        // same
                        logger.warning("Unable to delete: " + tempFile.toString() + "due to Security Exception: "
                                + se.getMessage());
                    }
                    return CreateDataFileResult.success(fileName, finalType, datafiles);
                } else {
                    logger.severe("No files added from directory of rezipped shapefiles");
                }
                return CreateDataFileResult.error(fileName, finalType);

            } else if (finalType.equalsIgnoreCase(BagItFileHandler.FILE_TYPE)) {
                
                try { 
                    Optional<BagItFileHandler> bagItFileHandler = CDI.current().select(BagItFileHandlerFactory.class).get().getBagItFileHandler();
                    if (bagItFileHandler.isPresent()) {
                        CreateDataFileResult result = bagItFileHandler.get().handleBagItPackage(ctxt.systemConfig(), version, fileName, tempFile.toFile());
                        return result;
                    }
                } catch (IOException ioex) {
                    throw new CommandExecutionException("Failed to process uploaded BagIt file", ioex, this);
                }
            }
        } else {
            // Default to suppliedContentType if set or the overall undetermined default if a contenttype isn't supplied
            finalType = StringUtils.isBlank(suppliedContentType) ? FileUtil.MIME_TYPE_UNDETERMINED_DEFAULT : suppliedContentType;
            String type = determineFileTypeByNameAndExtension(fileName);
            if (!StringUtils.isBlank(type)) {
                //Use rules for deciding when to trust browser supplied type
                if (useRecognizedType(finalType, type)) {
                    finalType = type;
                }
                logger.fine("Supplied type: " + suppliedContentType + ", finalType: " + finalType);
            }
        }
        // Finally, if none of the special cases above were applicable (or 
        // if we were unable to unpack an uploaded file, etc.), we'll just 
        // create and return a single DataFile:
        File newFile = null;
        if (tempFile != null) {
            newFile = tempFile.toFile();
        }
        
        // We have already checked that this file does not exceed the individual size limit; 
        // but if we are processing it as is, as a single file, we need to check if 
        // its size does not go beyond the allocated storage quota (if specified):
        
        long fileSize = newFile.length();
        
        if (storageQuotaLimit != null && fileSize > storageQuotaLimit) {
            try {
                tempFile.toFile().delete();
            } catch (Exception ex) {
                // ignore - but log a warning
                logger.warning("Could not remove temp file " + tempFile.getFileName());
            }
            throw new CommandExecutionException(MessageFormat.format(BundleUtil.getStringFromBundle("file.addreplace.error.quota_exceeded"), bytesToHumanReadable(fileSize), bytesToHumanReadable(storageQuotaLimit)), this);
        } 
        
        DataFile datafile = FileUtil.createSingleDataFile(version, newFile, newStorageIdentifier, fileName, finalType, newCheckSumType, newCheckSum);
        File f = null;
        if (tempFile != null) {
            f = tempFile.toFile();
        }
        if (datafile != null && ((f != null) || (newStorageIdentifier != null))) {

            if (warningMessage != null) {
                createIngestFailureReport(datafile, warningMessage);
                datafile.SetIngestProblem();
            }
            datafiles.add(datafile);

            return CreateDataFileResult.success(fileName, finalType, datafiles);
        }

        return CreateDataFileResult.error(fileName, finalType);
    }   // end createDataFiles
}
