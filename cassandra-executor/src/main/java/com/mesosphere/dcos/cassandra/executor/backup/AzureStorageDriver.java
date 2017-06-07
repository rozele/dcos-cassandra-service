package com.mesosphere.dcos.cassandra.executor.backup;

import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.mesosphere.dcos.cassandra.executor.backup.azure.PageBlobInputStream;
import com.mesosphere.dcos.cassandra.executor.backup.azure.PageBlobOutputStream;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudPageBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.security.InvalidKeyException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.mesosphere.dcos.cassandra.executor.backup.azure.PageBlobOutputStream.ORIGINAL_SIZE_KEY;

/**
 * Implements a BackupStorageDriver that provides upload and download
 * functionality to an Azure Storage using Page Blobs.
 * Page Blobs allow for 1TB file sizes.
 * Page Blobs require a Storage Account (but NOT a blob storage account)
 */
public class AzureStorageDriver implements BackupStorageDriver {

private static final Logger LOGGER = LoggerFactory.getLogger(
            AzureStorageDriver.class);
  private static final int DEFAULT_PART_SIZE_UPLOAD = 4 * 1024 * 1024; // Chunk size set to 4MB
  private static final int DEFAULT_PART_SIZE_DOWNLOAD = 4 * 1024 * 1024; // Chunk size set to 4MB

  @Override
  public void upload(BackupRestoreContext ctx) throws IOException {

    final String accountName = ctx.getAccountId();
    final String accountKey = ctx.getSecretKey();
    final String localLocation = ctx.getLocalLocation();
    final String backupName = ctx.getName();
    final String nodeId = ctx.getNodeId();

    final String key = String.format("%s/%s", backupName, nodeId);
    final String containerName = StringUtils.lowerCase(getContainerName(ctx.getExternalLocation()));
    // https://<account_name>.blob.core.windows.net/<container_name>
    final CloudBlobContainer container = getCloudBlobContainer(accountName, accountKey, containerName);

    final File dataDirectory = new File(localLocation);
    if (container == null || !dataDirectory.isDirectory()) {
      LOGGER.error("Error uploading snapshots.  Unable to connect to {}, for container {} or Directory {} doesn't exist.",
        ctx.getExternalLocation(), containerName, localLocation);
      return;
    }

    // Ex: data/<keyspace>/<cf>/snapshots/</snapshot-dir>/<files>
    for (File keyspaceDir : dataDirectory.listFiles()) {
      if (keyspaceDir.isFile()) {
        // Skip any files in the data directory.
        // Only enter keyspace directory.
        continue;
      }
      LOGGER.info("Entering keyspace: {}", keyspaceDir.getName());
      for (File cfDir : keyspaceDir.listFiles()) {
        LOGGER.info("Entering column family: {}", cfDir.getName());
        File snapshotDir = new File(cfDir, "snapshots");
        File backupDir = new File(snapshotDir, backupName);
        if (!StorageUtil.isValidBackupDir(keyspaceDir, cfDir, snapshotDir, backupDir)) {
          LOGGER.info("Skipping directory: {}", snapshotDir.getAbsolutePath());
          continue;
        }
        LOGGER.info(
          "Valid backup directories. KeyspaceDir: {} | ColumnFamilyDir: {} | SnapshotDir: {} | BackupName: {}",
          keyspaceDir.getAbsolutePath(), cfDir.getAbsolutePath(),
          snapshotDir.getAbsolutePath(), backupName);

        final Optional<File> snapshotDirectory = StorageUtil.getValidSnapshotDirectory(snapshotDir, backupName);
        LOGGER.info("Valid snapshot directory: {}", snapshotDirectory.isPresent());

        if (snapshotDirectory.isPresent()) {
          LOGGER.info("Going to upload directory: {}", snapshotDirectory.get().getAbsolutePath());

          uploadDirectory(snapshotDirectory.get().getAbsolutePath(), container, containerName, key,
            keyspaceDir.getName(), cfDir.getName());

        } else {
          LOGGER.warn(
            "Snapshots directory: {} doesn't contain the current backup directory: {}",
            snapshotDir.getName(), backupName);
        }
      }
    }

    LOGGER.info("Done uploading snapshots for backup: {}", backupName);
  }

  private void uploadDirectory(String localLocation,
    CloudBlobContainer azureContainer,
    String containerName,
    String key,
    String keyspaceName,
    String cfName) throws IOException {

    LOGGER.info(
      "uploadDirectory() localLocation: {}, containerName: {}, key: {}, keyspaceName: {}, cfName: {}",
      localLocation, containerName, key, keyspaceName, cfName);

    Files.walk(FileSystems.getDefault().getPath(localLocation)).forEach(filePath -> {
        File file = filePath.toFile();
        if (file.isFile()) {
          String fileKey = key + "/" + keyspaceName + "/" + cfName + "/" + file.getName();
          uploadFile(azureContainer, fileKey, file);
        }
      }
    );
  }

  private void uploadFile(CloudBlobContainer container, String fileKey, File sourceFile) {

    try (BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(sourceFile))) {

      LOGGER.info("Initiating upload for file: {} | key: {}",
        sourceFile.getAbsolutePath(), fileKey);
      uploadStream(container, fileKey, inputStream);
    } catch (IOException e) {
      LOGGER.error("Unable to store blob", e);
    }
  }
  
  private void uploadStream(CloudBlobContainer container, String fileKey, InputStream inputStream) {

    PageBlobOutputStream pageBlobOutputStream = null;
    SnappyOutputStream compress = null;
    BufferedOutputStream bufferedOutputStream = null;
    try {

      final CloudPageBlob blob = container.getPageBlobReference(fileKey);
      pageBlobOutputStream = new PageBlobOutputStream(blob);
      bufferedOutputStream = new BufferedOutputStream(pageBlobOutputStream);

      compress = new SnappyOutputStream(bufferedOutputStream, DEFAULT_PART_SIZE_UPLOAD);
      IOUtils.copy(inputStream, compress, DEFAULT_PART_SIZE_UPLOAD);
    } catch (StorageException | URISyntaxException | IOException e) {
      LOGGER.error("Unable to store blob", e);
    } finally {
      IOUtils.closeQuietly(compress);  // super important that the compress close is called first in order to flush
      IOUtils.closeQuietly(bufferedOutputStream);
      IOUtils.closeQuietly(pageBlobOutputStream);
    }
  }

  @Override
  public void uploadSchema(BackupRestoreContext ctx, String schema) {
    final String accountName = ctx.getAccountId();
    final String accountKey = ctx.getSecretKey();
    final String backupName = ctx.getName();
    final String nodeId = ctx.getNodeId();

    final String containerName = StringUtils.lowerCase(getContainerName(ctx.getExternalLocation()));
    // https://<account_name>.blob.core.windows.net/<container_name>
    final CloudBlobContainer container = getCloudBlobContainer(accountName, accountKey, containerName);

    if (container == null) {
      LOGGER.error("Error uploading schema.  Unable to connect to {}, for container {}.",
        ctx.getExternalLocation(), containerName);
      return;
    }

    final String key = backupName + "/" + nodeId + "/" + StorageUtil.SCHEMA_FILE;
    uploadText(container, key, schema);
  }

  private void uploadText(CloudBlobContainer container, String fileKey, String text) {
    final InputStream inputStream = new ByteArrayInputStream(schema.getBytes(StandardCharsets.UTF_8));
    LOGGER.info("Initiating upload for schema | key: {}", fileKey);
    uploadStream(container, fileKey, inputStream);
  }

  @Override
  public void download(BackupRestoreContext ctx) throws IOException {

    final String accountName = ctx.getAccountId();
    final String accountKey = ctx.getSecretKey();
    final String localLocation = ctx.getLocalLocation();
    final String backupName = ctx.getName();
    final String nodeId = ctx.getNodeId();

    final String containerName = StringUtils.lowerCase(getContainerName(ctx.getExternalLocation()));
    // https://<account_name>.blob.core.windows.net/<container_name>
    final CloudBlobContainer container = getCloudBlobContainer(accountName, accountKey, containerName);

    if (container == null) {
      LOGGER.error("Error downloading snapshots.  Unable to connect to {}, for container {}.",
        ctx.getExternalLocation(), containerName);
      return;
    }
    String keyPrefix = String.format("%s/%s", backupName, nodeId);

    final Map<String, Long> snapshotFileKeys = getSnapshotFileKeys(container, keyPrefix);
    LOGGER.info("Snapshot files for this node: {}", snapshotFileKeys);

    for (String fileKey : snapshotFileKeys.keySet()) {
      downloadFile(localLocation, container, fileKey, snapshotFileKeys.get(fileKey));
    }
  }

  private void downloadFile(String localLocation, CloudBlobContainer container, String fileKey, long originalSize) {

    LOGGER.info("Downloading |  Local location {} | fileKey: {} | Size: {}", localLocation, fileKey, originalSize);

    final String fileLocation = localLocation + File.separator + fileKey;
    File file = new File(fileLocation);
    // Only create parent directory once, if it doesn't exist.
    if (!createParentDir(file)) {
      LOGGER.error("Unable to create parent directories!");
      return;
    }

    InputStream inputStream = null;
    SnappyInputStream compress = null;

    try (
      FileOutputStream fileOutputStream = new FileOutputStream(file, true);
      BufferedOutputStream bos = new BufferedOutputStream(fileOutputStream)) {

      final CloudPageBlob pageBlobReference = container.getPageBlobReference(fileKey);
      inputStream = new PageBlobInputStream(pageBlobReference);
      compress = new SnappyInputStream(inputStream);

      IOUtils.copy(compress, bos, DEFAULT_PART_SIZE_DOWNLOAD);

    } catch (Exception e) {
      LOGGER.error("Unable to write file: {}", fileKey, e);
    } finally {
      IOUtils.closeQuietly(compress);
      IOUtils.closeQuietly(inputStream);
    }
  }

  @Override
  public String downloadSchema(BackupRestoreContext ctx) throws Exception {
    final String accountName = ctx.getAccountId();
    final String accountKey = ctx.getSecretKey();
    final String backupName = ctx.getName();
    final String nodeId = ctx.getNodeId();

    final String containerName = StringUtils.lowerCase(getContainerName(ctx.getExternalLocation()));
    // https://<account_name>.blob.core.windows.net/<container_name>
    final CloudBlobContainer container = getCloudBlobContainer(accountName, accountKey, containerName);

    if (container == null) {
      LOGGER.error("Error downloading snapshots.  Unable to connect to {}, for container {}.",
        ctx.getExternalLocation(), containerName);
      return;
    }

    final String key = backupName + "/" + nodeId + "/" + StorageUtil.SCHEMA_FILE;

    InputStream inputStream = null;
    SnappyInputStream compress = null;
    
    try {
      final CloudPageBlob pageBlobReference = container.getPageBlobReference(key);
      inputStream = new PageBlobInputStream(pageBlobReference);
      compress = new SnappyInputStream(inputStream);

      return IOUtils.toString(compress, "UTF-8");

    } catch (Exception e) {
      LOGGER.error("Unable to read schema from: {}", key, e);
      return new String("");
    } finally {
      IOUtils.closeQuietly(compress);
      IOUtils.closeQuietly(inputStream);
    }
  }

  private String getContainerName(String externalLocation) {
    return externalLocation.substring("azure://".length()).replace("/", "");
  }

  private CloudBlobContainer getCloudBlobContainer(String accountName, String accountKey, String containerName) {
    CloudBlobContainer container = null;

    if (StringUtils.isNotBlank(containerName)) {
      final String storageConnectionString = "DefaultEndpointsProtocol=https"
        + ";AccountName=" + accountName
        + ";AccountKey=" + accountKey;

      try {
        final CloudStorageAccount account = CloudStorageAccount.parse(storageConnectionString);
        CloudBlobClient serviceClient = account.createCloudBlobClient();

        container = serviceClient.getContainerReference(containerName);
        container.createIfNotExists();
      } catch (StorageException | URISyntaxException | InvalidKeyException e) {
        LOGGER.error("Error connecting to container for account {} and container name {}", accountName, containerName, e);
      }
    }

    return container;
  }

  private boolean createParentDir(File file) {
    final File parentDir = new File(file.getParent());
    if (!parentDir.isDirectory()) {
      final boolean parentDirCreated = parentDir.mkdirs();
      if (!parentDirCreated) {
        LOGGER.error("Error creating parent directory for file: {}. Skipping to next");
        return false;
      }
    }
    return true;
  }

  private Map<String, Long> getSnapshotFileKeys(CloudBlobContainer container, String keyPrefix) {
    Map<String, Long> snapshotFiles = new HashMap<>();

    try {
      for (ListBlobItem item : container.listBlobs(keyPrefix, true)) {
        if (item instanceof CloudPageBlob) {
          CloudPageBlob cloudBlob = (CloudPageBlob) item;
          snapshotFiles.put(cloudBlob.getName(), getOriginalFileSize(cloudBlob));
        }
      }
    } catch (StorageException e) {
      LOGGER.error("Unable to retrieve metadata.", e);
      // all or none
      snapshotFiles = new HashMap<>();
    }
    return snapshotFiles;
  }

  private long getOriginalFileSize(CloudPageBlob pageBlobReference) throws StorageException {
    long size = 0;

    pageBlobReference.downloadAttributes();
    HashMap<String, String> map = pageBlobReference.getMetadata();
    if (map != null && map.size() > 0) {
      try {
        size = Long.parseLong(map.get(ORIGINAL_SIZE_KEY));
      } catch (Exception e) {
        LOGGER.error("File size metadata missing or is not a number.");
      }
    }

    return size;
  }
}
