package com.mesosphere.dcos.cassandra.executor.backup;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupSchemaTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupUploadTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.DownloadSnapshotTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreSchemaTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Selects the storage driver for uploading and downloading.  The external location should start
 * with "s3://xyz" or "azure://xyz".  The default is S3.
 */
public class StorageDriverFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(StorageDriverFactory.class);

  public static BackupStorageDriver createStorageDriver(CassandraTask cassandraTask) {
    BackupRestoreContext context = null;
    switch (cassandraTask.getType()) {
      case BACKUP_SNAPSHOT:
        context = ((BackupUploadTask)cassandraTask).getBackupRestoreContext();
        break;
      case BACKUP_SCHEMA:
        context = ((BackupSchemaTask)cassandraTask).getBackupRestoreContext();
        break;
    case SCHEMA_RESTORE:
        context = ((RestoreSchemaTask)cassandraTask).getBackupRestoreContext();
        break;
    case SNAPSHOT_DOWNLOAD:
        context = ((DownloadSnapshotTask)cassandraTask).getBackupRestoreContext();
        break;
    }

    LOGGER.info(context.toString());
    return getBackupStorageDriver(context.getExternalLocation());
  }

  private static BackupStorageDriver getBackupStorageDriver(String externalLocation) {
    if (StorageUtil.isAzure(externalLocation)) {
      LOGGER.info("Using the Azure Driver.");
      return new AzureStorageDriver();
    } else {
      LOGGER.info("Using the S3 Driver.");
      return new S3StorageDriver();
    }
  }
}
