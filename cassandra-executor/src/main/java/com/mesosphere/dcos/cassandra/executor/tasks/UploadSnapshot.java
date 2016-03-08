package com.mesosphere.dcos.cassandra.executor.tasks;

import com.mesosphere.dcos.cassandra.common.backup.BackupContext;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupSnapshotStatus;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupUploadStatus;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupUploadTask;
import com.mesosphere.dcos.cassandra.common.util.TaskUtils;
import com.mesosphere.dcos.cassandra.executor.backup.BackupStorageDriver;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class UploadSnapshot implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(UploadSnapshot.class);
    private NodeProbe probe;
    private ExecutorDriver driver;
    final BackupContext context;
    private BackupUploadTask cassandraTask;
    private BackupStorageDriver backupStorageDriver;

    public UploadSnapshot(ExecutorDriver driver, NodeProbe probe, CassandraTask cassandraTask, BackupStorageDriver backupStorageDriver) {
        this.probe = probe;
        this.driver = driver;
        this.cassandraTask = (BackupUploadTask) cassandraTask;
        final int nodeId = TaskUtils.taskIdToNodeId(this.cassandraTask.getId());
        this.backupStorageDriver = backupStorageDriver;
        context = new BackupContext();
        context.setNodeId(nodeId+"");
        context.setName(this.cassandraTask.getBackupName());
        context.setExternalLocation(this.cassandraTask.getExternalLocation());
        context.setS3AccessKey(this.cassandraTask.getS3AccessKey());
        context.setS3SecretKey(this.cassandraTask.getS3SecretKey());
        context.setLocalLocation(this.cassandraTask.getLocalLocation());
    }

    @Override
    public void run() {
        try {
            // Send TASK_RUNNING
            sendStatus(driver, Protos.TaskState.TASK_RUNNING, "Started uploading snapshots");

            // Upload snapshots to external location.
            backupStorageDriver.upload(context);

            // Once we have uploaded all existing snapshots, let's clear on-disk snapshots
            this.probe.clearSnapshot(context.getName());

            // Send TASK_FINISHED
            sendStatus(driver, Protos.TaskState.TASK_FINISHED,
                    "Finished uploading snapshots");
        } catch (Exception e) {
            // Send TASK_FAILED
            final String errorMessage = "Failed uploading snapshot. Reason: " + e;
            LOGGER.error(errorMessage);
            sendStatus(driver, Protos.TaskState.TASK_FAILED, errorMessage);
        }
    }

    private void sendStatus(ExecutorDriver driver, Protos.TaskState state, String message) {
        Protos.TaskStatus status = BackupUploadStatus.create(
                state,
                cassandraTask.getId(),
                cassandraTask.getSlaveId(),
                cassandraTask.getExecutor().getId(),
                Optional.of(message)
        ).toProto();
        driver.sendStatusUpdate(status);
    }
}
