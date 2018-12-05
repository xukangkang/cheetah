package org.kk.cheetah.assist.messagewrite;

import java.io.FileDescriptor;
import java.io.IOException;
import java.io.OutputStream;
import java.io.SyncFailedException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FsyncDiskScheduled {
    private static Logger logger = LoggerFactory.getLogger(QueueMessageWriter.class);
    private ScheduledExecutorService fsyncDiskExecutorService;

    public FsyncDiskScheduled() {
        fsyncDiskExecutorService = Executors.newScheduledThreadPool(1);
    }

    private void fsyncDisk(FileDescriptor fd) {
        try {
            fd.sync();
        } catch (SyncFailedException e) {
            logger.error("fsyncDis", e);
        } catch (IOException e) {
            logger.error("fsyncDis", e);
        }
    }

    public void submit(OutputStream fos, FileDescriptor fd) {
        fsyncDiskExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    fos.flush();
                } catch (IOException e) {
                    logger.error("outputstream flush", e);
                }
                fsyncDisk(fd);
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

}
