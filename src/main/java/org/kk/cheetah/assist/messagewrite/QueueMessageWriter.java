package org.kk.cheetah.assist.messagewrite;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.kk.cheetah.assist.messagewrite.MessageWriteResult.MessageWriteResultEnum;
import org.kk.cheetah.common.model.response.ConsumerRecord;
import org.kk.cheetah.config.ServerConfig;
import org.kk.cheetah.serializer.MarshallingSerializer;
import org.kk.cheetah.serializer.MessageSerializer;
import org.kk.cheetah.util.ByteIntConvertUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueMessageWriter implements MessageWriter {
    private static Logger logger = LoggerFactory.getLogger(QueueMessageWriter.class);

    private BlockingQueue<ConsumerRecord> consumerRecordQueue;

    private MessageSerializer messageSerializer;

    private ExecutorService messageQueueExecutorService;

    private Map<String, OutputStream> fosMap = new HashMap<String, OutputStream>();

    private FsyncDiskScheduled fsyncDiskScheduled;

    public QueueMessageWriter() {
        messageSerializer = new MarshallingSerializer();
        consumerRecordQueue = new LinkedBlockingQueue<ConsumerRecord>();
        fsyncDiskScheduled = new FsyncDiskScheduled();
        messageQueueExecutorService = Executors.newSingleThreadExecutor();
        messageQueueExecutorService.execute(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        doWriteFile(consumerRecordQueue.take());
                    } catch (InterruptedException e) {
                        logger.error("consumerRecordQueue.take", e);
                        return;
                    }
                }
            }
        });
    }

    @Override
    public Future<MessageWriteResult> writeFile(ConsumerRecord consumerRecord) {
        QueueMessageWriteResultFuture queueMessageWriteResultFuture = null;
        try {
            //如果队列已满，等待5秒钟后响应失败
            boolean result = consumerRecordQueue.offer(consumerRecord, 5, TimeUnit.SECONDS);
            if (result) {
                queueMessageWriteResultFuture = new QueueMessageWriteResultFuture(
                        MessageWriteResultEnum.SUCCESS);
            } else {
                queueMessageWriteResultFuture = new QueueMessageWriteResultFuture(
                        MessageWriteResultEnum.FAIL);
            }
        } catch (InterruptedException e) {
            logger.error("writeFile", e);
            queueMessageWriteResultFuture = new QueueMessageWriteResultFuture(
                    MessageWriteResultEnum.FAIL);
        }

        return queueMessageWriteResultFuture;
    }

    private void doWriteFile(ConsumerRecord consumerRecord) {
        if (consumerRecord == null) {
            return;
        }
        try {
            OutputStream outputStream = fosMap.get(consumerRecord.getTopic());
            if (outputStream == null) {
                String fileName = new StringBuffer().append(ServerConfig.dataFilePath).append(consumerRecord.getTopic())
                        .toString();
                FileOutputStream fos = new FileOutputStream(fileName, true);
                fosMap.put(consumerRecord.getTopic(),
                        outputStream = new BufferedOutputStream(fos));
                fsyncDiskScheduled.submit(outputStream, fos.getFD());
            }
            byte[] bytes = messageSerializer.serialize(consumerRecord);
            outputStream.write(ByteIntConvertUtil.intToByteArray(bytes.length));
            outputStream.write(bytes);
        } catch (IOException e) {
            logger.error("doWriteFile", e);
        }
    }

    class QueueMessageWriteResultFuture implements Future<MessageWriteResult> {
        private MessageWriteResultEnum messageWriteResultEnum;

        public QueueMessageWriteResultFuture(MessageWriteResultEnum messageWriteResultEnum) {
            this.messageWriteResultEnum = messageWriteResultEnum;
        }

        @Override
        public MessageWriteResult get() throws InterruptedException, ExecutionException {
            MessageWriteResult messageWriteResult = new MessageWriteResult();
            messageWriteResult.setResult(messageWriteResultEnum);
            return messageWriteResult;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isCancelled() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isDone() {
            throw new UnsupportedOperationException();
        }

        @Override
        public MessageWriteResult get(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            throw new UnsupportedOperationException();
        }

    }
}
