package ru.yandex.yt.ytclient.proxy.internal;

import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;

import NYT.NChunkClient.NProto.DataStatistics.TDataStatistics;

import ru.yandex.bolts.collection.Cf;
import ru.yandex.yt.rpcproxy.TRowsetDescriptor;
import ru.yandex.yt.ytclient.tables.TableSchema;

public interface TableAttachmentReader<T> {
    TableAttachmentReader<byte[]> BYPASS = new TableAttachmentReader<byte[]>() {
        @Override
        public List<byte[]> parse(byte[] attachments) {
            if (attachments == null) {
                return null;
            } else {
                return Cf.list(attachments);
            }
        }

        @Override
        public List<byte[]> parse(byte[] attachments, int offset, int length) {
            if (attachments == null) {
                return null;
            } else {
                if (offset == 0 && length == attachments.length) {
                    return Cf.list(attachments);
                } else {
                    return Cf.list(Arrays.copyOfRange(attachments, offset, length));
                }
            }
        }

        @Override
        public long getTotalRowCount() {
            return 0;
        }

        @Override
        public TDataStatistics getDataStatistics() {
            return null;
        }

        @Nullable
        @Override
        public TableSchema getCurrentReadSchema() {
            return null;
        }

        @Nullable
        @Override
        public TRowsetDescriptor getRowsetDescriptor() {
            return null;
        }
    };

    List<T> parse(byte[] attachments) throws Exception;

    List<T> parse(byte[] attachments, int offset, int length) throws Exception;

    long getTotalRowCount();

    @Nullable
    TDataStatistics getDataStatistics();

    @Nullable
    TableSchema getCurrentReadSchema();

    @Nullable
    TRowsetDescriptor getRowsetDescriptor();
}
