package ru.yandex.yt.ytclient.proxy.internal;

import java.util.List;

import javax.annotation.Nullable;

import NYT.NChunkClient.NProto.DataStatistics.TDataStatistics;

import ru.yandex.bolts.collection.Cf;
import ru.yandex.yt.rpcproxy.TRowsetDescriptor;
import ru.yandex.yt.ytclient.tables.TableSchema;

public interface TableAttachmentReader<T> {

    List<T> parse(byte[] attachments) throws Exception;

    long getTotalRowCount();

    @Nullable
    TDataStatistics getDataStatistics();

    @Nullable
    TableSchema getCurrentReadSchema();

    @Nullable
    TRowsetDescriptor getRowsetDescriptor();


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
}
