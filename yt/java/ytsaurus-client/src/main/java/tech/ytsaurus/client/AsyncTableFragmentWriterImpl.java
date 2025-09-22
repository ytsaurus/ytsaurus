package tech.ytsaurus.client;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.google.protobuf.Parser;
import tech.ytsaurus.client.request.WriteFragmentResult;
import tech.ytsaurus.client.request.WriteTableFragment;
import tech.ytsaurus.client.rpc.LazyResponse;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.rpcproxy.TRspWriteTableFragment;

@NonNullApi
class AsyncTableFragmentWriterImpl<T> extends RawTableWriterImpl<T, TRspWriteTableFragment>
        implements AsyncFragmentWriter<T>, AsyncWriterSupport<T> {

    AsyncTableFragmentWriterImpl(WriteTableFragment<T> req, SerializationResolver serializationResolver) {
        super(req.getWindowSize(), req.getPacketSize(), serializationResolver, req.getSerializationContext());
    }

    @SuppressWarnings("unchecked")
    public CompletableFuture<AsyncFragmentWriter<T>> startUpload() {
        return super.startUploadImpl().thenApply(writer -> (AsyncFragmentWriter<T>) writer);
    }

    @Override
    public CompletableFuture<Void> write(List<T> rows) {
        return writeImpl(rows, schema);
    }

    @Override
    protected Parser<TRspWriteTableFragment> responseParser() {
        return TRspWriteTableFragment.parser();
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<WriteFragmentResult> finish() {
        return super.finish().thenApply(rsp -> {
            LazyResponse<TRspWriteTableFragment> lazyResponse = (LazyResponse<TRspWriteTableFragment>) rsp;
            return new WriteFragmentResult(lazyResponse.body().getSignedWriteResult());
        });
    }
}
