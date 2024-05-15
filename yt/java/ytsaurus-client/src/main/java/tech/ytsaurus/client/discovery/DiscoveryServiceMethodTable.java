package tech.ytsaurus.client.discovery;

import java.util.function.Supplier;

import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import tech.ytsaurus.TReqGetGroupMeta;
import tech.ytsaurus.TReqHeartbeat;
import tech.ytsaurus.TReqListGroups;
import tech.ytsaurus.TReqListMembers;
import tech.ytsaurus.TRspGetGroupMeta;
import tech.ytsaurus.TRspHeartbeat;
import tech.ytsaurus.TRspListGroups;
import tech.ytsaurus.TRspListMembers;
import tech.ytsaurus.client.RpcMethodDescriptor;

public class DiscoveryServiceMethodTable {
    public static final RpcMethodDescriptor<TReqListMembers.Builder, TRspListMembers> LIST_MEMBERS =
            clientServiceMethod("ListMembers", TReqListMembers::newBuilder, TRspListMembers.parser());

    public static final RpcMethodDescriptor<TReqGetGroupMeta.Builder, TRspGetGroupMeta> GET_GROUP_META =
            clientServiceMethod("GetGroupMeta", TReqGetGroupMeta::newBuilder, TRspGetGroupMeta.parser());

    public static final RpcMethodDescriptor<TReqHeartbeat.Builder, TRspHeartbeat> HEARTBEAT =
            clientServiceMethod("Heartbeat", TReqHeartbeat::newBuilder, TRspHeartbeat.parser());

    public static final RpcMethodDescriptor<TReqListGroups.Builder, TRspListGroups> LIST_GROUPS =
            clientServiceMethod("ListGroups", TReqListGroups::newBuilder, TRspListGroups.parser());

    private DiscoveryServiceMethodTable() {
    }

    public static <TReqBuilder extends MessageLite.Builder, TRes extends MessageLite>
    RpcMethodDescriptor<TReqBuilder, TRes> clientServiceMethod(
            String name,
            Supplier<TReqBuilder> reqSupplier,
            Parser<TRes> parser
    ) {
        return new RpcMethodDescriptor<>(
                0,
                "DiscoveryClientService",
                name,
                reqSupplier,
                parser);
    }
}
