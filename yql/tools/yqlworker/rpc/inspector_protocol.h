#pragma once

#include <yql/tools/yqlworker/proto/inspector.pb.h>
#include <yql/tools/yqlworker/rpc/inspector_client.h>

#include <library/cpp/messagebus/protobuf/ybusbuf.h>

#include <library/cpp/deprecated/enum_codegen/enum_codegen.h>
#include <util/generic/ptr.h>


namespace NYql {

#define INSPECTOR_MESSAGE_TYPES(xx) \
    xx(MTYPE_UPDATE_STATUS_REQUEST, 0x0011) \
    xx(MTYPE_UPDATE_STATUS_RESPONSE, 0x0012) \
    xx(MTYPE_HEARTBEAT_REQUEST, 0x0013) \
    xx(MTYPE_HEARTBEAT_RESPONSE, 0x0014) \
    xx(MTYPE_TASK_SUBSCR_NOTIFY_REQUEST, 0x0015) \
    xx(MTYPE_TASK_SUBSCR_NOTIFY_RESPONSE, 0x0016)

enum EInspectorMessageType {
    INSPECTOR_MESSAGE_TYPES(ENUM_VALUE_GEN)
};

ENUM_TO_STRING(EInspectorMessageType, INSPECTOR_MESSAGE_TYPES)

using TBusStatusUpdateResponse = NBus::TBusBufferMessage<NProto::TStatusUpdateResponse, MTYPE_UPDATE_STATUS_RESPONSE>;

struct TWithRetries {
    ui32 Retries = 0u;
};

struct TBusStatusUpdateRequest: public NBus::TBusBufferMessage<NProto::TStatusUpdateRequest, MTYPE_UPDATE_STATUS_REQUEST>, TWithRetries {
    TStatusUpdateCallback Callback;
};

using TBusTaskSubscriptionNotifyRequest = NBus::TBusBufferMessage<NProto::TTaskSubscriptionNotifyRequest, MTYPE_TASK_SUBSCR_NOTIFY_REQUEST>;
using TBusTaskSubscriptionNotifyResponse = NBus::TBusBufferMessage<NProto::TTaskSubscriptionNotifyResponse, MTYPE_TASK_SUBSCR_NOTIFY_RESPONSE>;

using TBusHeartbeatResponse = NBus::TBusBufferMessage<NProto::THeartbeatResponse, MTYPE_HEARTBEAT_RESPONSE>;
class TBusHeartbeatRequest: public NBus::TBusBufferMessage<NProto::THeartbeatRequest, MTYPE_HEARTBEAT_REQUEST> {
    using TCallback = std::function<void(const NAddr::IRemoteAddr& peerAddr, NProto::THeartbeatResponse&&)>;

public:
    TCallback Callback;
};

//////////////////////////////////////////////////////////////////////////////
// TMsgBusInspectorProtocol
//////////////////////////////////////////////////////////////////////////////
class TMsgBusInspectorProtocol: public NBus::TBusBufferProtocol {
public:
    TMsgBusInspectorProtocol(int port)
        : NBus::TBusBufferProtocol("yqlinspector", port)
    {
        RegisterType(new TBusStatusUpdateRequest);
        RegisterType(new TBusStatusUpdateResponse);
        RegisterType(new TBusHeartbeatRequest);
        RegisterType(new TBusHeartbeatResponse);
        RegisterType(new TBusTaskSubscriptionNotifyRequest);
        RegisterType(new TBusTaskSubscriptionNotifyResponse);
    }
};

} // namespace NYql
