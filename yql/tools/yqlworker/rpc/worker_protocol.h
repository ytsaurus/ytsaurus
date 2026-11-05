
#include <yql/tools/yqlworker/proto/worker.pb.h>

#include <library/cpp/messagebus/protobuf/ybusbuf.h>
#include <library/cpp/deprecated/enum_codegen/enum_codegen.h>


namespace NYql {

#define WORKER_MESSAGE_TYPES(xx) \
    xx(MTYPE_PROCESS_TASK_REQUEST, 0x0001, ProcessTaskRequest) \
    xx(MTYPE_PROCESS_TASK_RESPONSE, 0x0002, ProcessTaskResponse) \
    xx(MTYPE_KILL_TASK_REQUEST, 0x0003, KillTaskRequest) \
    xx(MTYPE_KILL_TASK_RESPONSE, 0x0004, KillTaskResponse) \
    xx(MTYPE_SUBSCRIBE_REQUEST, 0x0005, SubscribeRequest) \
    xx(MTYPE_SUBSCRIBE_RESPONSE, 0x0006, SubscribeResponse) \
    xx(MTYPE_UNSUBSCRIBE_REQUEST, 0x0007, UnsubscribeRequest) \
    xx(MTYPE_UNSUBSCRIBE_RESPONSE, 0x0008, UnsubscribeResponse)

#define BUS_USING_GEN(type, value, name) \
    using TBus##name = NBus::TBusBufferMessage<NProto::T##name, type>;

enum EWorkerMessageType {
    WORKER_MESSAGE_TYPES(ENUM_VALUE_GEN)
};

WORKER_MESSAGE_TYPES(BUS_USING_GEN)

//////////////////////////////////////////////////////////////////////////////
// TMsgBusWorkerProtocol
//////////////////////////////////////////////////////////////////////////////
struct TMsgBusWorkerProtocol: public NBus::TBusBufferProtocol {
    explicit TMsgBusWorkerProtocol(int port)
        : NBus::TBusBufferProtocol("yqlworker", port)
    {
        RegisterType(new TBusProcessTaskRequest);
        RegisterType(new TBusProcessTaskResponse);
        RegisterType(new TBusKillTaskRequest);
        RegisterType(new TBusKillTaskResponse);
        RegisterType(new TBusSubscribeRequest);
        RegisterType(new TBusSubscribeResponse);
        RegisterType(new TBusUnsubscribeRequest);
        RegisterType(new TBusUnsubscribeResponse);
    }
};

} // namespace NYql
