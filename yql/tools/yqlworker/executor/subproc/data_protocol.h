#pragma once
#ifdef STATUS_PENDING
#undef STATUS_PENDING
#endif
#include <yql/tools/yqlworker/executor/subproc/data_protocol.pb.h>

#include <library/cpp/messagebus/handler.h>
#include <library/cpp/messagebus/network.h>
#include <library/cpp/messagebus/protobuf/ybusbuf.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/noncopyable.h>
#include <util/generic/ptr.h>


namespace NYql {
namespace NSubprocessTaskExecutor {

enum EMessageType {
    MTYPE_GET_TASK_REQUEST = 0x0001,
    MTYPE_GET_TASK_RESPONSE = 0x0002,
    MTYPE_TASK_WATCH_EVENT_REQUEST = 0x0003,
    MTYPE_TASK_WATCH_EVENT_RESPONSE = 0x0004,
    MTYPE_METRICS_REGISTRY_SNAPSHOT_REQUEST = 0x0005,
    MTYPE_METRICS_REGISTRY_SNAPSHOT_RESPONSE = 0x0006,
};

using TBusTaskWatchEventResponse = NBus::TBusBufferMessage<NProto::TTaskWatchEventResponse, MTYPE_TASK_WATCH_EVENT_RESPONSE>;

//////////////////////////////////////////////////////////////////////////////
// TDataProtocol
//////////////////////////////////////////////////////////////////////////////
struct TDataProtocol: public NBus::TBusBufferProtocol
{
    TDataProtocol(int port);
};

//////////////////////////////////////////////////////////////////////////////
// IDataServerHandler
//////////////////////////////////////////////////////////////////////////////
struct IDataServerHandler
{
    virtual ~IDataServerHandler() = default;

    virtual NProto::TGetTaskResponse OnGetTask(
            NProto::TGetTaskRequest&& request) = 0;

    virtual void OnTaskWatchEvent(
            NProto::TTaskWatchEventRequest&& request, NBus::TOnMessageContext* taskWatchCtx = nullptr) = 0;

    virtual void OnMetricsRegistrySnapshot(
            NProto::TMetricsRegistrySnapshotRequest&& request) = 0;
};

//////////////////////////////////////////////////////////////////////////////
// TDataServer
//////////////////////////////////////////////////////////////////////////////
class TDataServer: private TNonCopyable
{
public:
    TDataServer(
            const TVector<NBus::TBindResult>& bindTo,
            int nThreads,
            IDataServerHandler* handler);

    ~TDataServer();

    void Shutdown();

    int ActualPort() const;

private:
    struct TImpl;
    THolder<TImpl> Impl_;
};

//////////////////////////////////////////////////////////////////////////////
// TDataClient
//////////////////////////////////////////////////////////////////////////////
class TDataClient: private TNonCopyable
{
public:
    explicit TDataClient(int port);
    ~TDataClient();

    NThreading::TFuture<NProto::TGetTaskResponse> GetTask(
            NProto::TGetTaskRequest&& request) const;

    NThreading::TFuture<void> SendTaskWatchEvent(
            NProto::TTaskWatchEventRequest&& request) const;

    NThreading::TFuture<void> SendMetricsRegistrySnapshot(
            NProto::TMetricsRegistrySnapshotRequest&& request) const;

private:
    struct TImpl;
    THolder<TImpl> Impl_;
};

} // namespace NSubprocessTaskExecutor
} // namespace NYql
