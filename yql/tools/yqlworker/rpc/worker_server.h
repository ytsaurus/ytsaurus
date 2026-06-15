#pragma once

#include <yql/tools/yqlworker/proto/worker.pb.h>

#include <library/cpp/messagebus/network.h>

#include <util/generic/ptr.h>


namespace NYql {
namespace NProto {
    class TWorkerServerConfig;
} // namespace NProto

/**
 * @brief Base interface of worker RPC server handler implementations.
 */
struct IWorkerServerHandler: public TThrRefBase {
    virtual ~IWorkerServerHandler() = default;

    virtual NProto::TProcessTaskResponse OnProcessTask(
            const NAddr::IRemoteAddr& peerAddr,
            NProto::TProcessTaskRequest&& request) = 0;

    virtual NProto::TKillTaskResponse OnKillTask(
            const NAddr::IRemoteAddr& peerAddr,
            NProto::TKillTaskRequest&& request) = 0;

    virtual NProto::TSubscribeResponse OnSubscribe(
            const NAddr::IRemoteAddr& peerAddr,
            NProto::TSubscribeRequest&& request) = 0;

    virtual NProto::TUnsubscribeResponse OnUnsubscribe(
            const NAddr::IRemoteAddr& peerAddr,
            NProto::TUnsubscribeRequest&& request) = 0;
};

using IWorkerServerHandlerPtr = TIntrusivePtr<IWorkerServerHandler>;

/**
 * @brief Wraps given worker server handler with handler performing counting
 *        and execution time measurement.
 * @param h existing worker server handler
 * @return profiling worker server handler (will take ownership of h)
 */
IWorkerServerHandlerPtr WrapWithProfilingHandler(IWorkerServerHandlerPtr h);

/**
 * @brief Base interface of worker RPC server implementations.
 */
struct IWorkerServer: public TThrRefBase {
    virtual ~IWorkerServer() = default;

    virtual void Shutdown() = 0;
    virtual int ActualPort() const = 0;
};

using IWorkerServerPtr = TIntrusivePtr<IWorkerServer>;

/**
 * @brief Creates MessageBus worker server implementation.
 * @param config   worker server config
 * @param bindTo   already bounded ports
 * @param handler  worker server handler implementation
 * @return MessageBus worker server implementation
 */
IWorkerServerPtr CreateMsgBusWorkerServer(
        const NProto::TWorkerServerConfig& config,
        const TVector<NBus::TBindResult>& bindTo,
        IWorkerServerHandlerPtr handler);


} // namspace NYql
