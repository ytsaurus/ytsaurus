#pragma once

#include <yql/tools/yqlworker/proto/inspector.pb.h>

#include <util/generic/ptr.h>
#include <util/network/address.h>

#include <functional>
#include <memory>


namespace NYql {
namespace NProto {
    class TInspectorConfig;
} // namespace NProto

using THeartbeatCallback = std::function<void(
        const NAddr::IRemoteAddr&,
        NProto::THeartbeatResponse&&)>;

using TStatusUpdateCallback = std::function<void(
        const NAddr::IRemoteAddr&,
        NProto::TStatusUpdateResponse&&)>;

/**
 * @brief Base interface of inspector RPC client implementations.
 */
struct IInspectorClient: public std::enable_shared_from_this<IInspectorClient> {
    virtual ~IInspectorClient() = default;

    virtual void SendHeartbeatToAll(
            NProto::THeartbeatRequest&& request,
            THeartbeatCallback callback) = 0;

    virtual void SendStatusUpdate(
            const NAddr::IRemoteAddr& prefferedAddr,
            NProto::TStatusUpdateRequest&& request,
            TStatusUpdateCallback callback) = 0;

    virtual void NotifyTaskSubscription(
            const NAddr::IRemoteAddr& peerAddress,
            NProto::TTaskSubscriptionNotifyRequest&& request) = 0;

    virtual void CloseConnectionWith(const NAddr::IRemoteAddr& peerAddr) = 0;
    virtual size_t GetConnectionsCount() = 0;

    virtual void WaitAllMessagesDelivered() const = 0;
};

using IInspectorClientPtr = std::shared_ptr<IInspectorClient>;

/**
 * @brief Wraps given inspector client with client performing counting
 *        and execution time measurement.
 * @param c  existen inspector client
 * @return profiling inspector client (will take ownership of c)
 */
IInspectorClientPtr WrapWithProfilingClient(IInspectorClientPtr c);

/**
 * @brief Creates MessageBus inspector client implementation.
 * @param config   worker server config
 * @param bindTo   already bounded ports
 * @param handler  worker server handler implementation
 * @return MessageBus worker server implementation
 */
IInspectorClientPtr CreateMsgBusInspectorClient(
        const NProto::TInspectorConfig& config);

} // namespace NYql
