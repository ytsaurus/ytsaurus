#include "connection.h"

#include "config.h"
#include "private.h"

#include <yt/yt/client/zookeeper/packet.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/pollable_detail.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/misc/atomic_object.h>

#include <yt/yt/core/net/connection.h>

namespace NYT::NZookeeperProxy {

using namespace NBus;
using namespace NConcurrency;
using namespace NZookeeper;

////////////////////////////////////////////////////////////////////////////////

struct TZookeeperConnectionTag
{ };

////////////////////////////////////////////////////////////////////////////////

class TConnection
    : public IConnection
{
public:
    TConnection(
        TZookeeperServerConfigPtr config,
        NNet::IConnectionPtr connection,
        IInvokerPtr invoker,
        TRequestHandler requestHandler,
        TFailHandler failHandler)
        : ConnectionId_(TConnectionId::Create())
        , Config_(std::move(config))
        , Connection_(std::move(connection))
        , Invoker_(CreateSerializedInvoker(std::move(invoker), "zookeeper_proxy_connection"))
        , RequestHandler_(std::move(requestHandler))
        , FailHandler_(std::move(failHandler))
        , PacketEncoder_(GetZookeeperPacketTranscoderFactory()->CreateEncoder(
            ZookeeperProxyLogger))
        , PacketDecoder_(GetZookeeperPacketTranscoderFactory()->CreateDecoder(
            ZookeeperProxyLogger,
            /*verifyChecksum*/ false))
    { }

    void Start() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_VERIFY(!Started_.exchange(true));

        Invoker_->Invoke(BIND(&TConnection::ArmReader, MakeWeak(this)));
    }

    TFuture<void> Terminate() override
    {
        Stopping_ = true;

        if (ReadFuture_) {
            ReadFuture_.Cancel(TError("Connection terminated"));
        }

        return Connection_->Abort();
    }

    TFuture<void> PostMessage(TMessage message) override
    {
        YT_VERIFY(Started_);

        return BIND(&TConnection::DoPostMessage, MakeStrong(this), message)
            .AsyncVia(Invoker_)
            .Run();
    }

    TConnectionId GetConnectionId() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return ConnectionId_;
    }

    void SetSessionId(TSessionId sessionId) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        SessionId_.Store(sessionId);
    }

    TSessionId GetSessionId() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return SessionId_.Load();
    }

private:
    const TConnectionId ConnectionId_;
    TAtomicObject<TSessionId> SessionId_;

    const TZookeeperServerConfigPtr Config_;

    const NNet::IConnectionPtr Connection_;
    const IPollerPtr Poller_;
    const IInvokerPtr Invoker_;

    const TRequestHandler RequestHandler_;
    const TFailHandler FailHandler_;

    const std::unique_ptr<IPacketEncoder> PacketEncoder_;
    const std::unique_ptr<IPacketDecoder> PacketDecoder_;
    bool ReadingMessage_ = false;

    TFuture<size_t> ReadFuture_;

    std::atomic<bool> Started_ = false;
    std::atomic<bool> Stopping_ = false;

    void ArmReader()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        if (Stopping_) {
            return;
        }

        auto fragment = PacketDecoder_->GetFragment();
        TSharedMutableRef sharedFragment(
            fragment.begin(),
            fragment.size(),
            MakeStrong(this));

        ReadFuture_ = Connection_->Read(sharedFragment);
        if (ReadingMessage_) {
            ReadFuture_ = ReadFuture_.WithTimeout(Config_->ReadIdleTimeout);
        }

        ReadFuture_
            .Subscribe(BIND(&TConnection::OnRead, MakeWeak(this))
                .Via(Invoker_));
    }

    void OnRead(TErrorOr<size_t> bytesReadOrError)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        if (Stopping_) {
            return;
        }

        if (!bytesReadOrError.IsOK()) {
            FailHandler_(MakeStrong(this), TError(bytesReadOrError));
            return;
        }

        auto bytesRead = bytesReadOrError.ValueOrThrow();
        if (!PacketDecoder_->Advance(bytesRead)) {
            auto error = TError("Failed to parse packet");
            FailHandler_(MakeStrong(this), error);
            return;
        }

        if (PacketDecoder_->IsFinished()) {
            auto message = PacketDecoder_->GrabMessage();
            RequestHandler_(MakeStrong(this), message);

            PacketDecoder_->Restart();
        }

        ArmReader();
    }

    TFuture<void> DoPostMessage(TMessage message)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        if (!PacketEncoder_->Start(
            EPacketType::Message,
            EPacketFlags::None,
            /*generateChecksums*/ false,
            /*checksummedPartCount*/ 0,
            /*packetId*/ {},
            message))
        {
            auto error = TError("Invalid message");
            return MakeFuture<void>(error);
        }

        std::vector<TSharedRef> fragments;
        fragments.reserve(2);
        while (!PacketEncoder_->IsFinished()) {
            const auto& fragment = PacketEncoder_->GetFragment();
            if (PacketEncoder_->IsFragmentOwned()) {
                TSharedRef sharedFragment(
                    fragment.begin(),
                    fragment.size(),
                    MakeStrong(this));
                fragments.emplace_back(std::move(sharedFragment));
            } else {
                auto clonedFragment = TSharedMutableRef::Allocate<TZookeeperConnectionTag>(
                    fragment.size());
                memcpy(clonedFragment.begin(), fragment.begin(), fragment.size());
                fragments.emplace_back(std::move(clonedFragment));
            }

            PacketEncoder_->NextFragment();
        }

        TSharedRefArray parts(std::move(fragments), TSharedRefArray::TMoveParts{});
        return Connection_->WriteV(parts)
            .WithTimeout(Config_->WriteIdleTimeout)
            .ToUncancelable();
    }
};

////////////////////////////////////////////////////////////////////////////////

IConnectionPtr CreateConnection(
    TZookeeperServerConfigPtr config,
    NNet::IConnectionPtr connection,
    IInvokerPtr invoker,
    TRequestHandler requestHandler,
    TFailHandler failHandler)
{
    return New<TConnection>(
        std::move(config),
        std::move(connection),
        std::move(invoker),
        std::move(requestHandler),
        std::move(failHandler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeperProxy
