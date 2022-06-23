#include "connection.h"

#include "bus.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/pollable_detail.h>

#include <yt/yt/core/net/connection.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/ring_queue.h>

namespace NYT::NZookeeper {

using namespace NConcurrency;
using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

class TConnection
    : public IConnection
    , public TPollableBase
{
public:
    TConnection(
        NNet::IConnectionPtr connection,
        IPollerPtr poller,
        IInvokerPtr invoker,
        TRequestHandler requestHandler,
        TFailHandler failHandler,
        TString loggingTag)
        : Connection_(std::move(connection))
        , Poller_(std::move(poller))
        , Invoker_(CreateSerializedInvoker(std::move(invoker)))
        , RequestHandler_(std::move(requestHandler))
        , FailHandler_(std::move(failHandler))
        , LoggingTag_(std::move(loggingTag))
    { }

    // IConnection implementation
    bool Start() override
    {
        if (!Poller_->TryRegister(MakeStrong(this))) {
            return false;
        }

        Poller_->Arm(GetReadHandle(), MakeStrong(this), EPollControl::Read | EPollControl::EdgeTriggered);

        return true;
    }

    TFuture<void> Stop() override
    {
        Stopping_ = true;

        Poller_->Unarm(GetReadHandle(), MakeStrong(this));

        std::vector<TFuture<void>> futures{
            Connection_->Abort(),
            Poller_->Unregister(MakeStrong(this))
        };
        return AllSucceeded(std::move(futures));
    }

    // IPollable implementation
    const TString& GetLoggingTag() const override
    {
        return LoggingTag_;
    }

    void OnEvent(EPollControl control) override
    {
        YT_VERIFY(Any(control & EPollControl::Read));

        Invoker_->Invoke(BIND(&TConnection::ReceiveRequest, MakeWeak(this)));
    }

    void OnShutdown() override
    {
        Stopping_ = true;
    }

private:
    const NNet::IConnectionPtr Connection_;
    const IPollerPtr Poller_;
    const IInvokerPtr Invoker_;

    const TRequestHandler RequestHandler_;
    const TFailHandler FailHandler_;

    const TString LoggingTag_;

    TRingQueue<TSharedRef> Requests_;
    bool ProcessingRequest_ = false;

    std::atomic<bool> Stopping_ = false;

    void ReceiveRequest()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        if (Stopping_) {
            return;
        }

        TSharedRef request;
        try {
            request = ReceiveMessage(Connection_);
        } catch (const std::exception& ex) {
            auto error = TError("Fail to post receive message") << ex;
            FailHandler_(error);
            return;
        }

        Requests_.push(request);
        ProcessRequest();
    }

    void ProcessRequest()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        if (Requests_.empty()) {
            return;
        }

        if (Stopping_) {
            return;
        }

        if (std::exchange(ProcessingRequest_, true)) {
            return;
        }
        auto guard = Finally([&] { ProcessingRequest_ = false; });

        auto request = Requests_.front();
        Requests_.pop();

        if (auto response = RequestHandler_(std::move(request))) {
            try {
                PostMessage(std::move(response), Connection_);
            } catch (const std::exception& ex) {
                auto error = TError("Fail to post response message") << ex;
                FailHandler_(error);
            }
        }

        Invoker_->Invoke(BIND(&TConnection::ProcessRequest, MakeWeak(this)));
    }

    int GetReadHandle() const
    {
        return static_cast<NNet::IConnectionReaderPtr>(Connection_)->GetHandle();
    }
};

////////////////////////////////////////////////////////////////////////////////

IConnectionPtr CreateConnection(
    NNet::IConnectionPtr connection,
    IPollerPtr poller,
    IInvokerPtr invoker,
    TRequestHandler requestHandler,
    TFailHandler failHandler,
    TString loggingTag)
{
    return New<TConnection>(
        std::move(connection),
        std::move(poller),
        std::move(invoker),
        std::move(requestHandler),
        std::move(failHandler),
        std::move(loggingTag));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeper
