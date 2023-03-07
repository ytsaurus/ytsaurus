#include "listener.h"
#include "connection.h"

#include <yt/core/concurrency/poller.h>

#include <yt/core/net/socket.h>

#include <yt/core/misc/proc.h>

namespace NYT::NNet {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TListener
    : public IPollable
    , public IListener
{
public:
    TListener(
        SOCKET serverSocket,
        const TNetworkAddress& address,
        const TString& name,
        IPollerPtr poller,
        IPollerPtr acceptor)
        : Name_(name)
        , Address_(address)
        , ServerSocket_(serverSocket)
        , Poller_(poller)
        , Acceptor_(acceptor)
    {
        Acceptor_->Register(this);
        Acceptor_->Arm(ServerSocket_, this, EPollControl::Read | EPollControl::EdgeTriggered);
    }

    // IPollable implementation
    virtual const TString& GetLoggingId() const override
    {
        return Name_;
    }

    virtual void OnEvent(EPollControl control) override
    {
        try {
            while (true) {
                TPromise<IConnectionPtr> promise;

                {
                    auto guard = Guard(Lock_);
                    if (!Error_.IsOK()) {
                        break;
                    }
                    if (Queue_.empty()) {
                        Pending_ = true;
                        break;
                    }
                    promise = std::move(Queue_.front());
                    Queue_.pop_front();
                    Pending_ = false;
                }

                if (!TryAccept(promise)) {
                    auto guard = Guard(Lock_);
                    Queue_.push_back(promise);
                    if (!Pending_) {
                        break;
                    }
                }
            }
        } catch (const TErrorException& ex) {
            auto error = ex << TErrorAttribute("listener", Name_);
            Abort(error);
        }
    }

    virtual void OnShutdown() override
    {
        decltype(Queue_) queue;
        {
            auto guard = Guard(Lock_);
            if (Error_.IsOK()) {
                Error_ = TError("Listener is shut down");
            }
            std::swap(Queue_, queue);
            YT_VERIFY(TryClose(ServerSocket_, false));
        }

        for (auto& promise : queue) {
           promise.Set(Error_);
        }
    }

    virtual const TNetworkAddress& GetAddress() const override
    {
        return Address_;
    }

    // IListener implementation
    virtual TFuture<IConnectionPtr> Accept() override
    {
        auto promise = NewPromise<IConnectionPtr>();

        if (!Pending_ || !TryAccept(promise)) {
            auto guard = Guard(Lock_);
            if (Error_.IsOK()) {
                Queue_.push_back(promise);
                if (Pending_) {
                    Pending_ = false;
                    Acceptor_->Retry(this);
                }
            } else {
                promise.Set(Error_);
            }
        }

        promise.OnCanceled(BIND([promise, this, this_ = MakeStrong(this)] (const TError& error) {
            {
                auto guard = Guard(Lock_);
                auto it = std::find(Queue_.begin(), Queue_.end(), promise);
                if (it != Queue_.end()) {
                    Queue_.erase(it);
                }
            }
            promise.TrySet(TError(NYT::EErrorCode::Canceled, "Accept canceled")
                << error);
        }));

        return promise.ToFuture();
    }

    virtual void Shutdown() override
    {
        Abort(TError("Listener is shut down"));
    }

private:
    const TString Name_;
    const TNetworkAddress Address_;
    SOCKET ServerSocket_ = INVALID_SOCKET;
    IPollerPtr Poller_;
    IPollerPtr Acceptor_;

    TSpinLock Lock_;
    std::atomic<bool> Pending_ = {false};
    std::deque<TPromise<IConnectionPtr>> Queue_;
    TError Error_;


    void Abort(const TError& error)
    {
        YT_VERIFY(!error.IsOK());

        auto guard = Guard(Lock_);

        if (!Error_.IsOK()) {
            return;
        }

        Pending_ = false;
        Error_ = error
            << TErrorAttribute("listener", Name_);
        Acceptor_->Unarm(ServerSocket_);
        Acceptor_->Unregister(this);
    }

    bool TryAccept(TPromise<IConnectionPtr> &promise)
    {
        TNetworkAddress clientAddress;
        auto clientSocket = AcceptSocket(ServerSocket_, &clientAddress);
        if (clientSocket == INVALID_SOCKET) {
            return false;
        }

        auto localAddress = GetSocketName(clientSocket);
        promise.TrySet(CreateConnectionFromFD(
            clientSocket,
            localAddress,
            clientAddress,
            Poller_));

        return true;
    }
};

DECLARE_REFCOUNTED_CLASS(TListener)
DEFINE_REFCOUNTED_TYPE(TListener)

////////////////////////////////////////////////////////////////////////////////

IListenerPtr CreateListener(
    const TNetworkAddress& address,
    const NConcurrency::IPollerPtr& poller,
    const NConcurrency::IPollerPtr& acceptor,
    int maxBacklogSize)
{
    auto serverSocket = address.GetSockAddr()->sa_family == AF_UNIX
        ? CreateUnixServerSocket()
        : CreateTcpServerSocket();

    try {
        BindSocket(serverSocket, address);
        // Client might have specified port == 0, find real address.
        auto realAddress = GetSocketName(serverSocket);

        ListenSocket(serverSocket, maxBacklogSize);
        return New<TListener>(
            serverSocket,
            realAddress,
            Format("Listener{%v}", realAddress),
            poller,
            acceptor);
    } catch (const std::exception& ) {
        YT_VERIFY(TryClose(serverSocket, false));
        throw;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNet
