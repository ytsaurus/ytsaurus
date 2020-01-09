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
    }

    // IPollable implementation
    virtual const TString& GetLoggingId() const override
    {
        return Name_;
    }

    virtual void OnEvent(EPollControl control) override
    {
        try {
            while (TryAccept())
            { }
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
        {
            auto guard = Guard(Lock_);
            if (Error_.IsOK()) {
                TNetworkAddress clientAddress;
                auto clientSocket = AcceptSocket(ServerSocket_, &clientAddress);
                if (clientSocket != INVALID_SOCKET) {
                    auto localAddress = GetSocketName(clientSocket);
                    promise.Set(CreateConnectionFromFD(
                        clientSocket,
                        localAddress,
                        clientAddress,
                        Poller_));
                } else {
                    Queue_.push_back(promise);
                    if (!Active_) {
                        Active_ = true;
                        Acceptor_->Arm(ServerSocket_, this, EPollControl::Read);
                    }
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
    bool Active_ = false;
    std::deque<TPromise<IConnectionPtr>> Queue_;
    TError Error_;


    void Abort(const TError& error)
    {
        YT_VERIFY(!error.IsOK());

        auto guard = Guard(Lock_);

        if (!Error_.IsOK()) {
            return;
        }

        Error_ = error
            << TErrorAttribute("listener", Name_);
        Acceptor_->Unarm(ServerSocket_);
        Acceptor_->Unregister(this);
    }

    bool TryAccept()
    {
        {
            auto guard = Guard(Lock_);
            if (!Error_.IsOK()) {
                return false;
            }
            if (Queue_.empty()) {
                Active_ = false;
                return false;
            }
        }

        TNetworkAddress clientAddress;
        auto clientSocket = AcceptSocket(ServerSocket_, &clientAddress);

        TPromise<IConnectionPtr> promise;
        bool active = false;
        {
            auto guard = Guard(Lock_);
            if (clientSocket == INVALID_SOCKET) {
                Acceptor_->Arm(ServerSocket_, this, EPollControl::Read);
                return false;
            }

            promise = std::move(Queue_.front());
            Queue_.pop_front();
            active = Active_ = !Queue_.empty();
        }

        auto localAddress = GetSocketName(clientSocket);
        promise.TrySet(CreateConnectionFromFD(
            clientSocket,
            localAddress,
            clientAddress,
            Poller_));
        return active;
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
