#include "listener.h"
#include "connection.h"

#include <yt/core/concurrency/poller.h>

#include <yt/core/net/socket.h>
#include <yt/core/misc/proc.h>

namespace NYT {
namespace NNet {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TListenerImpl
    : public IPollable
{
public:
    TListenerImpl(
        SOCKET serverSocket,
        const TNetworkAddress& address,
        const TString& name,
        IPollerPtr poller)
        : Name_(name)
        , Address_(address)
        , ServerSocket_(serverSocket)
        , Poller_(poller)
    {
        Poller_->Register(this);
    }

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
        YCHECK(TryClose(ServerSocket_, false));
        for (auto& promise : Queue_) {
            promise.Set(Error_);
        }
    }

    const TNetworkAddress& Address() const
    {
        return Address_;
    }
    
    TFuture<IConnectionPtr> Accept()
    {
        auto promise = NewPromise<IConnectionPtr>();
        {
            auto guard = Guard(Lock_);
            if (Error_.IsOK()) {
                Queue_.push_back(promise);
                if (!Active_) {
                    Active_ = true;
                    Poller_->Arm(ServerSocket_, this, EPollControl::Read);
                }
            } else {
                promise.Set(Error_);
            }
        }

        return promise.ToFuture();
    }

    void Abort(const TError& error)
    {
        auto guard = Guard(Lock_);
        if (!Error_.IsOK()) {
            return;
        }

        Error_ = error
            << TErrorAttribute("listener", Name_);
        Poller_->Unarm(ServerSocket_);
        Poller_->Unregister(this);
    }

private:
    const TString Name_;
    const TNetworkAddress Address_;
    SOCKET ServerSocket_ = INVALID_SOCKET;
    IPollerPtr Poller_;

    TSpinLock Lock_;
    bool Active_ = false;
    std::deque<TPromise<IConnectionPtr>> Queue_;
    TError Error_;

    bool TryAccept()
    {
        {
            auto guard = Guard(Lock_);
            if (Queue_.empty()) {
                Active_ = false;
                return false;
            }
        }
    
        TNetworkAddress clientAddress;
        SOCKET clientSocket = AcceptSocket(ServerSocket_, &clientAddress);

        TPromise<IConnectionPtr> promise;
        bool active = false;
        {
            auto guard = Guard(Lock_);
            if (clientSocket == INVALID_SOCKET) {
                Poller_->Arm(ServerSocket_, this, EPollControl::Read);
                return false;
            }

            promise = std::move(Queue_.front());
            Queue_.pop_front();
            active = Active_ = !Queue_.empty();
        }

        auto localAddress = GetSocketName(clientSocket);
        promise.Set(CreateConnectionFromFD(
            clientSocket,
            localAddress,
            clientAddress,
            Poller_));
        return active;
    }
};

DECLARE_REFCOUNTED_CLASS(TListenerImpl);
DEFINE_REFCOUNTED_TYPE(TListenerImpl);

////////////////////////////////////////////////////////////////////////////////

class TListener
    : public IListener
{
public:
    explicit TListener(const TListenerImplPtr& impl)
        : Impl_(impl)
    { }

    virtual TFuture<IConnectionPtr> Accept() override
    {
        return Impl_->Accept();
    }

    virtual const TNetworkAddress& Address() const override
    {
        return Impl_->Address();
    }

    ~TListener()
    {
        Impl_->Abort(TError("Listener destroyed"));
    }

private:
    const TListenerImplPtr Impl_;
};

DEFINE_REFCOUNTED_TYPE(TListener);

////////////////////////////////////////////////////////////////////////////////

IListenerPtr CreateListener(
    const TNetworkAddress& address,
    const NConcurrency::IPollerPtr& poller)
{
    int family = address.GetSockAddr()->sa_family;
    SOCKET serverSocket;
    if (family == AF_UNIX) {
        serverSocket = CreateUnixServerSocket();
    } else {
        serverSocket = CreateTcpServerSocket();
    }

    try {
        BindSocket(serverSocket, address);
        // Client might have specified port == 0, find real address.
        auto realAddress = GetSocketName(serverSocket);

        const int ListenBacklogSize = 128;
        ListenSocket(serverSocket, ListenBacklogSize);
        auto impl = New<TListenerImpl>(
            serverSocket,
            realAddress,
            Format("listener[%v]", realAddress),
            poller);
        return New<TListener>(impl);
    } catch (const std::exception& ) {
        YCHECK(TryClose(serverSocket, false));
        throw;
    }
}    

////////////////////////////////////////////////////////////////////////////////

} // namespace NNet
} // namespace NYT
