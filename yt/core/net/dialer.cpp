#include "dialer.h"
#include "connection.h"

#include <yt/core/concurrency/poller.h>

#include <yt/core/misc/proc.h>
#include <yt/core/misc/socket.h>

namespace NYT {
namespace NNet {

using namespace NYT::NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TDialSession
    : public IPollable
{
public:
    TDialSession(
        SOCKET clientSocket,
        const TNetworkAddress& remote,
        const IPollerPtr& poller)
        : Name_(Format("dialer[%v]", remote))
        , RemoteAddress_(remote)
        , Poller_(poller)
        , ClientSocket_(clientSocket)
    {
        Poller_->Register(this);

        Promise_.OnCanceled(BIND([this, this_ = MakeStrong(this)] {
            Abort();
        }));

        Poller_->Arm(ClientSocket_, this, EPollControl::Write);
    }

    ~TDialSession()
    {
        YCHECK(ClientSocket_ == -1);
    }

    virtual const TString& GetLoggingId() const override
    {
        return Name_;
    }

    virtual void OnEvent(EPollControl control) override
    {
        if (Finished_.test_and_set()) {
            return;
        }
    
        int error = GetSocketError(ClientSocket_);
        if (error != 0) {
            Promise_.Set(TError("Connect error")
                << TError::FromSystem(error)
                << TErrorAttribute("dialer", Name_));
        } else {
            auto localAddress = GetSocketName(ClientSocket_);
            Promise_.Set(CreateConnectionFromFD(ClientSocket_, localAddress, RemoteAddress_, Poller_));
            ClientSocket_ = -1;
        }

        Poller_->Unregister(this);
    }

    virtual void OnShutdown() override
    {
        if (ClientSocket_ != -1) {
            YCHECK(TryClose(ClientSocket_, false));
            ClientSocket_ = -1;
        }
    }

    TFuture<IConnectionPtr> ToFuture() const
    {
        return Promise_.ToFuture();
    }

private:
    const TString Name_;
    const TNetworkAddress RemoteAddress_;
    const IPollerPtr Poller_;

    TSpinLock Lock_;
    SOCKET ClientSocket_ = -1;

    std::atomic_flag Finished_ = {false};
    TPromise<IConnectionPtr> Promise_ = NewPromise<IConnectionPtr>();

    void Abort()
    {
        if (Finished_.test_and_set()) {
            return;
        }

        Promise_.Set(TError("Dial aborted")
            << TErrorAttribute("dialer", Name_));
        Poller_->Unarm(ClientSocket_);
        Poller_->Unregister(this);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDialer
    : public IDialer
{
public:
    explicit TDialer(const IPollerPtr& poller)
        : Poller_(poller)
    { }

    virtual TFuture<IConnectionPtr> Dial(const TNetworkAddress& remote) override
    {
        int family = remote.GetSockAddr()->sa_family;
        SOCKET clientSocket;
        if (family == AF_UNIX) {
            clientSocket = CreateUnixClientSocket();
        } else {
            clientSocket = CreateTcpClientSocket();
        }

        try {
            int result = ConnectSocket(clientSocket, remote);
            if (result == 0) {
                auto localAddress = GetSocketName(clientSocket);
                return MakeFuture(CreateConnectionFromFD(
                    clientSocket,
                    localAddress,
                    remote,
                    Poller_));
            } else {
                auto connector = New<TDialSession>(
                    clientSocket,
                    remote,
                    Poller_);
                return connector->ToFuture();
            }
        } catch (const std::exception&) {
            YCHECK(TryClose(clientSocket, false));
            throw;
        }
    }

private:
    const IPollerPtr Poller_;
};

DEFINE_REFCOUNTED_TYPE(TDialer);

////////////////////////////////////////////////////////////////////////////////

IDialerPtr CreateDialer(const IPollerPtr& poller)
{
    return New<TDialer>(poller);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNet
} // namespace NYT
