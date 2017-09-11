#include "dialer.h"
#include "connection.h"
#include "config.h"

#include <yt/core/concurrency/poller.h>

#include <yt/core/misc/proc.h>
#include <yt/core/misc/socket.h>

#include <util/random/random.h>

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
            clientSocket = CreateTcpClientSocket(family);
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

class TAsyncDialerSession
    : public IAsyncDialerSession
{
public:
    TAsyncDialerSession(
        TDialerConfigPtr config,
        IPollerPtr poller,
        const NLogging::TLogger& logger,
        const TNetworkAddress& address,
        TAsyncDialerCallback onFinished)
        : Config_(std::move(config))
        , Poller_(std::move(poller))
        , Address_(address)
        , OnFinished_(std::move(onFinished))
        , Id_(TGuid::Create())
        , Logger(NLogging::TLogger(logger)
            .AddTag("TAsyncDialerSession: %v", Id_))
        , Timeout_(Config_->MinRto * GetRandomVariation())
    { }

    ~TAsyncDialerSession()
    {
        TGuard<TSpinLock> guard(SpinLock_);
        Finished_ = true;
        CloseSocket();
    }

    virtual void Dial() override
    {
        TGuard<TSpinLock> guard(SpinLock_);

        YCHECK(!Dialed_);
        Dialed_ = true;

        Connect();
        if (Finished_) {
            guard.Release();
            Finish();
        }
    }

private:
    class TPollable
        : public NConcurrency::IPollable
    {
    public:
        TPollable(TAsyncDialerSession* owner, const TGuid& id, int socket)
            : Owner_(MakeWeak(owner))
            , LoggingId_(Format("TAsyncDialerSession:%v:%v", id, socket))
        { }

        virtual const TString& GetLoggingId() const override
        {
            return LoggingId_;
        }

        virtual void OnEvent(EPollControl control) override
        {
            if (auto owner = Owner_.Lock()) {
                owner->OnConnected(this);
            }
        }

        virtual void OnShutdown() override
        { }

    private:
        const TWeakPtr<TAsyncDialerSession> Owner_;
        const TString LoggingId_;
    };

    const TDialerConfigPtr Config_;
    const IPollerPtr Poller_;
    const TNetworkAddress Address_;
    const TAsyncDialerCallback OnFinished_;
    const TGuid Id_;
    const NLogging::TLogger Logger;

    SOCKET Socket_ = INVALID_SOCKET;
    bool Dialed_ = false;
    TError Error_;
    std::atomic<bool> Finished_{false};
    TSpinLock SpinLock_;
    TDuration Timeout_;
    NConcurrency::TDelayedExecutorCookie TimeoutCookie_;
    TIntrusivePtr<TPollable> Pollable_;

    void CloseSocket()
    {
        if (Socket_ != INVALID_SOCKET) {
            close(Socket_);
            Socket_ = INVALID_SOCKET;
        }
    }

    void RegisterPollable()
    {
        Pollable_ = New<TPollable>(this, Id_, Socket_);
        Poller_->Register(Pollable_);
        Poller_->Arm(Socket_, Pollable_, EPollControl::Read|EPollControl::Write);
    }

    void UnregisterPollable()
    {
        Poller_->Unarm(Socket_);
        Poller_->Unregister(Pollable_);
        Pollable_.Reset();
    }

    void Connect()
    {
        try {
            int family = Address_.GetSockAddr()->sa_family;

            YCHECK(Socket_ == INVALID_SOCKET);
            if (Address_.GetSockAddr()->sa_family == AF_UNIX) {
                Socket_ = CreateUnixClientSocket();
            } else {
                Socket_ = CreateTcpClientSocket(family);
            }

            if (Config_->EnableNoDelay && family != AF_UNIX) {
                if (Config_->EnableNoDelay) {
                    SetSocketNoDelay(Socket_);
                }

                SetSocketPriority(Socket_, Config_->Priority);
                SetSocketKeepAlive(Socket_);
            }

            if (::NYT::ConnectSocket(Socket_, Address_) == 0) {
                Finished_ = true;
                return;
            }

            if (Config_->EnableAggressiveReconnect) {
               TimeoutCookie_ = NConcurrency::TDelayedExecutor::Submit(
                        BIND(&TAsyncDialerSession::OnTimeout, MakeWeak(this)),
                        Timeout_);
            }

            RegisterPollable();
        } catch (const std::exception& ex) {
            Error_ = TError(ex);
            CloseSocket();
            Finished_ = true;
        }
    }

    void Finish()
    {
        Y_ASSERT(Finished_);
        if (Socket_ == INVALID_SOCKET) {
            OnFinished_(INVALID_SOCKET, Error_);
        } else {
            auto socket = Socket_;
            Socket_ = INVALID_SOCKET;
            OnFinished_(socket, TError());
        }
    }

    void OnConnected(TIntrusivePtr<TPollable> pollable)
    {
        if (Finished_.load(std::memory_order_relaxed)) {
            return;
        }

        TGuard<TSpinLock> guard(SpinLock_);

        if (Finished_ || pollable != Pollable_) {
            return;
        }

        NConcurrency::TDelayedExecutor::CancelAndClear(TimeoutCookie_);
        UnregisterPollable();
        Finished_ = true;
        Finish();
    }

    void OnTimeout()
    {
        if (Finished_.load(std::memory_order_relaxed)) {
            return;
        }

        TGuard<TSpinLock> guard(SpinLock_);

        if (Finished_) {
            return;
        }

        UnregisterPollable();
        CloseSocket();

        if (Timeout_ < Config_->MaxRto) {
            Timeout_ *= Config_->RtoScale * GetRandomVariation();
        }

        LOG_DEBUG("Connect timeout, trying to reconnect (Timeout: %v)", Timeout_);

        Connect();
        if (Finished_) {
            guard.Release();
            Finish();
        }
    }

    static float GetRandomVariation()
    {
        return (0.9 + RandomNumber<float>() / 5);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TAsyncDialer
    : public IAsyncDialer
{
public:
    TAsyncDialer(
        TDialerConfigPtr config,
        IPollerPtr poller,
        const NLogging::TLogger& logger)
        : Config_(std::move(config))
        , Poller_(std::move(poller))
        , Logger(logger)
    { }

    virtual IAsyncDialerSessionPtr CreateSession(
        const TNetworkAddress& address,
        TAsyncDialerCallback onFinished) override
    {
        return New<TAsyncDialerSession>(
            Config_,
            Poller_,
            Logger,
            address,
            std::move(onFinished));
    }

private:
    const TDialerConfigPtr Config_;
    const IPollerPtr Poller_;
    const NLogging::TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

IAsyncDialerPtr CreateAsyncDialer(
    TDialerConfigPtr config,
    IPollerPtr poller,
    const NLogging::TLogger& logger)
{
    return New<TAsyncDialer>(
        std::move(config),
        std::move(poller),
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNet
} // namespace NYT
