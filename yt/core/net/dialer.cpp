#include "dialer.h"
#include "connection.h"
#include "config.h"

#include <yt/core/concurrency/poller.h>

#include <yt/core/misc/proc.h>
#include <yt/core/net/socket.h>

#include <util/random/random.h>

namespace NYT::NNet {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TDialSession
    : public TRefCounted
{
public:
    TDialSession(
        const TNetworkAddress& remoteAddress,
        const IAsyncDialerPtr& asyncDialer,
        IPollerPtr poller)
        : Name_(Format("dialer[%v]", remoteAddress))
        , RemoteAddress_(remoteAddress)
        , Poller_(std::move(poller))
        , Session_(asyncDialer->CreateSession(
            remoteAddress,
            BIND(&TDialSession::OnDialerFinished, MakeWeak(this))))
    {
        Session_->Dial();

        Promise_.OnCanceled(BIND([this, this_ = MakeStrong(this)] (const TError& error) {
            Promise_.TrySet(TError(NYT::EErrorCode::Canceled, "Dial canceled")
                << TErrorAttribute("dialer", Name_)
                << error);
        }));
    }

    void OnDialerFinished(const TErrorOr<SOCKET>& socketOrError)
    {
        if (socketOrError.IsOK()) {
            auto socket = socketOrError.Value();
            Promise_.TrySet(CreateConnectionFromFD(
                socket,
                GetSocketName(socket),
                RemoteAddress_,
                Poller_));
        } else {
            Promise_.TrySet(socketOrError
                << TErrorAttribute("dialer", Name_));
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
    const IAsyncDialerSessionPtr Session_;

    TPromise<IConnectionPtr> Promise_ = NewPromise<IConnectionPtr>();
};

////////////////////////////////////////////////////////////////////////////////

class TDialer
    : public IDialer
{
public:
    TDialer(
        TDialerConfigPtr config,
        IPollerPtr poller,
        const NLogging::TLogger& logger)
        : AsyncDialer_(CreateAsyncDialer(std::move(config),
            poller,
            logger))
        , Poller_(std::move(poller))
    { }

    virtual TFuture<IConnectionPtr> Dial(const TNetworkAddress& remote) override
    {
        auto session = New<TDialSession>(
            remote,
            AsyncDialer_,
            Poller_);
        return session->ToFuture();
    }

private:
    IAsyncDialerPtr AsyncDialer_;
    IPollerPtr Poller_;
};

DEFINE_REFCOUNTED_TYPE(TDialer);

////////////////////////////////////////////////////////////////////////////////

IDialerPtr CreateDialer(
    TDialerConfigPtr config,
    IPollerPtr poller,
    const NLogging::TLogger& logger)
{
    return New<TDialer>(
        std::move(config),
        std::move(poller),
        logger);
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
            .AddTag("AsyncDialerSession: %v", Id_))
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

        YT_VERIFY(!Dialed_);
        Dialed_ = true;

        Connect();
        if (Finished_) {
            guard.Release();
            Finish();
        }
    }

private:
    class TPollable
        : public IPollable
    {
    public:
        TPollable(TAsyncDialerSession* owner, TGuid id, int socket)
            : Owner_(MakeWeak(owner))
            , LoggingId_(Format("AsyncDialerSession{%v:%v}", id, socket))
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
    std::atomic<bool> Finished_ = {false};
    TSpinLock SpinLock_;
    TDuration Timeout_;
    TDelayedExecutorCookie TimeoutCookie_;
    TIntrusivePtr<TPollable> Pollable_;

    void CloseSocket()
    {
        if (Socket_ != INVALID_SOCKET) {
            YT_VERIFY(TryClose(Socket_));
            Socket_ = INVALID_SOCKET;
        }
    }

    void RegisterPollable()
    {
        Pollable_ = New<TPollable>(this, Id_, Socket_);
        Poller_->Register(Pollable_);
        Poller_->Arm(Socket_, Pollable_, EPollControl::Read | EPollControl::Write | EPollControl::EdgeTriggered);
    }

    void UnregisterPollable()
    {
        if (Socket_ != INVALID_SOCKET) {
            Poller_->Unarm(Socket_);
        }
        Poller_->Unregister(Pollable_);
        Pollable_.Reset();
    }

    void Connect()
    {
        try {
            int family = Address_.GetSockAddr()->sa_family;

            YT_VERIFY(Socket_ == INVALID_SOCKET);
            if (Address_.GetSockAddr()->sa_family == AF_UNIX) {
                Socket_ = CreateUnixClientSocket();
            } else {
                Socket_ = CreateTcpClientSocket(family);
            }

            if (Config_->EnableNoDelay && family != AF_UNIX) {
                if (Config_->EnableNoDelay) {
                    if (!TrySetSocketNoDelay(Socket_)) {
                        YT_LOG_DEBUG("Failed to set socket no delay option");
                    }
                }

                if (!TrySetSocketKeepAlive(Socket_)) {
                    YT_LOG_DEBUG("Failed to set socket keep alive option");
                }
            }

            if (ConnectSocket(Socket_, Address_) == 0) {
                Finished_ = true;
                return;
            }

            if (Config_->EnableAggressiveReconnect) {
               TimeoutCookie_ = TDelayedExecutor::Submit(
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
        YT_ASSERT(Finished_);
        if (Socket_ == INVALID_SOCKET) {
            OnFinished_(Error_);
        } else {
            auto socket = Socket_;
            Socket_ = INVALID_SOCKET;

            int error = GetSocketError(socket);
            if (error != 0) {
                YT_VERIFY(TryClose(socket, false));
                socket = INVALID_SOCKET;
                Error_ = TError(NRpc::EErrorCode::TransportError, "Connect error")
                    << TError::FromSystem(error);
            }

            OnFinished_(Error_.IsOK() ? TErrorOr<SOCKET>(socket) : TErrorOr<SOCKET>(Error_));
        }
    }

    void OnConnected(TPollable* pollable)
    {
        if (Finished_.load(std::memory_order_relaxed)) {
            return;
        }

        {
            TGuard<TSpinLock> guard(SpinLock_);

            if (Finished_ || pollable != Pollable_) {
                return;
            }

            TDelayedExecutor::CancelAndClear(TimeoutCookie_);
            Finished_ = true;
            Finish();
        }

        UnregisterPollable();
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

        YT_LOG_DEBUG("Connect timeout; trying to reconnect (Timeout: %v)",
            Timeout_);

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

} // namespace NYT::NNet
