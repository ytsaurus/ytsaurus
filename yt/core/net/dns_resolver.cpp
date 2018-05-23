#include "dns_resolver.h"

#include <yt/core/actions/future.h>
#include <yt/core/actions/invoker.h>

#include <yt/core/net/address.h>

#include <yt/core/misc/proc.h>
#include <yt/core/misc/mpsc_queue.h>

#include <yt/core/concurrency/notification_handle.h>
#include <yt/core/concurrency/delayed_executor.h>

#include <yt/core/logging/log_manager.h>

#include <contrib/libs/c-ares/ares.h>

#ifdef _linux_
#define YT_DNS_RESOLVER_USE_EPOLL
#endif

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>

#ifdef YT_DNS_RESOLVER_USE_EPOLL
#include <sys/epoll.h>
#else
#include <sys/select.h>
#endif

////////////////////////////////////////////////////////////////////////////////

void FormatValue(NYT::TStringBuilder* builder, struct hostent* hostent, TStringBuf spec)
{
    bool empty = true;

    auto appendIf = [&] (bool condition, auto function) {
        if (condition) {
            if (!empty) {
                builder->AppendString(", ");
            }
            function();
            empty = false;
        }
    };

    builder->AppendString("{");

    appendIf(hostent->h_name, [&] {
        builder->AppendString("Canonical: ");
        builder->AppendString(hostent->h_name);
    });

    appendIf(hostent->h_aliases, [&] {
        builder->AppendString("Aliases: {");
        for (int i = 0; hostent->h_aliases[i]; ++i) {
            if (i > 0) {
                builder->AppendString(", ");
            }
            builder->AppendString(hostent->h_aliases[i]);
        }
        builder->AppendString("}");
    });

    appendIf(hostent->h_addr_list, [&] {
        auto stringSize = 0;
        if (hostent->h_addrtype == AF_INET) {
            stringSize = INET_ADDRSTRLEN;
        }
        if (hostent->h_addrtype == AF_INET6) {
            stringSize = INET6_ADDRSTRLEN;
        }
        builder->AppendString("Addresses: {");
        for (int i = 0; hostent->h_addr_list[i]; ++i) {
            if (i > 0) {
                builder->AppendString(", ");
            }
            auto string = builder->Preallocate(stringSize);
            ares_inet_ntop(
                hostent->h_addrtype,
                hostent->h_addr_list[i],
                string,
                stringSize);
            builder->Advance(strlen(string));
        }
        builder->AppendString("}");
    });

    builder->AppendString("}");
}

////////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace NNet {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto Logger = NLogging::TLogger("Dns");

////////////////////////////////////////////////////////////////////////////////

class TDnsResolver::TImpl
{
public:
    TImpl(
        int retries,
        TDuration resolveTimeout,
        TDuration maxResolveTimeout,
        TDuration warningTimeout);
    ~TImpl();

    void Start();
    void Stop();

    TFuture<TNetworkAddress> ResolveName(
        TString hostname,
        bool enableIPv4,
        bool enableIPv6);

private:
    const int Retries_;
    const TDuration ResolveTimeout_;
    const TDuration MaxResolveTimeout_;
    const TDuration WarningTimeout_;

    struct TNameRequest
    {
        TImpl* Owner;
        TGuid RequestId;
        TPromise<TNetworkAddress> Promise;
        TString HostName;
        bool EnableIPv4;
        bool EnableIPv6;
        NProfiling::TWallTimer Timer;
        TDelayedExecutorCookie TimeoutCookie;

        TMpscQueueHook QueueHook;
    };

    using TNameRequestQueue = TMpscQueue<TNameRequest, &TNameRequest::QueueHook>;

    TNameRequestQueue Queue_;

    TThread ResolverThread_;
#ifdef YT_DNS_RESOLVER_USE_EPOLL
    int EpollFD_ = -1;
#endif
    TNotificationHandle WakeupHandle_;
    std::atomic<bool> Alive_ = {true};

    ares_channel Channel_;
    ares_options Options_;

    static std::atomic_flag LibraryLock_;

    static void* ResolverThreadMain(void* opaque);
    void ResolverThreadMain();

    static int OnSocketCreated(ares_socket_t socket, int type, void* opaque);
    static void OnSocketStateChanged(void* opaque, ares_socket_t socket, int readable, int writable);
    static void OnNameResolution(void* opaque, int status, int timeouts, struct hostent* hostent);
};

std::atomic_flag TDnsResolver::TImpl::LibraryLock_ = ATOMIC_FLAG_INIT;

TDnsResolver::TImpl::TImpl(
    int retries,
    TDuration resolveTimeout,
    TDuration maxResolveTimeout,
    TDuration warningTimeout)
    : Retries_(retries)
    , ResolveTimeout_(resolveTimeout)
    , MaxResolveTimeout_(maxResolveTimeout)
    , WarningTimeout_(warningTimeout)
    , ResolverThread_(&ResolverThreadMain, this)
{
#ifdef YT_DNS_RESOLVER_USE_EPOLL
    EpollFD_ = HandleEintr(epoll_create1, EPOLL_CLOEXEC);
    YCHECK(EpollFD_ >= 0);
#endif
    
    int wakeupFD = WakeupHandle_.GetFD();
    OnSocketCreated(wakeupFD, AF_UNSPEC, this);
    OnSocketStateChanged(this, wakeupFD, 1, 0);

    // Init library globals.
    // c-ares 1.10+ provides recursive behaviour of init/cleanup.
    while (LibraryLock_.test_and_set(std::memory_order_acquire));
    YCHECK(ares_library_init(ARES_LIB_INIT_ALL) == ARES_SUCCESS);
    LibraryLock_.clear(std::memory_order_release);

    memset(&Channel_, 0, sizeof(Channel_));
    memset(&Options_, 0, sizeof(Options_));

    // See https://c-ares.haxx.se/ares_init_options.html for full details.
    int mask = 0;
    Options_.flags |= ARES_FLAG_STAYOPEN;
    mask |= ARES_OPT_FLAGS;
    Options_.timeout = static_cast<int>(ResolveTimeout_.MilliSeconds());
    mask |= ARES_OPT_TIMEOUTMS;
#ifndef YT_IN_ARCADIA
    Options_.maxtimeout = static_cast<int>(MaxResolveTimeout_.MilliSeconds());
    mask |= ARES_OPT_MAXTIMEOUTMS;
#endif
    Options_.tries = Retries_;
    mask |= ARES_OPT_TRIES;
    Options_.sock_state_cb = &TDnsResolver::TImpl::OnSocketStateChanged;
    Options_.sock_state_cb_data = this;
    mask |= ARES_OPT_SOCK_STATE_CB;

    YCHECK(ares_init_options(&Channel_, &Options_, mask) == ARES_SUCCESS);

    ares_set_socket_callback(Channel_, &TDnsResolver::TImpl::OnSocketCreated, this);
}

TDnsResolver::TImpl::~TImpl()
{
    if (Alive_.load(std::memory_order_relaxed)) {
        Stop();
    }

    ares_destroy(Channel_);

    // Cleanup library globals.
    // c-ares 1.10+ provides recursive behaviour of init/cleanup.
    while (LibraryLock_.test_and_set(std::memory_order_acquire));
    ares_library_cleanup();
    LibraryLock_.clear(std::memory_order_release);

#ifdef YT_DNS_RESOLVER_USE_EPOLL
    YCHECK(HandleEintr(close, EpollFD_) == 0);
#endif
}

void TDnsResolver::TImpl::Start()
{
    YCHECK(Alive_.load(std::memory_order_relaxed));
    ResolverThread_.Start();
}

void TDnsResolver::TImpl::Stop()
{
    YCHECK(Alive_.load(std::memory_order_relaxed));
    Alive_.store(false, std::memory_order_relaxed);
    WakeupHandle_.Raise();
    ResolverThread_.Join();
}

TFuture<TNetworkAddress> TDnsResolver::TImpl::ResolveName(
    TString hostName,
    bool enableIPv4,
    bool enableIPv6)
{
    if (!Alive_.load(std::memory_order_relaxed)) {
        return MakeFuture<TNetworkAddress>(TError("DNS resolver is not alive"));
    }

    auto promise = NewPromise<TNetworkAddress>();
    auto future = promise.ToFuture();

    auto requestId = TGuid::Create();
    auto timeoutCookie = TDelayedExecutor::Submit(
        BIND([promise, requestId] () mutable {
            LOG_WARNING("Resolve timed out (RequestId: %v)",
                requestId);
            promise.TrySet(TError("Resolve timed out"));
        }),
        MaxResolveTimeout_);

    LOG_DEBUG("Resolving host name (RequestId: %v, Host: %v, EnableIPv4: %v, EnableIPv6: %v)",
        requestId,
        hostName,
        enableIPv4,
        enableIPv6);

    auto request = std::unique_ptr<TNameRequest>{new TNameRequest{
        this,
        requestId,
        std::move(promise),
        std::move(hostName),
        enableIPv4,
        enableIPv6,
        {},
        std::move(timeoutCookie),
        {}}};

    Queue_.Push(std::move(request));

    WakeupHandle_.Raise();

    return future;
}

void* TDnsResolver::TImpl::ResolverThreadMain(void* opaque)
{
    reinterpret_cast<TDnsResolver::TImpl*>(opaque)->ResolverThreadMain();
    return nullptr;
}

void TDnsResolver::TImpl::ResolverThreadMain()
{
    TThread::CurrentThreadSetName("DnsResolver");

    constexpr size_t MaxRequestsPerDrain = 100;

    auto drainQueue = [&] {
        for (size_t iteration = 0; iteration < MaxRequestsPerDrain; ++iteration) {
            auto request = Queue_.Pop();
            if (!request) {
                return true;
            }

            // Try to reduce number of lookups to save some time.
            int family = AF_UNSPEC;
            if (request->EnableIPv4 && !request->EnableIPv6) {
                family = AF_INET;
            }
            if (request->EnableIPv6 && !request->EnableIPv4) {
                family = AF_INET6;
            }

            ares_gethostbyname(
                Channel_,
                request->HostName.c_str(),
                family,
                &OnNameResolution,
                request.get());
            // Releasing unique_ptr on a separate line,
            // because argument evaluation order is not specified.
            request.release();
        }
        return false;
    };

    while (Alive_.load(std::memory_order_relaxed)) {
        bool drain = false;
        constexpr int PollTimeoutMs = 1000;

    #ifdef YT_DNS_RESOLVER_USE_EPOLL
        constexpr size_t MaxEventsPerPoll = 10;
        struct epoll_event events[MaxEventsPerPoll];
        int count = HandleEintr(epoll_wait, EpollFD_, events, MaxEventsPerPoll, PollTimeoutMs);
        YCHECK(count >= 0);

        if (count == 0) {
            ares_process_fd(Channel_, ARES_SOCKET_BAD, ARES_SOCKET_BAD);
        } else {
            // According to c-ares implementation this loop would cost O(#dns-servers-total * #dns-servers-active).
            // Hope that we are not creating too many connections!
            for (int i = 0; i < count; ++i) {
                int triggeredFD = events[i].data.fd;
                if (triggeredFD == WakeupHandle_.GetFD()) {
                    drain = true;
                } else {
                    int readFD = (events[i].events & EPOLLIN) ? triggeredFD : ARES_SOCKET_BAD;
                    int writeFD = (events[i].events & EPOLLOUT) ? triggeredFD : ARES_SOCKET_BAD;
                    ares_process_fd(Channel_, readFD, writeFD);
                }
            }
        }
#else
        fd_set readFDs, writeFDs;
        FD_ZERO(&readFDs);
        FD_ZERO(&writeFDs);

        int wakeupFD = WakeupHandle_.GetFD();
        int nFDs = ares_fds(Channel_, &readFDs, &writeFDs);
        nFDs = std::max(nFDs, 1 + wakeupFD);
        FD_SET(wakeupFD, &readFDs);

        YCHECK(nFDs <= FD_SETSIZE); // This is inherent limitation by select().

        timeval timeout;
        timeout.tv_sec = PollTimeoutMs / 1000;
        timeout.tv_usec = (PollTimeoutMs % 1000) * 1000;

        int result = select(nFDs, &readFDs, &writeFDs, nullptr, &timeout);
        YCHECK(result >= 0);

        ares_process(Channel_, &readFDs, &writeFDs);

        if (FD_ISSET(wakeupFD, &readFDs)) {
            drain = true;
        }
#endif

        if (drain && drainQueue()) {
            WakeupHandle_.Clear();
            while (!drainQueue());
        }
    }

    // Make sure that there are no more enqueued requests.
    // We have observed `Alive_ == false` previously,
    // which implies that producers no longer pushing to the queue.
    while (!drainQueue());
    // Cancel out all pending requests.
    ares_cancel(Channel_);
}

int TDnsResolver::TImpl::OnSocketCreated(
    ares_socket_t socket,
    int /*type*/,
    void* opaque)
{
    int result = 0;
#ifdef YT_DNS_RESOLVER_USE_EPOLL
    auto this_ = static_cast<TDnsResolver::TImpl*>(opaque);
    struct epoll_event event{0, {0}};
    event.data.fd = socket;
    result = epoll_ctl(this_->EpollFD_, EPOLL_CTL_ADD, socket, &event);
    if (result != 0) {
        LOG_WARNING(TError::FromSystem(), "epoll_ctl() failed");
        result = -1;
    }
#else
    if (socket >= FD_SETSIZE) {
        LOG_WARNING("File descriptor is out of valid range (FD: %v, Limit: %v)",
            socket,
            FD_SETSIZE);
        result = -1;
    }
#endif
    return result;
}

void TDnsResolver::TImpl::OnSocketStateChanged(
    void* opaque,
    ares_socket_t socket,
    int readable,
    int writable)
{
#ifdef YT_DNS_RESOLVER_USE_EPOLL
    auto this_ = static_cast<TDnsResolver::TImpl*>(opaque);
    struct epoll_event event{0, {0}};
    event.data.fd = socket;
    int op = EPOLL_CTL_MOD;
    if (readable) {
        event.events |= EPOLLIN;
    }
    if (writable) {
        event.events |= EPOLLOUT;
    }
    if (!readable && !writable) {
        op = EPOLL_CTL_DEL;
    }
    YCHECK(epoll_ctl(this_->EpollFD_, op, socket, &event) == 0);
#else
    YCHECK(socket < FD_SETSIZE);
#endif
}

void TDnsResolver::TImpl::OnNameResolution(
    void* opaque,
    int status,
    int timeouts,
    struct hostent* hostent)
{
    // TODO(sandello): Protect against exceptions here?
    auto request = std::unique_ptr<TNameRequest>{static_cast<TNameRequest*>(opaque)};

    TDelayedExecutor::CancelAndClear(request->TimeoutCookie);

    auto elapsed = request->Timer.GetElapsedTime();
    if (elapsed > request->Owner->WarningTimeout_ || timeouts > 0) {
        LOG_WARNING("Resolve took too long (RequestId: %v, HostName: %v, Duration: %v, Timeouts: %v)",
            request->RequestId,
            request->HostName,
            elapsed,
            timeouts);
    }

    if (status != ARES_SUCCESS) {
        LOG_WARNING("Resolve failed (RequestId: %v, HostName: %v)",
            request->RequestId,
            request->HostName);
        request->Promise.TrySet(TError("DNS resolve failed for %Qv",
            request->HostName)
            << TError(ares_strerror(status)));
        return;
    }

    YCHECK(hostent->h_addrtype == AF_INET || hostent->h_addrtype == AF_INET6);
    YCHECK(hostent->h_addr_list && hostent->h_addr_list[0]);

    TNetworkAddress result(hostent->h_addrtype, hostent->h_addr, hostent->h_length);
    LOG_DEBUG("Host name resolved (RequestId: %v, HostName: %v, Result: %v, Hostent: %v)",
        request->RequestId,
        request->HostName,
        result,
        hostent);

    request->Promise.TrySet(result);
}

////////////////////////////////////////////////////////////////////////////////

TDnsResolver::TDnsResolver(
    int retries,
    TDuration resolveTimeout,
    TDuration maxResolveTimeout,
    TDuration warningTimeout)
    : Impl_(std::make_unique<TImpl>(
        retries,
        resolveTimeout,
        maxResolveTimeout,
        warningTimeout))
{ }

TDnsResolver::~TDnsResolver() = default;

void TDnsResolver::Start()
{
    Impl_->Start();
}

void TDnsResolver::Stop()
{
    Impl_->Stop();
}

TFuture<TNetworkAddress> TDnsResolver::ResolveName(
    TString hostName,
    bool enableIPv4,
    bool enableIPv6)
{
    return Impl_->ResolveName(std::move(hostName), enableIPv4, enableIPv6);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNet
} // namespace NYT

