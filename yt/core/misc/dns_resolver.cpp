#include "dns_resolver.h"

#include <yt/core/actions/future.h>
#include <yt/core/actions/invoker.h>

#include <yt/core/misc/address.h>
#include <yt/core/misc/proc.h>
#include <yt/core/misc/mpsc_queue.h>

#include <yt/core/logging/log_manager.h>

#include <contrib/libs/c-ares/ares.h>

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static const auto Logger = NLogging::TLogger("Dns");

////////////////////////////////////////////////////////////////////////////////

class TDnsResolver::TImpl
{
public:
    TImpl(
        int retries,
        TDuration resolveTimeout,
        TDuration warningTimeout);
    ~TImpl();

    void Start();
    void Stop();

    TFuture<TNetworkAddress> ResolveName(
        Stroka hostname,
        bool enableIPv4,
        bool enableIPv6);

private:
    const int Retries_;
    const TDuration ResolveTimeout_;
    const TDuration WarningTimeout_;

    struct TNameRequest
    {
        TImpl* Owner;
        TPromise<TNetworkAddress> Promise;
        Stroka HostName;
        bool EnableIPv4;
        bool EnableIPv6;
        NProfiling::TCpuInstant Start;
        NProfiling::TCpuInstant Stop;

        TMpscQueueHook QueueHook;
    };

    using TNameRequestQueue = TMpscQueue<TNameRequest, &TNameRequest::QueueHook>;

    TNameRequestQueue Queue_;

    TThread ResolverThread_;
    int EpollFd = -1;
    int EventFd = -1;
    std::atomic<bool> Alive_ = {true};

    ares_channel Channel_;
    ares_options Options_;

    void RaiseNotification();
    void ClearNotification();

    static std::atomic_flag LibraryLock_;

    static void* ResolverThreadMain(void* opaque);
    void ResolverThreadMain();

    static int OnSocketCreated(int socket, int type, void* opaque);
    static void OnSocketStateChanged(void* opaque, int socket, int read, int write);
    static void OnNameResolution(void* opaque, int status, int timeouts, struct hostent* hostent);
    static void OnNameResolutionDebugLog(TStringBuilder* info, bool* empty, struct hostent* hostent);
};

std::atomic_flag TDnsResolver::TImpl::LibraryLock_ = ATOMIC_FLAG_INIT;

TDnsResolver::TImpl::TImpl(
    int retries,
    TDuration resolveTimeout,
    TDuration warningTimeout)
    : Retries_(retries)
    , ResolveTimeout_(resolveTimeout)
    , WarningTimeout_(warningTimeout)
    , ResolverThread_(&ResolverThreadMain, this)
{
    EpollFd = HandleEintr(epoll_create1, EPOLL_CLOEXEC);
    YCHECK(EpollFd >= 0);
    EventFd = HandleEintr(eventfd, 0, EFD_CLOEXEC | EFD_NONBLOCK);
    YCHECK(EventFd >= 0);

    struct epoll_event event{0, {0}};
    event.events = EPOLLIN;
    event.data.fd = EventFd;
    YCHECK(epoll_ctl(EpollFd, EPOLL_CTL_ADD, EventFd, &event) == 0);

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
    Options_.timeout = ResolveTimeout_.MilliSeconds();
    mask |= ARES_OPT_TIMEOUTMS;
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

    YCHECK(HandleEintr(close, EventFd) == 0);
    YCHECK(HandleEintr(close, EpollFd) == 0);
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
    RaiseNotification();
    ResolverThread_.Join();
}

TFuture<TNetworkAddress> TDnsResolver::TImpl::ResolveName(
    Stroka hostName,
    bool enableIPv4,
    bool enableIPv6)
{
    if (!Alive_.load(std::memory_order_relaxed)) {
        return MakeFuture<TNetworkAddress>(TError("DNS resolver is not alive"));
    }

    LOG_DEBUG(
        "Resolving host name (Host: %v, EnableIPv4: %v, EnableIPv6: %v)",
        hostName,
        enableIPv4,
        enableIPv6);

    auto promise = NewPromise<TNetworkAddress>();
    auto future = promise.ToFuture();

    auto request = std::unique_ptr<TNameRequest>{new TNameRequest{
        this,
        std::move(promise),
        std::move(hostName),
        enableIPv4,
        enableIPv6,
        NProfiling::GetCpuInstant(),
        0}};

    Queue_.Push(std::move(request));

    RaiseNotification();

    return future;
}

void TDnsResolver::TImpl::RaiseNotification()
{
    size_t one = 1;
    YCHECK(HandleEintr(write, EventFd, &one, sizeof(one)) == sizeof(one));
}

void TDnsResolver::TImpl::ClearNotification()
{
    size_t count = 0;
    YCHECK(HandleEintr(read, EventFd, &count, sizeof(count)) == sizeof(count));
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
    constexpr size_t MaxEventsPerPoll = 10;

    auto drainQueue = [this] () -> bool {
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
            // Releasing unique_ptr on a separate line, because argument evaluation order is not specified.
            request.release();
        }
        return false;
    };

    struct epoll_event events[MaxEventsPerPoll];

    while (Alive_.load(std::memory_order_relaxed)) {
        struct timeval* tvp, tv;
        tvp = ares_timeout(Channel_, nullptr, &tv);

        bool drain = false;

        int timeout = tvp
            ? tvp->tv_sec * 1000 + tvp->tv_usec / 1000
            : -1;

        int count = HandleEintr(epoll_wait, EpollFd, events, MaxEventsPerPoll, timeout);

        if (count < 0) {
            LOG_FATAL(TError::FromSystem(), "epoll_wait() failed");
        } else if (count == 0) {
            ares_process_fd(Channel_, ARES_SOCKET_BAD, ARES_SOCKET_BAD);
        } else {
            // According to c-ares implementation this loop would cost O(#dns-servers-total * #dns-servers-active).
            // Hope that we are not creating too much connections!
            for (int i = 0; i < count; ++i) {
                int fd = events[i].data.fd;
                if (fd == EventFd) {
                    drain = true;
                    continue;
                }
                int read = (events[i].events & EPOLLIN) != 0;
                int write = (events[i].events & EPOLLOUT) != 0;
                ares_process_fd(Channel_, read ? fd : ARES_SOCKET_BAD, write ? fd : ARES_SOCKET_BAD);
            }
        }

        if (drain && drainQueue()) {
            ClearNotification();
        }
    }

    // Make sure that there are no more enqueued requests. We have observed `Alive_ == false` previously,
    // which implies that producers no longer pushing to the queue.
    while (!drainQueue());
    // Cancel out all pending requests.
    ares_cancel(Channel_);
}

int TDnsResolver::TImpl::OnSocketCreated(int socket, int type, void* opaque)
{
    auto this_ = static_cast<TDnsResolver::TImpl*>(opaque);
    struct epoll_event event{0, {0}};
    event.data.fd = socket;
    int result = epoll_ctl(this_->EpollFd, EPOLL_CTL_ADD, socket, &event);
    if (result < 0) {
        LOG_WARNING(TError::FromSystem(), "epoll_ctl() failed");
    }
    return result;
}

void TDnsResolver::TImpl::OnSocketStateChanged(void* opaque, int socket, int read, int write)
{
    auto this_ = static_cast<TDnsResolver::TImpl*>(opaque);
    struct epoll_event event{0, {0}};
    event.data.fd = socket;
    int op = EPOLL_CTL_MOD;
    if (read) {
        event.events |= EPOLLIN;
    }
    if (write) {
        event.events |= EPOLLOUT;
    }
    if (!read && !write) {
        op = EPOLL_CTL_DEL;
    }
    int result = epoll_ctl(this_->EpollFd, op, socket, &event);
    if (result < 0) {
        LOG_WARNING(TError::FromSystem(), "epoll_ctl() failed");
    }
}

void TDnsResolver::TImpl::OnNameResolution(
    void* opaque,
    int status,
    int timeouts,
    struct hostent* hostent)
{
    // TODO(sandello): Protect against exceptions here?
    auto request = std::unique_ptr<TNameRequest>{static_cast<TNameRequest*>(opaque)};
    request->Stop = NProfiling::GetCpuInstant();

    auto duration = NProfiling::CpuDurationToDuration(request->Stop - request->Start);
    if (duration > request->Owner->WarningTimeout_ || timeouts > 0) {
        LOG_WARNING("DNS resolve took too long (HostName: %v, Duration: %v, Timeouts: %v)",
            request->HostName,
            duration,
            timeouts);
    }

    // We will be setting promise indirectly to avoid calling arbitrary code in resolver thread.
    if (status != ARES_SUCCESS) {
        LOG_WARNING("DNS resolve failed (HostName: %v)", request->HostName);
        request->Promise.Set(TError(ares_strerror(status)));
        return;
    }

    YCHECK(hostent->h_addrtype == AF_INET || hostent->h_addrtype == AF_INET6);
    YCHECK(hostent->h_addr_list && hostent->h_addr_list[0]);
    TNetworkAddress result(hostent->h_addrtype, hostent->h_addr, hostent->h_length);

    if (Logger.IsEnabled(NLogging::ELogLevel::Debug)) {
        TStringBuilder info;
        bool empty;
        OnNameResolutionDebugLog(&info, &empty, hostent);
        LOG_DEBUG(
            "Host name resolved: %v -> %v (%v)",
            request->HostName,
            result,
            info.GetBuffer());
    }

    request->Promise.Set(result);
}

void TDnsResolver::TImpl::OnNameResolutionDebugLog(
    TStringBuilder* info,
    bool* empty,
    struct hostent* hostent)
{
    auto appendIf = [&] (bool condition, auto function) {
        if (condition) {
            if (!*empty) {
                info->AppendString(", ");
            }
            function();
            *empty = false;
        }
    };

    *empty = true;

    appendIf(hostent->h_name, [&] () {
        info->AppendString("Canonical: ");
        info->AppendString(hostent->h_name);
    });

    appendIf(hostent->h_aliases, [&] () {
        info->AppendString("Aliases: {");
        for (int i = 0; hostent->h_aliases[i]; ++i) {
            if (i > 0) {
                info->AppendString(", ");
            }
            info->AppendString(hostent->h_aliases[i]);
        }
        info->AppendString("}");
    });

    appendIf(hostent->h_addr_list, [&] () {
        auto stringSize = 0;
        if (hostent->h_addrtype == AF_INET) {
            stringSize = INET_ADDRSTRLEN;
        }
        if (hostent->h_addrtype == AF_INET6) {
            stringSize = INET6_ADDRSTRLEN;
        }
        info->AppendString("Addresses: {");
        for (int i = 0; hostent->h_addr_list[i]; ++i) {
            if (i > 0) {
                info->AppendString(", ");
            }
            auto string = info->Preallocate(stringSize);
            ares_inet_ntop(
                hostent->h_addrtype,
                hostent->h_addr_list[i],
                string,
                stringSize);
            info->Advance(strlen(string));
        }
        info->AppendString("}");
    });
}

////////////////////////////////////////////////////////////////////////////////

TDnsResolver::TDnsResolver(
    int retries,
    TDuration resolveTimeout,
    TDuration warningTimeout)
    : Impl_{new TImpl{retries, resolveTimeout, warningTimeout}}
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
    Stroka hostName,
    bool enableIPv4,
    bool enableIPv6)
{
    return Impl_->ResolveName(std::move(hostName), enableIPv4, enableIPv6);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

