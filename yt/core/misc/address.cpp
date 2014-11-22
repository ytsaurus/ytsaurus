#include "stdafx.h"
#include "address.h"
#include "lazy_ptr.h"

#include <core/misc/singleton.h>

#include <core/concurrency/action_queue.h>
#include <core/concurrency/periodic_executor.h>

#include <core/logging/log.h>

#include <core/profiling/profiler.h>
#include <core/profiling/scoped_timer.h>

#include <util/generic/singleton.h>

#ifdef _win_
    #include <ws2ipdef.h>
    #include <winsock2.h>
#else
    #include <netdb.h>
    #include <netinet/in.h>
    #include <arpa/inet.h>
    #include <sys/socket.h>
    #include <sys/un.h>
    #include <unistd.h>
#endif

#include <array>

namespace NYT {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("Network");
static NProfiling::TProfiler Profiler("/network");

// TOOD(babenko): get rid of this, write truly asynchronous address resolver.

////////////////////////////////////////////////////////////////////////////////

Stroka BuildServiceAddress(const TStringBuf& hostName, int port)
{
    return Stroka(hostName) + ":" + ToString(port);
}

void ParseServiceAddress(const TStringBuf& address, TStringBuf* hostName, int* port)
{
    int colonIndex = address.find_last_of(':');
    if (colonIndex == Stroka::npos) {
        THROW_ERROR_EXCEPTION("Service address %Qv is malformed, <host>:<port> format is expected",
            address);
    }

    if (hostName) {
        *hostName = address.substr(0, colonIndex);
    }

    if (port) {
        try {
            *port = FromString<int>(address.substr(colonIndex + 1));
        } catch (const std::exception) {
            THROW_ERROR_EXCEPTION("Port number in service address %Qv is malformed",
                address);
        }
    }
}

int GetServicePort(const TStringBuf& address)
{
    int result;
    ParseServiceAddress(address, nullptr, &result);
    return result;
}

TStringBuf GetServiceHostName(const TStringBuf& address)
{
    TStringBuf result;
    ParseServiceAddress(address, &result, nullptr);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TNetworkAddress::TNetworkAddress()
{
    memset(&Storage, 0, sizeof(Storage));
    Storage.ss_family = AF_UNSPEC;
    Length = sizeof(Storage);
}

TNetworkAddress::TNetworkAddress(const TNetworkAddress& other)
{
    memcpy(&Storage, &other.Storage, sizeof(Storage));
    Length = other.Length;
}

TNetworkAddress::TNetworkAddress(const TNetworkAddress& other, int port)
{
    memcpy(&Storage, &other.Storage, sizeof(Storage));
    switch (Storage.ss_family) {
        case AF_INET:
            reinterpret_cast<sockaddr_in*>(&Storage)->sin_port = htons(port);
            Length = sizeof(sockaddr_in);
            break;
        case AF_INET6:
            reinterpret_cast<sockaddr_in6*>(&Storage)->sin6_port = htons(port);
            Length = sizeof(sockaddr_in6);
            break;
        default:
            YUNREACHABLE();
    }
}

TNetworkAddress::TNetworkAddress(const sockaddr& other, socklen_t length)
{
    Length = length == 0 ? GetGenericLength(other) : length;
    memcpy(&Storage, &other, Length);
}

TNetworkAddress::~TNetworkAddress()
{ }

sockaddr* TNetworkAddress::GetSockAddr()
{
    return reinterpret_cast<sockaddr*>(&Storage);
}

const sockaddr* TNetworkAddress::GetSockAddr() const
{
    return reinterpret_cast<const sockaddr*>(&Storage);
}

socklen_t TNetworkAddress::GetGenericLength(const sockaddr& sockAddr)
{
    switch (sockAddr.sa_family) {
#ifdef _linux_
        case AF_UNIX:
            return sizeof(sockaddr_un);
#endif
        case AF_INET:
            return sizeof(sockaddr_in);
        case AF_INET6:
            return sizeof(sockaddr_in6);
        default:
            // Don't know its actual size, report the maximum possible.
            return sizeof(sockaddr_storage);
    }
}

socklen_t TNetworkAddress::GetLength() const
{
    return Length;
}

TErrorOr<TNetworkAddress> TNetworkAddress::TryParse(const TStringBuf& address)
{
    int closingBracketIndex = address.find(']');
    if (closingBracketIndex == Stroka::npos || address[0] != '[') {
        return TError("Address %Qv is malformed, expected [<addr>]:<port> or [<addr>] format",
            address);
    }

    int colonIndex = address.find(':', closingBracketIndex + 1);
    TNullable<int> port;
    if (colonIndex != Stroka::npos) {
        try {
            port = FromString<int>(address.substr(colonIndex + 1));
        } catch (const std::exception) {
            return TError("Port number in address %Qv is malformed",
                address);
        }
    }

    Stroka ipAddress = Stroka(address.substr(1, closingBracketIndex - 1));
    {
        // Try to parse as ipv4.
        struct sockaddr_in sa;
        if (inet_pton(AF_INET, ~ipAddress, &sa.sin_addr) == 1) {
            if (port) {
                sa.sin_port = htons(*port);
            }
            sa.sin_family = AF_INET;
            return TNetworkAddress(*reinterpret_cast<sockaddr*>(&sa));
        }
    }
    {
        // Try to parse as ipv6.
        struct sockaddr_in6 sa;
        if (inet_pton(AF_INET6, ipAddress.c_str(), &(sa.sin6_addr))) {
            if (port) {
                sa.sin6_port = htons(*port);
            }
            sa.sin6_family = AF_INET6;
            return TNetworkAddress(*reinterpret_cast<sockaddr*>(&sa));
        }
    }

    return TError("Address %Qv is neither a valid IPv4 nor a valid IPv6 address",
        ipAddress);
}

TNetworkAddress TNetworkAddress::Parse(const TStringBuf& address)
{
    auto result = TryParse(address);
    THROW_ERROR_EXCEPTION_IF_FAILED(result);
    return result.Value();
}

Stroka ToString(const TNetworkAddress& address, bool withPort)
{
    const auto& sockAddr = address.GetSockAddr();

    const void* ipAddr;
    int port = 0;
    bool ipv6 = false;
    switch (sockAddr->sa_family) {
#ifdef _linux_
        case AF_UNIX: {
            auto* typedAddr = reinterpret_cast<const sockaddr_un*>(sockAddr);
            return
                typedAddr->sun_path[0] == 0
                ? Format("unix://[%v]", typedAddr->sun_path + 1)
                : Format("unix://%v", typedAddr->sun_path);
        }
#endif
        case AF_INET: {
            auto* typedAddr = reinterpret_cast<const sockaddr_in*>(sockAddr);
            ipAddr = &typedAddr->sin_addr;
            port = typedAddr->sin_port;
            ipv6 = false;
            break;
        }
        case AF_INET6: {
            auto* typedAddr = reinterpret_cast<const sockaddr_in6*>(sockAddr);
            ipAddr = &typedAddr->sin6_addr;
            port = typedAddr->sin6_port;
            ipv6 = true;
            break;
        }
        default:
            return Format("unknown://family(%v)", sockAddr->sa_family);
    }

    std::array<char, 256> buffer;
    if (!inet_ntop(
        sockAddr->sa_family,
        const_cast<void*>(ipAddr),
        buffer.data(),
        buffer.size()))
    {
        return "invalid://";
    }

    Stroka result("tcp://");

    if (ipv6) {
        result.append('[');
    }

    result.append(buffer.data());

    if (ipv6) {
        result.append(']');
    }

    if (withPort) {
        result.append(':');
        result.append(ToString(ntohs(port)));
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TAddressResolver::TImpl
{
public:
    static const TDuration WarningDuration;

    // Sometimes the local DNS resolver crashes when the program is still running.
    // This can prevent our services from working properly (e.g. all spawned jobs will fail).
    // To avoid this, we run periodic checks to see if localhost can still be resolved.
    static const TDuration CheckerDuration;

    TImpl()
        : Thread_(NConcurrency::TActionQueue::CreateFactory("AddressResolver"))
        , Config_(New<TAddressResolverConfig>())
        , LocalHostChecker_(New<TPeriodicExecutor>(
            Thread_->GetInvoker(),
            BIND(&TImpl::CheckLocalHostResolution, this),
            CheckerDuration,
            EPeriodicExecutorMode::Automatic,
            CheckerDuration))
    { }

    ~TImpl()
    {
        if (Thread_.HasValue()) {
            Thread_->Shutdown();
        }
    }

    TFuture<TErrorOr<TNetworkAddress>> Resolve(const Stroka& address)
    {
        // Check if |address| parses into a valid IPv4 or IPv6 address.
        {
            auto result = TNetworkAddress::TryParse(address);
            if (result.IsOK()) {
                return MakeFuture(result);
            }
        }

        // Lookup cache.
        {
            TGuard<TSpinLock> guard(CacheLock_);
            auto it = Cache_.find(address);
            if (it != Cache_.end()) {
                auto result = it->second;
                guard.Release();
                LOG_DEBUG("Address cache hit: %v -> %v",
                    address,
                    result);
                return MakeFuture(TErrorOr<TNetworkAddress>(result));
            }
        }

        // Run async resolution.
        return
            BIND(&TImpl::DoResolve, this, address, nullptr)
            .AsyncVia(Thread_->GetInvoker())
            .Run();
    }

    Stroka GetLocalHostName()
    {
        {
            TGuard<TSpinLock> guard(LocalHostLock_);
            if (!LocalHostName_.empty()) {
                return LocalHostName_;
            }
        }

        try {
            return DoGetLocalHostName();
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Localhost has failed to resolve");
            {
                TGuard<TSpinLock> guard(LocalHostLock_);
                return LocalHostName_;
            }
        }
    }

    TNetworkAddress GetLocalHostAddress()
    {
        return Resolve(GetLocalHostName()).Get().ValueOrThrow();
    }

    void PurgeCache()
    {
        {
            TGuard<TSpinLock> guard(CacheLock_);
            Cache_.clear();
        }

        {
            TGuard<TSpinLock> guard(LocalHostLock_);
            LocalHostName_.clear();
        }

        LOG_INFO("Address cache purged");
    }

    void Configure(TAddressResolverConfigPtr config)
    {
        Config_ = std::move(config);

        PurgeCache();
    }

private:
    TErrorOr<TNetworkAddress> DoResolve(
        const Stroka& hostName,
        Stroka* canonicalName)
    {
        addrinfo hints;
        memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_UNSPEC; // Allow both IPv4 and IPv6 addresses.
        hints.ai_socktype = SOCK_STREAM;
        if (canonicalName) {
            hints.ai_flags |= AI_CANONNAME;
        }

        addrinfo* rawAddrInfo = nullptr;
        std::unique_ptr<addrinfo, decltype(&freeaddrinfo)> addrInfo(nullptr, &freeaddrinfo);

        LOG_DEBUG("Started resolving host %v", hostName);

        NProfiling::TScopedTimer timer;

        int gaiResult;
        PROFILE_TIMING("/dns_resolve_time") {
            gaiResult = getaddrinfo(
                hostName.c_str(),
                nullptr,
                &hints,
                &rawAddrInfo);
        }

        auto duration = timer.GetElapsed();
        if (duration > WarningDuration) {
            LOG_WARNING("DNS resolve took too long (Host: %v, Duration: %v)",
                hostName,
                duration);
        }

        if (gaiResult != 0) {
            auto gaiError = TError(Stroka(gai_strerror(gaiResult)))
                << TErrorAttribute("errno", gaiResult);
            auto error = TError("Failed to resolve host %v", hostName)
                << gaiError;
            LOG_WARNING(error);
            return error;
        } else {
            addrInfo.reset(rawAddrInfo);
        }

        TNullable<TNetworkAddress> result;

        if (canonicalName) {
            *canonicalName = addrInfo->ai_canonname;
        }

        for (auto* currentInfo = addrInfo.get(); currentInfo; currentInfo = currentInfo->ai_next) {
            if ((currentInfo->ai_family == AF_INET && Config_->EnableIPv4) ||
                (currentInfo->ai_family == AF_INET6 && Config_->EnableIPv6))
            {
                result = TNetworkAddress(*currentInfo->ai_addr);
                if (canonicalName && currentInfo->ai_canonname) {
                    *canonicalName = currentInfo->ai_canonname;
                }
                break;
            }
        }

        if (result) {
            // Put result into the cache.
            {
                TGuard<TSpinLock> guard(CacheLock_);
                Cache_[hostName] = result.Get();
                if (canonicalName) {
                    Cache_[*canonicalName] = result.Get();
                }
            }
            LOG_DEBUG("Host resolved: %v -> %v",
                hostName,
                result.Get());
            return result.Get();
        } else {
            TError error("No IPv4 or IPv6 address can be found for %v", hostName);
            LOG_WARNING(error);
            return error;
        }
    }

    Stroka DoGetLocalHostName()
    {
        // Prevent reentrant lookups from TError::ctor().
        {
            TGuard<TSpinLock> guard(LocalHostLock_);
            LocalHostName_ = "<unknown>";
        }

        if (Config_->LocalHostFqdn) {
            LOG_INFO("Localhost FQDN overriden to %v", Config_->LocalHostFqdn);
            return *Config_->LocalHostFqdn;
        }

        Stroka canonicalName;

        char hostName[1024] = {};
        if (gethostname(hostName, sizeof(hostName) - 1) != 0) {
            THROW_ERROR_EXCEPTION("Unable to determine localhost FQDN: gethostname failed")
                << TError::FromSystem();
        }

        LOG_INFO("Localhost FQDN reported by gethostname: %v", hostName);

        auto result = DoResolve(hostName, &canonicalName);
        if (!result.IsOK()) {
            THROW_ERROR_EXCEPTION("Unable to determine localhost FQDN: getaddrinfo failed")
                << result;
        }

        if (canonicalName.empty()) {
            THROW_ERROR_EXCEPTION("Unable to determine localhost FQDN: no matching addrinfo entry found", hostName);
        }

        {
            TGuard<TSpinLock> guard(LocalHostLock_);
            LocalHostName_ = canonicalName;
        }

        LOG_INFO("Localhost FQDN resolved by getaddrinfo: %v", canonicalName);

        return canonicalName;
    }

    void CheckLocalHostResolution()
    {
        try {
            DoGetLocalHostName();
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Localhost has failed to resolve");
        }
    }

private:
    TLazyIntrusivePtr<NConcurrency::TActionQueue> Thread_;

    TAddressResolverConfigPtr Config_;

    TSpinLock CacheLock_;
    yhash_map<Stroka, TNetworkAddress> Cache_;

    TSpinLock LocalHostLock_;
    NConcurrency::TPeriodicExecutorPtr LocalHostChecker_;
    Stroka LocalHostName_;
};

const TDuration TAddressResolver::TImpl::WarningDuration = TDuration::MilliSeconds(100);
const TDuration TAddressResolver::TImpl::CheckerDuration = TDuration::Minutes(1);

////////////////////////////////////////////////////////////////////////////////

TAddressResolver::TAddressResolver()
    : Impl_(new TImpl())
{ }

TAddressResolver::~TAddressResolver()
{ }

TFuture<TErrorOr<TNetworkAddress>> TAddressResolver::Resolve(const Stroka& address)
{
    return Impl_->Resolve(address);
}

Stroka TAddressResolver::GetLocalHostName()
{
    return Impl_->GetLocalHostName();
}

TNetworkAddress TAddressResolver::GetLocalHostAddress()
{
    return Impl_->GetLocalHostAddress();
}

void TAddressResolver::PurgeCache()
{
    return Impl_->PurgeCache();
}

void TAddressResolver::Configure(TAddressResolverConfigPtr config)
{
    return Impl_->Configure(std::move(config));
}

TAddressResolver* TAddressResolver::Get()
{
    return TSingleton::Get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

