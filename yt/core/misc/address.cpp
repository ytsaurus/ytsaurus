#include "stdafx.h"
#include "address.h"
#include "lazy_ptr.h"

#include <core/concurrency/action_queue.h>

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

static const auto WarningDuration = TDuration::MilliSeconds(100);
static const Stroka FailedLocalHostName("<unknown>");

// TOOD(babenko): get rid of this, write truly asynchronous address resolver.
static TLazyIntrusivePtr<TActionQueue> AddressResolverQueue(TActionQueue::CreateFactory("AddressResolver"));

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
    memset(&Storage, 0, sizeof (Storage));
    Storage.ss_family = AF_UNSPEC;
    Length = sizeof (Storage);
}

TNetworkAddress::TNetworkAddress(const TNetworkAddress& other, int port)
{
    memcpy(&Storage, &other.Storage, sizeof (Storage));
    switch (Storage.ss_family) {
        case AF_INET:
            reinterpret_cast<sockaddr_in*>(&Storage)->sin_port = htons(port);
            Length = sizeof (sockaddr_in);
            break;
        case AF_INET6:
            reinterpret_cast<sockaddr_in6*>(&Storage)->sin6_port = htons(port);
            Length = sizeof (sockaddr_in6);
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
            return sizeof (sockaddr_un);
#endif
        case AF_INET:
            return sizeof (sockaddr_in);
        case AF_INET6:
            return sizeof (sockaddr_in6);
        default:
            // Don't know its actual size, report the maximum possible.
            return sizeof (sockaddr_storage);
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
    return TryParse(address).ValueOrThrow();
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

TAddressResolver::TAddressResolver()
    : Config(New<TAddressResolverConfig>())
{ }

TAddressResolver* TAddressResolver::Get()
{
    return Singleton<TAddressResolver>();
}

void TAddressResolver::Shutdown()
{
    if (AddressResolverQueue.HasValue()) {
        AddressResolverQueue->Shutdown();
    }
}

TFuture<TNetworkAddress> TAddressResolver::Resolve(const Stroka& address)
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
        TGuard<TSpinLock> guard(CacheLock);
        auto it = Cache.find(address);
        if (it != Cache.end()) {
            auto result = it->second;
            guard.Release();
            LOG_DEBUG("Address cache hit: %v -> %v",
                address,
                result);
            return MakeFuture(result);
        }
    }

    // Run async resolution.
    return BIND(&TAddressResolver::DoResolve, this, address)
        .AsyncVia(AddressResolverQueue->GetInvoker())
        .Run();
}

TNetworkAddress TAddressResolver::DoResolve(const Stroka& hostName)
{
    try {
        addrinfo hints;
        memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_UNSPEC;    // Allow both IPv4 and IPv6 addresses.
        hints.ai_socktype = SOCK_STREAM;

        addrinfo* addrInfo = nullptr;

        LOG_DEBUG("Started resolving host %v", hostName);

        NProfiling::TScopedTimer timer;

        int gaiResult;
        PROFILE_TIMING("/dns_resolve_time") {
            gaiResult = getaddrinfo(
                ~hostName,
                nullptr,
                &hints,
                &addrInfo);
        }

        auto duration = timer.GetElapsed();

        if (gaiResult != 0) {
            auto gaiError = TError(Stroka(gai_strerror(gaiResult)))
                << TErrorAttribute("errno", gaiResult);
            THROW_ERROR_EXCEPTION("Failed to resolve host %v", hostName)
                 << gaiError;
        } else if (duration > WarningDuration) {
            LOG_WARNING("DNS resolve took too long (Host: %v, Duration: %v)",
                hostName,
                duration);
        }

        TNullable<TNetworkAddress> result;

        for (const auto* currentInfo = addrInfo; currentInfo; currentInfo = currentInfo->ai_next) {
            if ((currentInfo->ai_family == AF_INET && Config->EnableIPv4) ||
                (currentInfo->ai_family == AF_INET6 && Config->EnableIPv6))
            {
                result = TNetworkAddress(*currentInfo->ai_addr);
                break;
            }
        }

        freeaddrinfo(addrInfo);

        if (result) {
            // Put result into the cache.
            {
                TGuard<TSpinLock> guard(CacheLock);
                Cache[hostName] = *result;
            }
            LOG_DEBUG("Host resolved: %v -> %v",
                hostName,
                *result);
            return *result;
        }

        THROW_ERROR_EXCEPTION("No IPv4 or IPv6 address can be found for %v", hostName);
    } catch (const std::exception& ex) {
        LOG_WARNING(TError(ex));
        throw;
    }
}

Stroka TAddressResolver::GetLocalHostName()
{
    if (GetLocalHostNameFailed) {
        return FailedLocalHostName;
    }

    {
        TGuard<TSpinLock> guard(CachedLocalHostNameLock);
        if (!CachedLocalHostName.empty()) {
            return CachedLocalHostName;
        }
    }

    Stroka result;
    try {
        result = DoGetLocalHostName();
    } catch (const std::exception& ex) {
        GetLocalHostNameFailed = true;
        LOG_ERROR(ex, "Unable to determine localhost FQDN");
        return FailedLocalHostName;
    }

    {
        TGuard<TSpinLock> guard(CachedLocalHostNameLock);
        if (CachedLocalHostName.empty()) {
            CachedLocalHostName = result;
        }
        // Sometimes the local DNS resolver crashes when the program is still running.
        // This can prevent our services from working properly (e.g. all spawned jobs will fail).
        // To avoid this, we run periodic checks to see if localhost can still be resolved.
        if (!LocalHostChecker) {
            LocalHostChecker = New<TPeriodicExecutor>(
                AddressResolverQueue->GetInvoker(),
                BIND(&TAddressResolver::CheckLocalHostResolution, this),
                TDuration::Minutes(1),
                EPeriodicExecutorMode::Automatic,
                TDuration::Minutes(1));
        }
    }

    return result;
}

Stroka TAddressResolver::DoGetLocalHostName()
{
    char hostName[1024];
    memset(hostName, 0, sizeof (hostName));

    if (gethostname(hostName, sizeof (hostName) - 1) == -1) {
        THROW_ERROR_EXCEPTION("gethostname failed")
            << TError::FromSystem();
    }

    LOG_INFO("Localhost reported by gethostname: %v", hostName);

    addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;    // Allow both IPv4 and IPv6 addresses.
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags |= AI_CANONNAME;

    addrinfo* addrInfo = nullptr;

    int gaiResult = getaddrinfo(
        hostName,
        nullptr,
        &hints,
        &addrInfo);

    if (gaiResult != 0) {
        GetLocalHostNameFailed = true;
        auto gaiError = TError(Stroka(gai_strerror(gaiResult)))
            << TErrorAttribute("errno", gaiResult);
        THROW_ERROR_EXCEPTION("getaddrinfo failed")
            << gaiError;
    }

    char* canonname = nullptr;
    if (addrInfo) {
        canonname = addrInfo->ai_canonname;
    }

    for (auto* currentInfo = addrInfo; currentInfo; currentInfo = currentInfo->ai_next) {
        if ((currentInfo->ai_family == AF_INET && Config->EnableIPv4) ||
            (currentInfo->ai_family == AF_INET6 && Config->EnableIPv6))
        {
            Stroka fqdn(canonname);
            LOG_INFO("Localhost FQDN reported by getaddrinfo: %v", fqdn);
            return fqdn;
        }
    }

    freeaddrinfo(addrInfo);

    THROW_ERROR_EXCEPTION("No matching addrinfo entry found");
}

void TAddressResolver::CheckLocalHostResolution()
{
    try {
        DoGetLocalHostName();
    } catch (const std::exception& ex) {
        LOG_FATAL(ex, "Localhost has failed to resolve");
    }
}

void TAddressResolver::PurgeCache()
{
    {
        TGuard<TSpinLock> guard(CacheLock);
        Cache.clear();
    }
    LOG_INFO("Address cache purged");
}

void TAddressResolver::Configure(TAddressResolverConfigPtr config)
{
    Config = config;

    if (config->LocalHostFqdn) {
        TGuard<TSpinLock> guard(CachedLocalHostNameLock);
        CachedLocalHostName = *config->LocalHostFqdn;
        LOG_INFO("LocalHost FQDN configured: %v", CachedLocalHostName);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

