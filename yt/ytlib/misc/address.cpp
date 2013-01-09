#include "stdafx.h"
#include "address.h"
#include "lazy_ptr.h"

#include <ytlib/actions/action_queue.h>
#include <ytlib/logging/log.h>
#include <ytlib/profiling/timing.h>

#include <util/system/hostname.h>
#include <util/generic/singleton.h>

#ifdef _win_
    #include <ws2ipdef.h>
#else
    #include <netdb.h>
    #include <netinet/in.h>
    #include <arpa/inet.h>
    #include <sys/socket.h>
    #include <sys/un.h>
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("Network");

////////////////////////////////////////////////////////////////////////////////

Stroka GetLocalHostName()
{
    static char hostName[256] = { 0 };
    static TSpinLock hostNameLock;

    TGuard<TSpinLock> guard(hostNameLock);
    if (hostName[0] == 0) {
        auto info = gethostbyname(::GetHostName());
        if (!info) {
            // TError and related stuff will call GetLocalHostName.
            // Release the spinlock to avoid deadlock here.
            strcpy(hostName, "<unknown>");
            guard.Release();

            THROW_ERROR_EXCEPTION("Unable to determine local host name");
        }
        strcpy(hostName, info->h_name);
    }
    return hostName;
}

Stroka BuildServiceAddress(const TStringBuf& hostName, int port)
{
    return Stroka(hostName) + ":" + ToString(port);
}

void ParseServiceAddress(const TStringBuf& address, TStringBuf* hostName, int* port)
{
    int colonIndex = address.find_last_of(':');
    if (colonIndex == Stroka::npos) {
        THROW_ERROR_EXCEPTION("Service address %s is malformed, <host>:<port> format is expected",
            ~Stroka(address).Quote());
    }

    if (hostName) {
        *hostName = address.substr(0, colonIndex);
    }

    if (port) {
        try {
            *port = FromString<int>(address.substr(colonIndex + 1));
        } catch (const std::exception) {
            THROW_ERROR_EXCEPTION("Port number in service address %s is malformed",
                ~Stroka(address).Quote());
        }
    }
}

int GetServicePort(const TStringBuf& address)
{
    int result;
    ParseServiceAddress(address, NULL, &result);
    return result;
}

TStringBuf GetServiceHostName(const TStringBuf& address)
{
    TStringBuf result;
    ParseServiceAddress(address, &result, NULL);
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

bool TNetworkAddress::TryParse(const TStringBuf& address, TNetworkAddress* networkAddress)
{
    try {
        *networkAddress = Parse(address);
        return true;
    } catch (...) {
        return false;
    }
}

TNetworkAddress TNetworkAddress::Parse(const TStringBuf& address)
{
    int closingBracketIndex = address.find(']');
    if (closingBracketIndex == Stroka::npos || address[0] != '[') {
        THROW_ERROR_EXCEPTION("Address %s is malformed, format [<addr>](:<port>) expected",
            ~Stroka(address).Quote());
    }

    int colonIndex = address.find(':', closingBracketIndex + 1);
    TNullable<int> port;
    if (colonIndex != Stroka::npos) {
        try {
            port = FromString<int>(address.substr(colonIndex + 1));
        } catch (const std::exception) {
            THROW_ERROR_EXCEPTION("Port number in address %s is malformed",
                ~Stroka(address).Quote());
        }
    }

    Stroka ipAddress = Stroka(address.substr(1, closingBracketIndex - 1));
    {
        // try to parse as ipv4
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
        // try to parse as ipv6
        struct sockaddr_in6 sa;
        if (inet_pton(AF_INET6, ipAddress.c_str(), &(sa.sin6_addr))) {
            if (port) {
                sa.sin6_port = htons(*port);
            }
            sa.sin6_family = AF_INET6;
            return TNetworkAddress(*reinterpret_cast<sockaddr*>(&sa));
        }
    }

    THROW_ERROR_EXCEPTION("Address %s is not a valid IPV4 or IPV6 address",
        ~Stroka(ipAddress).Quote());
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
                ? Sprintf("unix://[%s]", typedAddr->sun_path + 1)
                : Sprintf("unix://%s", typedAddr->sun_path);
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
            return Sprintf("unknown://family(%d)", sockAddr->sa_family);
    }

    char buffer[256];
    if (!inet_ntop(
        sockAddr->sa_family,
        const_cast<void*>(ipAddr),
        buffer,
        ARRAY_SIZE(buffer)))
    {
        return "invalid://";
    }

    Stroka result("tcp://");

    if (ipv6) {
        result.append('[');
    }

    result.append(buffer);

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

// TOOD(babenko): get rid of this, write truly asynchronous address resolver.
static TLazyPtr<TActionQueue> AddressResolverQueue(TActionQueue::CreateFactory("AddressResolver"));

TAddressResolver* TAddressResolver::Get()
{
    return Singleton<TAddressResolver>();
}

TFuture< TValueOrError<TNetworkAddress> > TAddressResolver::Resolve(const Stroka& hostName)
{
    // Cache lookup.
    {
        TGuard<TSpinLock> guard(SpinLock);
        auto it = Cache.find(hostName);
        if (it != Cache.end()) {
            auto result = it->second;
            guard.Release();
            LOG_DEBUG("Address cache hit: %s -> %s",
                ~hostName,
                ~ToString(result));
            return MakeFuture(TValueOrError<TNetworkAddress>(result));
        }
    }

    // Run async resolution.
    return
        BIND(&TAddressResolver::DoResolve, this, hostName)
        .AsyncVia(AddressResolverQueue->GetInvoker())
        .Run();
}

TValueOrError<TNetworkAddress> TAddressResolver::DoResolve(const Stroka& hostName)
{
    static const auto WarningDuration = TDuration::MilliSeconds(100);

    addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;    // Allow both IPv4 and IPv6 addresses.
    hints.ai_socktype = SOCK_STREAM;

    addrinfo* addrInfo = NULL;

    LOG_DEBUG("Started resolving host %s", ~hostName);

    auto startTime = NProfiling::GetCpuInstant();
    int gaiResult = getaddrinfo(
        ~hostName,
        NULL,
        &hints,
        &addrInfo);
    auto duration = NProfiling::CpuDurationToDuration(NProfiling::GetCpuInstant() - startTime);

    if (gaiResult != 0) {
        auto gaiError = TError(Stroka(gai_strerror(gaiResult)))
            << TErrorAttribute("errno", gaiResult);
        auto error = TError("Failed to resolve host %s", ~hostName)
            << gaiError;
        LOG_WARNING(error);
        return error;
    } else if (duration > WarningDuration) {
        LOG_WARNING("Too long dns lookup (Host: %s, Duration: %s)",
            ~hostName,
            ~ToString(duration));
    }

    TNullable<TNetworkAddress> result;

    for (auto* currentInfo = addrInfo; currentInfo; currentInfo = currentInfo->ai_next) {
        if (currentInfo->ai_family == AF_INET || currentInfo->ai_family == AF_INET6) {
            result = TNetworkAddress(*currentInfo->ai_addr);
            break;
        }
    }

    freeaddrinfo(addrInfo);

    if (result) {
        // Put result into the cache.
        {
            TGuard<TSpinLock> guard(SpinLock);
            Cache[hostName] = result.Get();
        }
        LOG_DEBUG("Host resolved: %s -> %s",
            ~hostName,
            ~ToString(result.Get()));
        return result.Get();
    }

    {
        TError error("No IPv4 or IPv6 address can be found for %s", ~hostName);
        LOG_WARNING(error);
        return error;
    }
}

void TAddressResolver::PurgeCache()
{
    {
        TGuard<TSpinLock> guard(SpinLock);
        Cache.clear();
    }
    LOG_INFO("Address cache purged");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

