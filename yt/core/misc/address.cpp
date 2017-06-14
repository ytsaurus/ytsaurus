#include "address.h"
#include "lazy_ptr.h"

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/dns_resolver.h>
#include <yt/core/misc/local_address.h>
#include <yt/core/misc/singleton.h>
#include <yt/core/misc/expiring_cache.h>
#include <yt/core/misc/shutdown.h>

#include <yt/core/profiling/profiler.h>
#include <yt/core/profiling/scoped_timer.h>

#include <util/generic/singleton.h>

#ifdef _win_
    #include <ws2ipdef.h>
    #include <winsock2.h>
#else
    #include <ifaddrs.h>
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

static const NLogging::TLogger Logger("Network");
static const NProfiling::TProfiler Profiler("/network");

////////////////////////////////////////////////////////////////////////////////

TString BuildServiceAddress(const TStringBuf& hostName, int port)
{
    return TString(hostName) + ":" + ToString(port);
}

void ParseServiceAddress(const TStringBuf& address, TStringBuf* hostName, int* port)
{
    int colonIndex = address.find_last_of(':');
    if (colonIndex == TString::npos) {
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
            Y_UNREACHABLE();
    }
}

TNetworkAddress::TNetworkAddress(const sockaddr& other, socklen_t length)
{
    Length = length == 0 ? GetGenericLength(other) : length;
    memcpy(&Storage, &other, Length);
}

TNetworkAddress::TNetworkAddress(int family, const char* addr, size_t size)
{
    memset(&Storage, 0, sizeof(Storage));
    Storage.ss_family = family;
    switch (Storage.ss_family) {
        case AF_INET: {
            auto* typedSockAddr = reinterpret_cast<sockaddr_in*>(&Storage);
            Y_ASSERT(size <= sizeof(sockaddr_in));
            memcpy(&typedSockAddr->sin_addr, addr, size);
            Length = sizeof(sockaddr_in);
            break;
        }
        case AF_INET6: {
            auto* typedSockAddr = reinterpret_cast<sockaddr_in6*>(&Storage);
            Y_ASSERT(size <= sizeof(sockaddr_in6));
            memcpy(&typedSockAddr->sin6_addr, addr, size);
            Length = sizeof(sockaddr_in6);
            break;
        }
        default:
            Y_UNREACHABLE();
    }
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
#ifdef _unix_
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
    if (closingBracketIndex == TString::npos || address.empty() || address[0] != '[') {
        return TError("Address %Qv is malformed, expected [<addr>]:<port> or [<addr>] format",
            address);
    }

    int colonIndex = address.find(':', closingBracketIndex + 1);
    TNullable<int> port;
    if (colonIndex != TString::npos) {
        try {
            port = FromString<int>(address.substr(colonIndex + 1));
        } catch (const std::exception) {
            return TError("Port number in address %Qv is malformed",
                address);
        }
    }

    auto ipAddress = TString(address.substr(1, closingBracketIndex - 1));
    {
        // Try to parse as ipv4.
        struct sockaddr_in sa;
        if (inet_pton(AF_INET, ipAddress.c_str(), &sa.sin_addr) == 1) {
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

TString ToString(const TNetworkAddress& address, bool withPort)
{
    const auto& sockAddr = address.GetSockAddr();

    const void* ipAddr;
    int port = 0;
    bool ipv6 = false;
    switch (sockAddr->sa_family) {
#ifdef _unix_
        case AF_UNIX: {
            const auto* typedAddr = reinterpret_cast<const sockaddr_un*>(sockAddr);
            return typedAddr->sun_path[0] == 0
                ? Format("unix://[%v]", typedAddr->sun_path + 1)
                : Format("unix://%v", typedAddr->sun_path);
        }
#endif
        case AF_INET: {
            const auto* typedAddr = reinterpret_cast<const sockaddr_in*>(sockAddr);
            ipAddr = &typedAddr->sin_addr;
            port = typedAddr->sin_port;
            ipv6 = false;
            break;
        }
        case AF_INET6: {
            const auto* typedAddr = reinterpret_cast<const sockaddr_in6*>(sockAddr);
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

    TString result("tcp://");

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

bool operator == (const TNetworkAddress& lhs, const TNetworkAddress& rhs)
{
    auto lhsAddr = lhs.GetSockAddr();
    auto rhsAddr = rhs.GetSockAddr();
    if (lhsAddr->sa_family != rhsAddr->sa_family) {
        return false;
    }

    switch (lhsAddr->sa_family) {
        case AF_INET:
            return reinterpret_cast<const sockaddr_in*>(lhsAddr)->sin_addr.s_addr ==
                reinterpret_cast<const sockaddr_in*>(rhsAddr)->sin_addr.s_addr;
        case AF_INET6: {
            const auto& lhsAddrIn6 = reinterpret_cast<const sockaddr_in6*>(lhsAddr)->sin6_addr;
            const auto& rhsAddrIn6 = reinterpret_cast<const sockaddr_in6*>(rhsAddr)->sin6_addr;
            return memcmp(
                reinterpret_cast<const char*>(&lhsAddrIn6),
                reinterpret_cast<const char*>(&rhsAddrIn6),
                sizeof(lhsAddrIn6)) == 0;
        }
        default:
            Y_UNREACHABLE();
    }
}

bool operator != (const TNetworkAddress& lhs, const TNetworkAddress& rhs)
{
    return !(lhs == rhs);
}

////////////////////////////////////////////////////////////////////////////////

//! Performs asynchronous host name resolution.
class TAddressResolver::TImpl
    : public virtual TRefCounted
    , private TExpiringCache<TString, TNetworkAddress>
{
public:
    TImpl(TAddressResolverConfigPtr config)
        : TExpiringCache(config)
        , Config_(config)
        , DnsResolver_(config->Retries, config->ResolveTimeout, config->MaxResolveTimeout, config->WarningTimeout)
    {
        DnsResolver_.Start();

        Configure(Config_);
    }

    void Shutdown();

    TFuture<TNetworkAddress> Resolve(const TString& hostName);

    bool IsLocalHostNameOK();

    bool IsLocalAddress(const TNetworkAddress& address);

    void PurgeCache();

    void Configure(TAddressResolverConfigPtr config);

private:
    TAddressResolverConfigPtr Config_;

    std::atomic<bool> HasCachedLocalAddresses_ = {false};
    std::vector<TNetworkAddress> CachedLocalAddresses_;
    TReaderWriterSpinLock CacheLock_;

    const TActionQueuePtr Queue_ = New<TActionQueue>("AddressResolver");

    TDnsResolver DnsResolver_;

    virtual TFuture<TNetworkAddress> DoGet(const TString& hostName) override;

    const std::vector<TNetworkAddress>& GetLocalAddresses();
};

void TAddressResolver::TImpl::Shutdown()
{
    DnsResolver_.Stop();

    Queue_->Shutdown();
}

TFuture<TNetworkAddress> TAddressResolver::TImpl::Resolve(const TString& hostName)
{
    // Check if |address| parses into a valid IPv4 or IPv6 address.
    {
        auto result = TNetworkAddress::TryParse(hostName);
        if (result.IsOK()) {
            return MakeFuture(result);
        }
    }

    // Run async resolution.
    return Get(hostName);
}

TFuture<TNetworkAddress> TAddressResolver::TImpl::DoGet(const TString& hostname)
{
    return DnsResolver_
        .ResolveName(hostname, Config_->EnableIPv4, Config_->EnableIPv6)
        .Apply(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<TNetworkAddress>& result) {
            // Empty callback just to forward future callbacks into proper thread.
            return result.ValueOrThrow();
        })
        .AsyncVia(Queue_->GetInvoker()));
}

bool TAddressResolver::TImpl::IsLocalHostNameOK()
{
    // Force check & resolution.
    if (Config_->LocalHostFqdn) {
        return true;
    } else {
        return ::NYT::UpdateLocalHostName([] (const char*, const char*) {});
    }
}

bool TAddressResolver::TImpl::IsLocalAddress(const TNetworkAddress& address)
{
    const auto& localAddresses = GetLocalAddresses();
    auto&& it = std::find(localAddresses.begin(), localAddresses.end(), address);
    auto jt = localAddresses.end();
    return it != jt;
}

const std::vector<TNetworkAddress>& TAddressResolver::TImpl::GetLocalAddresses()
{
    if (HasCachedLocalAddresses_) {
        return CachedLocalAddresses_;
    }

    struct ifaddrs* ifAddresses;
    if (getifaddrs(&ifAddresses) == -1) {
         THROW_ERROR_EXCEPTION("getifaddrs failed")
             << TError::FromSystem();
    }

    auto holder = std::unique_ptr<ifaddrs, decltype(&freeifaddrs)>(ifAddresses, &freeifaddrs);

    std::vector<TNetworkAddress> localAddresses;
    for (const auto* currentAddress = ifAddresses;
        currentAddress;
        currentAddress = currentAddress->ifa_next)
    {
        if (currentAddress->ifa_addr == nullptr) {
            continue;
        }

        auto family = currentAddress->ifa_addr->sa_family;
        if (family != AF_INET && family != AF_INET6) {
            continue;
        }
        localAddresses.push_back(TNetworkAddress(*currentAddress->ifa_addr));
    }

    {
        TWriterGuard guard(CacheLock_);
        // NB: Only update CachedLocalAddresses_ once.
        if (!HasCachedLocalAddresses_) {
            CachedLocalAddresses_ = std::move(localAddresses);
            HasCachedLocalAddresses_ = true;
        }
    }

    return CachedLocalAddresses_;
}

void TAddressResolver::TImpl::PurgeCache()
{
    Clear();
    LOG_INFO("Address cache purged");
}

void TAddressResolver::TImpl::Configure(TAddressResolverConfigPtr config)
{
    Config_ = std::move(config);

    if (Config_->LocalHostFqdn) {
        ::NYT::SetLocalHostName(*Config_->LocalHostFqdn);
    } else {
        ::NYT::UpdateLocalHostName([&] (const char* message, const char* details) {
            LOG_INFO("Localhost FQDN resolution failed: %v: %v", message, details);
        });
    }

    LOG_INFO("Localhost FQDN configured: %v", ::NYT::GetLocalHostName());
}

////////////////////////////////////////////////////////////////////////////////

TAddressResolver::TAddressResolver()
    : Impl_(New<TImpl>(New<TAddressResolverConfig>()))
{ }

TAddressResolver::~TAddressResolver()
{ }

TAddressResolver* TAddressResolver::Get()
{
    return Singleton<TAddressResolver>();
}

void TAddressResolver::StaticShutdown()
{
    Get()->Shutdown();
}

void TAddressResolver::Shutdown()
{
    Impl_->Shutdown();
}

TFuture<TNetworkAddress> TAddressResolver::Resolve(const TString& address)
{
    return Impl_->Resolve(address);
}

bool TAddressResolver::IsLocalHostNameOK()
{
    return Impl_->IsLocalHostNameOK();
}

bool TAddressResolver::IsLocalAddress(const TNetworkAddress& address)
{
    return Impl_->IsLocalAddress(address);
}

void TAddressResolver::PurgeCache()
{
    Y_ASSERT(Impl_);
    return Impl_->PurgeCache();
}

void TAddressResolver::Configure(TAddressResolverConfigPtr config)
{
    Y_ASSERT(Impl_);
    return Impl_->Configure(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

REGISTER_SHUTDOWN_CALLBACK(2, TAddressResolver::StaticShutdown);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

