#pragma once

#include "common.h"
#include "error.h"

#include <core/ytree/yson_serializable.h>

#include <core/actions/future.h>

#ifdef _WIN32
    #include <ws2tcpip.h>
#else
    #include <sys/socket.h>
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Constructs an address of the form |hostName:port|.
Stroka BuildServiceAddress(const TStringBuf& hostName, int port);

//! Parses service address into host name and port number.
//! Both #hostName and #port can be |NULL|.
//! Throws if the address is malformed.
void ParseServiceAddress(
    const TStringBuf& address,
    TStringBuf* hostName,
    int* port);

//! Extracts port number from a service address.
//! Throws if the address is malformed.
int GetServicePort(const TStringBuf& address);

//! Extracts host name from a service address.
TStringBuf GetServiceHostName(const TStringBuf& address);

////////////////////////////////////////////////////////////////////////////////

//! Configuration for TAddressResolver singleton.
class TAddressResolverConfig
    : public NYTree::TYsonSerializable
{
public:
    bool EnableIPv4;
    bool EnableIPv6;
    TNullable<Stroka> LocalHostFqdn;

    TAddressResolverConfig()
    {
        RegisterParameter("enable_ipv4", EnableIPv4)
            .Default(true);
        RegisterParameter("enable_ipv6", EnableIPv6)
            .Default(true);
        RegisterParameter("localhost_fqdn", LocalHostFqdn)
            .Default(Null);
    }
};

typedef TIntrusivePtr<TAddressResolverConfig> TAddressResolverConfigPtr;

////////////////////////////////////////////////////////////////////////////////

//! An opaque wrapper for |sockaddr| type.
class TNetworkAddress
{
public:
    TNetworkAddress();
    TNetworkAddress(const TNetworkAddress& other);
    TNetworkAddress(const TNetworkAddress& other, int port);
    explicit TNetworkAddress(const sockaddr& other, socklen_t length = 0);

    ~TNetworkAddress();

    sockaddr* GetSockAddr();
    const sockaddr* GetSockAddr() const;
    socklen_t GetLength() const;

    static TErrorOr<TNetworkAddress> TryParse(const TStringBuf& address);
    static TNetworkAddress Parse(const TStringBuf& address);

private:
    sockaddr_storage Storage;
    socklen_t Length;

    static socklen_t GetGenericLength(const sockaddr& sockAddr);
};

Stroka ToString(const TNetworkAddress& address, bool withPort = true);

////////////////////////////////////////////////////////////////////////////////

//! Performs asynchronous host name resolution.
class TAddressResolver
{
public:
    //! Returns the singleton instance.
    static TAddressResolver* Get();

    //! Resolves #address asynchronously.
    /*!
     *  Calls |getaddrinfo| and returns the first entry belonging to |AF_INET| or |AF_INET6| family.
     *  Caches successful resolutions.
     */
    TFuture<TErrorOr<TNetworkAddress>> Resolve(const Stroka& address);

    //! Returns the FQDN of the local host.
    Stroka GetLocalHostName();

    //! Returns the address of the local host.
    TNetworkAddress GetLocalHostAddress();

    //! Removes all cached resolutions.
    void PurgeCache();

    //! Updates resolver configuration.
    void Configure(TAddressResolverConfigPtr config);

    DECLARE_SINGLETON_DEFAULT_MIXIN(TAddressResolver);

private:
    TAddressResolver();

    ~TAddressResolver();

    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
