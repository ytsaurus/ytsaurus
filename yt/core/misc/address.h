#pragma once

#include "common.h"
#include "error.h"

#include <yt/core/actions/future.h>

#include <yt/core/ytree/yson_serializable.h>

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

//! Strips '-i' from remote address if the DCs of local and remote addresses differ.
//! If DC of local or remote address cannot be detected, than returns remote host name without changes.
//! Supposes that address has the form {service_letter}{number}-{dc_name}[-i].*
// NB: Function is public for testing purposes.
Stroka StripInterconnectFromAddress(const Stroka& localHostName, const Stroka& remoteHostName);

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
            .Default();
    }
};

typedef TIntrusivePtr<TAddressResolverConfig> TAddressResolverConfigPtr;

////////////////////////////////////////////////////////////////////////////////

//! An opaque wrapper for |sockaddr| type.
class TNetworkAddress
{
public:
    TNetworkAddress();
    TNetworkAddress(const TNetworkAddress& other, int port);
    explicit TNetworkAddress(const sockaddr& other, socklen_t length = 0);

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
    ~TAddressResolver();

    //! Returns the singleton instance.
    static TAddressResolver* Get();

    //! Destroys the singleton instance.
    static void StaticShutdown();

    //! Shuts down all internals of address resolver.
    void Shutdown();

    //! Resolves #address asynchronously.
    /*!
     *  Calls |getaddrinfo| and returns the first entry belonging to |AF_INET| or |AF_INET6| family.
     *  Caches successful resolutions.
     */
    TFuture<TNetworkAddress> Resolve(const Stroka& address);

    //! Returns the FQDN of the local host.
    /*!
     *  If for some reason this FQDN could not be determined, |<unknown>| string is used.
     *  \see IsLocalHostNameOK
     */
    Stroka GetLocalHostName();

    //! Return |true| if the local host FQDN can be properly determined.
    bool IsLocalHostNameOK();

    //! Removes all cached resolutions.
    void PurgeCache();

    //! Updates resolver configuration.
    void Configure(TAddressResolverConfigPtr config);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

    TAddressResolver();

    Y_DECLARE_SINGLETON_FRIEND();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
