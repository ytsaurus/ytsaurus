#pragma once

#include "common.h"
#include "error.h"

#include <ytlib/actions/future.h>

#ifdef _WIN32
    #include <ws2tcpip.h>
#else
    #include <sys/socket.h>
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Returns the FQDN of the local host.
Stroka GetLocalHostName();

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

//! An opaque wrapper for |sockaddr| type.
class TNetworkAddress
{
public:
    TNetworkAddress();
    TNetworkAddress(const TNetworkAddress& other, int port);
    explicit TNetworkAddress(const sockaddr& other);

    sockaddr* GetSockAddr();
    const sockaddr* GetSockAddr() const;
    socklen_t GetLength() const;

private:
    sockaddr_storage Storage;

    static socklen_t GetLength(const sockaddr& sockAddr);

};

Stroka ToString(const TNetworkAddress& address, bool withPort = true);

////////////////////////////////////////////////////////////////////////////////

//! Performs asynchronous host name resolution.
class TAddressResolver
{
public:
    //! Returns the singleton instance.
    static TAddressResolver* Get();

    //! Resolves #hostName asynchronously.
    /*!
     *  Calls |getaddrinfo| and returns the first entry belonging to |AF_INET| or |AF_INET6| family.
     *  Caches successful resolutions.
     */
    TFuture< TValueOrError<TNetworkAddress> > Resolve(const Stroka& hostName);

    //! Removes all cached resolutions.
    void PurgeCache();

private:
    TSpinLock SpinLock;
    yhash_map<Stroka, TNetworkAddress> Cache;

    TValueOrError<TNetworkAddress> DoResolve(const Stroka& hostName);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT