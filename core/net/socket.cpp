#include "socket.h"
#include "address.h"

#include <yt/core/misc/proc.h>

#ifdef _unix_
    #include <netinet/ip.h>
    #include <netinet/tcp.h>
    #include <sys/socket.h>
    #include <sys/un.h>
    #include <sys/types.h>
    #include <sys/stat.h>
#endif

namespace NYT {
namespace NNet {

////////////////////////////////////////////////////////////////////////////////

SOCKET CreateTcpServerSocket()
{
    int type = SOCK_STREAM;

#ifdef _linux_
    type |= SOCK_CLOEXEC;
    type |= SOCK_NONBLOCK;
#endif

    SOCKET serverSocket = socket(AF_INET6, type, IPPROTO_TCP);
    if (serverSocket == INVALID_SOCKET) {
        auto lastError = LastSystemError();
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::TransportError,
            "Failed to create a server socket")
            << TError::FromSystem(lastError);
    }

#ifndef _linux_
    {
        int flags = fcntl(serverSocket, F_GETFL);
        int result = fcntl(serverSocket, F_SETFL, flags | O_NONBLOCK);
        if (result != 0) {
            auto lastError = LastSystemError();
            SafeClose(serverSocket, false);
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::TransportError,
                "Failed to enable nonblocking mode")
                << TError::FromSystem(lastError);
        }
    }

    {
        int flags = fcntl(serverSocket, F_GETFD);
        int result = fcntl(serverSocket, F_SETFD, flags | FD_CLOEXEC);
        if (result != 0) {
            auto lastError = LastSystemError();
            SafeClose(serverSocket, false);
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::TransportError,
                "Failed to enable close-on-exec mode")
                << TError::FromSystem(lastError);
        }
    }
#endif

    {
        int flag = 0;
        if (setsockopt(serverSocket, IPPROTO_IPV6, IPV6_V6ONLY, (const char*) &flag, sizeof(flag)) != 0) {
            auto lastError = LastSystemError();
            SafeClose(serverSocket, false);
            THROW_ERROR_EXCEPTION(
               NRpc::EErrorCode::TransportError,
               "Failed to configure IPv6 protocol")
                << TError::FromSystem(lastError);
        }
    }

    {
        int flag = 1;
        if (setsockopt(serverSocket, SOL_SOCKET, SO_REUSEADDR, (const char*) &flag, sizeof(flag)) != 0) {
            auto lastError = LastSystemError();
            SafeClose(serverSocket, false);
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::TransportError,
                "Failed to configure socket address reuse")
                << TError::FromSystem(lastError);
        }
    }

    return serverSocket;
}

SOCKET CreateUnixServerSocket()
{
    int type = SOCK_STREAM;

#ifdef _linux_
    type |= SOCK_CLOEXEC;
    type |= SOCK_NONBLOCK;
#endif

    SOCKET serverSocket = socket(AF_UNIX, type, 0);
    if (serverSocket == INVALID_SOCKET) {
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::TransportError,
            "Failed to create a local server socket")
            << TError::FromSystem();
    }

    return serverSocket;
}

SOCKET CreateTcpClientSocket(int family)
{
    YCHECK(family == AF_INET6 || family == AF_INET);

    int protocol = IPPROTO_TCP;
    int type = SOCK_STREAM;

#ifdef _linux_
    type |= SOCK_CLOEXEC;
    type |= SOCK_NONBLOCK;
#endif

    SOCKET clientSocket = socket(family, type, protocol);
    if (clientSocket == INVALID_SOCKET) {
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::TransportError,
            "Failed to create client socket")
            << TError::FromSystem();
    }

    {
        int value = 0;
        if (setsockopt(clientSocket, IPPROTO_IPV6, IPV6_V6ONLY, (const char*) &value, sizeof(value)) != 0) {
            auto lastError = LastSystemError();
            SafeClose(clientSocket, false);
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::TransportError,
                "Failed to configure IPv6 protocol")
                << TError::FromSystem(lastError);
        }
    }

#if defined _unix_ && !defined _linux_
    {
        int flags = fcntl(clientSocket, F_GETFL);
        int result = fcntl(clientSocket, F_SETFL, flags | O_NONBLOCK);
        if (result != 0) {
            auto lastError = LastSystemError();
            SafeClose(clientSocket, false);
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::TransportError,
                "Failed to enable nonblocking mode")
                << TError::FromSystem(lastError);
        }
    }

    {
        int flags = fcntl(clientSocket, F_GETFD);
        int result = fcntl(clientSocket, F_SETFD, flags | FD_CLOEXEC);
        if (result != 0) {
            auto lastError = LastSystemError();
            SafeClose(clientSocket, false);
            THROW_ERROR_EXCEPTION("Failed to enable close-on-exec mode")
                << TError::FromSystem(lastError);
        }
    }
#endif

    return clientSocket;
}

SOCKET CreateUnixClientSocket()
{
    int family = AF_UNIX;
    int protocol = 0;
    int type = SOCK_STREAM;

#ifdef _linux_
    type |= SOCK_CLOEXEC;
    type |= SOCK_NONBLOCK;
#endif

    SOCKET clientSocket = socket(family, type, protocol);
    if (clientSocket == INVALID_SOCKET) {
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::TransportError,
            "Failed to create client socket")
            << TError::FromSystem();
    }

#if defined _unix_ && !defined _linux_
    {
        int flags = fcntl(clientSocket, F_GETFL);
        int result = fcntl(clientSocket, F_SETFL, flags | O_NONBLOCK);
        if (result != 0) {
            auto lastError = LastSystemError();
            SafeClose(clientSocket, false);
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::TransportError,
                "Failed to enable nonblocking mode")
                << TError::FromSystem(lastError);
        }
    }

    {
        int flags = fcntl(clientSocket, F_GETFD);
        int result = fcntl(clientSocket, F_SETFD, flags | FD_CLOEXEC);
        if (result != 0) {
            auto lastError = LastSystemError();
            SafeClose(clientSocket, false);
            THROW_ERROR_EXCEPTION("Failed to enable close-on-exec mode")
                << TError::FromSystem(lastError);
        }
    }
#endif

    return clientSocket;
}

int ConnectSocket(SOCKET clientSocket, const TNetworkAddress& address)
{
    int result = HandleEintr(connect, clientSocket, address.GetSockAddr(), address.GetLength());
    if (result != 0) {
        int error = LastSystemError();
        if (error != EAGAIN && error != EWOULDBLOCK && error != EINPROGRESS) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::TransportError,
                "Error connecting to %v",
                address)
                << TError::FromSystem(error);
        }
    }

    return result;
}

void BindSocket(SOCKET serverSocket, const TNetworkAddress& address)
{
    if (bind(serverSocket, address.GetSockAddr(), address.GetLength()) != 0) {
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::TransportError,
            "Failed to bind a server socket to %v",
            address)
            << TError::FromSystem();
    }
}

int AcceptSocket(SOCKET serverSocket, TNetworkAddress* clientAddress)
{
    SOCKET clientSocket;

#ifdef _linux_
    clientSocket = accept4(
        serverSocket,
        clientAddress->GetSockAddr(),
        clientAddress->GetLengthPtr(),
        SOCK_CLOEXEC | SOCK_NONBLOCK);
#else
    clientSocket = accept(
        serverSocket,
        clientAddress->GetSockAddr(),
        clientAddress->GetLengthPtr());
#endif

    if (clientSocket == INVALID_SOCKET) {
        if (LastSystemError() != EAGAIN && LastSystemError() != EWOULDBLOCK) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::TransportError,
                "Error accepting connection")
                << TError::FromSystem();
        }

        return clientSocket;
    }

#ifndef _linux_
    {
        int flags = fcntl(clientSocket, F_GETFL);
        int result = fcntl(clientSocket, F_SETFL, flags | O_NONBLOCK);
        if (result != 0) {
            auto lastError = LastSystemError();
            SafeClose(serverSocket, false);
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::TransportError,
                "Failed to enable nonblocking mode")
                << TError::FromSystem(lastError);
        }
    }

    {
        int flags = fcntl(clientSocket, F_GETFD);
        int result = fcntl(clientSocket, F_SETFD, flags | FD_CLOEXEC);
        if (result != 0) {
            auto lastError = LastSystemError();
            SafeClose(serverSocket, false);
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::TransportError,
                "Failed to enable close-on-exec mode")
                << TError::FromSystem(lastError);
        }
    }
#endif

    return clientSocket;
}

void ListenSocket(SOCKET serverSocket, int backlog)
{
    if (listen(serverSocket, backlog) == -1) {
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::TransportError,
            "Failed to listen to server socket")
            << TError::FromSystem();
    }
}

int GetSocketError(SOCKET socket)
{
    int error;
    socklen_t errorLen = sizeof (error);
    getsockopt(socket, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&error), &errorLen);
    return error;
}

TNetworkAddress GetSocketName(SOCKET socket)
{
    TNetworkAddress address;
    auto lengthPtr = address.GetLengthPtr();
    int result = getsockname(socket, address.GetSockAddr(), lengthPtr);
    if (result != 0) {
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::TransportError,
            "Failed to get socket name")
            << TError::FromSystem();
    }

    return address;
}

TNetworkAddress GetSocketPeerName(SOCKET socket)
{
    TNetworkAddress address;
    auto lengthPtr = address.GetLengthPtr();
    int result = getpeername(socket, address.GetSockAddr(), lengthPtr);
    if (result != 0) {
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::TransportError,
            "Failed to get socket peer name")
            << TError::FromSystem();
    }

    return address;
}

bool TrySetSocketNoDelay(SOCKET socket)
{
    int value = 1;
    if (setsockopt(socket, IPPROTO_TCP, TCP_NODELAY, (const char*) &value, sizeof(value)) != 0) {
        return false;
    }
    return true;
}

bool TrySetSocketKeepAlive(SOCKET socket)
{
#ifdef _linux_
    int value = 1;
    if (setsockopt(socket, SOL_SOCKET, SO_KEEPALIVE, (const char*) &value, sizeof(value)) != 0) {
        return false;
    }
#endif
    return true;
}

bool TrySetSocketEnableQuickAck(SOCKET socket)
{
#ifdef _linux_
    int value = 1;
    if (setsockopt(socket, IPPROTO_TCP, TCP_QUICKACK, (const char*) &value, sizeof(value)) != 0) {
        return false;
    }
#endif
    return true;
}

bool TrySetSocketTosLevel(SOCKET socket, int tosLevel)
{
    if (setsockopt(socket, IPPROTO_IP, IP_TOS, &tosLevel, sizeof(tosLevel)) != 0) {
        return false;
    }
    if (setsockopt(socket, IPPROTO_IPV6, IPV6_TCLASS, &tosLevel, sizeof(tosLevel)) != 0) {
        return false;
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNet
} // namespace NYT
