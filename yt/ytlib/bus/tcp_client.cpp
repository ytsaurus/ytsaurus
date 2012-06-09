#include "stdafx.h"
#include "tcp_client.h"
#include "private.h"
#include "client.h"
#include "bus.h"
#include "config.h"
#include "tcp_connection.h"

#include <ytlib/misc/thread.h>
#include <ytlib/misc/host_name.h>
#include <ytlib/misc/error.h>
#include <ytlib/misc/thread_affinity.h>

#include <util/network/socket.h>
#include <util/system/error.h>

#include <errno.h>

#ifndef _WIN32
#include <netinet/tcp.h>
#include <sys/socket.h>
#endif

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = BusLogger;
static NProfiling::TProfiler& Profiler = BusProfiler;

////////////////////////////////////////////////////////////////////////////////

//! A lightweight proxy controlling the lifetime of client #TTcpConnection.
/*!
 *  When the last strong reference vanishes, it calls IBus::Terminate
 *  for the underlying connection.
 */
class TTcpClientBusProxy
    : public IBus
{
public:
    TTcpClientBusProxy(TTcpBusClientConfigPtr config, IMessageHandlerPtr handler)
        : Config(config)
        , Handler(handler)
        , Id(TConnectionId::Create())
        , Socket(INVALID_SOCKET)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YASSERT(config);
        YASSERT(handler);
    }

    ~TTcpClientBusProxy()
    {
        VERIFY_THREAD_AFFINITY_ANY();
        if (Connection) {
            Connection->Terminate(TError("Bus terminated"));
        }
    }

    void Open(const sockaddr_in6& sockAddress)
    {
        VERIFY_THREAD_AFFINITY_ANY();
    
        LOG_INFO("Connecting to %s (ConnectionId: %s)",
            ~Config->Address,
            ~Id.ToString());
    
        OpenSocket(sockAddress);
        ConnectSocket(sockAddress);
    
        Connection = New<TTcpConnection>(
            EConnectionType::Client,
            Id,
            Socket,
            Config->Address,
            Handler);
        TTcpDispatcher::TImpl::Get()->AsyncRegister(Connection);
    }

    virtual TSendResult Send(IMessagePtr message)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        return Connection->Send(message);
    }

    virtual void Terminate(const TError& error)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        Connection->Terminate(error);
    }

    virtual void SubscribeTerminated(const TCallback<void(TError)>& callback)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        Connection->SubscribeTerminated(callback);
    }

    virtual void UnsubscribeTerminated(const TCallback<void(TError)>& callback)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        Connection->UnsubscribeTerminated(callback);
    }

private:
    TTcpBusClientConfigPtr Config;
    IMessageHandlerPtr Handler;
    TTcpConnectionPtr Connection;

    TConnectionId Id;
    int Socket;

    void OpenSocket(const sockaddr_in6& sockAddress)
    {
        bool isIPV6 = sockAddress.sin6_family == AF_INET6;
        Socket = socket(isIPV6 ? AF_INET6 : AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (Socket == INVALID_SOCKET) {
            int error = LastSystemError();
            ythrow yexception() << Sprintf("Failed to create client socket (ErrorCode: %d)\n%s",
                error,
                LastSystemErrorText(error));
        }
        
        // TODO(babenko): check results
        if (isIPV6) {
            int flag = 0;
            setsockopt(Socket, IPPROTO_IPV6, IPV6_V6ONLY, (const char*) &flag, sizeof(flag));
        }
        
        {
            int flag = 1;
            setsockopt(Socket, IPPROTO_TCP, TCP_NODELAY, (const char*) &flag, sizeof(flag));
        }

#if !defined(_WIN32) && !defined(__APPLE__)
        {
            int priority = Config->Priority;
            setsockopt(Socket, SOL_SOCKET, SO_PRIORITY, (const char*) &priority, sizeof(priority));
        }
#endif

#ifdef _WIN32
        unsigned long dummy = 1;
        ioctlsocket(Socket, FIONBIO, &dummy);
#else
        fcntl(Socket, F_SETFL, O_NONBLOCK);
        fcntl(Socket, F_SETFD, FD_CLOEXEC);
#endif
    }
        
    void CloseSocket()
    {
        if (Socket != INVALID_SOCKET) {
#ifdef _WIN32
            closesocket(Socket);
#else
            close(Socket);
#endif
        }
        Socket = INVALID_SOCKET;
    }

    void ConnectSocket(const sockaddr_in6& sockAddress)
    {
        int result;
        PROFILE_TIMING ("/connect_time") {
            result = connect(Socket, reinterpret_cast<const sockaddr*>(&sockAddress), sizeof (sockaddr_in6));
        }
        
        if (result != 0) {
            int error = LastSystemError();
            if (IsSocketError(error)) {
                CloseSocket();
                ythrow yexception() << Sprintf("Error connecting to %s (ErrorCode: %d)\n%s",
                    ~Config->Address,
                    error,
                    LastSystemErrorText(error));
            }
        }
    }

    bool IsSocketError(ssize_t result)
    {
#ifdef _WIN32
        return result != 0 && result != WSAEINPROGRESS && result != WSAEWOULDBLOCK;
#else
        return result != EINPROGRESS && result != EWOULDBLOCK;
#endif
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTcpBusClient
    : public IBusClient
{
public:
    explicit TTcpBusClient(TTcpBusClientConfigPtr config)
        : Config(config)
    { }

    void Init()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        ResolveAddress();
    }

    virtual IBusPtr CreateBus(IMessageHandlerPtr handler)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto proxy = New<TTcpClientBusProxy>(Config, handler);
        proxy->Open(SockAddress);
        return proxy;
    }

private:
    TTcpBusClientConfigPtr Config;
    sockaddr_in6 SockAddress;

    void ResolveAddress()
    {
        TStringBuf hostName;
        int port;
        ParseServiceAddress(
            Config->Address,
            &hostName,
            &port);

        // TODO(babenko): get rid of this util stuff
        TNetworkAddress networkAddress(Stroka(hostName), port);
        for (auto it = networkAddress.Begin(); it != networkAddress.End(); ++it) {
            sockaddr *addr = it->ai_addr;
            if (addr && (addr->sa_family == AF_INET || addr->sa_family == AF_INET6)) {
                SockAddress = *reinterpret_cast<sockaddr_in6*>(addr);
                SockAddress.sin6_port = htons(port);
                return;
            }
        }

        ythrow yexception() << Sprintf("Failed to resolve %s", ~hostName);
    }
};

////////////////////////////////////////////////////////////////////////////////

IBusClientPtr CreateTcpBusClient(TTcpBusClientConfigPtr config)
{
    auto client = New<TTcpBusClient>(config);
    client->Init();
    return client;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
