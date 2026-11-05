#pragma once

#include "platform.h"
#include "input.h"
#include "output.h"
#include "endpoints_iterator.h"

#include <cstddef>
#include <string>
#include <chrono>

#if defined(_win_)
#   include <winsock2.h>
#   include <ws2tcpip.h>
#else
#   include <arpa/inet.h>
#   include <sys/types.h>
#   include <sys/socket.h>
#   include <poll.h>

#   if !defined(SOCKET)
#       define SOCKET int
#   endif
#endif

#include <memory>
#include <system_error>

struct addrinfo;

namespace clickhouse {

struct ClientOptions;

/** Address of a host to establish connection to.
 *
 */
class NetworkAddress {
public:
    explicit NetworkAddress(const std::string& host,
                            const std::string& port = "0");
    ~NetworkAddress();

    const struct addrinfo* Info() const;
    const std::string & Host() const;

private:
    const std::string host_;
    struct addrinfo* info_;
};

#if defined(_win_)

class windowsErrorCategory : public std::error_category {
public:
    char const* name() const noexcept override final;
    std::string message(int c) const override final;

    static windowsErrorCategory const& category();
};

#endif

#if defined(_unix_)

class getaddrinfoErrorCategory : public std::error_category {
public:
    char const* name() const noexcept override final;
    std::string message(int c) const override final;

    static getaddrinfoErrorCategory const& category();
};

#endif


class SocketBase {
public:
    virtual ~SocketBase();

    virtual std::unique_ptr<InputStream> makeInputStream() const = 0;
    virtual std::unique_ptr<OutputStream> makeOutputStream() const = 0;
};


class SocketFactory {
public:
    virtual ~SocketFactory();

    // TODO: move connection-related options to ConnectionOptions structure.

    virtual std::unique_ptr<SocketBase> connect(const ClientOptions& opts, const Endpoint& endpoint) = 0;

    virtual void sleepFor(const std::chrono::milliseconds& duration);
};


struct SocketTimeoutParams {
    std::chrono::milliseconds connect_timeout{ 5000 };
    std::chrono::milliseconds recv_timeout{ 0 };
    std::chrono::milliseconds send_timeout{ 0 };
};

class Socket : public SocketBase {
public:
    Socket(const NetworkAddress& addr, const SocketTimeoutParams& timeout_params);
    Socket(const NetworkAddress& addr);
    Socket(Socket&& other) noexcept;
    Socket& operator=(Socket&& other) noexcept;

    ~Socket() override;

    /// @params idle the time (in seconds) the connection needs to remain
    ///         idle before TCP starts sending keepalive probes.
    /// @params intvl the time (in seconds) between individual keepalive probes.
    /// @params cnt the maximum number of keepalive probes TCP should send
    ///         before dropping the connection.
    void SetTcpKeepAlive(int idle, int intvl, int cnt) noexcept;

    /// @params nodelay whether to enable TCP_NODELAY
    void SetTcpNoDelay(bool nodelay) noexcept;

    std::unique_ptr<InputStream> makeInputStream() const override;
    std::unique_ptr<OutputStream> makeOutputStream() const override;

protected:
    Socket(const Socket&) = delete;
    Socket& operator = (const Socket&) = delete;
    void Close();

    SOCKET handle_;
};


class NonSecureSocketFactory : public SocketFactory {
public:
    ~NonSecureSocketFactory() override;

    std::unique_ptr<SocketBase> connect(const ClientOptions& opts, const Endpoint& endpoint) override;

protected:
    virtual std::unique_ptr<Socket> doConnect(const NetworkAddress& address, const ClientOptions& opts);

    void setSocketOptions(Socket& socket, const ClientOptions& opts);
};


class SocketInput : public InputStream {
public:
    explicit SocketInput(SOCKET s);
    ~SocketInput();

protected:
    bool Skip(size_t bytes) override;
    size_t DoRead(void* buf, size_t len) override;

private:
    SOCKET s_;
};

class SocketOutput : public OutputStream {
public:
    explicit SocketOutput(SOCKET s);
    ~SocketOutput();

protected:
    size_t DoWrite(const void* data, size_t len) override;

private:
    SOCKET s_;
};

static struct NetrworkInitializer {
    NetrworkInitializer();
} gNetrworkInitializer;

}
