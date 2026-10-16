#pragma once

#include <util/network/socket.h>

#include <utility>


namespace google {
namespace protobuf {
    class Message;
} // namspace protobuf
} // namspace google


namespace NYql {

/**
 * @brief can be thrown on read from duplex channel to indicate thar remote
 *        peer is closed socket.
 */
class TPeerDead: public yexception {
};

/**
 * @brief Provides sequenced, bidirectional IPC channel endpoint for
 *        sending/receiving protobuf messages. This channel allows to mix
 *        blocking and nonblocking operations.
 *
 *        Supports only SOCK_DGRAM or SOCK_SEQPACKET socket types and only
 *        AF_LOCAL (AF_UNIX) domain.
 */
class TIpcChannelEndpoint {
public:
    using TMessage = ::google::protobuf::Message;

public:
    // sd - must be a descriptor of either SOCK_DGRAM or SOCK_SEQPACKET
    //      socket type
    explicit TIpcChannelEndpoint(SOCKET sd = INVALID_SOCKET);

    TIpcChannelEndpoint(TIpcChannelEndpoint&& rhs) noexcept {
        Swap(rhs);
    }

    TIpcChannelEndpoint& operator=(TIpcChannelEndpoint&& rhs) noexcept {
        Swap(rhs);
        return *this;
    }

    void SendMsg(const TMessage& msg);
    void RecvMsg(TMessage* msg);

    // same like above member-functions but returns false if no messages are
    // available at the socket (for receiving) or the message does not fit into
    // the send buffer of the socket (for sending).
    [[nodiscard]] bool SendMsgNonBlock(const TMessage& msg);
    [[nodiscard]] bool RecvMsgNonBlock(TMessage* msg);

    void Close() noexcept {
        if (!S_.Closed()) S_.Close();
    }

    TSocketHolder Release() noexcept {
        TSocketHolder s;
        DoSwap(s, S_);
        return std::move(s);
    }

    void Swap(TIpcChannelEndpoint& rhs) noexcept {
        DoSwap(S_, rhs.S_);
    }

    bool IsClosed() const noexcept { return S_.Closed(); }
    SOCKET GetFd() const noexcept { return S_; }

private:
    TSocketHolder S_;
};

/**
 * @brief Creates duplex IPC channel of binded to each other IPC endpoints.
 * @throws TSystemError when some error ocured on creating binded sockets pair.
 */
std::pair<TIpcChannelEndpoint, TIpcChannelEndpoint> DuplexIpcChannel();

} // namspace NYql
