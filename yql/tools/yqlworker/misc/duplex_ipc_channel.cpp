#include "duplex_ipc_channel.h"

#include <yql/essentials/utils/signals/utils.h>

#include <util/string/hex.h>

#include <google/protobuf/message.h>

#include <errno.h>


namespace NYql {
namespace {

const int MAX_MESSAGE_SIZE = 8192;

void ValidateSocket(SOCKET sd) {
    int type = 0;
    CheckedGetSockOpt(sd, SOL_SOCKET, SO_TYPE, type, "get socket type");
    Y_ENSURE(type == SOCK_DGRAM || type == SOCK_SEQPACKET,
             "socket must be either SOCK_DGRAM or SOCK_SEQPACKET");
}

int Recv(SOCKET sd, void* data, size_t size, int flags) {
    for (;;) {
        int readn = recv(sd, data, size, flags);
        if (Y_UNLIKELY(readn == -1)) {
            switch (errno) {
                case EINTR: continue;
                case ECONNRESET: ythrow TPeerDead();
                case EAGAIN: return -1;
            }
            ythrow TSystemError() << "cannot read from socket";
        }
        return readn;
    }
}

int Send(SOCKET sd, const void* data, size_t size, int flags) {
    for (;;) {
        int writen = send(sd, data, size, flags);
        if (Y_UNLIKELY(writen == -1)) {
            switch (errno) {
                case EINTR: continue;
                case ECONNRESET: case EPIPE: ythrow TPeerDead();
                case EAGAIN: return -1;
            }
            ythrow TSystemError() << "cannot write to socket";
        } else if (Y_UNLIKELY(writen != static_cast<int>(size))) {
            ythrow yexception() << "partial write (expected: " << size
                                << ", written: " << writen << ')';
        }
        return writen;
    }
}

} // namspace

TIpcChannelEndpoint::TIpcChannelEndpoint(SOCKET sd)
    : S_(sd)
{
    if (sd != INVALID_SOCKET) {
        ValidateSocket(sd);
    }
}

void TIpcChannelEndpoint::SendMsg(const TMessage& msg) {
    int msgSize = msg.ByteSize();
    Y_DEBUG_ABORT_UNLESS(msgSize <= MAX_MESSAGE_SIZE);

    TTempBuf buf(msgSize);
    if (!msg.SerializeToArray(buf.Data(), buf.Size())) {
        ythrow yexception() << "cannot serialize message "
                            << PbMessageToStr(msg);
    }

    Send(S_, buf.Data(), msgSize, MSG_NOSIGNAL);
}

void TIpcChannelEndpoint::RecvMsg(TMessage* msg) {
    int msgSize = MAX_MESSAGE_SIZE;

#ifdef _linux_
    // requires linux kernel >= 3.4
    msgSize = Recv(S_, nullptr, 0, MSG_TRUNC | MSG_PEEK);
    Y_DEBUG_ABORT_UNLESS(msgSize >= 0 && msgSize <= MAX_MESSAGE_SIZE);
    if (msgSize == 0) {
        ythrow TPeerDead();
    }
#endif

    TTempBuf buf(msgSize);
    int readn = Recv(S_, buf.Data(), buf.Size(), MSG_TRUNC);
    Y_DEBUG_ABORT_UNLESS(readn >= 0 && readn <= MAX_MESSAGE_SIZE);

    if (readn == 0) {
        ythrow TPeerDead();
    }

    if (!msg->ParseFromArray(buf.Data(), readn)) {
        ythrow yexception() << "cannot parse message: "
                            << HexEncode(buf.Data(), Min(readn, 0x100))
                            << ", actual size: " << readn;
    }
}

bool TIpcChannelEndpoint::SendMsgNonBlock(const TMessage& msg) {
    int msgSize = msg.ByteSize();
    Y_DEBUG_ABORT_UNLESS(msgSize <= MAX_MESSAGE_SIZE);

    TTempBuf buf(msgSize);
    if (!msg.SerializeToArray(buf.Data(), buf.Size())) {
        ythrow yexception() << "cannot serialize message "
                            << PbMessageToStr(msg);
    }

    int writen = Send(S_, buf.Data(), msgSize, MSG_NOSIGNAL | MSG_DONTWAIT);
    return writen == -1 ? false : true;
}

bool TIpcChannelEndpoint::RecvMsgNonBlock(TMessage* msg) {
    int flags = MSG_TRUNC | MSG_DONTWAIT;
    int msgSize = MAX_MESSAGE_SIZE;

#ifdef _linux_
    // requires linux kernel >= 3.4
    msgSize = Recv(S_, nullptr, 0, flags | MSG_PEEK);
    Y_DEBUG_ABORT_UNLESS(msgSize >= -1 && msgSize <= MAX_MESSAGE_SIZE);
    if (msgSize == -1) {
        return false;
    } else if (msgSize == 0) {
        ythrow TPeerDead();
    }
#endif

    TTempBuf buf(msgSize);
    int readn = Recv(S_, buf.Data(), buf.Size(), flags);
    Y_DEBUG_ABORT_UNLESS(readn >= -1 && readn <= MAX_MESSAGE_SIZE);

    if (readn == -1) {
        return false;
    } else if (readn == 0) {
        ythrow TPeerDead();
    }

    if (!msg->ParseFromArray(buf.Data(), readn)) {
        ythrow yexception() << "cannot parse message: "
                            << HexEncode(buf.Data(), Min(readn, 0x100))
                            << ", actual size: " << readn;
    }
    return true;
}

std::pair<TIpcChannelEndpoint, TIpcChannelEndpoint> DuplexIpcChannel() {
    SOCKET socks[2];

    /*
     * Used SOCK_SEQPACKET here for:
     *
     * 1. Easily distinguish individual requests from each other. Stream
     *    socket require manual messages boundaries.
     *
     * 2. Make callers to note when the peer become dead by seeing the
     *    disconnected socket. In case of SOCK_DGRAM socket callers would
     *    just get stuck on receiving response.
     */

#ifdef _linux_
#   define SOCK_TYPE SOCK_SEQPACKET
#else
#   define SOCK_TYPE SOCK_DGRAM
#endif

    if (socketpair(AF_LOCAL, SOCK_TYPE, 0, socks) != 0) {
        ythrow TSystemError() << "cannot create socketpair";
    }

#undef SOCK_TYPE

    return { TIpcChannelEndpoint(socks[0]), TIpcChannelEndpoint(socks[1]) };
}

} // namspace NYql
