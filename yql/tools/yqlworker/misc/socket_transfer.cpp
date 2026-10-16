#include "socket_transfer.h"

#include <util/string/builder.h>
#include <util/system/unaligned_mem.h>

#ifdef _unix_
#include <sys/types.h>
#include <sys/socket.h>
#endif

namespace NYql {

TSocketTransferServer::TSocketTransferServer(THolder<ISockAddr>&& localAddr, TSocketHolder&& toTransfer)
    : LocalAddr_(std::move(localAddr))
    , ToTransfer_(std::move(toTransfer))
{
#ifdef _unix_
    TPipeHandle::Pipe(Read_, Write_, EOpenModeFlag::CloseOnExec);
#else
    ythrow yexception() << "Socket transfer is not supported on this platform";
#endif
}

TSocketTransferServer::~TSocketTransferServer() {
    Y_ABORT_UNLESS(!Started_, "Stop() was not called");
}

void TSocketTransferServer::Start() {
    if (Started_) {
        return;
    }

    if (ServerSock_.Bind(LocalAddr_.Get(), 00600)) {
        ythrow TSystemError() << "Bind";
    }

    if (ServerSock_.Listen(1)) {
        ythrow TSystemError() << "Bind";
    }

    State_ = EState::ACCEPTING;
    Started_ = true;
    Thread_ = std::thread([this](){ return EventLoop(); });
}

void TSocketTransferServer::Stop() {
    if (!Started_) {
        return;
    }

    char c = 0;
    Y_ENSURE(Write_.Write(&c, sizeof(c)) == 1);
    Thread_.join();
    ServerSock_ = TLocalStreamSocket();
    Started_ = false;
    State_ = EState::ACCEPTING;
}

void TSocketTransferServer::EventLoop() {
    const static TDuration DEFAULT_IO_TIMEOUT = TDuration::Seconds(1);

    struct pollfd fds[2];
    for (;;) {
        fds[0] = { .fd = Read_, .events = POLLIN, .revents = 0};
        switch (State_) {
            case EState::ACCEPTING:
                fds[1] = { .fd = ServerSock_, .events = POLLIN, .revents = 0};
                break;
            case EState::SENDING: {
                Y_ABORT_UNLESS(ClientSock_ != INVALID_SOCKET);
                fds[1] = { .fd = ClientSock_, .events = POLLOUT, .revents = 0};
                break;
            }
        }

        ssize_t n = PollD(fds, 2, DEFAULT_IO_TIMEOUT.ToDeadLine());
        if (n < 0 && n != -ETIMEDOUT) {
            ythrow TSystemError() << "cannot poll on server socket transfer channel";
        }

        if (fds[0].revents & POLLIN) {
            char c;
            Read_.Read(&c, sizeof(c));
            break;
        } else if (fds[1].revents & POLLOUT) {
            SendSocket();
            ClientSock_.Close();
            State_ = EState::ACCEPTING;
        } else if (fds[1].revents & POLLIN) {
            if (ServerSock_.Accept(&ClientSock_)) {
                throw TSystemError() << "Accept";
            }
            State_ = EState::SENDING;
        }
    }
}

void TSocketTransferServer::SendSocket() {
#ifdef _unix_
    struct msghdr msg;
    std::memset(&msg, 0, sizeof(msg));

    char buf[CMSG_SPACE(sizeof(SOCKET))] = { 0 };
    char dummy = 0;
    struct iovec io = { .iov_base = &dummy, .iov_len = sizeof(dummy) };

    msg.msg_iov = &io;
    msg.msg_iovlen = 1;
    msg.msg_control = buf;
    msg.msg_controllen = sizeof(buf);

    struct cmsghdr* cmsg = CMSG_FIRSTHDR(&msg);
    Y_ENSURE(cmsg);
    cmsg->cmsg_level = SOL_SOCKET;
    cmsg->cmsg_type = SCM_RIGHTS;
    cmsg->cmsg_len = CMSG_LEN(sizeof(SOCKET));

    WriteUnaligned<SOCKET>((void*)CMSG_DATA(cmsg), ToTransfer_);
    msg.msg_controllen = CMSG_SPACE(sizeof(SOCKET));
    if (sendmsg(ClientSock_, &msg, 0) < 0) {
        ythrow TSystemError() << "sendmsg";
    }
#endif
}

TSocketHolder ReceiveSocket(const ISockAddr* addr) {
#ifdef _unix_
    TLocalStreamSocket sock;
    if (sock.Connect(addr)) {
        ythrow TSystemError() << "connect";
    }

    struct msghdr msg;
    std::memset(&msg, 0, sizeof(msg));

    char dummy;
    struct iovec io = {.iov_base = &dummy, .iov_len = sizeof(dummy)};
    msg.msg_iov = &io;
    msg.msg_iovlen = 1;

    char buf[256];
    msg.msg_control = buf;
    msg.msg_controllen = sizeof(buf);

    if (recvmsg(sock, &msg, 0) < 0) {
        ythrow TSystemError() << "recvmsg";
    }

    struct cmsghdr* cmsg = CMSG_FIRSTHDR(&msg);
    if (!cmsg || cmsg->cmsg_level != SOL_SOCKET || cmsg->cmsg_type != SCM_RIGHTS || cmsg->cmsg_len != CMSG_LEN(sizeof(SOCKET))) {
        ythrow yexception() << "Invalid control message received";
    }
    return TSocketHolder(ReadUnaligned<SOCKET>(CMSG_DATA(cmsg)));
#else
    Y_UNUSED(addr);
    ythrow yexception() << "Socket transfer is not supported on this platform";
#endif
}

namespace {

class TRobustLocalAddr : public TSockAddrLocal {
public:
    explicit TRobustLocalAddr(const TFsPath& path)
        : Path_(path)
    {
        if (IsPathTooLong(Path_)) {
#ifdef _linux_
            const TString dirPath = Path_.Dirname();
            Dir_ = TFileHandle(open(dirPath.c_str(), O_DIRECTORY));
            if (!Dir_.IsOpen()) {
                throw TSystemError() << "Unable to open directory " << Path_.Dirname();
            }

            TString newDirPath = TStringBuilder() << "/proc/self/fd/" << (FHANDLE)Dir_;
            Path_ = TFsPath(newDirPath) / Path_.Basename();
#endif
            if (IsPathTooLong(Path_)) {
                throw yexception() << path.GetPath() << " is too long for local socket";
            }
        }
        Set(Path_.GetPath().c_str());
    }
private:
    static bool IsPathTooLong(const TFsPath& path) {
        return path.GetPath().size() + 1 > sizeof(sockaddr_un().sun_path);
    }

    TFileHandle Dir_;
    TFsPath Path_;
};

}

THolder<ISockAddr> MakeLocalSocketAddress(const TFsPath& path) {
    return THolder<ISockAddr>(new TRobustLocalAddr(path));
}

} // namespace NYql
