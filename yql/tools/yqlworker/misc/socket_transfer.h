#pragma once

#include <util/network/sock.h>
#include <util/system/pipe.h>
#include <util/generic/ptr.h>

#include <thread>

namespace NYql {

class TSocketTransferServer : private TNonCopyable {
public:
    TSocketTransferServer(THolder<ISockAddr>&& localAddr, TSocketHolder&& toTransfer);
    void Start();
    void Stop();
    ~TSocketTransferServer();
private:
    void EventLoop();
    void SendSocket();

    enum class EState {
        ACCEPTING,
        SENDING,
    };
    bool Started_ = false;
    EState State_ = EState::ACCEPTING;

    THolder<ISockAddr> LocalAddr_;
    TLocalStreamSocket ServerSock_;
    TStreamSocket ClientSock_;
    TSocketHolder ToTransfer_;
    std::thread Thread_;
    TPipeHandle Read_;
    TPipeHandle Write_;
};

// simple synchronous client
TSocketHolder ReceiveSocket(const ISockAddr* addr);

// to workarould limitation AF_UNIX max path
THolder<ISockAddr> MakeLocalSocketAddress(const TFsPath& path);

} // namespace NYql
