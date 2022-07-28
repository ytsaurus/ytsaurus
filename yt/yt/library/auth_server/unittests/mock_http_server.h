#pragma once

#include <yt/yt/core/misc/public.h>

#include <library/cpp/http/server/http.h>

#include <library/cpp/testing/common/network.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NTests {

////////////////////////////////////////////////////////////////////////////////

TString HttpResponse(int code, TString body);

TString CollectMessages(const TError& error);

////////////////////////////////////////////////////////////////////////////////

class TMockHttpServer
{
public:
    using TCallback = std::function<void(TClientRequest*)>;

    void SetCallback(TCallback callback);

    void Start();
    void Stop();

    bool IsStarted() const;

    TString GetHost() const;
    int GetPort() const;

private:
    class THttpServerImpl
        : public THttpServer::ICallBack
    {
    public:
        TCallback GetCallback();
        void SetCallback(TCallback callback);
        TClientRequest* CreateClient() override;

    private:
        class TRequest
            : public TClientRequest
        {
        public:
            explicit TRequest(THttpServerImpl* owner);
            bool Reply(void* opaque) override;

        private:
            THttpServerImpl* const Owner_;
        };

        YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
        TCallback Callback_;
    };

    TCallback Callback_;
    NTesting::TPortHolder Port_;

    std::unique_ptr<THttpServerImpl> ServerImpl_;
    std::unique_ptr<THttpServer> Server_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTests
