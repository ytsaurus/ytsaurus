#pragma once

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/concurrency/spinlock.h>

#include <library/cpp/http/server/http.h>

#include <library/cpp/testing/unittest/tests_data.h>

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
        virtual TClientRequest* CreateClient() override;

    private:
        class TRequest
            : public TClientRequest
        {
        public:
            explicit TRequest(THttpServerImpl* owner);
            virtual bool Reply(void* opaque) override;

        private:
            THttpServerImpl* const Owner_;
        };

        YT_DECLARE_SPINLOCK(TAdaptiveLock, Lock_);
        TCallback Callback_;
    };

    TCallback Callback_;
    TPortManager PortManager_;

    std::unique_ptr<THttpServerImpl> ServerImpl_;
    std::unique_ptr<THttpServer> Server_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTests
