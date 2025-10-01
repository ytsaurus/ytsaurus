#include "dq_gateway_with_offloading.h"

#include <library/cpp/yt/assert/assert.h>

namespace NYT::NYqlPlugin::NNative {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDqGatewayWithOffloading
    : public NYql::IDqGateway
{
public:
    TDqGatewayWithOffloading(
        ::TIntrusivePtr<NYql::IDqGateway> underlying,
        IThreadPool* threadPool)
        : Underlying_(std::move(underlying))
        , ThreadPool_(threadPool)
    {
        YT_VERIFY(ThreadPool_);
    }

    ~TDqGatewayWithOffloading() override
    {
        ThreadPool_->SafeAddFunc([underlying = std::move(Underlying_)]() mutable {
            underlying.Reset();
        });
    }

    NThreading::TFuture<void> OpenSession(
        const TString& sessionId,
        const TString& username) override
    {
        return Underlying_->OpenSession(sessionId, username);
    }

    void CloseSession(const TString& sessionId) override
    {
        return Underlying_->CloseSession(sessionId);
    }

    NThreading::TFuture<void> CloseSessionAsync(const TString& sessionId) override
    {
        return Underlying_->CloseSessionAsync(sessionId);
    }

    NThreading::TFuture<TResult> ExecutePlan(
        const TString& sessionId,
        NYql::NDqs::TPlan&& plan,
        const TVector<TString>& columns,
        const THashMap<TString, TString>& secureParams,
        const THashMap<TString, TString>& graphParams,
        const NYql::TDqSettings::TPtr& settings,
        const TDqProgressWriter& progressWriter,
        const THashMap<TString, TString>& modulesMapping,
        bool discard,
        ui64 executionTimeout) override
    {
        return Underlying_->ExecutePlan(
            sessionId,
            std::move(plan),
            columns,
            secureParams,
            graphParams,
            settings,
            progressWriter,
            modulesMapping,
            discard,
            executionTimeout);
    }

    TString GetVanillaJobPath() override
    {
        return Underlying_->GetVanillaJobPath();
    }

    TString GetVanillaJobMd5() override
    {
        return Underlying_->GetVanillaJobMd5();
    }

    void Stop() override
    {
        return Underlying_->Stop();
    }

private:
    ::TIntrusivePtr<IDqGateway> Underlying_;
    IThreadPool* ThreadPool_;
};

////////////////////////////////////////////////////////////////////////////////

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////

::TIntrusivePtr<NYql::IDqGateway> CreateDqGatewayWithOffloading(
    ::TIntrusivePtr<NYql::IDqGateway> underlying,
    IThreadPool* threadPool)
{
    return MakeIntrusive<TDqGatewayWithOffloading>(std::move(underlying), threadPool);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin::NNative
