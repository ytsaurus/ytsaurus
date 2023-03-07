#include "job_probe.h"
#include "job_prober_service_proxy.h"

#include <yt/core/bus/tcp/client.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/rpc/bus/channel.h>

#include <yt/ytlib/job_tracker_client/public.h>

namespace NYT::NJobProberClient {

using NBus::TTcpBusClientConfigPtr;
using NChunkClient::TChunkId;
using NYson::TYsonString;
using NJobTrackerClient::TJobId;

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TJobProberClient
    : public IJobProbe
{
public:
    TJobProberClient(TTcpBusClientConfigPtr config, TJobId jobId)
        : TcpBusClientConfig_(config)
        , JobId_(jobId)
    { }

    virtual std::vector<TChunkId> DumpInputContext() override
    {
        auto* proxy = GetOrCreateJobProberProxy();

        auto req = proxy->DumpInputContext();
        ToProto(req->mutable_job_id(), JobId_);

        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);

        const auto& rsp = rspOrError.Value();
        return FromProto<std::vector<TChunkId>>(rsp->chunk_ids());
    }

    virtual TYsonString PollJobShell(const TYsonString& parameters) override
    {
        auto* proxy = GetOrCreateJobProberProxy();
        auto req = proxy->PollJobShell();
        ToProto(req->mutable_job_id(), JobId_);
        ToProto(req->mutable_parameters(), parameters.GetData());

        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);

        const auto& rsp = rspOrError.Value();
        return TYsonString(rsp->result());
    }

    virtual TString GetStderr() override
    {
        auto* proxy = GetOrCreateJobProberProxy();

        auto req = proxy->GetStderr();
        ToProto(req->mutable_job_id(), JobId_);

        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);
        const auto& rsp = rspOrError.Value();

        return rsp->stderr_data();
    }

    virtual void Interrupt() override
    {
        auto* proxy = GetOrCreateJobProberProxy();

        auto req = proxy->Interrupt();
        ToProto(req->mutable_job_id(), JobId_);

        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);
    }

    virtual void Fail() override
    {
        auto* proxy = GetOrCreateJobProberProxy();

        auto req = proxy->Fail();
        ToProto(req->mutable_job_id(), JobId_);

        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);
    }

private:
    const TTcpBusClientConfigPtr TcpBusClientConfig_;
    const TJobId JobId_;

    TSpinLock SpinLock_;
    std::unique_ptr<TJobProberServiceProxy> JobProberProxy_;


    TJobProberServiceProxy* GetOrCreateJobProberProxy()
    {
        auto guard = Guard(SpinLock_);
        if (!JobProberProxy_) {
            auto client = CreateTcpBusClient(TcpBusClientConfig_);
            auto channel = NRpc::NBus::CreateBusChannel(std::move(client));
            JobProberProxy_ = std::make_unique<TJobProberServiceProxy>(std::move(channel));
        }
        return JobProberProxy_.get();
    }
};

////////////////////////////////////////////////////////////////////////////////

IJobProbePtr CreateJobProbe(
    NBus::TTcpBusClientConfigPtr config,
    TJobId jobId)
{
    return New<TJobProberClient>(config, jobId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProberClient::NYT
