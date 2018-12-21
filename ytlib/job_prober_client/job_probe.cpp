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
        EnsureJobProberProxy();
        auto req = JobProberProxy_->DumpInputContext();

        ToProto(req->mutable_job_id(), JobId_);
        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);
        const auto& rsp = rspOrError.Value();

        return FromProto<std::vector<TChunkId>>(rsp->chunk_ids());
    }

    virtual TYsonString StraceJob() override
    {
        EnsureJobProberProxy();
        auto req = JobProberProxy_->Strace();

        ToProto(req->mutable_job_id(), JobId_);
        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);
        const auto& rsp = rspOrError.Value();

        return TYsonString(rsp->trace());
    }

    virtual void SignalJob(const TString& signalName) override
    {
        EnsureJobProberProxy();
        auto req = JobProberProxy_->SignalJob();

        ToProto(req->mutable_job_id(), JobId_);
        ToProto(req->mutable_signal_name(), signalName);
        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);
    }

    virtual TYsonString PollJobShell(const TYsonString& parameters) override
    {
        EnsureJobProberProxy();
        auto req = JobProberProxy_->PollJobShell();

        ToProto(req->mutable_job_id(), JobId_);
        ToProto(req->mutable_parameters(), parameters.GetData());
        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);
        const auto& rsp = rspOrError.Value();

        return TYsonString(rsp->result());
    }

    virtual TString GetStderr() override
    {
        EnsureJobProberProxy();
        auto req = JobProberProxy_->GetStderr();

        ToProto(req->mutable_job_id(), JobId_);
        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);
        const auto& rsp = rspOrError.Value();

        return rsp->stderr_data();
    }

    virtual void Interrupt() override
    {
        EnsureJobProberProxy();
        auto req = JobProberProxy_->Interrupt();

        ToProto(req->mutable_job_id(), JobId_);
        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);
    }

    virtual void Fail() override
    {
        EnsureJobProberProxy();
        auto req = JobProberProxy_->Fail();

        ToProto(req->mutable_job_id(), JobId_);
        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);
    }

private:
    const TTcpBusClientConfigPtr TcpBusClientConfig_;
    const TJobId JobId_;
    std::optional<TJobProberServiceProxy> JobProberProxy_;

    void EnsureJobProberProxy()
    {
        if (!JobProberProxy_) {
            auto client = CreateTcpBusClient(TcpBusClientConfig_);
            auto channel = NRpc::NBus::CreateBusChannel(std::move(client));
            JobProberProxy_.emplace(std::move(channel));
        }
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
