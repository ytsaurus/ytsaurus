#include "job_probe.h"
#include "job_prober_service_proxy.h"

#include <yt/yt/core/bus/tcp/client.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/rpc/bus/channel.h>

#include <yt/yt/ytlib/job_tracker_client/public.h>

namespace NYT::NJobProberClient {

using NBus::TBusClientConfigPtr;
using NChunkClient::TChunkId;
using NYson::TYsonString;
using NJobTrackerClient::TJobId;

using namespace NApi;
using namespace NConcurrency;
using namespace NJobProberClient;

////////////////////////////////////////////////////////////////////////////////

class TJobProberClient
    : public IJobProbe
{
public:
    TJobProberClient(TBusClientConfigPtr config, TJobId jobId)
        : TcpBusClientConfig_(config)
        , JobId_(jobId)
    { }

    std::vector<TChunkId> DumpInputContext() override
    {
        auto* proxy = GetOrCreateJobProberProxy();

        auto req = proxy->DumpInputContext();
        ToProto(req->mutable_job_id(), JobId_);

        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);

        const auto& rsp = rspOrError.Value();
        return FromProto<std::vector<TChunkId>>(rsp->chunk_ids());
    }

    TPollJobShellResponse PollJobShell(
        const TJobShellDescriptor& jobShellDescriptor,
        const TYsonString& parameters) override
    {
        auto* proxy = GetOrCreateJobProberProxy();
        auto req = proxy->PollJobShell();
        ToProto(req->mutable_job_id(), JobId_);
        ToProto(req->mutable_parameters(), parameters.ToString());
        req->set_subcontainer(jobShellDescriptor.Subcontainer);

        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);

        const auto& rsp = rspOrError.Value();

        return TPollJobShellResponse {
            .Result = TYsonString(rsp->result()),
            .LoggingContext = rsp->has_logging_context()
                ? TYsonString(rsp->logging_context(), NYson::EYsonType::MapFragment)
                : TYsonString(),
        };
    }

    TString GetStderr() override
    {
        auto* proxy = GetOrCreateJobProberProxy();

        auto req = proxy->GetStderr();
        ToProto(req->mutable_job_id(), JobId_);

        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);
        const auto& rsp = rspOrError.Value();

        return rsp->stderr_data();
    }

    void Interrupt() override
    {
        auto* proxy = GetOrCreateJobProberProxy();

        auto req = proxy->Interrupt();
        ToProto(req->mutable_job_id(), JobId_);

        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);
    }

    void Fail() override
    {
        auto* proxy = GetOrCreateJobProberProxy();

        auto req = proxy->Fail();
        ToProto(req->mutable_job_id(), JobId_);

        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);
    }

    TSharedRef DumpSensors() override
    {
        auto* proxy = GetOrCreateJobProberProxy();

        auto req = proxy->DumpSensors();

        auto rsp = WaitFor(req->Invoke())
            .ValueOrThrow();

        if (rsp->Attachments().size() != 1) {
            THROW_ERROR_EXCEPTION("Invalid attachment count")
                << TErrorAttribute("count", rsp->Attachments().size());
        }

        return rsp->Attachments()[0];
    }

private:
    const TBusClientConfigPtr TcpBusClientConfig_;
    const TJobId JobId_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    std::unique_ptr<TJobProberServiceProxy> JobProberProxy_;


    TJobProberServiceProxy* GetOrCreateJobProberProxy()
    {
        auto guard = Guard(SpinLock_);
        if (!JobProberProxy_) {
            auto client = CreateBusClient(TcpBusClientConfig_);
            auto channel = NRpc::NBus::CreateBusChannel(std::move(client));
            JobProberProxy_ = std::make_unique<TJobProberServiceProxy>(std::move(channel));
        }
        return JobProberProxy_.get();
    }
};

////////////////////////////////////////////////////////////////////////////////

IJobProbePtr CreateJobProbe(
    NBus::TBusClientConfigPtr config,
    TJobId jobId)
{
    return New<TJobProberClient>(config, jobId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProberClient::NYT
