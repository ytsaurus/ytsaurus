#include "job_probe.h"
#include "job_prober_service_proxy.h"

#include <yt/yt/core/bus/tcp/client.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/rpc/bus/channel.h>

#include <yt/yt/client/job_tracker_client/public.h>

namespace NYT::NJobProxy {

using NBus::TBusClientConfigPtr;
using NChunkClient::TChunkId;
using NYson::TYsonString;
using NJobTrackerClient::TJobId;

using namespace NApi;
using namespace NConcurrency;
using namespace NJobProberClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

class TJobProberClient
    : public IJobProbe
{
public:
    TJobProberClient(TBusClientConfigPtr config)
        : TcpBusClientConfig_(config)
    { }

    std::vector<TChunkId> DumpInputContext(TTransactionId transactionId) override
    {
        auto* proxy = GetOrCreateJobProberProxy();

        auto req = proxy->DumpInputContext();
        ToProto(req->mutable_transaction_id(), transactionId);

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

        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);
        const auto& rsp = rspOrError.Value();

        return rsp->stderr_data();
    }

    void Interrupt() override
    {
        auto* proxy = GetOrCreateJobProberProxy();

        auto req = proxy->Interrupt();

        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);
    }

    void Fail() override
    {
        auto* proxy = GetOrCreateJobProberProxy();

        auto req = proxy->Fail();

        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);
    }

    void GracefulAbort(TError error) override
    {
        auto* proxy = GetOrCreateJobProberProxy();

        auto req = proxy->GracefulAbort();

        ToProto(req->mutable_error(), std::move(error));

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
    NBus::TBusClientConfigPtr config)
{
    return New<TJobProberClient>(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
