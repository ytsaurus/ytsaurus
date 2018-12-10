#include "admin.h"

#include "api_service_proxy.h"

namespace NYT::NApi::NRpcProxy {

using namespace NRpc;
using namespace NJobTrackerClient;

///////////////////////////////////////////////////////////////////////////////

TAdmin::TAdmin(NRpc::IChannelPtr channel)
    : Channel_(std::move(channel))
{ }

TFuture<int> TAdmin::BuildSnapshot(const TBuildSnapshotOptions& options)
{
    TApiServiceProxy proxy(Channel_);

    auto req = proxy.BuildSnapshot();
    if (options.CellId) {
        ToProto(req->mutable_cell_id(), options.CellId);
    }
    req->set_set_read_only(options.SetReadOnly);

    return req->Invoke().Apply(BIND([] (const TErrorOr<TApiServiceProxy::TRspBuildSnapshotPtr>& rspOrError) -> int {
        const auto& rsp = rspOrError.ValueOrThrow();
        return rsp->snapshot_id();
    }));
}

TFuture<void> TAdmin::GCCollect(const TGCCollectOptions& options)
{
    TApiServiceProxy proxy(Channel_);

    auto req = proxy.GCCollect();
    if (options.CellId) {
        ToProto(req->mutable_cell_id(), options.CellId);
    }

    return req->Invoke().As<void>();
}

TFuture<void> TAdmin::KillProcess(const TString& /* address */, const TKillProcessOptions& /* options */)
{
    Y_UNIMPLEMENTED();
}

TFuture<TString> TAdmin::WriteCoreDump(const TString& /* address */, const TWriteCoreDumpOptions& /* options */)
{
    Y_UNIMPLEMENTED();
}

TFuture<TString> TAdmin::WriteOperationControllerCoreDump(const TOperationId& /* operationId */)
{
    Y_UNIMPLEMENTED();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
