#include "client_impl.h"

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/scheduler/pool_ypath_proxy.h>

namespace NYT::NApi::NNative {

using namespace NConcurrency;
using namespace NYson;
using namespace NObjectClient;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

TString GetPoolTreePath(const TString& poolTree)
{
    return Format("%v/%v", PoolTreesRootCypressPath, poolTree);
}

void TClient::DoTransferPoolResources(
    const TString& srcPool,
    const TString& dstPool,
    const TString& poolTree,
    NYTree::INodePtr resourceDelta,
    const TTransferPoolResourcesOptions& options)
{
    auto proxy = CreateObjectServiceWriteProxy();
    auto batchReq = proxy.ExecuteBatch();

    auto req = TSchedulerPoolYPathProxy::TransferPoolResources(GetPoolTreePath(poolTree));
    req->set_src_pool(srcPool);
    req->set_dst_pool(dstPool);
    req->set_resource_delta(ConvertToYsonString(resourceDelta).ToString());
    SetMutationId(req, options);

    batchReq->AddRequest(req);

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();
    batchRsp->GetResponse<TSchedulerPoolYPathProxy::TRspTransferPoolResources>(0)
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
