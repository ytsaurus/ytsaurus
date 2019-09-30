#pragma once

#include "public.h"

#include <yt/server/lib/core_dump/core_processor_service_proxy.h>
#include <yt/server/lib/core_dump/helpers.h>
#include <yt/server/lib/core_dump/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/ytlib/table_client/public.h>

#include <yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/core/rpc/service_detail.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

struct TCoreResult
{
    NCoreDump::TCoreInfos CoreInfos;
    NScheduler::NProto::TOutputResult BoundaryKeys;
};

////////////////////////////////////////////////////////////////////////////////

class TCoreProcessorService
    : public NRpc::TServiceBase
{
public:
    TCoreProcessorService(
        const NJobProxy::IJobHostPtr& jobHost,
        const NTableClient::TBlobTableWriterConfigPtr& blobTableWriterConfig,
        const NTableClient::TTableWriterOptionsPtr& tableWriterOptions,
        NObjectClient::TTransactionId transaction,
        NChunkClient::TChunkListId chunkList,
        const IInvokerPtr& controlInvoker,
        TDuration readTimeout);

    // If timeout is not zero, service uses it to wait for the first core to appear
    // and if it doesn't appear, returns a TCoreResult with a dummy TCoreInfo.
    TCoreResult Finalize(TDuration timeout = TDuration::Zero()) const;

    ~TCoreProcessorService();

private:
    class TCoreProcessor;
    const TIntrusivePtr<TCoreProcessor> CoreProcessor_;

    DECLARE_RPC_SERVICE_METHOD(NCoreDump::NProto, StartCoreDump);
};

DEFINE_REFCOUNTED_TYPE(TCoreProcessorService)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCoreDump
