#include "multiread_request_queue_provider.h"

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/library/query/proto/query_service.pb.h>

#include <yt/yt/core/rpc/request_queue_provider.h>
#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NQueryAgent {

using namespace NRpc;
using namespace NTabletClient;

///////////////////////////////////////////////////////////////////////////////

class TMultireadRequestQueueProvider
    : public NRpc::TRequestQueueProviderBase
{
public:
    TMultireadRequestQueueProvider();

    NRpc::TRequestQueue* GetQueue(const NRpc::NProto::TRequestHeader& header) override;

private:
    const NRpc::TRequestQueuePtr InMemoryMultireadRequestQueue_;
};

DECLARE_REFCOUNTED_CLASS(TMultireadRequestQueueProvider)
DEFINE_REFCOUNTED_TYPE(TMultireadRequestQueueProvider)

////////////////////////////////////////////////////////////////////////////////

TMultireadRequestQueueProvider::TMultireadRequestQueueProvider()
    : InMemoryMultireadRequestQueue_(CreateRequestQueue("in_memory"))
{ }

NRpc::TRequestQueue* TMultireadRequestQueueProvider::GetQueue(
    const NRpc::NProto::TRequestHeader& header)
{
    const auto& ext = header.GetExtension(NQueryClient::NProto::TReqMultireadExt::req_multiread_ext);
    auto inMemoryMode = FromProto<EInMemoryMode>(ext.in_memory_mode());
    return inMemoryMode == EInMemoryMode::None
        ? nullptr
        : InMemoryMultireadRequestQueue_.Get();
}

////////////////////////////////////////////////////////////////////////////////

NRpc::IRequestQueueProviderPtr CreateMultireadRequestQueueProvider()
{
    return New<TMultireadRequestQueueProvider>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent
