#include "per_user_and_workload_request_queue_provider.h"

namespace NYT::NCypressProxy {

using namespace NRpc;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TPerUserAndWorkloadRequestQueueProvider::TPerUserAndWorkloadRequestQueueProvider(
    TReconfigurationCallback reconfigurationCallback)
    : TBase(
        CreateKeyFromRequestHeaderCallback(),
        std::move(reconfigurationCallback))
{ }

TRequestQueuePtr TPerUserAndWorkloadRequestQueueProvider::CreateQueueForKey(const TKey& /*userNameAndWorkloadType*/)
{
    YT_UNIMPLEMENTED();
}

bool TPerUserAndWorkloadRequestQueueProvider::IsReconfigurationPermitted(const TKey& userNameAndWorkloadType) const
{
    return userNameAndWorkloadType.first != RootUserName;
}

TPerUserAndWorkloadRequestQueueProvider::TKeyFromRequestHeaderCallback TPerUserAndWorkloadRequestQueueProvider::CreateKeyFromRequestHeaderCallback()
{
    return BIND([] (const NRpc::NProto::TRequestHeader& header) -> TKey {
        auto userName = header.has_user() ? FromProto<std::string>(header.user()) : RootUserName;
        const auto& ypathExt = header.GetExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
        auto workloadType = ypathExt.mutating() ? EUserWorkloadType::Write : EUserWorkloadType::Read;
        return {userName, workloadType};
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
