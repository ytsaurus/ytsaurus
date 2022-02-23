#include "per_user_queue.h"

namespace NYT::NRpc {

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TRequestQueuePtr TPerUserRequestQueue::GetOrCreateUserQueue(const TString& name)
{
    auto [queue, _] = RequestQueues_.FindOrInsert(name, [&] {
        return CreateRequestQueue(name);
    });

    return *queue;
}

TRequestQueueProvider TPerUserRequestQueue::GetProvider()
{
    return BIND([=] (const NRpc::NProto::TRequestHeader& header) {
        const auto& name = header.has_user()
            ? header.user()
            : RootUserName;
        return GetOrCreateUserQueue(name).Get();
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
