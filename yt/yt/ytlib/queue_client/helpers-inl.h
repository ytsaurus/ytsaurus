#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

#include <yt/yt/ytlib/hive/cluster_directory.h>

namespace NYT::NQueueClient {

////////////////////////////////////////////////////////////////////////////////

template <class TTable>
TIntrusivePtr<TTable> CreateStateTableClientOrThrow(
    const TWeakPtr<NApi::NNative::IConnection>& connection,
    const std::optional<std::string>& cluster,
    const NYPath::TYPath& path,
    const std::string& user)
{
    auto localConnection = connection.Lock();
    if (!localConnection) {
        THROW_ERROR_EXCEPTION("Connection expired");
    }

    NApi::IClientPtr client;
    auto clientOptions = NApi::TClientOptions::FromUser(user);
    if (cluster) {
        auto remoteConnection = localConnection->GetClusterDirectory()->GetConnectionOrThrow(*cluster);
        client = remoteConnection->CreateClient(clientOptions);
    } else {
        client = localConnection->CreateClient(clientOptions);
    }

    return New<TTable>(path, std::move(client));
}

////////////////////////////////////////////////////////////////////////////////

}  // namespace NYT::NQueueClient
