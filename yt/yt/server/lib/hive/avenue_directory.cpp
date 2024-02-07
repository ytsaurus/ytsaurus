#include "avenue_directory.h"

#include "helpers.h"

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NHiveServer {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TCellId TSimpleAvenueDirectory::FindCellIdByEndpointId(TAvenueEndpointId endpointId) const
{
    auto it = Directory_.find(endpointId);
    return it == Directory_.end() ? TCellId() : it->second;
}

void TSimpleAvenueDirectory::UpdateEndpoint(TAvenueEndpointId endpointId, TCellId cellId)
{
    YT_VERIFY(IsAvenueEndpointType(TypeFromId(endpointId)));

    if (cellId) {
        Directory_[endpointId] = cellId;
    } else {
        Directory_.erase(endpointId);
    }

    EndpointUpdated_.Fire(endpointId, cellId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
