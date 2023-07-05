#include "avenue_directory.h"

namespace NYT::NHiveServer {

////////////////////////////////////////////////////////////////////////////////

TCellId TSimpleAvenueDirectory::FindCellIdByEndpointId(TAvenueEndpointId endpointId) const
{
    auto it = Directory_.find(endpointId);
    return it == Directory_.end() ? TCellId() : it->second;
}

void TSimpleAvenueDirectory::UpdateEndpoint(TAvenueEndpointId endpointId, TCellId cellId)
{
    if (cellId) {
        Directory_[endpointId] = cellId;
    } else {
        Directory_.erase(endpointId);
    }

    EndpointUpdated_.Fire(endpointId, cellId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
