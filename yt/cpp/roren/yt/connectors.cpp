#include "connectors.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

TParDoTreeBuilder::TPCollectionNodeId VerifiedGetNodeIdOfMapperConnector(const TOperationConnector& connector)
{
    Y_ABORT_UNLESS(std::holds_alternative<TMapperOutputConnector>(connector));
    const auto& mapperConnector = std::get<TMapperOutputConnector>(connector);
    Y_ABORT_UNLESS(mapperConnector.MapperIndex == 0);
    return mapperConnector.NodeId;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
