#include "operations.h"
#include "plain_graph.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

THashMap<TParDoTreeBuilder::TPCollectionNodeId, TParDoTreeBuilder::TPCollectionNodeId>
TMapReduceOperationNode::FusePrecedingMapper(ssize_t mapperIndex)
{
    Y_ABORT_UNLESS(mapperIndex < std::ssize(MapperBuilderList_));

    auto mapOperation = InputTables[mapperIndex]->OutputOf.Operation->VerifiedAsPtr<TMapOperationNode>();
    auto mapOperationConnector = InputTables[mapperIndex]->OutputOf.Connector;

    MergeTransformNames(mapOperation->TransformNames_);

    auto fusedBuilder = mapOperation->GetMapperBuilder();

    auto result = fusedBuilder.Fuse(MapperBuilderList_[mapperIndex], VerifiedGetNodeIdOfMapperConnector(mapOperationConnector));
    MapperBuilderList_[mapperIndex] = fusedBuilder;

    // Going to modify mapOperation connectors.
    // Don't want to modify hash map while traversing it,
    // so we collect connectors to modify into a vector.
    std::vector<TOperationConnector> connectorsToRelink;
    auto mapOperationOutputTablesCopy = mapOperation->OutputTables;
    for (const auto& [connector, outputTable] : mapOperationOutputTablesCopy) {
        if (outputTable->GetTableType() == ETableType::Output) {
            connectorsToRelink.push_back(connector);
        }
        // N.B. We don't want to relink intermediate tables.
        // They are connected to other MapReduce operations.
        // If such intermediate tables (and corresponding consuming MapReduce operations) are present,
        // this map operation will be also fused into other MapReduce operation.
    }
    for (const auto& connector : connectorsToRelink) {
        auto nodeId = VerifiedGetNodeIdOfMapperConnector(connector);
        auto outputTable = TYtGraphV2::TPlainGraph::UnlinkOperationOutput(mapOperation, connector);
        auto newConnector = TMapperOutputConnector{mapperIndex, nodeId};
        TYtGraphV2::TPlainGraph::LinkOperationOutput(this, newConnector, outputTable);
    }
    TYtGraphV2::TPlainGraph::ReplaceOperationInput(
        this,
        mapperIndex,
        mapOperation->VerifiedGetSingleInput()
    );
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
