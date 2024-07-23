#pragma once

#include "plain_graph.h"
#include "yt_graph_v2.h"
#include "visitors.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////o

class TTableProjection
{
public:
    TYtGraphV2::TTableNode* VerifiedGetClonedTable(const TYtGraphV2::TTableNode* oldTable)
    {
        auto it = TableMap_.find(oldTable);
        Y_ABORT_UNLESS(it != TableMap_.end());
        return it->second;
    }

    const THashSet<const TTableNode*> VerifiedGetOriginalTableSet(TYtGraphV2::TTableNode* newTable)
    {
        auto it = ReverseTableMap_.find(newTable);
        Y_ABORT_UNLESS(it != ReverseTableMap_.end());
        return it->second;
    }

    TOperationConnector ResolveNewConnection(const TTableNode* oldTableNode)
    {
        Y_ABORT_UNLESS(oldTableNode->OutputOf.Operation);
        Y_ABORT_UNLESS(oldTableNode->OutputOf.Operation->GetOperationType() == EOperationType::Map);

        auto it = TableMap_.find(oldTableNode);
        Y_ABORT_UNLESS(it != TableMap_.end());

        auto* newTableNode = it->second;
        return newTableNode->OutputOf.Connector;
    }

    void RegisterTableProjection(const TTableNode* original, TTableNode* cloned)
    {
        bool inserted;
        inserted = TableMap_.emplace(original, cloned).second;
        Y_ABORT_UNLESS(inserted);
        inserted = ReverseTableMap_[cloned].insert(original).second;
        Y_ABORT_UNLESS(inserted);
    }

    void DeregisterTableProjection(const TTableNode* original)
    {
        auto itTableMap = TableMap_.find(original);
        Y_ABORT_UNLESS(itTableMap != TableMap_.end());
        auto* cloned  = itTableMap->second;
        TableMap_.erase(itTableMap);

        auto itReverseTableMap = ReverseTableMap_.find(cloned);
        Y_ABORT_UNLESS(itReverseTableMap != ReverseTableMap_.end());
        auto& originalSet = itReverseTableMap->second;

        auto itOriginalSet = originalSet.find(original);
        Y_ABORT_UNLESS(itOriginalSet != originalSet.end());

        originalSet.erase(itOriginalSet);

        if (originalSet.empty()) {
            ReverseTableMap_.erase(itReverseTableMap);
        }
    }

    void ProjectTable(
        TYtGraphV2::TPlainGraph* plainGraph,
        TOperationNode* clonedOperation,
        TOperationConnector clonedConnector,
        const TTableNode* originalTable)
    {
        TTableNode* newClonedTable = nullptr;
        TTableNode* oldClonedTable = nullptr;
        if (clonedOperation) {
            if (auto it = clonedOperation->OutputTables.find(clonedConnector); it != clonedOperation->OutputTables.end()) {
                oldClonedTable = it->second;
            }
        }

        if (oldClonedTable == nullptr) {
            newClonedTable = plainGraph->CloneTable(clonedOperation, clonedConnector, originalTable);
            RegisterTableProjection(originalTable, newClonedTable);
        } else if (oldClonedTable->GetTableType() == ETableType::Intermediate) {
            Y_ABORT_UNLESS(
                originalTable->GetTableType() == ETableType::Output
                || dynamic_cast<const TTypedIntermediateTableNode*>(originalTable));

            auto unlinked = plainGraph->UnlinkOperationOutput(clonedOperation, clonedConnector);
            Y_ABORT_UNLESS(unlinked == oldClonedTable);

            newClonedTable = plainGraph->CloneTable(clonedOperation, clonedConnector, originalTable);
            RegisterTableProjection(originalTable, newClonedTable);

            plainGraph->RelinkTableConsumers(oldClonedTable, newClonedTable);

            auto oldOriginalList = VerifiedGetOriginalTableSet(oldClonedTable);
            for (auto* original : oldOriginalList) {
                DeregisterTableProjection(original);
                RegisterTableProjection(original, newClonedTable);
            }
        } else if (oldClonedTable->GetTableType() == ETableType::Output && originalTable->GetTableType() == ETableType::Intermediate) {
            RegisterTableProjection(originalTable, oldClonedTable);
        } else {
            Y_ABORT_UNLESS(oldClonedTable->GetTableType() == ETableType::Output);
            Y_ABORT_UNLESS(originalTable->GetTableType() == ETableType::Output);

            oldClonedTable->VerifiedAsPtr<TOutputTableNode>()->MergeFrom(
                *originalTable->VerifiedAsPtr<TOutputTableNode>()
            );
            RegisterTableProjection(originalTable, oldClonedTable);
        }
    }

private:
    THashMap<const TYtGraphV2::TTableNode*, TYtGraphV2::TTableNode*> TableMap_;
    THashMap<TTableNode*, THashSet<const TTableNode*>> ReverseTableMap_;
};

////////////////////////////////////////////////////////////////////////////////

class IYtGraphOptimizer
{
public:
    virtual ~IYtGraphOptimizer() = default;

    virtual TYtGraphV2::TPlainGraph Optimize(const TYtGraphV2::TPlainGraph& plainGraph) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TMapFuser
    : public IYtGraphOptimizer
{
public:
    virtual TYtGraphV2::TPlainGraph Optimize(const TYtGraphV2::TPlainGraph& plainGraph) override
    {
        TVisitor visitor;
        TraverseInTopologicalOrder(plainGraph, &visitor);
        return visitor.ExtractOutput();
    }

private:
    class TVisitor
        : public IPlainGraphVisitor
    {
    public:
        void OnTableNode(TTableNode* originalTable) override
        {
            if (originalTable->GetTableType() == ETableType::Input) {
                TableProjection_.ProjectTable(&PlainGraph_, nullptr, {}, originalTable);
            }
            // All non input tables are cloned during clonning of their producing operations.
        }

        void OnOperationNode(TYtGraphV2::TOperationNode* originalOperation) override
        {
            auto matchProducerConsumerPattern = [] (const TOperationNode* operationNode) -> TOperationNode* {
                if (operationNode->GetOperationType() == EOperationType::Map) {
                    auto originalInput = operationNode->VerifiedGetSingleInput();
                    auto originalPreviousOperation = originalInput->OutputOf.Operation;
                    if (originalPreviousOperation && originalPreviousOperation->GetOperationType() == EOperationType::Map) {
                        return originalPreviousOperation;
                    }
                }
                return nullptr;
            };

            if (FusionMap_.contains(originalOperation)) {
                return;
            } else if (auto originalPreviousOperation = matchProducerConsumerPattern(originalOperation)) {
                // Okay we have chained sequence of maps here.
                // Previous map must be already fused.
                auto clonedFusionDestination = FusionMap_[originalPreviousOperation];
                Y_ABORT_UNLESS(clonedFusionDestination);

                auto connectTo = TableProjection_.ResolveNewConnection(originalOperation->VerifiedGetSingleInput());
                FuseMapTo(originalOperation, clonedFusionDestination, VerifiedGetNodeIdOfMapperConnector(connectTo));
            } else {
                std::vector<TYtGraphV2::TTableNode*> clonedInputs;
                for (const auto* originalInputTable : originalOperation->InputTables) {
                    auto clonedInputTable = TableProjection_.VerifiedGetClonedTable(originalInputTable);
                    clonedInputs.push_back(clonedInputTable);
                }

                auto* clonedOperation = PlainGraph_.CloneOperation(clonedInputs, originalOperation);

                for (const auto& [originalConnector, originalOutputTable] : originalOperation->OutputTables) {
                    auto clonedConnector = originalConnector;
                    TableProjection_.ProjectTable(&PlainGraph_, clonedOperation, clonedConnector, originalOutputTable);
                }

                if (originalOperation->GetOperationType() == EOperationType::Map) {
                    auto inserted = FusionMap_.emplace(originalOperation, clonedOperation).second;
                    Y_ABORT_UNLESS(inserted);

                    Y_ABORT_UNLESS(std::ssize(clonedOperation->OutputTables) == std::ssize(originalOperation->OutputTables));
                    // 1. Fuse all siblings.
                    const auto* originalInput = originalOperation->VerifiedGetSingleInput();
                    for (const auto& inputFor : originalInput->InputFor) {
                        const auto* originalSibling = inputFor.Operation;
                        if (originalSibling != originalOperation && originalSibling->GetOperationType() == EOperationType::Map) {
                            FuseMapTo(originalSibling, clonedOperation, TParDoTreeBuilder::RootNodeId);
                        }
                    }
                }
            }
        }

        void FuseMapTo(const TOperationNode* originalOperation, TOperationNode* destination, TParDoTreeBuilder::TPCollectionNodeId fusionPoint)
        {
            bool inserted = false;

            const auto* originalMap = originalOperation->VerifiedAsPtr<TMapOperationNode>();
            auto* destinationMap = destination->VerifiedAsPtr<TMapOperationNode>();

            auto originalToDestinationConnectorMap = destinationMap->Fuse(*originalMap, fusionPoint);
            inserted = FusionMap_.emplace(originalOperation, destination).second;
            Y_ABORT_UNLESS(inserted);

            for (const auto& [originalConnector, originalOutputTable] : originalMap->OutputTables) {
                auto connector = originalToDestinationConnectorMap[VerifiedGetNodeIdOfMapperConnector(originalConnector)];

                TableProjection_.ProjectTable(&PlainGraph_, destination, TMapperOutputConnector{0, connector}, originalOutputTable);
            }
        }

        TYtGraphV2::TPlainGraph ExtractOutput()
        {
            auto result = std::move(PlainGraph_);
            *this = {};
            return result;
        }

    private:
        // Original map operation -> cloned map operation, that original operation is fused into
        // Trivial fusion is also counts, i.e. if fusion contains only one original map operation.
        THashMap<const TOperationNode*, TOperationNode*> FusionMap_;

        TTableProjection TableProjection_;

        TYtGraphV2::TPlainGraph PlainGraph_;
    };
};

////////////////////////////////////////////////////////////////////////////////

class TMapReduceFuser
    : public IYtGraphOptimizer
{
public:
    TYtGraphV2::TPlainGraph Optimize(const TYtGraphV2::TPlainGraph& plainGraph) override
    {
        TVisitor visitor;
        TraverseInTopologicalOrder(plainGraph, &visitor);
        auto result = visitor.ExtractOutput();

        RemoveHangingMapOperations(&result);

        return result;
    }

private:
    static void RemoveHangingMapOperations(TYtGraphV2::TPlainGraph* plainGraph)
    {
        while (true) {
            std::vector<TTableNodePtr> newTables;
            for (const auto& table : plainGraph->Tables) {
                if (table->GetTableType() == ETableType::Intermediate && table->InputFor.empty()) {
                    plainGraph->UnlinkOperationOutput(table->OutputOf.Operation, table->OutputOf.Connector);
                } else {
                    newTables.push_back(table);
                }
            }
            if (newTables.size() == plainGraph->Tables.size()) {
                break;
            }
            plainGraph->Tables = std::move(newTables);

            std::vector<TOperationNodePtr> newOperations;
            for (const auto& operation : plainGraph->Operations) {
                if (operation->OutputTables.empty()) {
                    Y_ABORT_UNLESS(operation->GetOperationType() == EOperationType::Map);
                    Y_ABORT_UNLESS(operation->InputTables.size() == 1);
                    plainGraph->ResetOperationInput(operation.get(), 0);
                } else {
                    newOperations.push_back(operation);
                }
            }
            if (newOperations.size() == plainGraph->Operations.size()) {
                break;
            }
            plainGraph->Operations = std::move(newOperations);
        }
    }

    class TVisitor
        : public IPlainGraphVisitor
    {
    public:
        void OnTableNode(TTableNode* originalTable) override
        {
            if (originalTable->GetTableType() == ETableType::Input) {
                TableProjection_.ProjectTable(&PlainGraph_, nullptr, {}, originalTable);
            }
        }

        void OnOperationNode(TYtGraphV2::TOperationNode* originalOperation) override
        {
            switch (originalOperation->GetOperationType()) {
                case EOperationType::Map: {
                    const auto* originalInput = originalOperation->VerifiedGetSingleInput();
                    auto* clonedInput = TableProjection_.VerifiedGetClonedTable(originalInput);
                    if (clonedInput->OutputOf.Operation
                        && clonedInput->OutputOf.Operation->GetOperationType() != EOperationType::Merge
                        && clonedInput->OutputOf.Operation->GetOperationType() != EOperationType::Sort) {
                        switch (clonedInput->OutputOf.Operation->GetOperationType()) {
                            case EOperationType::Merge: {
                                // We have just checked that this operation is not merge.
                                Y_ABORT("Unexpected Merge operation");
                            }
                            case EOperationType::Sort: {
                                // We have just checked that this operation is not sort.
                                Y_ABORT("Unexpected Sort operation");
                            }
                            case EOperationType::Map:
                                // It should not be EOperationType::Map operation
                                // since EOperationType::Map operation would be fused by previous stage Fuse maps
                                Y_ABORT("Unexpected Map operation");
                            case EOperationType::MapReduce: {
                                // It should not be EOperationType::Map operation
                                // since EOperationType::Map operation would be fused by previous stage Fuse maps
                                auto* clonedMapReduceOperation = clonedInput->OutputOf.Operation->VerifiedAsPtr<TMapReduceOperationNode>();
                                auto [clonedReducerBuilder, clonedReducerConnectorNodeId] = clonedMapReduceOperation->ResolveOperationConnector(clonedInput->OutputOf.Connector);
                                auto originalMapOperation = originalOperation->VerifiedAsPtr<TMapOperationNode>();
                                auto fusedConnectorMap = clonedMapReduceOperation->FuseToReducer(*originalMapOperation, clonedReducerConnectorNodeId);
                                for (const auto& [originalConnector, originalOutputTable] : originalOperation->OutputTables) {
                                    auto originalNodeId = VerifiedGetNodeIdOfMapperConnector(originalConnector);
                                    auto it = fusedConnectorMap.find(originalNodeId);
                                    Y_ABORT_UNLESS(it != fusedConnectorMap.end());
                                    auto clonedNodeId = it->second;
                                    TableProjection_.ProjectTable(
                                        &PlainGraph_,
                                        clonedMapReduceOperation,
                                        TReducerOutputConnector{clonedNodeId},
                                        originalOutputTable
                                    );
                                }
                                return;
                            }
                        }
                        Y_ABORT("Unexpected type of operation");
                    } else {
                        auto clonedOperation = PlainGraph_.CloneOperation({clonedInput}, originalOperation);
                        for (const auto& [originalConnector, originalOutputTable] : originalOperation->OutputTables) {
                            TableProjection_.ProjectTable(&PlainGraph_, clonedOperation, originalConnector, originalOutputTable);
                        }
                        return;
                    }
                }
                case EOperationType::MapReduce: {
                    std::vector<TTableNode*> clonedInputList;
                    for (const auto* originalInput : originalOperation->InputTables) {
                        auto* clonedInput = TableProjection_.VerifiedGetClonedTable(originalInput);
                        clonedInputList.push_back(clonedInput);
                    }
                    auto clonedOperation = PlainGraph_.CloneOperation({clonedInputList}, originalOperation)->VerifiedAsPtr<TMapReduceOperationNode>();
                    THashMap<std::pair<ssize_t, TParDoTreeBuilder::TPCollectionNodeId>, TParDoTreeBuilder::TPCollectionNodeId> mapperConnectorMap;

                    Y_ABORT_UNLESS(std::ssize(clonedOperation->InputTables) == std::ssize(clonedInputList));

                    // Fuse mapper stage with previous maps if possible
                    for (ssize_t i = 0; i < std::ssize(clonedOperation->InputTables); ++i) {
                        const auto& clonedInputTable = clonedOperation->InputTables[i];
                        if (clonedInputTable->OutputOf.Operation
                            && clonedInputTable->OutputOf.Operation->GetOperationType() == EOperationType::Map)
                        {
                            auto idMap = clonedOperation->FusePrecedingMapper(i);
                            for (const auto& item : idMap) {
                                auto inserted = mapperConnectorMap.emplace(std::pair{i, item.first}, item.second).second;
                                Y_ABORT_UNLESS(inserted);
                            }
                        }
                    }

                    auto resolveClonedConnector = [&mapperConnectorMap] (const TOperationConnector& connector) {
                        return std::visit([&mapperConnectorMap] (const auto& connector) -> TOperationConnector {
                            using TType = std::decay_t<decltype(connector)>;
                            if constexpr (std::is_same_v<TType, TMergeOutputConnector>) {
                                return TMergeOutputConnector{};
                            } else if constexpr (std::is_same_v<TType, TSortOutputConnector>) {
                                return TSortOutputConnector{};
                            } else if constexpr (std::is_same_v<TType, TStatefulConnector>) {
                                return TStatefulConnector{};
                            } else if constexpr (std::is_same_v<TType, TMapperOutputConnector>) {
                                auto it = mapperConnectorMap.find(std::pair{connector.MapperIndex, connector.NodeId});
                                if (it == mapperConnectorMap.end()) {
                                    return connector;
                                } else {
                                    return TMapperOutputConnector{connector.MapperIndex, it->second};
                                }
                            } else if constexpr (std::is_same_v<TType, TReducerOutputConnector>) {
                                return connector;
                            } else {
                                static_assert(TDependentFalse<TType>);
                            }
                        }, connector);
                    };

                    for (const auto& [originalConnector, originalOutputTable] : originalOperation->OutputTables) {
                        auto clonedConnector = resolveClonedConnector(originalConnector);
                        TableProjection_.ProjectTable(&PlainGraph_, clonedOperation, clonedConnector, originalOutputTable);
                    }

                    return;
                }
                case EOperationType::Merge: {
                    std::vector<TYtGraphV2::TTableNode*> clonedInputs;
                    for (const auto* originalInputTable : originalOperation->InputTables) {
                        auto clonedInputTable = TableProjection_.VerifiedGetClonedTable(originalInputTable);
                        clonedInputs.push_back(clonedInputTable);
                    }

                    auto* clonedOperation = PlainGraph_.CloneOperation(clonedInputs, originalOperation);

                    for (const auto& [originalConnector, originalOutputTable] : originalOperation->OutputTables) {
                        auto clonedConnector = originalConnector;
                        TableProjection_.ProjectTable(&PlainGraph_, clonedOperation, clonedConnector, originalOutputTable);
                    }

                    return;
                }
                case EOperationType::Sort: {
                    const auto* originalInput = originalOperation->VerifiedGetSingleInput();
                    auto* clonedInput = TableProjection_.VerifiedGetClonedTable(originalInput);
                    auto clonedOperation = PlainGraph_.CloneOperation({clonedInput}, originalOperation);
                    for (const auto& [originalConnector, originalOutputTable] : originalOperation->OutputTables) {
                        TableProjection_.ProjectTable(&PlainGraph_, clonedOperation, originalConnector, originalOutputTable);
                    }

                    return;
                }
            }
            Y_ABORT("unknown operation type");
        }

        TYtGraphV2::TPlainGraph ExtractOutput()
        {
            auto result = std::move(PlainGraph_);
            *this = {};
            return result;
        }

    private:
        TTableProjection TableProjection_;
        TYtGraphV2::TPlainGraph PlainGraph_;
    };
};

////////////////////////////////////////////////////////////////////////////////o

} // namespace NRoren::NPrivate
