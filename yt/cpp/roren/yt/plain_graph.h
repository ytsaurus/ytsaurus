#pragma once

#include "jobs.h"
#include "operations.h"
#include "tables.h"
#include "transforms.h"
#include "yt_graph_v2.h"
#include "yt_io_private.h"
#include "yt_proto_io.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TYtGraphV2::TPlainGraph
{
public:
    std::vector<TTableNodePtr> Tables;
    std::vector<TOperationNodePtr> Operations;

public:
    TMergeOperationNode* CreateMergeOperation(
        std::vector<TTableNode*> inputs,
        const TString& firstName)
    {
        auto operation = TMergeOperationNode::MakeShared(firstName, {firstName});
        Operations.push_back(operation);
        LinkWithInputs(inputs, operation.get());
        return operation.get();
    }

    std::pair<TMapOperationNode*, std::vector<TParDoTreeBuilder::TPCollectionNodeId>> CreateMapOperation(
        std::vector<TTableNode*> inputs,
        TString firstName,
        const IRawParDoPtr& rawParDo)
    {
        auto operation = TMapOperationNode::MakeShared(firstName, THashSet<TString>{firstName});
        Operations.push_back(operation);
        LinkWithInputs(std::move(inputs), operation.get());
        auto outputIdList = operation->MapperBuilder_.AddParDo(rawParDo, TParDoTreeBuilder::RootNodeId);
        return {operation.get(), std::move(outputIdList)};
    }

    void CreateIdentityMapOperation(
        std::vector<TTableNode*> inputs,
        TString firstName,
        TRowVtable outputVtable,
        IRawYtWritePtr rawYtWrite)
    {
        auto operation = TMapOperationNode::MakeShared(firstName, THashSet<TString>{firstName});
        Operations.push_back(operation);
        LinkWithInputs(std::move(inputs), operation.get());

        auto table = CreateOutputTable(operation.get(), TMapperOutputConnector{}, std::move(outputVtable), rawYtWrite);
        Y_UNUSED(table);
    }

    void CreateSortOperation(
        std::vector<TTableNode*> inputs,
        const TString& originalFirstName,
        const TString& temporaryDirectory,
        TRowVtable outputVtable,
        IRawYtSortedWritePtr rawYtSortedWrite)
    {
        auto mapFirstName = originalFirstName + "_Write";
        auto mapOperation = TMapOperationNode::MakeShared(mapFirstName, THashSet<TString>{mapFirstName});

        Operations.push_back(mapOperation);
        LinkWithInputs(std::move(inputs), mapOperation.get());

        auto intermediateTable = CreateTypedIntermediateTable(mapOperation.get(), TMapperOutputConnector{}, outputVtable, temporaryDirectory, rawYtSortedWrite);

        auto sortFirstName = originalFirstName;
        auto sortOperation = TSortOperationNode::MakeShared(sortFirstName, THashSet<TString>{sortFirstName});
        sortOperation->Write_ = rawYtSortedWrite.Get();

        Operations.push_back(sortOperation);
        LinkWithInputs({intermediateTable}, sortOperation.get());

        auto table = CreateOutputTable(sortOperation.get(), TSortOutputConnector{}, outputVtable, rawYtSortedWrite);
        Y_UNUSED(table);
    }

    TMapReduceOperationNode* CreateMapReduceOperation(std::vector<TTableNode*> inputs, const TString& firstName)
    {
        auto operation = TMapReduceOperationNode::MakeShared(firstName, THashSet<TString>{firstName}, std::ssize(inputs));
        Operations.push_back(operation);
        LinkWithInputs(std::move(inputs), operation.get());
        return operation.get();
    }

    std::pair<TMapReduceOperationNode*, TParDoTreeBuilder::TPCollectionNodeId> CreateGroupByKeyMapReduceOperation(
        TTableNode* inputTable,
        TString firstName,
        const IRawGroupByKeyPtr& rawGroupByKey,
        bool useProtoFormat)
    {
        auto* mapReduceOperation = CreateMapReduceOperation({inputTable}, firstName);

        auto parDoList = useProtoFormat
            ? std::vector<IRawParDoPtr>{
                CreateEncodingKeyValueProtoParDo(inputTable->Vtable),
                CreateWriteProtoParDo<TKVProto>(0)
            }
            : std::vector<IRawParDoPtr>{
                CreateEncodingKeyValueNodeParDo(inputTable->Vtable),
                CreateWriteNodeParDo(0)
            };

        mapReduceOperation->MapperBuilderList_[0].AddParDoChainVerifyNoOutput(
            TParDoTreeBuilder::RootNodeId,
            parDoList
        );

        auto nodeId = mapReduceOperation->ReducerBuilder_.AddParDoVerifySingleOutput(
            useProtoFormat
                ? CreateGbkImpulseReadProtoParDo(rawGroupByKey)
                : CreateGbkImpulseReadNodeParDo(rawGroupByKey),
            TParDoTreeBuilder::RootNodeId
        );
        return {mapReduceOperation, nodeId};
    }

    std::pair<TMapReduceOperationNode*, TParDoTreeBuilder::TPCollectionNodeId> CreateCoGroupByKeyMapReduceOperation(
        std::vector<TTableNode*> inputTableList,
        TString firstName,
        const IRawCoGroupByKeyPtr& rawCoGroupByKey,
        bool useProtoFormat)
    {
        auto* mapReduceOperation = CreateMapReduceOperation(inputTableList, firstName);
        Y_ABORT_UNLESS(std::ssize(mapReduceOperation->GetMapperBuilderList()) == std::ssize(inputTableList));
        for (ssize_t i = 0; i < std::ssize(inputTableList); ++i) {
            auto* inputTable = inputTableList[i];

            auto parDoList = useProtoFormat
                ? std::vector<IRawParDoPtr>{
                    CreateEncodingKeyValueProtoParDo(inputTable->Vtable),
                    CreateAddTableIndexProtoParDo(i),
                    CreateWriteProtoParDo<TKVProto>(0)
                }
                : std::vector<IRawParDoPtr>{
                    CreateEncodingKeyValueNodeParDo(inputTable->Vtable),
                    CreateAddTableIndexParDo(i),
                    CreateWriteNodeParDo(0)
                };

            mapReduceOperation->MapperBuilderList_[i].AddParDoChainVerifyNoOutput(
                TParDoTreeBuilder::RootNodeId,
                parDoList
            );
        }

        std::vector<TRowVtable> inputRowVtableList;
        for (const auto& inputTable : inputTableList) {
            inputRowVtableList.push_back(inputTable->Vtable);
        }

        auto nodeId = mapReduceOperation->ReducerBuilder_.AddParDoVerifySingleOutput(
            useProtoFormat
                ? CreateCoGbkImpulseReadProtoParDo(
                    rawCoGroupByKey,
                    inputRowVtableList
                )
                : CreateCoGbkImpulseReadNodeParDo(
                    rawCoGroupByKey,
                    inputRowVtableList
                ),
            TParDoTreeBuilder::RootNodeId
        );
        return {mapReduceOperation, nodeId};
    }

    std::pair<TMapReduceOperationNode*, TParDoTreeBuilder::TPCollectionNodeId> CreateCombinePerKeyMapReduceOperation(
        TTableNode* inputTable,
        TString firstName,
        const IRawCombinePtr& rawCombine,
        bool useProtoFormat)
    {
        auto* mapReduceOperation = CreateMapReduceOperation({inputTable}, firstName);
        auto parDoList = useProtoFormat
            ? std::vector<IRawParDoPtr>{
                CreateEncodingKeyValueProtoParDo(inputTable->Vtable),
                CreateWriteProtoParDo<TKVProto>(0)
            }
            : std::vector<IRawParDoPtr>{
                CreateEncodingKeyValueNodeParDo(inputTable->Vtable),
            CreateWriteNodeParDo(0)
            };
        mapReduceOperation->MapperBuilderList_[0].AddParDoChainVerifyNoOutput(
            TParDoTreeBuilder::RootNodeId,
            parDoList
        );

        mapReduceOperation->CombinerBuilder_.AddParDoChainVerifyNoOutput(
            TParDoTreeBuilder::RootNodeId,
            {
                useProtoFormat
                ? CreateCombineCombinerImpulseReadProtoParDo(rawCombine)
                : CreateCombineCombinerImpulseReadNodeParDo(rawCombine),
            }
        );

        auto nodeId = mapReduceOperation->ReducerBuilder_.AddParDoChainVerifySingleOutput(
            TParDoTreeBuilder::RootNodeId,
            {
                useProtoFormat
                ? CreateCombineReducerImpulseReadProtoParDo(rawCombine)
                : CreateCombineReducerImpulseReadNodeParDo(rawCombine),
            }
        );

        return {mapReduceOperation, nodeId};
    }

    std::pair<TMapReduceOperationNode*, TParDoTreeBuilder::TPCollectionNodeId> CreateStatefulParDoMapReduceOperation(
        std::vector<TTableNode*> inputs,
        TString firstName,
        const IRawStatefulParDoPtr& rawStatefulParDo,
        const TYtStateVtable* stateVtable,
        bool useProtoFormat)
    {
        Y_ABORT_UNLESS(!useProtoFormat, "StatefulParDo MapReduce doesn't support proto format for intermediate tables");

        auto operation = TMapReduceOperationNode::MakeShared(firstName, THashSet<TString>{}, inputs.size());

        for (ssize_t inputIndex = 0; inputIndex < std::ssize(inputs); ++inputIndex) {
            TTableNode* table = inputs[inputIndex];

            IRawParDoPtr encodingParDo =
                inputIndex == 0
                ? CreateStateEncodingParDo(*stateVtable)
                : CreateEncodingKeyValueNodeParDo(table->Vtable);

            std::vector<IRawParDoPtr> parDoList = {
                encodingParDo,
                CreateAddTableIndexParDo(inputIndex),
                CreateWriteNodeParDo(0)
            };
            operation->MapperBuilderList_[inputIndex].AddParDoChainVerifyNoOutput(
                TParDoTreeBuilder::RootNodeId,
                parDoList
            );
        }

        auto nodeId = operation->ReducerBuilder_.AddParDoVerifySingleOutput(
            CreateStatefulParDoReducerImpulseReadNode(rawStatefulParDo, *stateVtable),
            TParDoTreeBuilder::RootNodeId
        );

        Operations.push_back(operation);
        LinkWithInputs(std::move(inputs), operation.get());

        return {operation.get(), nodeId};
    }

    TInputTableNode* CreateInputTable(TRowVtable vtable, IRawYtReadPtr rawYtRead)
    {
        auto table = std::make_shared<TInputTableNode>(std::move(vtable), rawYtRead);
        Tables.push_back(table);
        return table.get();
    }

    TOutputTableNode* CreateOutputTable(
        TOperationNode* operation,
        TOperationConnector connector,
        TRowVtable vtable,
        IRawYtWritePtr rawYtWrite)
    {
        auto table = std::make_shared<TOutputTableNode>(std::move(vtable), rawYtWrite);
        Tables.push_back(table);
        LinkOperationOutput(operation, connector, table.get());
        return table.get();
    }

    TIntermediateTableNode* CreateIntermediateTable(
        TOperationNode* operation,
        TOperationConnector connector,
        TRowVtable vtable,
        TString temporaryDirectory,
        bool useProtoFormat)
    {
        auto table = std::make_shared<TIntermediateTableNode>(std::move(vtable), std::move(temporaryDirectory), useProtoFormat);
        Tables.push_back(table);
        LinkOperationOutput(operation, connector, table.get());
        return table.get();
    }

    TTypedIntermediateTableNode* CreateTypedIntermediateTable(
        TOperationNode* operation,
        TOperationConnector connector,
        TRowVtable vtable,
        TString temporaryDirectory,
        IRawYtWritePtr rawYtWrite)
    {
        auto table = std::make_shared<TTypedIntermediateTableNode>(
            std::move(vtable),
            std::move(temporaryDirectory),
            rawYtWrite);
        Tables.push_back(table);
        LinkOperationOutput(operation, connector, table.get());
        return table.get();
    }

    // Метод предназначен, для того чтобы клонировать вершину с операцией в новый граф.
    // Клонированная вершина выражает ту же самую пользовательскую логику, но оперирует на входных таблицах нового графа.
    // Этот метод не клонирует выходные таблицы, подразумевается, что пользователь сделает это сам, возможно с какими-то модификациями.
    TOperationNode* CloneOperation(std::vector<TTableNode*> clonedInputs, const TOperationNode* originalOperation)
    {
        Y_ABORT_UNLESS(clonedInputs.size() == originalOperation->InputTables.size());
        auto cloned = originalOperation->Clone();
        Operations.push_back(cloned);
        LinkWithInputs(std::move(clonedInputs), cloned.get());
        return cloned.get();
    }

    TTableNode* CloneTable(TOperationNode* clonedOperation, TOperationConnector clonedConnector, const TTableNode* originalTable)
    {
        auto clonedTable = originalTable->Clone();
        Tables.push_back(clonedTable);
        if (clonedOperation) {
            auto inserted = clonedOperation->OutputTables.emplace(clonedConnector, clonedTable.get()).second;
            Y_ABORT_UNLESS(inserted);
            clonedTable->OutputOf = {.Operation = clonedOperation, .Connector = clonedConnector};
        }
        return clonedTable.get();
    }

    static void LinkOperationOutput(TOperationNode* operation, TOperationConnector connector, TTableNode* table)
    {
        Y_ABORT_UNLESS(table->OutputOf.Operation == nullptr);
        auto inserted = operation->OutputTables.emplace(connector, table).second;
        Y_ABORT_UNLESS(inserted);
        table->OutputOf = {.Operation = operation, .Connector = connector};
    }

    static TTableNode* UnlinkOperationOutput(TOperationNode* operation, TOperationConnector connector)
    {
        auto it = operation->OutputTables.find(connector);
        Y_ABORT_UNLESS(it != operation->OutputTables.end());

        auto* table = it->second;
        table->OutputOf = {};

        operation->OutputTables.erase(it);
        return table;
    }

    static void ResetOperationInput(TOperationNode* operation, ssize_t inputIndex)
    {
        Y_ABORT_UNLESS(inputIndex >= 0);
        Y_ABORT_UNLESS(inputIndex < std::ssize(operation->InputTables));

        auto inputTable = operation->InputTables[inputIndex];

        // TODO: нужно заменить на set
        auto it = std::find(
            inputTable->InputFor.begin(),
            inputTable->InputFor.end(),
            TTableNode::TInputFor{operation, inputIndex}
        );
        Y_ABORT_UNLESS(it != inputTable->InputFor.end());
        inputTable->InputFor.erase(it);
        operation->InputTables[inputIndex] = nullptr;
    }

    static void ReplaceOperationInput(TOperationNode* operation, ssize_t inputIndex, TTableNode* newInputTable)
    {
        ResetOperationInput(operation, inputIndex);

        operation->InputTables[inputIndex] = newInputTable;
        newInputTable->InputFor.push_back(TTableNode::TInputFor{operation, inputIndex});
    }

    static void RelinkTableConsumers(TTableNode* oldTable, TTableNode* newTable)
    {
        for (const auto& inputFor : oldTable->InputFor) {
            auto* consumerOperation = inputFor.Operation;
            Y_ABORT_UNLESS(std::ssize(consumerOperation->InputTables) > inputFor.Index);
            Y_ABORT_UNLESS(consumerOperation->InputTables[inputFor.Index] == oldTable);

            consumerOperation->InputTables[inputFor.Index] = newTable;
            newTable->InputFor.push_back(inputFor);
        }
        oldTable->InputFor.clear();
    }

private:
    void LinkWithInputs(std::vector<TTableNode*> inputs, TOperationNode* operation)
    {
        operation->InputTables = std::move(inputs);
        for (ssize_t index = 0; index < std::ssize(operation->InputTables); ++index) {
            auto* table = operation->InputTables[index];
            table->InputFor.push_back({
                .Operation = operation,
                .Index = index,
            });
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
