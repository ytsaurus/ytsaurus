#include "yt_graph_v2.h"
#include "jobs.h"

#include "yt_graph.h"
#include "yt_io_private.h"

#include <yt/cpp/roren/yt/proto/config.pb.h>

#include <yt/cpp/roren/interface/private/par_do_tree.h>
#include <yt/cpp/roren/interface/roren.h>

#include <yt/cpp/mapreduce/interface/operation.h>
#include <yt/cpp/mapreduce/interface/client.h>

#include <library/cpp/yt/string/format.h>

#include <util/generic/hash_multi_map.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

using TTableNode = TYtGraphV2::TTableNode;
using TTableNodePtr = TYtGraphV2::TTableNodePtr;

using TOperationNode = TYtGraphV2::TOperationNode;
using TOperationNodePtr = TYtGraphV2::TOperationNodePtr;

////////////////////////////////////////////////////////////////////////////////

enum class EOperationType
{
    Map,
    MapReduce,
};

enum class EJobType
{
    Map,
    Reduce,
};

enum class ETableType
{
    Input,
    Output,
    Intermediate,
};

////////////////////////////////////////////////////////////////////////////////

using TOperationConnector = std::pair<EJobType, TParDoTreeBuilder::TPCollectionNodeId>;

class TInputTableNode;
class TOutputTableNode;

////////////////////////////////////////////////////////////////////////////////

const TPCollectionNode* VerifiedGetSingleSource(const TTransformNodePtr& transform)
{
    Y_VERIFY(transform->GetSourceCount() == 1);
    return transform->GetSource(0).Get();
}

const TPCollectionNode* VerifiedGetSingleSink(const TTransformNodePtr& transform)
{
    Y_VERIFY(transform->GetSinkCount() == 1);
    return transform->GetSink(0).Get();
}

////////////////////////////////////////////////////////////////////////////////

template <typename TDerived>
class TNodeHierarchyRoot
{
public:
    virtual ~TNodeHierarchyRoot() = default;

    template <typename T>
    T* VerifiedAsPtr()
    {
        static_assert(std::is_base_of_v<TDerived, T>, "Class is not derived from TNodeHierarchyRoot");
        auto* result = dynamic_cast<T*>(this);
        Y_VERIFY(result);
        return result;
    }

    template <typename T>
    const T* VerifiedAsPtr() const
    {
        static_assert(std::is_base_of_v<TDerived, T>, "Class is not derived from TNodeHierarchyRoot");
        const auto* result = dynamic_cast<const T*>(this);
        Y_VERIFY(result);
        return result;
    }

    template <typename T>
    T* TryAsPtr()
    {
        return dynamic_cast<T*>(this);
    }

    template <typename T>
    const T* TryAsPtr() const
    {
        return dynamic_cast<const T*>(this);
    }
};

class TAddTableIndexDoFn
    : public IDoFn<NYT::TNode, NYT::TNode>
{
public:
    TAddTableIndexDoFn() = default;

    TAddTableIndexDoFn(ssize_t tableIndex)
        : TableIndex_(tableIndex)
    {
        Y_VERIFY(tableIndex >= 0);
    }

    void Do(const NYT::TNode& row, TOutput<NYT::TNode>& output) override
    {
        auto result = row;
        result["table_index"] = TableIndex_;
        output.Add(result);
    }

private:
    ssize_t TableIndex_ = 0;
    Y_SAVELOAD_DEFINE_OVERRIDE(TableIndex_);
};

////////////////////////////////////////////////////////////////////////////////

class TYtGraphV2::TOperationNode
    : public TNodeHierarchyRoot<TYtGraphV2::TOperationNode>
{
public:
    std::vector<TTableNode*> InputTables;
    THashMap<TOperationConnector, TTableNode*> OutputTables;

public:
    TOperationNode(TString firstName)
        : FirstName_(std::move(firstName))
    {
        Y_VERIFY(!FirstName_.empty());
    }

    virtual ~TOperationNode() = default;

    virtual EOperationType GetOperationType() const = 0;

    TTableNode* VerifiedGetSingleInput() const
    {
        Y_VERIFY(InputTables.size() == 1);
        return InputTables[0];
    }

    const TString& GetFirstName() const
    {
        return FirstName_;
    }

private:
    // Клониурет вершину, сохраняя пользовательскую логику операции привязанную к ней, но не клонирует графовую структуру.
    virtual std::shared_ptr<TOperationNode> Clone() = 0;

private:
    TString FirstName_;

    friend class TPlainGraph;
};

class TMapOperationNode
    : public TYtGraphV2::TOperationNode
{
public:
    TParDoTreeBuilder MapperBuilder;

public:
    explicit TMapOperationNode(TString firstName)
        : TYtGraphV2::TOperationNode(firstName)
    { }

    EOperationType GetOperationType() const override
    {
        return EOperationType::Map;
    }

    std::shared_ptr<TOperationNode> Clone() override
    {
        auto result = std::make_shared<TMapOperationNode>(GetFirstName());
        result->MapperBuilder = MapperBuilder;
        return result;
    }
};

class TMapReduceOperationNode
    : public TYtGraphV2::TOperationNode
{
public:
    std::vector<TParDoTreeBuilder> MapperBuilderList;
    TParDoTreeBuilder CombinerBuilder;
    TParDoTreeBuilder ReducerBuilder;

public:
    TMapReduceOperationNode(
        TString firstName,
        ssize_t inputCount)
        : TYtGraphV2::TOperationNode(firstName)
        , MapperBuilderList(inputCount)
    {
        Y_VERIFY(inputCount >= 1);
    }

    EOperationType GetOperationType() const override
    {
        return EOperationType::MapReduce;
    }

    std::shared_ptr<TOperationNode> Clone() override
    {
        auto result = std::make_shared<TMapReduceOperationNode>(GetFirstName(), ssize(MapperBuilderList));
        result->MapperBuilderList = MapperBuilderList;
        result->ReducerBuilder = ReducerBuilder;
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TYtGraphV2::TTableNode
    : public TNodeHierarchyRoot<TYtGraphV2::TTableNode>
{
public:
    TRowVtable Vtable;

    struct TInputFor
    {
        TOperationNode* Operation = nullptr;
        ssize_t Index = 0;
    };

    std::vector<TInputFor> InputFor;
    struct TOutputOf {
        TOperationNode* Operation = nullptr;
        TOperationConnector Connector = {EJobType::Map, 0};
    } OutputOf = {};

public:
    explicit TTableNode(TRowVtable vtable)
        : Vtable(vtable)
    { }

    virtual ~TTableNode() = default;

    virtual ETableType GetTableType() const = 0;

    virtual IRawParDoPtr CreateTNodeDecodingParDo() const = 0;
    virtual IRawParDoPtr CreateTNodeEncodingParDo() const = 0;

    virtual NYT::TRichYPath GetPath() const = 0;

private:
    virtual std::shared_ptr<TTableNode> Clone() const = 0;

    friend class TPlainGraph;
};

////////////////////////////////////////////////////////////////////////////////

class TInputTableNode
    : public TYtGraphV2::TTableNode
{
public:
    TInputTableNode(TRowVtable vtable, IRawYtReadPtr rawYtRead)
        : TYtGraphV2::TTableNode(std::move(vtable))
        , RawYtRead_(rawYtRead)
    { }

    ETableType GetTableType() const override
    {
        return ETableType::Input;
    }

    NYT::TRichYPath GetPath() const override
    {
        return RawYtRead_->GetPath();
    }

    IRawParDoPtr CreateTNodeDecodingParDo() const override
    {
        return MakeRawIdComputation(MakeRowVtable<NYT::TNode>());
    }

    IRawParDoPtr CreateTNodeEncodingParDo() const override
    {
        Y_FAIL("TInputTableNode is not expected to be written into");
    }

private:
    std::shared_ptr<TTableNode> Clone() const override
    {
        return std::make_shared<TInputTableNode>(Vtable, RawYtRead_);
    }

private:
    const IRawYtReadPtr RawYtRead_;
};

////////////////////////////////////////////////////////////////////////////////

class TOutputTableNode
    : public TYtGraphV2::TTableNode
{
public:
    TOutputTableNode(TRowVtable vtable, IRawYtWritePtr rawYtWrite)
        : TYtGraphV2::TTableNode(std::move(vtable))
        , RawYtWrite_(std::move(rawYtWrite))
    { }

    ETableType GetTableType() const override
    {
        return ETableType::Output;
    }

    IRawParDoPtr CreateTNodeDecodingParDo() const override
    {
        return MakeRawIdComputation(MakeRowVtable<NYT::TNode>());
    }

    IRawParDoPtr CreateTNodeEncodingParDo() const override
    {
        return MakeRawIdComputation(MakeRowVtable<NYT::TNode>());
    }

    NYT::TRichYPath GetPath() const override
    {
        return RawYtWrite_->GetPath();
    }

    const NYT::TTableSchema& GetSchema() const
    {
        return RawYtWrite_->GetSchema();
    }

    std::vector<std::pair<NYT::TRichYPath, NYT::TTableSchema>> GetPathSchemaList() const
    {
        std::vector<std::pair<NYT::TRichYPath, NYT::TTableSchema>> result;
        result.emplace_back(GetPath(), GetSchema());
        for (const auto& write : SecondaryYtWriteList_) {
            result.emplace_back(write->GetPath(), write->GetSchema());
        }
        return result;
    }

    void MergeFrom(const TOutputTableNode& other)
    {
        SecondaryYtWriteList_.push_back(std::move(other.RawYtWrite_));
        SecondaryYtWriteList_.insert(SecondaryYtWriteList_.end(), other.SecondaryYtWriteList_.begin(), other.SecondaryYtWriteList_.end());
    }

private:
    std::shared_ptr<TTableNode> Clone() const override
    {
        return std::make_shared<TOutputTableNode>(Vtable, RawYtWrite_);
    }

private:
    const IRawYtWritePtr RawYtWrite_;
    std::vector<IRawYtWritePtr> SecondaryYtWriteList_;
};

////////////////////////////////////////////////////////////////////////////////

class TIntermediateTableNode
    : public TYtGraphV2::TTableNode
{
public:
    TIntermediateTableNode(TRowVtable vtable, TString temporaryDirectory)
        : TYtGraphV2::TTableNode(std::move(vtable))
        , TemporaryDirectory_(std::move(temporaryDirectory))
    { }

    ETableType GetTableType() const override
    {
        return ETableType::Intermediate;
    }

    IRawParDoPtr CreateTNodeDecodingParDo() const override
    {
        return CreateDecodingValueNodeParDo(Vtable);
    }

    IRawParDoPtr CreateTNodeEncodingParDo() const override
    {
        return CreateEncodingValueNodeParDo(Vtable);
    }

    virtual NYT::TRichYPath GetPath() const override
    {
        Y_VERIFY(OutputOf.Operation);

        TStringStream tableName;
        tableName << OutputOf.Operation->GetFirstName();
        tableName << "." << [&] {
            switch (OutputOf.Connector.first) {
                case EJobType::Map:
                    return "Map";
                case EJobType::Reduce:
                    return "Reduce";
            }
            Y_FAIL("unreachable");
        } () << "." << OutputOf.Connector.second;

        return TemporaryDirectory_ + "/" + tableName.Str();
    }

private:
    std::shared_ptr<TTableNode> Clone() const override
    {
        return std::make_shared<TIntermediateTableNode>(Vtable, TemporaryDirectory_);
    }

private:
    const TString TemporaryDirectory_;
};

////////////////////////////////////////////////////////////////////////////////

class TYtGraphV2::TPlainGraph
{
public:
    std::vector<TTableNodePtr> Tables;
    std::vector<TOperationNodePtr> Operations;

public:
    TMapOperationNode* CreateMapOperation(std::vector<TTableNode*> inputs, TString firstName)
    {
        auto operation = std::make_shared<TMapOperationNode>(std::move(firstName));
        Operations.push_back(operation);
        LinkWithInputs(std::move(inputs), operation.get());
        return operation.get();
    }

    std::pair<TOperationNode*,TTableNode*> CreateMergeOperation(
        std::vector<TTableNode*> inputs,
        TString operationFirstName,
        TRowVtable outputVtable,
        IRawYtWritePtr rawYtWrite)
    {
        auto operation = std::make_shared<TMapOperationNode>(std::move(operationFirstName));
        Operations.push_back(operation);
        LinkWithInputs(std::move(inputs), operation.get());

        auto table = std::make_shared<TOutputTableNode>(
            std::move(outputVtable),
            rawYtWrite);
        Tables.push_back(table);
        table->OutputOf = {.Operation = operation.get(), .Connector = {}};
        operation->OutputTables[TOperationConnector{EJobType::Map, TParDoTreeBuilder::RootNodeId}] = table.get();

        return {operation.get(), table.get()};
    }

    TMapReduceOperationNode* CreateMapReduceOperation(std::vector<TTableNode*> inputs, TString firstName)
    {
        auto operation = std::make_shared<TMapReduceOperationNode>(std::move(firstName), std::ssize(inputs));
        Operations.push_back(operation);
        LinkWithInputs(std::move(inputs), operation.get());
        return operation.get();
    }

    TInputTableNode* CreateInputTable(TRowVtable vtable, const IRawYtReadPtr& rawYtRead)
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

        auto inserted = operation->OutputTables.emplace(connector, table.get()).second;
        Y_VERIFY(inserted);
        table->OutputOf = {.Operation = operation, .Connector = connector};

        return table.get();
    }

    TIntermediateTableNode* CreateIntermediateTable(
        TOperationNode* operation,
        TOperationConnector connector,
        TRowVtable vtable,
        TString temporaryDirectory)
    {
        auto table = std::make_shared<TIntermediateTableNode>(std::move(vtable), std::move(temporaryDirectory));
        Tables.push_back(table);
        auto inserted = operation->OutputTables.emplace(connector, table.get()).second;
        Y_VERIFY(inserted);
        table->OutputOf = {.Operation = operation, .Connector = connector};

        return table.get();
    }

    // Метод предназначен, для того чтобы клонировать вершину с операцией в новый граф.
    // Клонированная вершина выражает ту же самую пользовательскую логику, но оперирует на входных таблицах нового графа.
    // Этот метод не клонирует выходные таблицы, подразумевается, что пользователь сделает это сам, возможно с какими-то модификациями.
    TOperationNode* CloneOperation(std::vector<TTableNode*> clonedInputs, TOperationNode* originalOperation)
    {
        Y_VERIFY(clonedInputs.size() == originalOperation->InputTables.size());
        auto cloned = originalOperation->Clone();
        Operations.push_back(cloned);
        LinkWithInputs(std::move(clonedInputs), cloned.get());
        return cloned.get();
    }

    TTableNode* CloneTable(TOperationNode* clonedOperation, TOperationConnector clonedConnector, TTableNode* originalTable)
    {
        auto clonedTable = originalTable->Clone();
        Tables.push_back(clonedTable);
        if (clonedOperation) {
            auto inserted = clonedOperation->OutputTables.emplace(clonedConnector, clonedTable.get()).second;
            Y_VERIFY(inserted);
            clonedTable->OutputOf = {.Operation = clonedOperation, .Connector = clonedConnector};
        }
        return clonedTable.get();
    }

    TTableNode* UnlinkOperationOutput(TOperationNode* operation, TOperationConnector connector)
    {
        auto it = operation->OutputTables.find(connector);
        Y_VERIFY(it != operation->OutputTables.end());

        auto* table = it->second;
        table->OutputOf = {};

        operation->OutputTables.erase(it);
        return table;
    }

    void RelinkTableConsumers(TTableNode* oldTable, TTableNode* newTable)
    {
        for (const auto& inputFor : oldTable->InputFor) {
            auto* consumerOperation = inputFor.Operation;
            Y_VERIFY(std::ssize(consumerOperation->InputTables) > inputFor.Index);
            Y_VERIFY(consumerOperation->InputTables[inputFor.Index] == oldTable);

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

class IPlainGraphVisitor
{
public:
    virtual ~IPlainGraphVisitor() = default;

    virtual void OnTableNode(TYtGraphV2::TTableNode* /*tableNode*/)
    { }

    virtual void OnOperationNode(TYtGraphV2::TOperationNode* /*operationNode*/)
    { }
};

////////////////////////////////////////////////////////////////////////////////

void TraverseInTopologicalOrder(const TYtGraphV2::TPlainGraph& plainGraph, IPlainGraphVisitor* visitor)
{
    THashSet<TYtGraphV2::TTableNode*> visited;
    auto tryVisitTable = [&] (TYtGraphV2::TTableNode* table) {
        auto inserted = visited.insert(table).second;
        if (inserted) {
            visitor->OnTableNode(table);
        }
    };

    for (const auto& operation : plainGraph.Operations) {
        for (const auto& table : operation->InputTables) {
            tryVisitTable(table);
        }
        visitor->OnOperationNode(operation.get());
        for (const auto& [connector, table] : operation->OutputTables) {
            tryVisitTable(table);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

/// TEphemeralImage is an image that is inside some TParDoTree builder.
struct TEphemeralImage
{
    TYtGraphV2::TOperationNode* Operation;
    TOperationConnector Connector;

    TEphemeralImage(TYtGraphV2::TOperationNode* operation, const TOperationConnector&& connector)
        : Operation(operation)
        , Connector(connector)
    { }
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
                ProjectTable(nullptr, {}, originalTable);
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
                Y_VERIFY(clonedFusionDestination);

                auto connectTo = ResolveNewConnection(originalOperation->VerifiedGetSingleInput());
                FuseMapTo(originalOperation, clonedFusionDestination, connectTo);
            } else {
                std::vector<TYtGraphV2::TTableNode*> clonedInputs;
                for (const auto* originalInputTable : originalOperation->InputTables) {
                    auto clonedInputTable = TableMap_[originalInputTable];
                    Y_VERIFY(clonedInputTable);
                    clonedInputs.push_back(clonedInputTable);
                }

                auto* clonedOperation = PlainGraph_.CloneOperation(clonedInputs, originalOperation);

                for (const auto& [originalConnector, originalOutputTable] : originalOperation->OutputTables) {
                    auto clonedConnector = originalConnector;
                    ProjectTable(clonedOperation, clonedConnector, originalOutputTable);
                }

                if (originalOperation->GetOperationType() == EOperationType::Map) {
                    auto inserted = FusionMap_.emplace(originalOperation, clonedOperation).second;
                    Y_VERIFY(inserted);

                    Y_VERIFY(std::ssize(clonedOperation->OutputTables) == std::ssize(originalOperation->OutputTables));
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

            auto originalToDestinationConnectorMap = destinationMap->MapperBuilder.Fuse(originalMap->MapperBuilder, fusionPoint);
            inserted = FusionMap_.emplace(originalOperation, destination).second;
            Y_VERIFY(inserted);

            for (const auto& [originalConnector, originalOutputTable] : originalMap->OutputTables) {
                auto connector = originalToDestinationConnectorMap[originalConnector.second];

                ProjectTable(destination, {EJobType::Map, connector}, originalOutputTable);
            }
        }

        TYtGraphV2::TPlainGraph ExtractOutput()
        {
            auto result = std::move(PlainGraph_);
            *this = {};
            return result;
        }

    private:
        void ProjectTable(TOperationNode* clonedOperation, TOperationConnector clonedConnector, TTableNode* originalTable)
        {
            TTableNode* newClonedTable = nullptr;
            TTableNode* oldClonedTable = nullptr;
            if (clonedOperation) {
                if (auto it = clonedOperation->OutputTables.find(clonedConnector); it != clonedOperation->OutputTables.end()) {
                    oldClonedTable = it->second;
                }
            }

            if (oldClonedTable == nullptr) {
                newClonedTable = PlainGraph_.CloneTable(clonedOperation, clonedConnector, originalTable);
                RegisterTableProjection(originalTable, newClonedTable);
            } else if (oldClonedTable->GetTableType() == ETableType::Intermediate) {
                Y_VERIFY(originalTable->GetTableType() == ETableType::Output);

                auto unlinked = PlainGraph_.UnlinkOperationOutput(clonedOperation, clonedConnector);
                Y_VERIFY(unlinked == oldClonedTable);

                newClonedTable = PlainGraph_.CloneTable(clonedOperation, clonedConnector, originalTable);
                RegisterTableProjection(originalTable, newClonedTable);

                PlainGraph_.RelinkTableConsumers(oldClonedTable, newClonedTable);

                auto it = ReverseTableMap_.find(oldClonedTable);
                Y_VERIFY(it != ReverseTableMap_.end());
                auto oldOriginalList = it->second;
                for (auto* original : oldOriginalList) {
                    DeregisterTableProjection(original);
                    RegisterTableProjection(original, newClonedTable);
                }
            } else if (oldClonedTable->GetTableType() == ETableType::Output && originalTable->GetTableType() == ETableType::Intermediate) {
                RegisterTableProjection(originalTable, oldClonedTable);
            } else {
                Y_VERIFY(oldClonedTable->GetTableType() == ETableType::Output);
                Y_VERIFY(originalTable->GetTableType() == ETableType::Output);

                oldClonedTable->VerifiedAsPtr<TOutputTableNode>()->MergeFrom(
                    *originalTable->VerifiedAsPtr<TOutputTableNode>()
                );
                RegisterTableProjection(originalTable, oldClonedTable);
            }
        }

        void RegisterTableProjection(const TTableNode* original, TTableNode* cloned)
        {
            bool inserted;
            inserted = TableMap_.emplace(original, cloned).second;
            Y_VERIFY(inserted);
            inserted = ReverseTableMap_[cloned].insert(original).second;
            Y_VERIFY(inserted);
        };

        void DeregisterTableProjection(const TTableNode* original)
        {
            auto itTableMap = TableMap_.find(original);
            Y_VERIFY(itTableMap != TableMap_.end());
            auto* cloned  = itTableMap->second;
            TableMap_.erase(itTableMap);

            auto itReverseTableMap = ReverseTableMap_.find(cloned);
            Y_VERIFY(itReverseTableMap != ReverseTableMap_.end());
            auto& originalSet = itReverseTableMap->second;

            auto itOriginalSet = originalSet.find(original);
            Y_VERIFY(itOriginalSet != originalSet.end());

            originalSet.erase(itOriginalSet);

            if (originalSet.empty()) {
                ReverseTableMap_.erase(itReverseTableMap);
            }
        };

        TParDoTreeBuilder::TPCollectionNodeId ResolveNewConnection(const TTableNode* oldTableNode)
        {
            Y_VERIFY(oldTableNode->OutputOf.Operation);
            Y_VERIFY(oldTableNode->OutputOf.Operation->GetOperationType() == EOperationType::Map);

            auto it = TableMap_.find(oldTableNode);
            Y_VERIFY(it != TableMap_.end());

            auto* newTableNode = it->second;
            return newTableNode->OutputOf.Connector.second;
        }

    private:
        // Original map operation -> cloned map operation, that original operation is fused into
        // Trivial fusion is also counts, i.e. if fusion contains only one original map operation.
        THashMap<const TOperationNode*, TOperationNode*> FusionMap_;

        THashMap<const TYtGraphV2::TTableNode*, TYtGraphV2::TTableNode*> TableMap_;
        THashMap<TTableNode*, THashSet<const TTableNode*>> ReverseTableMap_;

        TYtGraphV2::TPlainGraph PlainGraph_;
    };
};

////////////////////////////////////////////////////////////////////////////////

class TYtGraphV2Builder
    : public IRawPipelineVisitor
{
public:
    TYtGraphV2Builder(const TYtPipelineConfig& config)
        : Config_(std::make_shared<TYtPipelineConfig>(config))
    { }

    void OnTransform(TTransformNode* transformNode) override
    {
        auto rawTransform = transformNode->GetRawTransform();
        switch (rawTransform->GetType()) {
            case ERawTransformType::Read:
                if (auto* rawYtRead = dynamic_cast<IRawYtRead*>(&*rawTransform->AsRawRead())) {
                    Y_VERIFY(transformNode->GetSinkCount() == 1);
                    const auto* pCollectionNode = transformNode->GetSink(0).Get();
                    auto* tableNode = PlainGraph_->CreateInputTable(pCollectionNode->GetRowVtable(), IRawYtReadPtr{rawYtRead});
                    RegisterPCollection(pCollectionNode, tableNode);
                } else {
                    ythrow yexception() << transformNode->GetName() << " is not a YtRead and not supported";
                }
                break;
            case ERawTransformType::Write:
                if (auto* rawYtWrite = dynamic_cast<IRawYtWrite*>(&*rawTransform->AsRawWrite())) {
                    Y_VERIFY(transformNode->GetSourceCount() == 1);
                    const auto* sourcePCollection = transformNode->GetSource(0).Get();
                    auto inputTable = GetPCollectionImage(sourcePCollection);
                    PlainGraph_->CreateMergeOperation(
                        {inputTable},
                        transformNode->GetName(),
                        sourcePCollection->GetRowVtable(),
                        rawYtWrite
                    );
                } else {
                    Y_FAIL("YT executor doesn't support writes except YtWrite");
                }
                break;
            case ERawTransformType::ParDo: {
                // 1. Посмотреть к чему привязан вход
                const auto* sourcePCollection = VerifiedGetSingleSource(transformNode);
                auto* inputTable = GetPCollectionImage(sourcePCollection);
                auto rawParDo = rawTransform->AsRawParDo();

                // Наша нода-источник это таблица, надо проверить есть ли уже операции с её участием?
                Y_VERIFY(!transformNode->GetName().empty());
                auto* mapOperation = PlainGraph_->CreateMapOperation({inputTable}, transformNode->GetName());
                auto outputIdList = mapOperation->MapperBuilder.AddParDo(
                    rawParDo,
                    TParDoTreeBuilder::RootNodeId
                );

                Y_VERIFY(std::ssize(outputIdList) == transformNode->GetSinkCount());

                for (ssize_t i = 0; i < std::ssize(outputIdList); ++i) {
                    auto id = outputIdList[i];
                    const auto* pCollection = transformNode->GetSink(i).Get();
                    auto* outputTable = PlainGraph_->CreateIntermediateTable(
                        mapOperation,
                        {EJobType::Map, id},
                        pCollection->GetRowVtable(),
                        Config_->GetWorkingDir()
                    );
                    RegisterPCollection(pCollection, outputTable);
                }

                // 1. Узнать, к чему привязана эта нода, если к таблице, то создать новый узел операции.
                break;
            }
            case ERawTransformType::GroupByKey: {
                const auto* sourcePCollection = VerifiedGetSingleSource(transformNode);
                auto* inputTable = GetPCollectionImage(sourcePCollection);

                auto* mapReduceOperation = PlainGraph_->CreateMapReduceOperation({inputTable}, transformNode->GetName());
                mapReduceOperation->MapperBuilderList[0].AddParDoChainVerifyNoOutput(
                    TParDoTreeBuilder::RootNodeId,
                    {
                        CreateEncodingKeyValueNodeParDo(inputTable->Vtable),
                        CreateWriteNodeParDo(0)
                    }
                );

                auto nodeId = mapReduceOperation->ReducerBuilder.AddParDoVerifySingleOutput(
                    CreateGbkImpulseReadParDo(transformNode->GetRawTransform()->AsRawGroupByKey()),
                    TParDoTreeBuilder::RootNodeId
                );

                const auto* sinkPCollection = VerifiedGetSingleSink(transformNode);
                auto* outputTable = PlainGraph_->CreateIntermediateTable(
                    mapReduceOperation,
                    {EJobType::Reduce, nodeId},
                    sinkPCollection->GetRowVtable(),
                    Config_->GetWorkingDir()
                );
                RegisterPCollection(sinkPCollection, outputTable);
                break;
            }
            case ERawTransformType::CoGroupByKey: {
                std::vector<TTableNode*> inputTableList;
                std::vector<TRowVtable> inputRowVtableList;
                for (const auto& sourcePCollection : transformNode->GetSourceList()) {
                    auto* inputTable = GetPCollectionImage(sourcePCollection.Get());
                    inputTableList.push_back(inputTable);
                    inputRowVtableList.push_back(inputTable->Vtable);
                }

                auto* mapReduceOperation = PlainGraph_->CreateMapReduceOperation(inputTableList, transformNode->GetName());
                Y_VERIFY(mapReduceOperation->MapperBuilderList.size() == inputTableList.size());
                for (ssize_t i = 0; i < std::ssize(inputTableList); ++i) {
                    auto* inputTable = inputTableList[i];

                    mapReduceOperation->MapperBuilderList[i].AddParDoChainVerifyNoOutput(
                        TParDoTreeBuilder::RootNodeId,
                        {
                            CreateEncodingKeyValueNodeParDo(inputTable->Vtable),
                            MakeRawParDo(::MakeIntrusive<TAddTableIndexDoFn>(i)),
                            CreateWriteNodeParDo(0)
                        }
                    );
                }

                auto nodeId = mapReduceOperation->ReducerBuilder.AddParDoVerifySingleOutput(
                    CreateCoGbkImpulseReadParDo(
                        transformNode->GetRawTransform()->AsRawCoGroupByKey(),
                        inputRowVtableList
                    ),
                    TParDoTreeBuilder::RootNodeId
                );

                const auto* sinkPCollection = VerifiedGetSingleSink(transformNode);
                auto* outputTable = PlainGraph_->CreateIntermediateTable(
                    mapReduceOperation,
                    {EJobType::Reduce, nodeId},
                    sinkPCollection->GetRowVtable(),
                    Config_->GetWorkingDir()
                );
                RegisterPCollection(sinkPCollection, outputTable);
                break;
            }
            case ERawTransformType::CombinePerKey: {
                const auto* sourcePCollection = VerifiedGetSingleSource(transformNode);
                auto* inputTable = GetPCollectionImage(sourcePCollection);
                auto rawCombinePerKey = transformNode->GetRawTransform()->AsRawCombine();

                auto* mapReduceOperation = PlainGraph_->CreateMapReduceOperation({inputTable}, transformNode->GetName());
                mapReduceOperation->MapperBuilderList[0].AddParDoChainVerifyNoOutput(
                    TParDoTreeBuilder::RootNodeId,
                    {
                        CreateEncodingKeyValueNodeParDo(inputTable->Vtable),
                        CreateWriteNodeParDo(0)
                    }
                );

                mapReduceOperation->CombinerBuilder.AddParDoChainVerifyNoOutput(
                    TParDoTreeBuilder::RootNodeId,
                    {
                        CreateCombineCombinerImpulseReadParDo(rawCombinePerKey),
                    }
                );

                auto nodeId = mapReduceOperation->ReducerBuilder.AddParDoChainVerifySingleOutput(
                    TParDoTreeBuilder::RootNodeId,
                    {
                        CreateCombineReducerImpulseReadParDo(rawCombinePerKey),
                    }
                );

                const auto* sinkPCollection = VerifiedGetSingleSink(transformNode);
                auto* outputTable = PlainGraph_->CreateIntermediateTable(
                    mapReduceOperation,
                    {EJobType::Reduce, nodeId},
                    sinkPCollection->GetRowVtable(),
                    Config_->GetWorkingDir()
                );
                RegisterPCollection(sinkPCollection, outputTable);
                break;

            }
            case ERawTransformType::StatefulTimerParDo:
            case ERawTransformType::CombineGlobally:
            case ERawTransformType::Flatten:

            case ERawTransformType::StatefulParDo:
                Y_FAIL("Not implemented yet");
        }
    }

    std::unique_ptr<TYtGraphV2> Build()
    {
        return std::make_unique<TYtGraphV2>(std::move(PlainGraph_));
    }

private:
    void RegisterPCollection(const TPCollectionNode* pCollection, TTableNode* image)
    {
        auto inserted = PCollectionMap_.emplace(pCollection, std::move(image)).second;
        Y_VERIFY(inserted);
    }

    TTableNode* GetPCollectionImage(const TPCollectionNode* pCollection) const
    {
        auto it = PCollectionMap_.find(pCollection);
        Y_VERIFY(it != PCollectionMap_.end());
        return it->second;
    }

private:
    const std::shared_ptr<const TYtPipelineConfig> Config_;

    std::unique_ptr<TYtGraphV2::TPlainGraph> PlainGraph_ = std::make_unique<TYtGraphV2::TPlainGraph>();

    THashMap<const TPCollectionNode*, TTableNode*> PCollectionMap_;
};

////////////////////////////////////////////////////////////////////////////////

TYtGraphV2::TYtGraphV2(std::unique_ptr<TPlainGraph> plainGraph)
    : PlainGraph_(std::move(plainGraph))
{ }

TYtGraphV2::~TYtGraphV2()
{ }

void TYtGraphV2::Optimize()
{
    TMapFuser mapFuser;
    *PlainGraph_ = mapFuser.Optimize(*PlainGraph_);
}

std::vector<std::vector<IYtGraph::TOperationNodeId>> TYtGraphV2::GetOperationLevels() const
{
    std::vector<const TTableNode*> readyTables;

    THashMap<const TOperationNode*, std::set<const TTableNode*>> dependencyMap;
    THashMap<const TOperationNode*, std::vector<const TTableNode*>> outputMap;
    THashMap<const TOperationNode*, IYtGraph::TOperationNodeId> idMap;

    for (const auto& tableNode : PlainGraph_->Tables) {
        if (tableNode->GetTableType() == ETableType::Input) {
            readyTables.push_back(tableNode.get());
        }
    }

    for (ssize_t i = 0; i < std::ssize(PlainGraph_->Operations); ++i) {
        const auto& operationNode = PlainGraph_->Operations[i];
        idMap[operationNode.get()] = i;
        dependencyMap[operationNode.get()].insert(
            operationNode->InputTables.begin(),
            operationNode->InputTables.end());

        auto& curOutputs = outputMap[operationNode.get()];
        for (const auto& item : operationNode->OutputTables) {
            curOutputs.push_back(item.second);
        }
    }

    std::vector<std::vector<IYtGraph::TOperationNodeId>> operationNodeLevels;
    while (!readyTables.empty()) {
        std::vector<const TOperationNode*> readyOperations;

        for (const auto* table : readyTables) {
            for (const auto& inputFor : table->InputFor) {
                auto* operation = inputFor.Operation;
                auto it = dependencyMap.find(operation);
                Y_VERIFY(it != dependencyMap.end());
                auto& dependencies = it->second;
                dependencies.erase(table);
                if (dependencies.empty()) {
                    readyOperations.push_back(operation);
                    dependencyMap.erase(operation);
                }
            }

            operationNodeLevels.emplace_back();
            operationNodeLevels.back().reserve(readyOperations.size());
        }

        readyTables.clear();
        for (const auto* operation : readyOperations) {
            operationNodeLevels.back().push_back(idMap[operation]);
            for (const auto& item : operation->OutputTables) {
                readyTables.push_back(item.second);
            }
        }
    }

    return operationNodeLevels;
}

NYT::IOperationPtr TYtGraphV2::StartOperation(const NYT::IClientBasePtr& client, TOperationNodeId nodeId) const
{
    Y_VERIFY(nodeId >= 0 && nodeId < std::ssize(PlainGraph_->Operations));
    const auto& operation = PlainGraph_->Operations[nodeId];
    switch (operation->GetOperationType()) {
        case EOperationType::Map: {
            const auto* mapOperation = operation->VerifiedAsPtr<TMapOperationNode>();

            if (mapOperation->MapperBuilder.Empty()) {
                NYT::TMergeOperationSpec spec;
                for (const auto *table : operation->InputTables) {
                    spec.AddInput(table->GetPath());
                }
                Y_VERIFY(std::ssize(operation->OutputTables) == 1);
                for (const auto &[_, table] : operation->OutputTables) {
                    auto output = table->GetPath();
                    output.Schema(table->VerifiedAsPtr<TOutputTableNode>()->GetSchema());
                    spec.Output(table->GetPath());
                }
                return client->Merge(spec, NYT::TOperationOptions{}.Wait(false));
            } else {
                NYT::TRawMapOperationSpec spec;
                Y_VERIFY(operation->InputTables.size() == 1);
                const auto* inputTable = operation->InputTables[0];
                spec.AddInput(inputTable->GetPath());

                TParDoTreeBuilder tmpMapBuilder = mapOperation->MapperBuilder;
                std::vector<IYtJobOutputPtr> jobOutputs;
                ssize_t sinkIndex = 0;
                for (const auto &[key, table] : operation->OutputTables) {
                    Y_VERIFY(key.first == EJobType::Map);
                    auto output = table->GetPath();
                    if (auto* outputTable = table->TryAsPtr<TOutputTableNode>()) {
                        output.Schema(outputTable->GetSchema());
                    }
                    spec.AddOutput(table->GetPath());
                    auto nodeId = tmpMapBuilder.AddParDoVerifySingleOutput(
                        table->CreateTNodeEncodingParDo(),
                        key.second
                    );
                    tmpMapBuilder.AddParDoVerifyNoOutput(
                        CreateWriteNodeParDo(sinkIndex),
                        nodeId
                    );

                    ++sinkIndex;
                }
                TParDoTreeBuilder mapperBuilder;
                auto nodeId = mapperBuilder.AddParDoVerifySingleOutput(
                    CreateReadNodeImpulseParDo(1),
                    TParDoTreeBuilder::RootNodeId
                );
                nodeId = mapperBuilder.AddParDoVerifySingleOutput(
                    inputTable->CreateTNodeDecodingParDo(),
                    nodeId
                );
                mapperBuilder.Fuse(tmpMapBuilder, nodeId);

                spec.Format(NYT::TFormat::YsonBinary());
                NYT::IRawJobPtr job = CreateImpulseJob(mapperBuilder.Build());
                return client->RawMap(spec, job, NYT::TOperationOptions().Wait(false));
            }
        }
        case EOperationType::MapReduce: {
            const auto* mapReduceOperation = operation->VerifiedAsPtr<TMapReduceOperationNode>();

            Y_VERIFY(operation->InputTables.size() == mapReduceOperation->MapperBuilderList.size());

            NYT::TRawMapReduceOperationSpec spec;
            auto tmpMapperBuilderList = mapReduceOperation->MapperBuilderList;
            for (const auto* inputTable : operation->InputTables) {
                spec.AddInput(inputTable->GetPath());
            }

            auto reducerBuilder = mapReduceOperation->ReducerBuilder;
            ssize_t mapperOutputIndex = 1;
            ssize_t reducerOutputIndex = 0;
            for (const auto& [connector, outputTable] : mapReduceOperation->OutputTables) {
                switch (connector.first) {
                    case EJobType::Map:
                        tmpMapperBuilderList[0].AddParDoChainVerifyNoOutput(
                            connector.second,
                            {
                                outputTable->CreateTNodeEncodingParDo(),
                                CreateWriteNodeParDo(mapperOutputIndex)
                            }
                        );
                        spec.AddMapOutput(outputTable->GetPath());
                        ++mapperOutputIndex;
                        break;
                    case EJobType::Reduce:
                        reducerBuilder.AddParDoChainVerifyNoOutput(
                            connector.second,
                            {
                                outputTable->CreateTNodeEncodingParDo(),
                                CreateWriteNodeParDo(reducerOutputIndex)
                            }
                        );
                        spec.AddOutput(outputTable->GetPath());
                        ++reducerOutputIndex;
                        break;
                }
            }

            TParDoTreeBuilder mapBuilder;
            auto impulseOutputIdList = mapBuilder.AddParDo(
                CreateReadNodeImpulseParDo(std::ssize(operation->InputTables)),
                TParDoTreeBuilder::RootNodeId
            );
            Y_VERIFY(impulseOutputIdList.size() == tmpMapperBuilderList.size());
            Y_VERIFY(impulseOutputIdList.size() == operation->InputTables.size());
            for (ssize_t i = 0; i < std::ssize(impulseOutputIdList); ++i) {
                auto decodedId = mapBuilder.AddParDoVerifySingleOutput(
                    CreateDecodingValueNodeParDo(operation->InputTables[i]->Vtable),
                    impulseOutputIdList[i]
                );
                mapBuilder.Fuse(tmpMapperBuilderList[i], decodedId);
            }

            auto mapperJob = CreateImpulseJob(mapBuilder.Build());
            auto reducerJob = CreateImpulseJob(reducerBuilder.Build());

            NYT::IRawJobPtr combinerJob = nullptr;
            if (!mapReduceOperation->CombinerBuilder.Empty()) {
                spec.ForceReduceCombiners(true);
                auto combinerBuilder = mapReduceOperation->CombinerBuilder;
                combinerJob = CreateImpulseJob(combinerBuilder.Build());
            }

            spec.MapperFormat(NYT::TFormat::YsonBinary());
            spec.ReducerFormat(NYT::TFormat::YsonBinary());
            spec.ReduceBy({"key"});

            return client->RawMapReduce(
                spec,
                mapperJob,
                nullptr,
                reducerJob,
                NYT::TOperationOptions().Wait(false)
            );
        }
    }
}

TString TYtGraphV2::DumpDOTSubGraph(const TString&) const
{
    Y_FAIL("Not implemented yet");
    return TString{};
}

TString TYtGraphV2::DumpDOT(const TString&) const
{
    Y_FAIL("Not implemented yet");
    return TString{};
}

std::set<TString> TYtGraphV2::GetEdgeDebugStringSet() const
{
    std::set<TString> result;
    for (const auto& table : PlainGraph_->Tables) {
        if (table->GetTableType() == ETableType::Input) {
            for (const auto& inputFor : table->InputFor) {
                auto edge = NYT::Format("%v -> %v", table->GetPath().Path_, inputFor.Operation->GetFirstName());
                result.insert(std::move(edge));
            }
        }
    }

    for (const auto& operation : PlainGraph_->Operations) {
        for (const auto& [connector, outputTable] : operation->OutputTables) {
            switch (outputTable->GetTableType()) {
                case ETableType::Output: {
                    TStringStream tableRepresentation;
                    {
                        bool first = true;
                        auto pathSchemaList = outputTable->VerifiedAsPtr<TOutputTableNode>()->GetPathSchemaList();
                        for (const auto& pair : pathSchemaList) {
                            if (first) {
                                first = false;
                            } else {
                                tableRepresentation << ", ";
                            }
                            tableRepresentation << pair.first.Path_;
                        }
                    }
                    auto edge = NYT::Format("%v -> %v", operation->GetFirstName(), tableRepresentation.Str());
                    result.insert(std::move(edge));
                    break;
                }
                case ETableType::Intermediate: {
                    for (const auto& inputFor : outputTable->InputFor) {
                        auto edge = NYT::Format("%v -> %v", operation->GetFirstName(), inputFor.Operation->GetFirstName());
                        result.insert(std::move(edge));
                    }
                    break;
                }
                case ETableType::Input:
                    Y_FAIL("Unexpected");
            }
        }

    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TYtGraphV2> BuildYtGraphV2(const TPipeline& pipeline, const TYtPipelineConfig& config)
{
    TYtGraphV2Builder builder{config};
    TraverseInTopologicalOrder(GetRawPipeline(pipeline), &builder);
    return builder.Build();
}

std::unique_ptr<TYtGraphV2> BuildOptimizedYtGraphV2(const TPipeline& pipeline, const TYtPipelineConfig& config)
{
    auto graph = BuildYtGraphV2(pipeline, config);
    graph->Optimize();
    return graph;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
