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

enum class EOperationType
{
    // For all types of operations with user code: Map, Reduce, MapReduce
    MapReduce,

    Merge,
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

using TOperationConnector = std::pair<EJobType, TParDoTreeBuilder::TPCollectionNodeId>;

class TInputTableNode;
class TOutputTableNode;

////////////////////////////////////////////////////////////////////////////////

class TYtGraphV2::TTableNode
{
public:
    TRowVtable Vtable;

    std::vector<TOperationNode*> InputFor;
    TOperationNode* OutputOf = nullptr;

public:
    TTableNode(TRowVtable vtable, NYT::TRichYPath path)
        : Vtable(vtable)
        , Path_(std::move(path))
    { }

    virtual ~TTableNode() = default;

    virtual ETableType GetTableType() const = 0;
    virtual IYtJobInputPtr CreateJobInput() const = 0;
    virtual IYtJobOutputPtr CreateJobOutput(ssize_t sinkIndex) const = 0;

    const NYT::TRichYPath& GetPath() const
    {
        return Path_;
    }

    template <typename T>
    T& AsRef()
    {
        static_assert(std::is_base_of_v<TTableNode, T>, "Class is not derived from TTableNode");
        T* result = dynamic_cast<T*>(this);
        Y_VERIFY(result);
        return *result;
    }

    template <typename T>
    T* TryAsPtr()
    {
        return dynamic_cast<T*>(this);
    }

private:
    NYT::TRichYPath Path_;
};

////////////////////////////////////////////////////////////////////////////////

class TInputTableNode
    : public TYtGraphV2::TTableNode
{
public:
    TInputTableNode(TRowVtable vtable, IRawYtReadPtr rawYtRead)
        : TYtGraphV2::TTableNode(std::move(vtable), rawYtRead->GetPath())
        , RawYtRead_(rawYtRead)
    { }

    ETableType GetTableType() const override
    {
        return ETableType::Input;
    }

    IYtJobInputPtr CreateJobInput() const override
    {
        return RawYtRead_->CreateJobInput();
    }

    IYtJobOutputPtr CreateJobOutput(ssize_t /*sinkIndex*/) const override
    {
        Y_FAIL("input table is not expected to be written");
    }

private:
    IRawYtReadPtr RawYtRead_;
};

////////////////////////////////////////////////////////////////////////////////

class TOutputTableNode
    : public TYtGraphV2::TTableNode
{
public:
    TOutputTableNode(TRowVtable vtable, IRawYtWritePtr rawYtWrite)
        : TYtGraphV2::TTableNode(std::move(vtable), rawYtWrite->GetPath())
        , RawYtWrite_(std::move(rawYtWrite))
        , Schema_(RawYtWrite_->GetSchema())
    { }

    ETableType GetTableType() const override
    {
        return ETableType::Output;
    }

    IYtJobInputPtr CreateJobInput() const override
    {
        Y_FAIL("input table is not expected to be read");
    }

    IYtJobOutputPtr CreateJobOutput(ssize_t sinkIndex) const override
    {
        return RawYtWrite_->CreateJobOutput(sinkIndex);
    }

    const NYT::TTableSchema& GetSchema() const
    {
        return Schema_;
    }

private:
    IRawYtWritePtr RawYtWrite_;
    NYT::TTableSchema Schema_;
};

////////////////////////////////////////////////////////////////////////////////

class TIntermediateTableNode
    : public TYtGraphV2::TTableNode
{
public:
    TIntermediateTableNode(TRowVtable vtable, NYT::TRichYPath path)
        : TYtGraphV2::TTableNode(std::move(vtable), std::move(path))
    { }

    ETableType GetTableType() const
    {
        return ETableType::Intermediate;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TYtGraphV2::TOperationNode
{
    EOperationType OperationType;
    TString FirstName;

    TParDoTreeBuilder MapperBuilder;
    TParDoTreeBuilder ReducerBuilder;

    std::vector<TTableNode*> InputTables;
    THashMap<TOperationConnector, TTableNode*> OutputTables;

    TOperationNode(EOperationType operationType, TString firstName)
        : OperationType(operationType)
        , FirstName(std::move(firstName))
    { }
};

////////////////////////////////////////////////////////////////////////////////

class TYtGraphV2::TPlainGraph
{
public:
    std::vector<TTableNodePtr> Tables;
    std::vector<TOperationNodePtr> Operations;

public:
    TOperationNode* CreateMapReduceOperation(std::vector<TTableNode*> inputs, TString firstName)
    {
        auto operation = std::make_shared<TOperationNode>(EOperationType::MapReduce, std::move(firstName));
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
        auto operation = std::make_shared<TOperationNode>(EOperationType::Merge, std::move(operationFirstName));
        Operations.push_back(operation);
        LinkWithInputs(std::move(inputs), operation.get());

        auto table = std::make_shared<TOutputTableNode>(
            std::move(outputVtable),
            rawYtWrite);
        Tables.push_back(table);
        table->OutputOf = operation.get();
        operation->OutputTables[TOperationConnector{EJobType::Map, TParDoTreeBuilder::RootNodeId}] = table.get();

        return {operation.get(), table.get()};
    }

    TTableNode* CreatePipelineInputSchema(TRowVtable vtable, const IRawYtReadPtr& rawYtRead)
    {
        auto table = std::make_shared<TInputTableNode>(std::move(vtable), rawYtRead);
        Tables.push_back(table);
        return table.get();
    }

    TTableNode* CreateResultTable(
        TOperationNode* operation,
        TOperationConnector connector,
        TRowVtable vtable,
        IRawYtWritePtr rawYtWrite)
    {
        auto table = std::make_shared<TOutputTableNode>(std::move(vtable), rawYtWrite);
        Tables.push_back(table);

        auto inserted = operation->OutputTables.emplace(connector, table.get()).second;
        Y_VERIFY(inserted);
        table->OutputOf = operation;

        return table.get();
    }

private:
    void LinkWithInputs(std::vector<TTableNode*> inputs, TOperationNode* operation)
    {
        operation->InputTables = std::move(inputs);
        for (auto* table : operation->InputTables) {
            table->InputFor.push_back(operation);
        }
    }
};

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

using TPCollectionNodeImage = std::variant<
    TYtGraphV2::TTableNode*,
    TEphemeralImage
>;

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
                    auto* tableNode = PlainGraph_->CreatePipelineInputSchema(pCollectionNode->GetRowVtable(), IRawYtReadPtr{rawYtRead});
                    RegisterPCollection(pCollectionNode, tableNode);
                } else {
                    ythrow yexception() << transformNode->GetName() << " is not a YtRead and not supported";
                }
                break;
            case ERawTransformType::Write:
                if (auto* rawYtWrite = dynamic_cast<IRawYtWrite*>(&*rawTransform->AsRawWrite())) {
                    Y_VERIFY(transformNode->GetSourceCount() == 1);
                    const auto* source = transformNode->GetSource(0).Get();
                    auto sourceImage = GetPCollectionImage(source);

                    std::visit([&] (auto&& sourceImage) {
                        using TType = std::decay_t<decltype(sourceImage)>;

                        if constexpr (std::is_same_v<TType, TYtGraphV2::TTableNode*>) {
                            auto inputTable = sourceImage;
                            PlainGraph_->CreateMergeOperation(
                                {inputTable},
                                transformNode->GetName(),
                                source->GetRowVtable(),
                                rawYtWrite
                            );
                        } else if constexpr (std::is_same_v<TType, TEphemeralImage>) {
                            const auto& ephemeralImage = sourceImage;
                            PlainGraph_->CreateResultTable(
                                ephemeralImage.Operation,
                                ephemeralImage.Connector,
                                source->GetRowVtable(),
                                IRawYtWritePtr(rawYtWrite)
                            );
                        } else {
                            static_assert(std::is_same_v<TType, void>);
                        }
                    }, sourceImage);
                }
                break;
            case ERawTransformType::ParDo: {
                // 1. Посмотреть к чему привязан вход
                Y_VERIFY(transformNode->GetSourceCount() == 1);
                const auto* source = transformNode->GetSource(0).Get();
                const auto& sourceImage = GetPCollectionImage(source);

                auto rawParDo = rawTransform->AsRawParDo();

                std::visit([&] (auto&& sourceImage) {
                    using TType = std::decay_t<decltype(sourceImage)>;

                    if constexpr (std::is_same_v<TType, TYtGraphV2::TTableNode*>) {
                        // Наша нода-источник это таблица, надо проверить есть ли уже операции с её участием?
                        auto tableNodeBuilder = sourceImage;
                        if (tableNodeBuilder->InputFor.empty()) {
                            PlainGraph_->CreateMapReduceOperation({tableNodeBuilder}, transformNode->GetName());
                        }
                        auto* primaryOperation = tableNodeBuilder->InputFor[0];
                        auto outputIdList = primaryOperation->MapperBuilder.AddParDo(
                            rawParDo,
                            TParDoTreeBuilder::RootNodeId
                        );

                        Y_VERIFY(std::ssize(outputIdList) == transformNode->GetSinkCount());

                        for (ssize_t i = 0; i < std::ssize(outputIdList); ++i) {
                            auto id = outputIdList[i];
                            const auto* pCollection = transformNode->GetSink(i).Get();
                            auto curImage = TEphemeralImage{primaryOperation, {EJobType::Map, id}};
                            RegisterPCollection(pCollection, std::move(curImage));
                        }
                    } else if constexpr (std::is_same_v<TType, TEphemeralImage>) {
                        Y_FAIL("Not implemented yet");
                    } else {
                        static_assert(std::is_same_v<TType, void>);
                    }
                }, sourceImage);

                // 1. Узнать, к чему привязана эта нода, если к таблице, то создать новый узел операции.
                break;
            }
            case ERawTransformType::GroupByKey:
            case ERawTransformType::CoGroupByKey:
            case ERawTransformType::StatefulTimerParDo:
            case ERawTransformType::CombinePerKey:
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
    void RegisterPCollection(const TPCollectionNode* pCollection, TPCollectionNodeImage image)
    {
        auto inserted = PCollectionMap_.emplace(pCollection, std::move(image)).second;
        Y_VERIFY(inserted);
    }

    const TPCollectionNodeImage& GetPCollectionImage(const TPCollectionNode* pCollection) const
    {
        auto it = PCollectionMap_.find(pCollection);
        Y_VERIFY(it != PCollectionMap_.end());
        return it->second;
    }

private:
    const std::shared_ptr<const TYtPipelineConfig> Config_;

    std::unique_ptr<TYtGraphV2::TPlainGraph> PlainGraph_ = std::make_unique<TYtGraphV2::TPlainGraph>();

    THashMap<const TPCollectionNode*, TPCollectionNodeImage> PCollectionMap_;
};

////////////////////////////////////////////////////////////////////////////////

TYtGraphV2::TYtGraphV2(std::unique_ptr<TPlainGraph> plainGraph)
    : PlainGraph_(std::move(plainGraph))
{ }

TYtGraphV2::~TYtGraphV2()
{ }

void TYtGraphV2::Optimize()
{ }

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
            for (const auto* operation : table->InputFor) {
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
            readyTables.clear();
            for (const auto* operation : readyOperations) {
                operationNodeLevels.back().push_back(idMap[operation]);
                for (const auto& item : operation->OutputTables) {
                    readyTables.push_back(item.second);
                }
            }
        }
    }

    return operationNodeLevels;
}

NYT::IOperationPtr TYtGraphV2::StartOperation(const NYT::IClientBasePtr& client, TOperationNodeId nodeId) const
{
    Y_VERIFY(nodeId >= 0 && nodeId < std::ssize(PlainGraph_->Operations));
    const auto& operation = PlainGraph_->Operations[nodeId];
    switch (operation->OperationType) {
        case EOperationType::MapReduce: {
            NYT::TRawMapOperationSpec spec;
            Y_VERIFY(operation->InputTables.size() == 1);
            const auto* inputTable = operation->InputTables[0];
            spec.AddInput(inputTable->GetPath());
            auto jobInput = inputTable->CreateJobInput();

            TParDoTreeBuilder mapBuilder = operation->MapperBuilder;
            std::vector<IYtJobOutputPtr> jobOutputs;
            ssize_t sinkIndex = 0;
            for (const auto &[key, table] : operation->OutputTables) {
                Y_VERIFY(key.first == EJobType::Map);
                auto output = table->GetPath();
                if (auto* outputTable = table->TryAsPtr<TOutputTableNode>()) {
                    output.Schema(outputTable->GetSchema());
                }
                spec.AddOutput(table->GetPath());
                mapBuilder.MarkAsOutput(key.second);
                jobOutputs.push_back(table->CreateJobOutput(sinkIndex));
                ++sinkIndex;
            }
            spec.Format(NYT::TFormat::YsonBinary());

            NYT::IRawJobPtr job = CreateParDoMap(mapBuilder.Build(), jobInput, jobOutputs);

            return client->RawMap(spec, job, NYT::TOperationOptions().Wait(false));
        }
        case EOperationType::Merge: {
            NYT::TMergeOperationSpec spec;
            for (const auto *table : operation->InputTables) {
                spec.AddInput(table->GetPath());
            }
            Y_VERIFY(std::ssize(operation->OutputTables) == 1);
            for (const auto &[_, table] : operation->OutputTables) {
                auto output = table->GetPath();
                output.Schema(table->AsRef<TOutputTableNode>().GetSchema());
                spec.Output(table->GetPath());
            }
            return client->Merge(spec, NYT::TOperationOptions{}.Wait(false));
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
            for (const auto& operation : table->InputFor) {
                auto edge = NYT::Format("%v -> %v", table->GetPath().Path_, operation->FirstName);
                result.insert(std::move(edge));
            }
        }
    }

    for (const auto& operation : PlainGraph_->Operations) {
        for (const auto& [connector, outputTable] : operation->OutputTables) {
            switch (outputTable->GetTableType()) {
                case ETableType::Output: {
                    auto edge = NYT::Format("%v -> %v", operation->FirstName, outputTable->GetPath().Path_);
                    result.insert(std::move(edge));
                    break;
                }
                case ETableType::Intermediate: {
                    for (const auto* nextOperation : outputTable->InputFor) {
                        auto edge = NYT::Format("%v -> %v", operation->FirstName, nextOperation->FirstName);
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
