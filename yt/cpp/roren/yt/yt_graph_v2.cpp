#include "yt_graph_v2.h"

#include "yt_graph.h"
#include "yt_io_private.h"

#include <yt/cpp/roren/yt/proto/config.pb.h>

#include <yt/cpp/roren/interface/private/par_do_tree.h>
#include <yt/cpp/roren/interface/roren.h>

#include <yt/cpp/mapreduce/interface/operation.h>

#include <library/cpp/yt/string/format.h>

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

struct TYtGraphV2::TTableNode
{
    ETableType TableType;
    NYT::TRichYPath Path;
    std::optional<NYT::TTableSchema> Schema;

    std::vector<TOperationNode*> InputFor;
    TOperationNode* OutputOf = nullptr;

    TTableNode(ETableType type, NYT::TRichYPath path)
        : TableType(type)
        , Path(std::move(path))
    { }
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
        NYT::TRichYPath outputPath,
        NYT::TTableSchema outputSchema)
    {
        auto operation = std::make_shared<TOperationNode>(EOperationType::Merge, std::move(operationFirstName));
        Operations.push_back(operation);
        LinkWithInputs(std::move(inputs), operation.get());

        auto table = std::make_shared<TTableNode>(ETableType::Output, std::move(outputPath));
        Tables.push_back(table);
        table->Schema = std::move(outputSchema);
        table->OutputOf = operation.get();
        operation->OutputTables[TOperationConnector{EJobType::Map, TParDoTreeBuilder::RootNodeId}] = table.get();

        return {operation.get(), table.get()};
    }

    TTableNode* CreatePipelineInputSchema(NYT::TRichYPath path)
    {
        auto table = std::make_shared<TTableNode>(ETableType::Input, std::move(path));
        Tables.push_back(table);
        return table.get();
    }

    TTableNode* CreateResultTable(
        TOperationNode* operation,
        TOperationConnector connector,
        NYT::TRichYPath path,
        NYT::TTableSchema schema)
    {
        auto table = std::make_shared<TTableNode>(ETableType::Output, std::move(path));
        Tables.push_back(table);
        table->Schema = std::move(schema);

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
                if (const auto* rawYtInput = dynamic_cast<const IRawYtRead*>(&*rawTransform->AsRawRead())) {
                    Y_VERIFY(transformNode->GetSinkCount() == 1);
                    const auto* pCollectionNode = transformNode->GetSink(0).Get();
                    auto* tableNode = PlainGraph_->CreatePipelineInputSchema(rawYtInput->GetPath());
                    RegisterPCollection(pCollectionNode, tableNode);
                } else {
                    ythrow yexception() << transformNode->GetName() << " is not a YtRead and not supported";
                }
                break;
            case ERawTransformType::Write:
                if (const auto* rawYtOutput = dynamic_cast<const IRawYtWrite*>(&*rawTransform->AsRawWrite())) {
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
                                rawYtOutput->GetPath(),
                                rawYtOutput->GetSchema()
                            );
                        } else if constexpr (std::is_same_v<TType, TEphemeralImage>) {
                            const auto& ephemeralImage = sourceImage;
                            PlainGraph_->CreateResultTable(
                                ephemeralImage.Operation,
                                ephemeralImage.Connector,
                                rawYtOutput->GetPath(),
                                rawYtOutput->GetSchema()
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
{
     Y_FAIL("Not implemented yet");
}

std::vector<std::vector<IYtGraph::TOperationNodeId>> TYtGraphV2::GetOperationLevels() const
{
    Y_FAIL("Not implemented yet");
    return {};
}

NYT::IOperationPtr TYtGraphV2::StartOperation(const NYT::IClientBasePtr&, TOperationNodeId) const
{
    Y_FAIL("Not implemented yet");
    return nullptr;
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
        if (table->TableType == ETableType::Input) {
            for (const auto& operation : table->InputFor) {
                auto edge = NYT::Format("%v -> %v", table->Path.Path_, operation->FirstName);
                result.insert(std::move(edge));
            }
        }
    }

    for (const auto& operation : PlainGraph_->Operations) {
        for (const auto& [connector, outputTable] : operation->OutputTables) {
            switch (outputTable->TableType) {
                case ETableType::Output: {
                    auto edge = NYT::Format("%v -> %v", operation->FirstName, outputTable->Path.Path_);
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
