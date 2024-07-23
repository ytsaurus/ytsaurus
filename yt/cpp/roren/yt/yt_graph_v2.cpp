#include "yt_graph_v2.h"
#include "jobs.h"

#include "connectors.h"
#include "tables.h"
#include "operations.h"
#include "optimizers.h"
#include "plain_graph.h"
#include "visitors.h"

#include <yt/cpp/roren/yt/proto/config.pb.h>
#include <yt/cpp/roren/yt/proto/kv.pb.h>

#include <yt/cpp/roren/interface/roren.h>

#include <yt/cpp/mapreduce/interface/operation.h>
#include <yt/cpp/mapreduce/interface/client.h>

#include <library/cpp/yt/string/format.h>

#include <util/generic/hash_multi_map.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

const TPCollectionNode* VerifiedGetSingleSource(const TTransformNodePtr& transform)
{
    Y_ABORT_UNLESS(transform->GetSourceCount() == 1);
    return transform->GetSource(0).Get();
}

const TPCollectionNode* VerifiedGetSingleSink(const TTransformNodePtr& transform)
{
    Y_ABORT_UNLESS(transform->GetSinkCount() == 1);
    return transform->GetSink(0).Get();
}

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
                    Y_ABORT_UNLESS(transformNode->GetSinkCount() == 1);
                    const auto* pCollectionNode = transformNode->GetSink(0).Get();
                    auto* tableNode = PlainGraph_->CreateInputTable(pCollectionNode->GetRowVtable(), IRawYtReadPtr{rawYtRead});
                    RegisterPCollection(pCollectionNode, tableNode);
                } else {
                    ythrow yexception() << transformNode->GetName() << " is not a YtRead and not supported";
                }
                break;
            case ERawTransformType::Write:
                if (auto* rawYtWrite = dynamic_cast<IRawYtWrite*>(&*rawTransform->AsRawWrite())) {
                    Y_ABORT_UNLESS(transformNode->GetSourceCount() == 1);
                    const auto* sourcePCollection = transformNode->GetSource(0).Get();
                    auto inputTable = GetPCollectionImage(sourcePCollection);

                    if (auto* rawYtSortedWrite = dynamic_cast<IRawYtSortedWrite*>(rawYtWrite)) {
                        PlainGraph_->CreateSortOperation(
                            {inputTable},
                            transformNode->GetName(),
                            Config_->GetWorkingDir(),
                            sourcePCollection->GetRowVtable(),
                            rawYtSortedWrite
                        );
                    } else {
                        PlainGraph_->CreateIdentityMapOperation(
                            {inputTable},
                            transformNode->GetName(),
                            sourcePCollection->GetRowVtable(),
                            rawYtWrite
                        );
                    }
                } else {
                    Y_ABORT("YT executor doesn't support writes except YtWrite");
                }
                break;
            case ERawTransformType::ParDo: {
                const auto* sourcePCollection = VerifiedGetSingleSource(transformNode);
                auto* inputTable = GetPCollectionImage(sourcePCollection);
                auto rawParDo = rawTransform->AsRawParDo();

                Y_ABORT_UNLESS(!transformNode->GetName().empty());
                const auto& [mapOperation, outputIdList] = PlainGraph_->CreateMapOperation(
                    {inputTable},
                    transformNode->GetName(),
                    rawParDo
                );

                Y_ABORT_UNLESS(std::ssize(outputIdList) == transformNode->GetSinkCount());

                for (ssize_t i = 0; i < std::ssize(outputIdList); ++i) {
                    auto id = outputIdList[i];
                    const auto* pCollection = transformNode->GetSink(i).Get();
                    auto* outputTable = PlainGraph_->CreateIntermediateTable(
                        mapOperation,
                        TMapperOutputConnector{0, id},
                        pCollection->GetRowVtable(),
                        Config_->GetWorkingDir(),
                        Config_->GetEnableProtoFormatForIntermediates()
                    );
                    RegisterPCollection(pCollection, outputTable);
                }
                break;
            }
            case ERawTransformType::GroupByKey: {
                const auto* sourcePCollection = VerifiedGetSingleSource(transformNode);
                auto* inputTable = GetPCollectionImage(sourcePCollection);

                const auto& [mapReduceOperation, nodeId] = PlainGraph_->CreateGroupByKeyMapReduceOperation(
                    inputTable,
                    transformNode->GetName(),
                    transformNode->GetRawTransform()->AsRawGroupByKey(),
                    Config_->GetEnableProtoFormatForIntermediates()
                );

                const auto* sinkPCollection = VerifiedGetSingleSink(transformNode);
                auto* outputTable = PlainGraph_->CreateIntermediateTable(
                    mapReduceOperation,
                    TReducerOutputConnector{nodeId},
                    sinkPCollection->GetRowVtable(),
                    Config_->GetWorkingDir(),
                    Config_->GetEnableProtoFormatForIntermediates()
                );
                RegisterPCollection(sinkPCollection, outputTable);
                break;
            }
            case ERawTransformType::CoGroupByKey: {
                std::vector<TTableNode*> inputTableList;
                for (const auto& sourcePCollection : transformNode->GetSourceList()) {
                    auto* inputTable = GetPCollectionImage(sourcePCollection.Get());
                    inputTableList.push_back(inputTable);
                }

                const auto& [mapReduceOperation, nodeId] = PlainGraph_->CreateCoGroupByKeyMapReduceOperation(
                    inputTableList,
                    transformNode->GetName(),
                    transformNode->GetRawTransform()->AsRawCoGroupByKey(),
                    Config_->GetEnableProtoFormatForIntermediates()
                );

                const auto* sinkPCollection = VerifiedGetSingleSink(transformNode);
                auto* outputTable = PlainGraph_->CreateIntermediateTable(
                    mapReduceOperation,
                    TReducerOutputConnector{nodeId},
                    sinkPCollection->GetRowVtable(),
                    Config_->GetWorkingDir(),
                    Config_->GetEnableProtoFormatForIntermediates()
                );
                RegisterPCollection(sinkPCollection, outputTable);
                break;
            }
            case ERawTransformType::CombinePerKey: {
                const auto* sourcePCollection = VerifiedGetSingleSource(transformNode);
                auto* inputTable = GetPCollectionImage(sourcePCollection);
                auto rawCombinePerKey = transformNode->GetRawTransform()->AsRawCombine();

                const auto& [mapReduceOperation, nodeId] = PlainGraph_->CreateCombinePerKeyMapReduceOperation(
                    inputTable,
                    transformNode->GetName(),
                    transformNode->GetRawTransform()->AsRawCombine(),
                    Config_->GetEnableProtoFormatForIntermediates()
                );

                const auto* sinkPCollection = VerifiedGetSingleSink(transformNode);
                auto* outputTable = PlainGraph_->CreateIntermediateTable(
                    mapReduceOperation,
                    TReducerOutputConnector{nodeId},
                    sinkPCollection->GetRowVtable(),
                    Config_->GetWorkingDir(),
                    Config_->GetEnableProtoFormatForIntermediates()
                );
                RegisterPCollection(sinkPCollection, outputTable);
                break;
            }
            case ERawTransformType::Flatten: {
                std::vector<TTableNode*> inputTableList;
                for (const auto& sourcePCollection : transformNode->GetSourceList()) {
                    auto* inputTable = GetPCollectionImage(sourcePCollection.Get());
                    inputTableList.push_back(inputTable);
                }

                Y_ABORT_UNLESS(!transformNode->GetName().Empty());
                auto* mergeOperation = PlainGraph_->CreateMergeOperation(
                    inputTableList,
                    transformNode->GetName()
                );

                const auto* sinkPCollection = VerifiedGetSingleSink(transformNode);
                auto* outputTable = PlainGraph_->CreateIntermediateTable(
                    mergeOperation,
                    TMergeOutputConnector{},
                    sinkPCollection->GetRowVtable(),
                    Config_->GetWorkingDir(),
                    Config_->GetEnableProtoFormatForIntermediates()
                );
                RegisterPCollection(sinkPCollection, outputTable);
                break;
            }
            case ERawTransformType::StatefulParDo: {
                const TYtStateVtable* stateVtable = NPrivate::GetAttribute(*transformNode->GetPStateNode(), YtStateVtableTag);
                const TString* stateInPath = NPrivate::GetAttribute(*transformNode->GetPStateNode(), YtStateInPathTag);
                const TString* stateOutPath = NPrivate::GetAttribute(*transformNode->GetPStateNode(), YtStateOutPathTag);
                Y_ABORT_UNLESS(stateVtable);
                Y_ABORT_UNLESS(stateInPath);
                Y_ABORT_UNLESS(stateOutPath);

                const auto* sourcePCollection = VerifiedGetSingleSource(transformNode);
                auto* inputTable = GetPCollectionImage(sourcePCollection);

                auto stateRawYtRead = MakeYtNodeInput(*stateInPath);
                NPrivate::SetAttribute(*stateRawYtRead, DecodingParDoTag, CreateStateDecodingParDo(*stateVtable));
                auto inputStateTable = PlainGraph_->CreateInputTable(
                    stateVtable->StateTKVvtable,
                    stateRawYtRead
                );

                Y_ABORT_UNLESS(!transformNode->GetName().empty());
                const auto& [statefulMapReduceOperation, outputNodeId] = PlainGraph_->CreateStatefulParDoMapReduceOperation(
                    {inputStateTable, inputTable},
                    transformNode->GetName(),
                    rawTransform->AsRawStatefulParDo(),
                    stateVtable,
                    Config_->GetEnableProtoFormatForIntermediates()
                );

                auto outputStateTable = PlainGraph_->CreateOutputTable(
                    statefulMapReduceOperation,
                    TStatefulConnector{},
                    stateVtable->StateTKVvtable,
                    MakeYtNodeWrite(*stateOutPath, NYT::TTableSchema{}.Strict(false))
                );
                Y_UNUSED(outputStateTable);

                Y_ABORT_UNLESS(transformNode->GetSinkCount() == 1);
                const auto* pCollection = transformNode->GetSink(0).Get();
                auto* outputTable = PlainGraph_->CreateIntermediateTable(
                    statefulMapReduceOperation,
                    TReducerOutputConnector{outputNodeId},
                    pCollection->GetRowVtable(),
                    Config_->GetWorkingDir(),
                    Config_->GetEnableProtoFormatForIntermediates()
                );
                RegisterPCollection(pCollection, outputTable);
                break;
            }
            case ERawTransformType::StatefulTimerParDo:
            case ERawTransformType::CombineGlobally:
                Y_ABORT("Not implemented yet");
        }
    }

    std::shared_ptr<TYtGraphV2> Build()
    {
        return std::make_shared<TYtGraphV2>(std::move(PlainGraph_), *Config_);
    }

private:
    void RegisterPCollection(const TPCollectionNode* pCollection, TTableNode* image)
    {
        auto inserted = PCollectionMap_.emplace(pCollection, std::move(image)).second;
        Y_ABORT_UNLESS(inserted);
    }

    TTableNode* GetPCollectionImage(const TPCollectionNode* pCollection) const
    {
        auto it = PCollectionMap_.find(pCollection);
        Y_ABORT_UNLESS(it != PCollectionMap_.end());
        return it->second;
    }

private:
    const std::shared_ptr<const TYtPipelineConfig> Config_;

    std::unique_ptr<TYtGraphV2::TPlainGraph> PlainGraph_ = std::make_unique<TYtGraphV2::TPlainGraph>();

    THashMap<const TPCollectionNode*, TTableNode*> PCollectionMap_;
};

////////////////////////////////////////////////////////////////////////////////

TYtGraphV2::TYtGraphV2(std::unique_ptr<TPlainGraph> plainGraph, const TYtPipelineConfig& config)
    : PlainGraph_(std::move(plainGraph))
    , Config_(config)
{ }

TYtGraphV2::~TYtGraphV2()
{ }

void TYtGraphV2::Optimize()
{
    TMapFuser mapFuser;
    *PlainGraph_ = mapFuser.Optimize(*PlainGraph_);

    TMapReduceFuser mapReduceFuser;
    *PlainGraph_ = mapReduceFuser.Optimize(*PlainGraph_);
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
                Y_ABORT_UNLESS(it != dependencyMap.end());
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

THashMap<IYtGraph::TOperationNodeId, std::vector<IYtGraph::TOperationNodeId>>
TYtGraphV2::GetNextOperationMapping() const
{
    THashMap<const TOperationNode*, IYtGraph::TOperationNodeId> idMap;
    THashMap<IYtGraph::TOperationNodeId, std::vector<IYtGraph::TOperationNodeId>> operationMap;
    for (ssize_t i = 0; i < std::ssize(PlainGraph_->Operations); ++i) {
        const auto& operationNode = PlainGraph_->Operations[i];
        idMap[operationNode.get()] = i;
        operationMap[i] = {};
    }

    for (ssize_t i = 0; i < std::ssize(PlainGraph_->Operations); ++i) {
        const auto& operationNode = PlainGraph_->Operations[i];

        for (const auto& [_, outputTable] : operationNode->OutputTables) {
            for (const auto& nextOperationNode : outputTable->InputFor) {
                operationMap[idMap[operationNode.get()]].push_back(idMap[nextOperationNode.Operation]);
            }
        }
    }

    return operationMap;
}

static void PatchOperationSpec(NYT::TNode* result, const NYT::TNode& patch, const TString& currentPath = {})
{
    if (result->IsUndefined()) {
        *result = patch;
    }
    if (!patch.IsMap()) {
        if (*result == patch) {
            return;
        } else {
            ythrow yexception() << "Conflicting values for key " << currentPath << '\n'
                << NYT::NodeToYsonString(*result) << '\n'
                << NYT::NodeToYsonString(patch);
        }
    }

    Y_ABORT_UNLESS(patch.IsMap());
    if (!result->IsMap()) {
            ythrow yexception() << "Conflicting types for key " << currentPath << '\n'
                << result->GetType() << " and " << patch.GetType();
    }

    for (auto&& [key, value] : patch.AsMap()) {
        // Perfect code would escape '/' char in `key` but we are not perfect :(
        auto newPath = currentPath + "/" + key;
        PatchOperationSpec(&(*result)[key], value, newPath);
    }
}

static NYT::TNode GetOperationSpecPatch(const THashSet<TString>& transformNames, const TYtPipelineConfig& config)
{
    auto normalizeName = [] (const TString& input) {
        TString result;
        if (!input.StartsWith('/')) {
            result.push_back('/');
        }
        result += input;
        if (!result.EndsWith('/')) {
            result.push_back('/');
        }
        return result;
    };

    // This is not the fastest algorithm, but we don't expect large amount of config patches here
    // so it should work ok.
    NYT::TNode result;

    std::vector<TString> normalizedTransformNameList;
    for (const auto& transformName : transformNames) {
        normalizedTransformNameList.push_back(normalizeName(transformName));
    }

    for (const auto& [namePattern, operationConfig] : config.GetOperatinonConfig()) {
        auto normalizedNamePattern = normalizeName(namePattern);
        for (const auto& normalizedTransformName : normalizedTransformNameList) {
            if (normalizedTransformName.find(normalizedNamePattern) != TString::npos) {
                auto patch = NYT::NodeFromYsonString(operationConfig.GetSpecPatch());
                PatchOperationSpec(&result, patch);
                break;
            }
        }
    }
    return result;
}

NYT::IOperationPtr TYtGraphV2::StartOperation(const NYT::IClientBasePtr& client, TOperationNodeId nodeId, const TStartOperationContext& /*context*/) const
{
    auto addLocalFiles = [] (const IRawParDoPtr& parDo, NYT::TUserJobSpec* spec) {
        const auto& resourceFileList = TFnAttributesOps::GetResourceFileList(parDo->GetFnAttributes());

        for (const auto& resourceFile : resourceFileList) {
            spec->AddLocalFile(resourceFile);
        }
    };

    auto materializeIfIntermediateTable = [&client, this, nodeId] (const TTableNode* table) {
        const auto* upcastedTable = dynamic_cast<const TIntermediateTableNode*>(table);
        if (!upcastedTable) {
            return;
        }

        IntermediateTables_.emplace_back(
            MakeHolder<NYT::TTempTable>(
                client,
                "",
                upcastedTable->GetPath().Path_,
                NYT::TCreateOptions{}.Recursive(true).IgnoreExisting(true),
                false),
            nodeId);
    };

    Y_ABORT_UNLESS(nodeId >= 0 && nodeId < std::ssize(PlainGraph_->Operations));
    const auto& operation = PlainGraph_->Operations[nodeId];

    auto operationOptions = NYT::TOperationOptions().Wait(false);
    auto specPatch = GetOperationSpecPatch(operation->GetTransformNames(), Config_);
    if (!specPatch.IsUndefined()) {
        operationOptions.Spec(specPatch);
    }

    switch (operation->GetOperationType()) {
        case EOperationType::Merge:
        case EOperationType::Map: {
            const auto* mapOperation = operation->GetOperationType() == EOperationType::Map
                ? operation->VerifiedAsPtr<TMapOperationNode>()
                : nullptr;

            if (!mapOperation || mapOperation->GetMapperBuilder().Empty()) {
                TTableNode* outputTable;
                Y_ABORT_UNLESS(operation->OutputTables.size() == 1);
                for (const auto& [connector, table] : operation->OutputTables) {
                    outputTable = table;
                }

                THashSet<ETableType> inputTableTypes;
                for (const auto* table : operation->InputTables) {
                    inputTableTypes.insert(table->GetTableType());
                }

                if (
                    inputTableTypes == THashSet<ETableType>{ETableType::Intermediate}
                    && outputTable->GetTableType() == ETableType::Intermediate
                ) {
                    NYT::TMergeOperationSpec spec;
                    for (const auto *table : operation->InputTables) {
                        spec.AddInput(table->GetPath());

                        materializeIfIntermediateTable(table);
                    }
                    Y_ABORT_UNLESS(std::ssize(operation->OutputTables) == 1);
                    for (const auto &[_, table] : operation->OutputTables) {
                        spec.Output(table->GetPath());

                        materializeIfIntermediateTable(table);
                    }

                    spec.Pool(Config_.GetPool());

                    return client->Merge(spec, operationOptions);
                } else {
                    NYT::TRawMapOperationSpec spec;
                    TParDoTreeBuilder mapperBuilder;
                    auto impulseOutputIdList = mapperBuilder.AddParDo(
                        CreateReadImpulseParDo(operation->InputTables),
                        TParDoTreeBuilder::RootNodeId
                    );
                    Y_ABORT_UNLESS(impulseOutputIdList.size() == operation->InputTables.size());

                    for (ssize_t i = 0; i < std::ssize(impulseOutputIdList); ++i) {
                        auto* inputTable = operation->InputTables[i];
                        mapperBuilder.AddParDoChainVerifyNoOutput(
                            impulseOutputIdList[i],
                            {
                                inputTable->CreateDecodingParDo(),
                                outputTable->CreateEncodingParDo(),
                                outputTable->CreateWriteParDo(i),
                            }
                        );
                        spec.AddInput(inputTable->GetPath());

                        materializeIfIntermediateTable(inputTable);
                    }

                    switch (outputTable->GetTableType()) {
                        case ETableType::Input:
                            Y_ABORT("Output table cannot have type ETableType::Input");
                        case ETableType::Intermediate:
                            spec.AddOutput(outputTable->GetPath());
                            break;
                        case ETableType::Output: {
                            NYT::TRichYPath outputPath = outputTable->GetPath();
                            spec.AddOutput(
                                NYT::TRichYPath(outputTable->GetPath())
                                    .Schema(outputTable->VerifiedAsPtr<TOutputTableNode>()->GetSchema())
                            );
                            break;
                        }
                    }
                    materializeIfIntermediateTable(outputTable);

                    spec.InputFormat(GetFormat(operation->InputTables));
                    spec.OutputFormat(GetFormat(operation->OutputTables));
                    spec.Pool(Config_.GetPool());

                    NYT::IRawJobPtr job = CreateImpulseJob(mapperBuilder.Build());
                    return client->RawMap(spec, job, operationOptions);
                }
            } else {
                NYT::TRawMapOperationSpec spec;
                Y_ABORT_UNLESS(operation->InputTables.size() == 1);
                const auto* inputTable = operation->InputTables[0];
                spec.AddInput(inputTable->GetPath());
                materializeIfIntermediateTable(inputTable);

                TParDoTreeBuilder tmpMapBuilder = mapOperation->GetMapperBuilder();
                std::vector<IYtJobOutputPtr> jobOutputs;
                ssize_t sinkIndex = 0;
                for (const auto &[connector, table] : operation->OutputTables) {
                    Y_ABORT_UNLESS(std::holds_alternative<TMapperOutputConnector>(connector));
                    const auto& mapperConnector = std::get<TMapperOutputConnector>(connector);
                    Y_ABORT_UNLESS(mapperConnector.MapperIndex == 0);
                    auto output = table->GetPath();
                    if (auto* outputTable = table->TryAsPtr<TOutputTableNode>()) {
                        output.Schema(outputTable->GetSchema());
                    }
                    spec.AddOutput(table->GetPath());
                    materializeIfIntermediateTable(table);
                    auto nodeId = tmpMapBuilder.AddParDoVerifySingleOutput(
                        table->CreateEncodingParDo(),
                        mapperConnector.NodeId
                    );
                    tmpMapBuilder.AddParDoVerifyNoOutput(
                        table->CreateWriteParDo(sinkIndex),
                        nodeId
                    );

                    ++sinkIndex;
                }
                TParDoTreeBuilder mapperBuilder;
                auto nodeId = mapperBuilder.AddParDoVerifySingleOutput(
                    CreateReadImpulseParDo(operation->InputTables),
                    TParDoTreeBuilder::RootNodeId
                );
                nodeId = mapperBuilder.AddParDoVerifySingleOutput(
                    inputTable->CreateDecodingParDo(),
                    nodeId
                );
                mapperBuilder.Fuse(tmpMapBuilder, nodeId);

                spec.InputFormat(GetFormat(operation->InputTables));
                spec.OutputFormat(GetFormat(operation->OutputTables));
                spec.Pool(Config_.GetPool());

                auto mapperParDo = mapperBuilder.Build();
                addLocalFiles(mapperParDo, &spec.MapperSpec_);
                NYT::IRawJobPtr job = CreateImpulseJob(mapperParDo);

                return client->RawMap(spec, job, operationOptions);
            }
        }
        case EOperationType::MapReduce: {
            const auto* mapReduceOperation = operation->VerifiedAsPtr<TMapReduceOperationNode>();

            Y_ABORT_UNLESS(std::ssize(operation->InputTables) == std::ssize(mapReduceOperation->GetMapperBuilderList()));

            NYT::TRawMapReduceOperationSpec spec;
            auto tmpMapperBuilderList = mapReduceOperation->GetMapperBuilderList();
            for (const auto* inputTable : operation->InputTables) {
                spec.AddInput(inputTable->GetPath());
                materializeIfIntermediateTable(inputTable);
            }

            auto reducerBuilder = mapReduceOperation->GetReducerBuilder();
            ssize_t mapperOutputIndex = 1;
            ssize_t reducerOutputIndex = 0;
            std::vector<TTableNode*> mapperOutputs;
            std::vector<TTableNode*> reducerOutputs;
            for (const auto& tableInfo : mapReduceOperation->OutputTables) {
                const auto& connector = tableInfo.first;
                const auto& outputTable = tableInfo.second;
                std::visit([&] (const auto& connector) {
                    using TType = std::decay_t<decltype(connector)>;
                    if constexpr (std::is_same_v<TType, TStatefulConnector>) {
                        spec.AddOutput(outputTable->GetPath());
                        reducerOutputs.emplace_back(outputTable);
                        ++reducerOutputIndex;
                    }
                }, connector);
            }
            for (const auto& tableInfo : mapReduceOperation->OutputTables) {
                const auto& connector = tableInfo.first;
                const auto& outputTable = tableInfo.second;
                materializeIfIntermediateTable(outputTable);
                std::visit([&] (const auto& connector) {
                    using TType = std::decay_t<decltype(connector)>;
                    if constexpr (std::is_same_v<TType, TMergeOutputConnector>) {
                        Y_ABORT("Unexpected TMergeOutputConnector inside MapReduce operation");
                    } else if constexpr(std::is_same_v<TType, TSortOutputConnector>) {
                        Y_ABORT("Unexpected TSortOutputConnector inside MapReduce operation");
                    } else if constexpr (std::is_same_v<TType, TStatefulConnector>) {
                        // StatefulConnector processed beforehand
                    } else if constexpr (std::is_same_v<TType, TMapperOutputConnector>) {
                        tmpMapperBuilderList[connector.MapperIndex].AddParDoChainVerifyNoOutput(
                            connector.NodeId,
                            {
                                outputTable->CreateEncodingParDo(),
                                outputTable->CreateWriteParDo(mapperOutputIndex)
                            }
                        );
                        spec.AddMapOutput(outputTable->GetPath());
                        mapperOutputs.emplace_back(outputTable);
                        ++mapperOutputIndex;
                    } else if constexpr (std::is_same_v<TType, TReducerOutputConnector>) {
                        reducerBuilder.AddParDoChainVerifyNoOutput(
                            connector.NodeId, {
                                outputTable->CreateEncodingParDo(),
                                outputTable->CreateWriteParDo(reducerOutputIndex)
                            }
                        );
                        spec.AddOutput(outputTable->GetPath());
                        reducerOutputs.emplace_back(outputTable);
                        ++reducerOutputIndex;
                    } else {
                        static_assert(TDependentFalse<TType>);
                    }
                }, connector);
            }

            TParDoTreeBuilder mapBuilder;
            auto impulseOutputIdList = mapBuilder.AddParDo(
                CreateReadImpulseParDo(operation->InputTables),
                TParDoTreeBuilder::RootNodeId
            );
            Y_ABORT_UNLESS(impulseOutputIdList.size() == tmpMapperBuilderList.size());
            Y_ABORT_UNLESS(impulseOutputIdList.size() == operation->InputTables.size());
            for (ssize_t i = 0; i < std::ssize(impulseOutputIdList); ++i) {
                auto decodedId = mapBuilder.AddParDoVerifySingleOutput(
                    operation->InputTables[i]->CreateDecodingParDo(),
                    impulseOutputIdList[i]
                );
                mapBuilder.Fuse(tmpMapperBuilderList[i], decodedId);
            }

            auto mapperParDo = mapBuilder.Build();
            addLocalFiles(mapperParDo, &spec.MapperSpec_);
            auto mapperJob = CreateImpulseJob(mapperParDo);

            auto reducerParDo = reducerBuilder.Build();
            addLocalFiles(reducerParDo, &spec.ReducerSpec_);
            auto reducerJob = CreateImpulseJob(reducerParDo);

            NYT::IRawJobPtr combinerJob = nullptr;
            if (!mapReduceOperation->GetCombinerBuilder().Empty()) {
                spec.ForceReduceCombiners(true);
                auto combinerBuilder = mapReduceOperation->GetCombinerBuilder();
                auto combinerParDo = combinerBuilder.Build();
                addLocalFiles(combinerParDo, &spec.ReduceCombinerSpec_);
                combinerJob = CreateImpulseJob(combinerParDo);
            }

            spec.MapperInputFormat(GetFormat(mapReduceOperation->InputTables));
            spec.MapperOutputFormat(GetFormatWithIntermediate(Config_.GetEnableProtoFormatForIntermediates(), mapperOutputs));
            if (combinerJob) {
                spec.ReduceCombinerFormat(GetFormatWithIntermediate(Config_.GetEnableProtoFormatForIntermediates(), {}));
            }
            spec.ReducerInputFormat(GetFormatWithIntermediate(Config_.GetEnableProtoFormatForIntermediates(), {}));
            spec.ReducerOutputFormat(GetFormat(reducerOutputs));
            spec.ReduceBy({"key"});
            spec.Pool(Config_.GetPool());

            return client->RawMapReduce(
                spec,
                mapperJob,
                combinerJob,
                reducerJob,
                operationOptions
            );
        }
        case EOperationType::Sort: {
            NYT::TSortOperationSpec spec;
            Y_ABORT_UNLESS(std::ssize(operation->InputTables) == 1);
            for (const auto *table : operation->InputTables) {
                spec.AddInput(table->GetPath());
                materializeIfIntermediateTable(table);
            }

            auto rawYtSortedWrite = std::dynamic_pointer_cast<TSortOperationNode>(operation)->GetWrite();
            Y_ABORT_UNLESS(std::ssize(operation->OutputTables) == 1);
            for (const auto &[_, table] : operation->OutputTables) {
                auto path = table->GetPath();
                rawYtSortedWrite->FillSchema(*path.Schema_);

                spec.Output(std::move(path));
                materializeIfIntermediateTable(table);
            }

            spec.SortBy(rawYtSortedWrite->GetColumnsToSort());
            spec.Title(operation->GetFirstName());
            spec.Pool(Config_.GetPool());

            return client->Sort(spec, operationOptions);
        }
    }
    Y_ABORT("Unknown operation");
}

TString TYtGraphV2::DumpDOTSubGraph(const TString&) const
{
    Y_ABORT("Not implemented yet");
    return TString{};
}

TString TYtGraphV2::DumpDOT(const TString&) const
{
    TDotVisitor visitor;
    visitor.Prologue();
    TraverseInTopologicalOrder(*PlainGraph_, &visitor);
    visitor.Epilogue();

    return visitor.GetResult();
}

void TYtGraphV2::CreateWorkingDir(NYT::IClientBasePtr client) const
{
    client->Create(
        Config_.GetWorkingDir(),
        NYT::ENodeType::NT_MAP,
        NYT::TCreateOptions{}
            .Recursive(true)
            .IgnoreExisting(true));
}

void TYtGraphV2::ClearIntermediateTables() const
{
    IntermediateTables_.clear();
}

void TYtGraphV2::LeaveIntermediateTables(NYT::IClientBasePtr client, NYT::ITransactionPtr tx, TOperationNodeId operationNodeId) const
{
    for (auto& [table, id] : IntermediateTables_) {
        if (id == operationNodeId) {
            auto path = table->Release();

            client->Merge(
                NYT::TMergeOperationSpec{}
                    .AddInput(NYT::TRichYPath(path).TransactionId(tx->GetId()))
                    .Output(path + "_failed"));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<TYtGraphV2> BuildYtGraphV2(const TPipeline& pipeline, const TYtPipelineConfig& config)
{
    TYtGraphV2Builder builder{config};
    TraverseInTopologicalOrder(GetRawPipeline(pipeline), &builder);
    return builder.Build();
}

std::shared_ptr<TYtGraphV2> BuildOptimizedYtGraphV2(const TPipeline& pipeline, const TYtPipelineConfig& config)
{
    auto graph = BuildYtGraphV2(pipeline, config);
    graph->Optimize();
    return graph;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
