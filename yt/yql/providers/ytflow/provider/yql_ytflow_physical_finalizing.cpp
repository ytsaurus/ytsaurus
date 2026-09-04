#include "yql_ytflow_provider_impl.h"
#include "yql_ytflow_constants.h"

#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_opt_utils.h>

#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.h>
#include <yt/yql/providers/ytflow/integration/interface/yql_ytflow_integration.h>
#include <yt/yql/providers/ytflow/integration/interface/yql_ytflow_optimization.h>
#include <yt/yql/providers/ytflow/provider/yql_ytflow_utils.h>

#include <util/digest/multi.h>
#include <util/generic/size_literals.h>

#include <library/cpp/iterator/enumerate.h>
#include <library/cpp/iterator/zip.h>


namespace NYql {

using namespace NNodes;


namespace {

struct TWorldKey {
    bool IsSync;
    TVector<const TExprNode*> Inputs;

    bool operator==(const TWorldKey& other) const = default;
};

TWorldKey GetWorldKey(const TExprBase& world) {
    if (world.Maybe<TCoSync>()) {
        TVector<const TExprNode*> inputs;
        inputs.reserve(world.Ref().ChildrenSize());
        for (const auto& child : world.Ref().Children()) {
            inputs.push_back(child.Get());
        }

        return TWorldKey{
            .IsSync = true,
            .Inputs = std::move(inputs)
        };
    }

    return TWorldKey{
        .IsSync = false,
        .Inputs = {world.Raw()}
    };
}

TExprNode::TPtr GetFlowOutputFromWriteWrap(
    const TYtflowWriteWrap& writeWrap,
    TExprContext& ctx,
    const TYtflowState::TPtr& state
) {
    const auto& providerWrite = writeWrap.Input().Ref();

    auto* ytflowIntegration = GetYtflowIntegration(providerWrite, *state->Types);
    YQL_ENSURE(ytflowIntegration);

    auto content = ytflowIntegration->GetWriteContent(providerWrite, ctx);

    auto maybeOutput = TMaybeNode<TYtflowOutput>(content);
    if (!maybeOutput) {
        return nullptr;
    }

    return maybeOutput.Cast().Ptr();
}

bool IsExtendImplementation(const TYtflowMapBase& map)
{
    return map.Maybe<TYtflowExtend>() || HasSetting(map.Settings().Ref(), EXTEND_SETTING);
}

} // anonymous namespace

class TYtflowPhysicalFinalizingTransformer : public TSyncTransformerBase {
private:
    struct TLambdaWithInputIndex {
        TExprBase Lambda;
        size_t InputIndex;
    };

    struct TOutputMapsInfo {
        TVector<TExprNode::TPtr> GenericMapNodes;
        bool HasNonGenericConsumer = false;
        bool InjectInputMessageId = false;
    };

    using TOutputMapsByOutputIndex = THashMap<ui32, TOutputMapsInfo>;
    using TOutputMapsByInputMap = THashMap<TExprNode::TPtr, TOutputMapsByOutputIndex>;
    using TIndexRemaps = THashMap<ui32, ui32>;

    struct TBuildLambdasAndNewSinksResult {
        TVector<TLambdaWithInputIndex> LambdasWithInputIndex;
        TVector<TExprNode::TPtr> NewSinks;
        TIndexRemaps InputMapIndexRemaps;
        THashMap<TExprNode::TPtr, TIndexRemaps> OutputMapsIndexRemaps;
    };

    struct TSourceMapGroupKey {
        const TExprNode* ReadWrap;
        TWorldKey WorldKey;
        TString SourceName;
        TString SettingsCacheKey;

        bool operator==(const TSourceMapGroupKey& other) const = default;
    };

    struct TSourceMapGroupKeyHash {
        size_t operator()(const TSourceMapGroupKey& key) const {
            auto hash = MultiHash(
                key.ReadWrap,
                key.WorldKey.IsSync,
                key.WorldKey.Inputs.size(),
                key.SourceName,
                key.SettingsCacheKey);
            for (const auto* input : key.WorldKey.Inputs) {
                hash = CombineHashes(hash, THash<const TExprNode*>()(input));
            }

            return hash;
        }
    };

    struct TSourceMapGroup {
        TExprNode::TPtr Settings;
        TVector<TExprNode::TPtr> SourceMaps;
    };

    struct TPhysicalOutputKey {
        const TExprNode* Operation;
        ui32 OutputIndex;

        bool operator==(const TPhysicalOutputKey& other) const = default;
    };

    struct TPhysicalOutputKeyHash {
        size_t operator()(const TPhysicalOutputKey& key) const {
            return MultiHash(key.Operation, key.OutputIndex);
        }
    };

public:
    TYtflowPhysicalFinalizingTransformer(TYtflowState::TPtr state)
        : State_(std::move(state))
    {
    }

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;

        TParentsMap parentsMap;
        GatherParents(*input, parentsMap);

        if (auto status = WriteWrapOverOutput(output, output, ctx); status != TStatus::Ok) {
            return status;
        }

        if (auto status = OperationWithUnusedSinks(output, output, parentsMap, ctx); status != TStatus::Ok) {
            return status;
        }

        if (auto status = FanOutDuplicateExtendInputs(output, output, ctx); status != TStatus::Ok) {
            return status;
        }

        if (auto status = InjectInputMessageIdForExtendInputs(output, output, ctx); status != TStatus::Ok) {
            return status;
        }

        if (auto status = YtflowSourceMapsOverSameReadWrap(output, output, parentsMap, ctx); status != TStatus::Ok) {
            return status;
        }

        if (auto status = YtflowMapOverMap(output, output, parentsMap, ctx); status != TStatus::Ok) {
            return status;
        }

        return TStatus::Ok;
    }

    TStatus WriteWrapOverOutput(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
        output = input;

        TNodeMap<TVector<TExprNode::TPtr>> writeWrapsByOperation;
        TNodeOnNodeOwnedMap remaps;
        VisitExpr(input, [&](const TExprNode::TPtr& node) {
            if (auto maybeWriteWrap = TMaybeNode<TYtflowWriteWrap>(node)) {
                auto writeWrap = maybeWriteWrap.Cast();
                auto flowOutput = GetFlowOutputFromWriteWrap(writeWrap, ctx, State_);
                if (!flowOutput) {
                    return true;
                }

                auto operation = TYtflowOutput(flowOutput).Operation().Ptr();
                writeWrapsByOperation[operation.Get()].push_back(writeWrap.Ptr());

                auto operationWithLeft = Build<TCoLeft>(ctx, operation->Pos())
                    .Input(operation)
                    .Done().Ptr();
                remaps.emplace(writeWrap.Raw(), operationWithLeft);
            }

            return true;
        });

        if (writeWrapsByOperation.empty()) {
            return TStatus::Ok;
        }

        YQL_CLOG(INFO, ProviderYtflow) << "PhysicalFinalizing-WriteWrapOverOutput";

        auto remapSettings = TOptimizeExprSettings(State_->Types);
        remapSettings.VisitChanges = true;

        auto remapStatus = RemapExpr(output, output, remaps, ctx, remapSettings);
        if (remapStatus != TStatus::Ok) {
            YQL_ENSURE(remapStatus == TStatus::Error);
            return remapStatus;
        }

        remaps.clear();

        for (const auto& [operation, writeWraps] : writeWrapsByOperation) {
            auto opBase = TYtflowOpBase(operation);

            TVector<TExprNode::TPtr> newPersistentSinks;
            for (const auto& writeWrapPtr : writeWraps) {
                auto writeWrap = TYtflowWriteWrap(writeWrapPtr);
                auto providerWrite = writeWrap.Input().Ptr();
                auto* ytflowOptimization = GetYtflowOptimization(*providerWrite, *State_->Types);
                YQL_ENSURE(ytflowOptimization);

                auto newWrite = ytflowOptimization->TrimWriteContent(providerWrite, ctx);
                if (!newWrite) {
                    return TStatus::Error;
                }

                auto newWriteWrap = Build<TYtflowWriteWrap>(ctx, writeWrap.Pos())
                    .InitFrom(writeWrap)
                    .Input(newWrite)
                    .Done().Ptr();

                auto flowOutput = GetFlowOutputFromWriteWrap(writeWrap, ctx, State_);
                YQL_ENSURE(flowOutput);

                auto outputIndex = FromString<ui32>(TYtflowOutput(flowOutput).OutputIndex().Value());
                auto newSink = Build<TYtflowPersistentSink>(ctx, writeWrap.Pos())
                    .Name()
                        .Value("")
                        .Build()
                    .OutputIndex()
                        .Value(outputIndex)
                        .Build()
                    .Input(std::move(newWriteWrap))
                    .Done().Ptr();

                newPersistentSinks.push_back(std::move(newSink));
            }

            TVector<TExprNode::TPtr> newSinks;
            for (auto sink : opBase.Sinks()) {
                YQL_ENSURE(sink.Maybe<TYtflowIntermediateSink>(), "Unexpected node: " << sink.Ref().Content());
                newSinks.push_back(sink.Ptr());
            }

            Copy(newPersistentSinks.begin(), newPersistentSinks.end(), std::back_inserter(newSinks));

            auto newOpBase = ctx.ChangeChild(
                opBase.Ref(),
                TYtflowOpBase::idx_Sinks,
                Build<TExprList>(ctx, opBase.Sinks().Pos())
                    .Add(std::move(newSinks))
                    .Done().Ptr());

            remaps.emplace(operation, newOpBase);
        }

        auto status = RemapExpr(output, output, remaps, ctx, remapSettings);
        if (status != TStatus::Ok) {
            return status;
        }

        return TStatus(TStatus::Repeat, /*hasRestart*/ true);
    }

    TStatus FanOutDuplicateExtendInputs(
        TExprNode::TPtr input,
        TExprNode::TPtr& output,
        TExprContext& ctx
    ) {
        output = input;

        TNodeOnNodeOwnedMap remaps;
        VisitExpr(input, [&remaps, &ctx](const TExprNode::TPtr& node) {
            auto maybeMap = TMaybeNode<TYtflowMapBase>(node);
            if (!maybeMap || !IsExtendImplementation(maybeMap.Cast())) {
                return true;
            }

            auto extend = maybeMap.Cast();
            THashSet<TPhysicalOutputKey, TPhysicalOutputKeyHash> seenOutputs;
            TVector<TExprNode::TPtr> newSources;
            newSources.reserve(extend.Sources().Size());
            bool changed = false;
            for (auto source : extend.Sources()) {
                auto maybeOutput = source.Maybe<TYtflowOutput>();
                YQL_ENSURE(maybeOutput, "Unexpected " << source.Ref().Content()
                    << " as source of " << extend.Ref().Content());

                auto originalOutput = maybeOutput.Cast();
                auto operation = originalOutput.Operation();
                auto outputIndex = ::FromString<ui32>(originalOutput.OutputIndex());
                if (seenOutputs.emplace(TPhysicalOutputKey{
                    .Operation = operation.Raw(),
                    .OutputIndex = outputIndex
                }).second) {
                    newSources.push_back(source.Ptr());
                    continue;
                }

                auto sinks = operation.Sinks();
                YQL_ENSURE(outputIndex < sinks.Size(), "Unexpected output index '"
                    << outputIndex << "', sinks size: " << sinks.Size());
                auto producerSink = sinks.Item(outputIndex).Cast<TYtflowIntermediateSink>();

                auto identityMap = Build<TYtflowMap>(ctx, originalOutput.Pos())
                    .World<TCoSync>()
                        .Add(operation.World())
                        .Build()
                    .Sources()
                        .Add(originalOutput)
                        .Build()
                    .Sinks()
                        .Add<TYtflowIntermediateSink>()
                            .Name()
                                .Value("")
                                .Build()
                            .OutputIndex()
                                .Value(0)
                                .Build()
                            .RowType(producerSink.RowType())
                            .Build()
                        .Build()
                    .Settings()
                        .Build()
                    .Lambda()
                        .Args({"stream"})
                        .Body("stream")
                        .Build()
                    .Done();

                newSources.push_back(Build<TYtflowOutput>(ctx, originalOutput.Pos())
                    .Operation(std::move(identityMap))
                    .OutputIndex()
                        .Value(0)
                        .Build()
                    .Done().Ptr());
                changed = true;
            }

            if (changed) {
                remaps.emplace(extend.Raw(), ctx.ChangeChild(
                    extend.Ref(),
                    TYtflowMapBase::idx_Sources,
                    Build<TExprList>(ctx, extend.Sources().Pos())
                        .Add(std::move(newSources))
                        .Done().Ptr()));
            }

            return true;
        });

        if (remaps.empty()) {
            return TStatus::Ok;
        }

        YQL_CLOG(INFO, ProviderYtflow) << "PhysicalFinalizing-FanOutDuplicateExtendInputs";

        auto remapSettings = TOptimizeExprSettings(State_->Types);
        remapSettings.VisitChanges = true;

        auto status = RemapExpr(output, output, remaps, ctx, remapSettings);
        if (status != TStatus::Ok) {
            return status;
        }

        return TStatus(TStatus::Repeat, /*hasRestart*/ true);
    }

    TStatus InjectInputMessageIdForExtendInputs(
        TExprNode::TPtr input,
        TExprNode::TPtr& output,
        TExprContext& ctx
    ) {
        output = input;

        TNodeOnNodeOwnedMap remaps;
        VisitExpr(input, [&remaps, &ctx](const TExprNode::TPtr& node) {
            auto maybeMap = TMaybeNode<TYtflowMapBase>(node);
            if (!maybeMap || !IsExtendImplementation(maybeMap.Cast())) {
                return true;
            }

            auto extend = maybeMap.Cast();
            bool requiresInputMessageId = false;
            for (const auto& column : node->Child(TYtflowExtend::idx_GroupByColumns)->Children()) {
                if (column->Content() == YTFLOW_INPUT_MESSAGE_ID_FIELD) {
                    requiresInputMessageId = true;
                    break;
                }
            }

            if (!requiresInputMessageId) {
                return true;
            }

            for (auto source : extend.Sources()) {
                auto maybeOutput = source.Maybe<TYtflowOutput>();
                YQL_ENSURE(maybeOutput, "Unexpected " << source.Ref().Content()
                    << " as source of " << extend.Ref().Content());

                auto operation = maybeOutput.Cast().Operation();
                auto maybeMap = operation.Maybe<TYtflowMapBase>();
                YQL_ENSURE(maybeMap, "Unexpected " << operation.Ref().Content()
                    << " as operation producing input for " << extend.Ref().Content());

                auto map = maybeMap.Cast();
                if (GetSetting(map.Settings().Ref(), INJECT_INPUT_MESSAGE_ID_SETTING) ||
                    remaps.find(map.Raw()) != remaps.end()
                ) {
                    continue;
                }

                auto newSettings = AddSetting(
                    map.Settings().Ref(),
                    map.Settings().Pos(),
                    TString(INJECT_INPUT_MESSAGE_ID_SETTING),
                    ctx.NewAtom(map.Settings().Pos(), ""),
                    ctx);
                remaps.emplace(
                    map.Raw(),
                    ctx.ChangeChild(
                        map.Ref(),
                        TYtflowMapBase::idx_Settings,
                        std::move(newSettings)));
            }

            return true;
        });

        if (remaps.empty()) {
            return TStatus::Ok;
        }

        YQL_CLOG(INFO, ProviderYtflow) << "PhysicalFinalizing-InjectInputMessageIdForExtendInputs";

        auto remapSettings = TOptimizeExprSettings(State_->Types);
        remapSettings.VisitChanges = true;

        auto status = RemapExpr(output, output, remaps, ctx, remapSettings);
        if (status != TStatus::Ok) {
            return status;
        }

        return TStatus(TStatus::Repeat, /*hasRestart*/ true);
    }

    TStatus YtflowSourceMapsOverSameReadWrap(
        TExprNode::TPtr input, TExprNode::TPtr& output,
        const TParentsMap& parentsMap, TExprContext& ctx
    ) {
        output = input;

        TVector<TSourceMapGroup> sourceMapGroups;
        THashMap<TSourceMapGroupKey, TVector<size_t>, TSourceMapGroupKeyHash> groupIndicesByKey;
        VisitExpr(input, [
            &sourceMapGroups,
            &groupIndicesByKey,
            &parentsMap,
            &ctx
        ](const TExprNode::TPtr& node) {
            auto maybeSourceMap = TMaybeNode<TYtflowSourceMap>(node);
            if (!maybeSourceMap) {
                return true;
            }

            auto sourceMap = maybeSourceMap.Cast();
            auto sources = sourceMap.Sources();
            if (sources.Size() != 1) {
                return true;
            }

            auto maybePersistentSource = (*sources.begin()).Maybe<TYtflowPersistentSource>();
            if (!maybePersistentSource) {
                return true;
            }

            auto persistentSource = maybePersistentSource.Cast();
            if (!persistentSource.Input().Maybe<TYtflowReadWrap>()) {
                return true;
            }

            auto parentsIterator = parentsMap.find(sourceMap.Raw());
            if (parentsIterator == parentsMap.end()) {
                return true;
            }

            for (const auto* parent : parentsIterator->second) {
                if (!TMaybeNode<TYtflowOutput>(parent) && !TMaybeNode<TCoLeft>(parent)) {
                    return true;
                }
            }

            auto settings = RemoveSetting(
                sourceMap.Settings().Ref(),
                INJECT_INPUT_MESSAGE_ID_SETTING,
                ctx);
            TSourceMapGroupKey key{
                .ReadWrap = persistentSource.Input().Raw(),
                .WorldKey = GetWorldKey(sourceMap.World()),
                .SourceName = TString(persistentSource.Name().Value()),
                .SettingsCacheKey = MakeCacheKey(*settings)
            };

            auto& groupIndices = groupIndicesByKey[key];
            for (auto groupIndex : groupIndices) {
                const TExprNode* groupSettings = sourceMapGroups[groupIndex].Settings.Get();
                const TExprNode* sourceMapSettings = settings.Get();
                if (CompareExprTrees(groupSettings, sourceMapSettings)) {
                    sourceMapGroups[groupIndex].SourceMaps.push_back(sourceMap.Ptr());
                    return true;
                }
            }

            groupIndices.push_back(sourceMapGroups.size());
            sourceMapGroups.push_back(TSourceMapGroup{
                .Settings = std::move(settings),
                .SourceMaps = {sourceMap.Ptr()}
            });

            return true;
        });

        EraseIf(sourceMapGroups, [](const auto& group) {
            return group.SourceMaps.size() <= 1;
        });

        if (sourceMapGroups.empty()) {
            return TStatus::Ok;
        }

        YQL_CLOG(INFO, ProviderYtflow) << "PhysicalFinalizing-YtflowSourceMapsOverSameReadWrap";

        TNodeOnNodeOwnedMap remaps;
        for (const auto& sourceMapGroup : sourceMapGroups) {
            const auto& sourceMaps = sourceMapGroup.SourceMaps;
            auto firstSourceMap = TYtflowSourceMap(sourceMaps.front());

            TVector<TExprNode::TPtr> newSinks;
            TVector<TLambdaWithInputIndex> switchLambdas;
            TVector<TIndexRemaps> sourceMapIndexRemaps;
            ui32 currentSinkOutputIndex = 0;
            bool injectInputMessageId = false;
            for (const auto& sourceMapPtr : sourceMaps) {
                auto sourceMap = TYtflowMapBase(sourceMapPtr);
                sourceMapIndexRemaps.push_back(AddOutputMapBranch(
                    sourceMap,
                    0,
                    sourceMap.Pos(),
                    currentSinkOutputIndex,
                    newSinks,
                    switchLambdas,
                    ctx));

                if (auto setting = GetSetting(sourceMap.Settings().Ref(), INJECT_INPUT_MESSAGE_ID_SETTING)) {
                    injectInputMessageId = true;
                }
            }

            auto inputLambda = Build<TCoLambda>(ctx, firstSourceMap.Lambda().Pos())
                .Args({"stream"})
                .Body("stream")
                .Done();
            auto newLambda = BuildSwitchLambda(inputLambda, switchLambdas, ctx);

            auto newSourceMap = ctx.ChangeChild(
                firstSourceMap.Ref(),
                TYtflowMapBase::idx_Sinks,
                Build<TExprList>(ctx, firstSourceMap.Sinks().Pos())
                    .Add(std::move(newSinks))
                    .Done().Ptr());
            newSourceMap = ctx.ChangeChild(
                *newSourceMap,
                TYtflowMapBase::idx_Lambda,
                std::move(newLambda));

            if (injectInputMessageId &&
                !GetSetting(firstSourceMap.Settings().Ref(), INJECT_INPUT_MESSAGE_ID_SETTING)
            ) {
                newSourceMap = ctx.ChangeChild(
                    *newSourceMap,
                    TYtflowMapBase::idx_Settings,
                    AddSetting(
                        *newSourceMap->Child(TYtflowMapBase::idx_Settings),
                        input->Pos(),
                        TString(INJECT_INPUT_MESSAGE_ID_SETTING),
                        ctx.NewAtom(input->Pos(), ""),
                        ctx));
            }

            for (auto [index, sourceMap] : Enumerate(sourceMaps)) {
                FillRemaps(
                    sourceMap,
                    sourceMapIndexRemaps[index],
                    newSourceMap,
                    parentsMap,
                    remaps,
                    ctx);
            }
        }

        auto remapSettings = TOptimizeExprSettings(State_->Types);
        remapSettings.VisitChanges = true;

        auto status = RemapExpr(output, output, remaps, ctx, remapSettings);
        if (status != TStatus::Ok) {
            return status;
        }

        return TStatus(TStatus::Repeat, /*hasRestart*/ true);
    }

    TStatus YtflowMapOverMap(
        TExprNode::TPtr input, TExprNode::TPtr& output,
        const TParentsMap& parentsMap, TExprContext& ctx
    ) {
        output = input;

        // input map -> (output sink index, output maps)
        TOutputMapsByInputMap outputMapsByNonGenericInputMap;
        THashSet<TExprNode::TPtr> sinksWithPersistentMap;
        VisitExpr(input, [
            this,
            &outputMapsByNonGenericInputMap
        ](const TExprNode::TPtr& node) {
            auto maybeYtflowMapBase = TMaybeNode<TYtflowMapBase>(node);
            if (!maybeYtflowMapBase) {
                return true;
            }

            auto ytflowMapBase = maybeYtflowMapBase.Cast();
            auto sources = ytflowMapBase.Sources();
            if (IsExtendImplementation(ytflowMapBase)) {
                for (auto source : sources) {
                    auto maybeYtflowOutput = source.Maybe<TYtflowOutput>();
                    YQL_ENSURE(maybeYtflowOutput, "Unexpected " << source.Ref().Content()
                        << " as source of " << ytflowMapBase.Ref().Content());
                    CollectYtflowConsumer(
                        ytflowMapBase,
                        maybeYtflowOutput.Cast(),
                        outputMapsByNonGenericInputMap);
                }

                return true;
            }

            YQL_ENSURE(sources.Size() == 1, "Unexpected sources count of "
                << ytflowMapBase.Ref().Content() << ": " << sources.Size());

            auto maybeYtflowOutput = (*sources.begin()).Maybe<TYtflowOutput>();
            if (maybeYtflowOutput) {
                CollectYtflowConsumer(
                    ytflowMapBase,
                    maybeYtflowOutput.Cast(),
                    outputMapsByNonGenericInputMap);
            }

            return true;
        });

        EraseNodesIf(outputMapsByNonGenericInputMap, [](const auto& keyValue) {
            const auto& outputMapsByOutputIndex = keyValue.second;
            for (const auto& [_, outputMaps] : outputMapsByOutputIndex) {
                if (!outputMaps.GenericMapNodes.empty()) {
                    return false;
                }
            }

            return true;
        });

        if (outputMapsByNonGenericInputMap.empty()) {
            return TStatus::Ok;
        }

        YQL_CLOG(INFO, ProviderYtflow) << "PhysicalFinalizing-YtflowMapOverMap";

        TNodeOnNodeOwnedMap remaps;
        for (const auto& [parent, outputMapsByOutputIndex] : outputMapsByNonGenericInputMap) {
            auto buildLambdasAndNewSinksResult = BuildLambdasAndNewSinks(parent, outputMapsByOutputIndex, ctx);
            auto parentMap = TYtflowMapBase(parent);
            auto newLambda = BuildSwitchLambda(
                parentMap.Lambda(),
                buildLambdasAndNewSinksResult.LambdasWithInputIndex,
                ctx);

            bool injectInputMessageId = false;
            for (const auto& [outputIndex, outputMapsInfo] : outputMapsByOutputIndex) {
                if (outputMapsInfo.InjectInputMessageId) {
                    injectInputMessageId = true;
                    break;
                }
            }

            auto newMap = ctx.ChangeChild(*parent, TYtflowMapBase::idx_Sinks,
                Build<TExprList>(ctx, TYtflowMapBase(parent).Sinks().Pos())
                    .Add(buildLambdasAndNewSinksResult.NewSinks)
                    .Done().Ptr());

            newMap = ctx.ChangeChild(*newMap, TYtflowMapBase::idx_Lambda, std::move(newLambda));

            if (injectInputMessageId) {
                newMap = ctx.ChangeChild(
                    *newMap,
                    TYtflowMapBase::idx_Settings,
                    AddSetting(
                        *newMap->Child(TYtflowMapBase::idx_Settings),
                        input->Pos(),
                        TString(INJECT_INPUT_MESSAGE_ID_SETTING),
                        ctx.NewAtom(input->Pos(), ""),
                        ctx));
            }

            FillRemaps(
                parent,
                buildLambdasAndNewSinksResult.InputMapIndexRemaps,
                newMap,
                parentsMap,
                remaps,
                ctx,
                &outputMapsByOutputIndex);

            for (const auto& [_, outputMaps] : outputMapsByOutputIndex) {
                for (const auto& map : outputMaps.GenericMapNodes) {
                    const auto& outputMapsIndexRemaps =
                        buildLambdasAndNewSinksResult.OutputMapsIndexRemaps;

                    auto outputMapIndexRemapsIterator = outputMapsIndexRemaps.find(map);
                    YQL_ENSURE(outputMapIndexRemapsIterator != outputMapsIndexRemaps.end(),
                        "Unknown remaps for node: " << map->Content());

                    FillRemaps(
                        map,
                        outputMapIndexRemapsIterator->second,
                        newMap,
                        parentsMap,
                        remaps,
                        ctx);
                }
            }
        }

        auto remapSettings = TOptimizeExprSettings(State_->Types);
        remapSettings.VisitChanges = true;

        auto status = RemapExpr(output, output, remaps, ctx, remapSettings);
        if (status != TStatus::Ok) {
            return status;
        }

        return TStatus(TStatus::Repeat, /*hasRestart*/ true);
    }

    TStatus OperationWithUnusedSinks(
        TExprNode::TPtr input, TExprNode::TPtr& output,
        const TParentsMap& parentsMap, TExprContext& ctx
    ) {
        output = input;

        TNodeOnNodeOwnedMap remaps;
        VisitExpr(output, [
            this,
            &parentsMap,
            &remaps,
            &ctx
        ](const TExprNode::TPtr& node) {
            if (TMaybeNode<TYtflowOpBase>(node)) {
                OperationWithUnusedSinksImpl(node, parentsMap, remaps, ctx);
            }

            return true;
        });

        if (remaps.empty()) {
            return TStatus::Ok;
        }

        YQL_CLOG(INFO, ProviderYtflow) << "PhysicalFinalizing-OperationWithUnusedSinks";

        auto remapSettings = TOptimizeExprSettings(State_->Types);
        remapSettings.VisitChanges = true;

        auto remapStatus = RemapExpr(output, output, remaps, ctx, remapSettings);
        if (remapStatus != TStatus::Ok) {
            YQL_ENSURE(remapStatus == TStatus::Error);
            return remapStatus;
        }

        return TStatus(TStatus::Repeat, /*hasRestart*/ true);
    }

    void Rewind() final {
    }

private:
    void CollectYtflowConsumer(
        const TYtflowMapBase& consumer,
        const TYtflowOutput& source,
        TOutputMapsByInputMap& outputMapsByNonGenericInputMap
    ) {
        auto operation = source.Operation();
        if (operation.Maybe<TYtflowMap>()) {
            return;
        }

        YQL_ENSURE(operation.Maybe<TYtflowMapBase>(), "Unexpected " << TYtflowOutput::CallableName()
            << " operation: " << operation.Ref().Content());

        auto outputIndex = ::FromString<ui32>(source.OutputIndex());
        auto& outputMapsInfo = outputMapsByNonGenericInputMap[operation.Ptr()][outputIndex];
        if (consumer.Maybe<TYtflowMap>()) {
            outputMapsInfo.GenericMapNodes.push_back(consumer.Ptr());
        } else {
            outputMapsInfo.HasNonGenericConsumer = true;
        }

        if (GetSetting(consumer.Settings().Ref(), INJECT_INPUT_MESSAGE_ID_SETTING)) {
            outputMapsInfo.InjectInputMessageId = true;
        }
    }

    void FillRemaps(
        const TExprNode::TPtr& node,
        const TIndexRemaps& indexRemaps,
        const TExprNode::TPtr& newMap,
        const TParentsMap& parentsMap,
        TNodeOnNodeOwnedMap& remaps,
        TExprContext& ctx,
        const TOutputMapsByOutputIndex* outputMapsByOutputIndex = nullptr
    ) {
        const auto& parents = parentsMap.at(node.Get());
        for (const auto* parent : parents) {
            if (auto maybeLeft = TMaybeNode<TCoLeft>(parent)) {
                auto left = maybeLeft.Cast();
                remaps.emplace(left.Raw(), Build<TCoLeft>(ctx, left.Pos())
                    .InitFrom(left)
                    .Input(newMap)
                    .Done().Ptr());
            } else if (auto maybeOutput = TMaybeNode<TYtflowOutput>(parent)) {
                auto output = maybeOutput.Cast();
                auto outputIndex = ::FromString<ui32>(output.OutputIndex());

                if (outputMapsByOutputIndex) {
                    auto childMapsByOutputIndexIterator = outputMapsByOutputIndex->find(outputIndex);
                    YQL_ENSURE(childMapsByOutputIndexIterator != outputMapsByOutputIndex->end(),
                        "Unknown output index: " << outputIndex);

                    if (!childMapsByOutputIndexIterator->second.HasNonGenericConsumer) {
                        continue;
                    }
                }

                auto indexRemapsIterator = indexRemaps.find(outputIndex);
                YQL_ENSURE(indexRemapsIterator != indexRemaps.end(),
                    "Unknown remap for index: " << outputIndex);

                remaps.emplace(output.Raw(), Build<TYtflowOutput>(ctx, output.Pos())
                    .InitFrom(output)
                    .OutputIndex()
                        .Value(indexRemapsIterator->second)
                        .Build()
                    .Operation(newMap)
                    .Done().Ptr());
            } else {
                YQL_ENSURE(false, "Unexpected " << parent->Content() << " as parent of " << node->Content());
            }
        }
    }

    TExprNode::TPtr BuildSwitchLambda(
        const TCoLambda& inputLambda,
        const TVector<TLambdaWithInputIndex>& lambdasWithInputIndex,
        TExprContext& ctx
    ) {
        TVector<TExprBase> switchArgs;
        for (auto [lambda, index] : lambdasWithInputIndex) {
            auto handlerIndex = Build<TCoAtomList>(ctx, lambda.Pos())
                .Add()
                    .Value(index)
                .Build()
                .Done();

            switchArgs.push_back(handlerIndex);
            switchArgs.push_back(lambda);
        }

        auto bufferSize = State_->Configuration->_SwitchComputationNodeBufferSizeBytes.Get();
        YQL_ENSURE(bufferSize, "Ytflow._SwitchComputationNodeBufferSizeBytes system setting is not set");

        return Build<TCoLambda>(ctx, inputLambda.Pos())
            .Args({"stream"})
            .template Body<TCoSwitch>()
                .template Input<TExprApplier>()
                    .Apply(inputLambda)
                    .With(0, "stream")
                .Build()
                .BufferBytes()
                    .Value(*bufferSize)
                .Build()
                .FreeArgs()
                    .Add(switchArgs)
                .Build()
            .Build()
            .Done().Ptr();
    }

    TIndexRemaps AddOutputMapBranch(
        const TYtflowMapBase& outputMap,
        ui32 switchInputIndex,
        TPositionHandle outputIndexPos,
        ui32& currentSinkOutputIndex,
        TVector<TExprNode::TPtr>& newSinks,
        TVector<TLambdaWithInputIndex>& switchLambdas,
        TExprContext& ctx
    ) {
        TIndexRemaps indexRemaps;
        auto outputMapSinks = outputMap.Sinks();
        const auto sinkOutputIndexOffset = currentSinkOutputIndex;
        for (auto [childSinkIndex, outputMapSink] : Enumerate(outputMapSinks)) {
            auto sinkOutputIndex = ::FromString<ui32>(
                outputMapSink.Cast<TYtflowSinkBase>().OutputIndex());
            auto newSinkOutputIndex = sinkOutputIndexOffset + sinkOutputIndex;

            auto newSink = ctx.ChangeChild(outputMapSink.Ref(), TYtflowSinkBase::idx_OutputIndex,
                ctx.NewAtom(outputIndexPos, newSinkOutputIndex));
            newSinks.push_back(newSink);
            indexRemaps.emplace(childSinkIndex, newSinks.size() - 1);

            currentSinkOutputIndex = std::max(currentSinkOutputIndex, newSinkOutputIndex + 1);
        }

        auto lambda = outputMap.Lambda();
        switchLambdas.push_back(TLambdaWithInputIndex{
            .Lambda = std::move(lambda),
            .InputIndex = switchInputIndex
        });

        return indexRemaps;
    }

    TBuildLambdasAndNewSinksResult BuildLambdasAndNewSinks(
        const TExprNode::TPtr& inputMap,
        const TOutputMapsByOutputIndex& outputMapsByOutputIndex,
        TExprContext& ctx
    ) {
        TVector<TLambdaWithInputIndex> switchLambdas;
        TVector<TExprNode::TPtr> newSinks;
        TIndexRemaps inputMapIndexRemaps;
        THashMap<TExprNode::TPtr, TIndexRemaps> outputMapsIndexRemaps;
        ui32 currentSinkOutputIndex = 0;
        auto inputMapSinks = TYtflowMapBase(inputMap).Sinks();
        for (auto [inputMapSinkIndex, inputMapSink] : Enumerate(inputMapSinks)) {
            auto sinkOutputIndex = ::FromString<ui32>(inputMapSink.Cast<TYtflowSinkBase>().OutputIndex());

            auto addNewSink = [
                &newSinks,
                &ctx,
                &inputMapSink
            ](const auto& sink, auto newSinkIndex) {
                auto newSink = ctx.ChangeChild(sink.Ref(), TYtflowSinkBase::idx_OutputIndex,
                    ctx.NewAtom(inputMapSink.Pos(), newSinkIndex));

                newSinks.push_back(newSink);
            };

            auto addSwitchLambda = [
                &switchLambdas,
                sinkOutputIndex
            ](auto lambda) {
                switchLambdas.push_back(TLambdaWithInputIndex{
                    .Lambda = std::move(lambda),
                    .InputIndex = sinkOutputIndex
                });
            };

            auto outputMapsIterator = outputMapsByOutputIndex.find(inputMapSinkIndex);
            bool hasOutputMaps = outputMapsIterator != outputMapsByOutputIndex.end();
            if (!hasOutputMaps || outputMapsIterator->second.HasNonGenericConsumer) {
                YQL_ENSURE(hasOutputMaps || inputMapSink.Maybe<TYtflowPersistentSink>());

                addNewSink(inputMapSink, currentSinkOutputIndex);

                addSwitchLambda(Build<TCoLambda>(ctx, inputMapSink.Pos())
                    .Args({"arg"})
                    .Body("arg")
                    .Done());

                inputMapIndexRemaps.emplace(inputMapSinkIndex, newSinks.size() - 1);

                ++currentSinkOutputIndex;
            }

            if (!hasOutputMaps) {
                continue;
            }

            const auto& outputGenericMaps = outputMapsIterator->second.GenericMapNodes;
            for (const auto& outputMap : outputGenericMaps) {
                auto outputYtflowMap = TYtflowMapBase(outputMap);
                outputMapsIndexRemaps.emplace(outputMap, AddOutputMapBranch(
                    outputYtflowMap,
                    sinkOutputIndex,
                    inputMapSink.Pos(),
                    currentSinkOutputIndex,
                    newSinks,
                    switchLambdas,
                    ctx));
            }
        }

        return TBuildLambdasAndNewSinksResult{
            .LambdasWithInputIndex = std::move(switchLambdas),
            .NewSinks = std::move(newSinks),
            .InputMapIndexRemaps = std::move(inputMapIndexRemaps),
            .OutputMapsIndexRemaps = std::move(outputMapsIndexRemaps)
        };
    }

    void OperationWithUnusedSinksImpl(
        const TExprNode::TPtr& node, const TParentsMap& parentsMap,
        TNodeOnNodeOwnedMap& remaps, TExprContext& ctx
    ) {
        auto opBase = TYtflowOpBase(node);

        auto sinks = opBase.Sinks();
        if (sinks.Size() == 1) {
            return;
        }

        auto parentsIterator = parentsMap.find(opBase.Raw());
        YQL_ENSURE(
            parentsIterator != parentsMap.end(),
            "Unknown parent of " << opBase.Ref().Content());

        const auto& parents = parentsIterator->second;

        THashSet<ui32> usedIntermediateSinkIndices;
        for (const auto* parent : parents) {
            if (auto maybeOutput = TMaybeNode<TYtflowOutput>(parent)) {
                auto outputIndex = ::FromString<ui32>(maybeOutput.Cast().OutputIndex());
                YQL_ENSURE(
                    outputIndex < sinks.Size(),
                    "Unexpected output index '"
                        << outputIndex << "', sinks size: " << sinks.Size());

                usedIntermediateSinkIndices.emplace(outputIndex);
            }
        }

        auto switchBufferSize = State_->Configuration->_SwitchComputationNodeBufferSizeBytes.Get();
        YQL_ENSURE(switchBufferSize, "Ytflow._SwitchComputationNodeBufferSizeBytes system setting is not set");

        TVector<std::pair<ui32, TExprNode::TPtr>> sinksWithOriginIndices;
        for (auto [index, sink] : Enumerate(sinks)) {
            sinksWithOriginIndices.push_back(std::pair(index, sink.Ptr()));
        }

        Sort(sinksWithOriginIndices, [](const auto& left, const auto& right) {
            auto leftIndex = ::FromString<ui32>(TYtflowSinkBase(left.second).OutputIndex());
            auto rightIndex = ::FromString<ui32>(TYtflowSinkBase(right.second).OutputIndex());
            return leftIndex < rightIndex;
        });

        TVector<TExprNode::TPtr> newSinks;
        TVector<TExprBase> switchArgs;
        THashSet<ui32> usedOutputs;
        THashMap<ui32, ui32> indicesRemap;
        ui32 newIndex = -1;
        for (auto [oldIndex, sinkPtr] : sinksWithOriginIndices) {
            auto sink = TYtflowSinkBase(sinkPtr);
            if (sink.Maybe<TYtflowIntermediateSink>() && !usedIntermediateSinkIndices.contains(oldIndex)) {
                continue;
            }

            auto outputIndex = ::FromString<ui32>(sink.OutputIndex());
            if (usedOutputs.insert(outputIndex).second) {
                ++newIndex;
                switchArgs.push_back(Build<TCoAtomList>(ctx, node->Pos())
                    .Add()
                        .Value(sink.OutputIndex())
                        .Build()
                    .Done());
                switchArgs.push_back(Build<TCoLambda>(ctx, node->Pos())
                    .Args({"arg"})
                    .Body("arg")
                    .Done());
            }

            newSinks.push_back(ctx.ChangeChild(sink.Ref(),
                TYtflowSinkBase::idx_OutputIndex,
                ctx.NewAtom(sink.Pos(), newIndex)));

            indicesRemap.emplace(oldIndex, newIndex);
        }

        if (newSinks.size() == sinks.Size()) {
            return;
        }

        TExprNode::TPtr resultOperation;
        if (auto maybeMap = TMaybeNode<TYtflowMapBase>(node)) {
            auto map = maybeMap.Cast();
            auto lambda = map.Lambda();

            auto newLambda = Build<TCoLambda>(ctx, lambda.Pos())
                .Args({"stream"})
                .Body<TCoSwitch>()
                    .Input<TExprApplier>()
                        .Apply(lambda)
                        .With(0, "stream")
                        .Build()
                    .BufferBytes()
                        .Value(*switchBufferSize)
                        .Build()
                    .FreeArgs()
                        .Add(std::move(switchArgs))
                        .Build()
                    .Build()
                .Done().Ptr();

            auto newMap = ctx.ChangeChild(map.Ref(), TYtflowMapBase::idx_Sinks,
                Build<TExprList>(ctx, sinks.Pos())
                    .Add(std::move(newSinks))
                .Done().Ptr());
            resultOperation = ctx.ChangeChild(*newMap, TYtflowMapBase::idx_Lambda, std::move(newLambda));
        } else if (auto maybeHoppingAggregate = TMaybeNode<TYtflowHoppingAggregate>(node)) {
            auto hoppingAggregate = maybeHoppingAggregate.Cast();
            auto postprocessLambda = hoppingAggregate.PostprocessLambda();

            auto switchLambda = Build<TCoLambda>(ctx, postprocessLambda.Pos())
                .Args({"applyResult"})
                .Body<TExprList>()
                    .Add<TCoSwitch>()
                        .Input<TCoNth>()
                            .Tuple("applyResult")
                            .Index()
                                .Value(0)
                                .Build()
                            .Build()
                        .BufferBytes()
                            .Value(*switchBufferSize)
                            .Build()
                        .FreeArgs()
                            .Add(std::move(switchArgs))
                            .Build()
                        .Build()
                    .Add<TCoNth>()
                        .Tuple("applyResult")
                        .Index()
                            .Value(1)
                            .Build()
                        .Build()
                    .Add<TCoNth>()
                        .Tuple("applyResult")
                        .Index()
                            .Value(2)
                            .Build()
                        .Build()
                    .Build()
                .Done();

            auto newLambda = Build<TCoLambda>(ctx, postprocessLambda.Pos())
                .Args({"key", "savedState", "maxHopStartTime"})
                .Body<TExprApplier>()
                    .Apply(switchLambda)
                        .With<TExprApplier>(0)
                            .Apply(postprocessLambda)
                                .With(0, "key")
                                .With(1, "savedState")
                                .With(2, "maxHopStartTime")
                            .Build()
                        .Build()
                    .Done().Ptr();

            auto newHoppingAggregate = ctx.ChangeChild(hoppingAggregate.Ref(), TYtflowHoppingAggregate::idx_Sinks,
                Build<TExprList>(ctx, sinks.Pos())
                    .Add(std::move(newSinks))
                .Done().Ptr());
            resultOperation = ctx.ChangeChild(
                *newHoppingAggregate, TYtflowHoppingAggregate::idx_PostprocessLambda, std::move(newLambda));
        } else {
            YQL_ENSURE(false, "Unexpected node: " << node->Content());
        }

        auto addRemap = [&remaps](const auto* oldNode, const auto& newNode) {
            auto [_, emplaced] = remaps.emplace(oldNode, newNode);
            YQL_ENSURE(emplaced, "Got duplicate node for remap: " << oldNode->Content());
        };

        for (const auto* parent : parents) {
            if (auto maybeOutput = TMaybeNode<TYtflowOutput>(parent)) {
                auto output = maybeOutput.Cast();
                auto oldOutputIndex = ::FromString<ui32>(output.OutputIndex());
                auto indicesRemapIterator = indicesRemap.find(oldOutputIndex);
                YQL_ENSURE(
                    indicesRemapIterator != indicesRemap.end(),
                    "Unknown remap for index: " << oldOutputIndex);

                addRemap(parent, Build<TYtflowOutput>(ctx, output.Pos())
                    .InitFrom(output)
                    .Operation(resultOperation)
                    .OutputIndex()
                        .Value(indicesRemapIterator->second)
                        .Build()
                    .Done().Ptr());
            } else if (auto maybeLeft = TMaybeNode<TCoLeft>(parent)) {
                auto left = maybeLeft.Cast();
                addRemap(parent, Build<TCoLeft>(ctx, left.Pos())
                    .InitFrom(left)
                    .Input(resultOperation)
                    .Done().Ptr());
            }
        }
    }

private:
    TYtflowState::TPtr State_;
};

THolder<IGraphTransformer> CreateYtflowPhysicalFinalizingTransformer(TYtflowState::TPtr state) {
    return THolder(new TYtflowPhysicalFinalizingTransformer(state));
}

} // namespace
