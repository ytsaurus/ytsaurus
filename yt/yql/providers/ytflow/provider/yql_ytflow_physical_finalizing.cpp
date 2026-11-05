#include "yql_ytflow_provider_impl.h"
#include "yql_ytflow_constants.h"

#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_opt_utils.h>

#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.h>
#include <yt/yql/providers/ytflow/provider/yql_ytflow_utils.h>

#include <util/digest/multi.h>
#include <util/generic/map.h>

#include <library/cpp/iterator/enumerate.h>

#include <algorithm>


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

bool IsExtendImplementation(const TYtflowMapBase& map)
{
    return map.Maybe<TYtflowExtend>() || HasSetting(map.Settings().Ref(), EXTEND_SETTING);
}

ui32 GetOutputCount(const TYtflowOpBase& operation)
{
    const auto operationType = operation.Ref().GetTypeAnn();
    YQL_ENSURE(operationType && operationType->GetKind() == ETypeAnnotationKind::Tuple,
        "Unexpected type of " << operation.Ref().Content());

    const auto& operationItems = operationType->Cast<TTupleExprType>()->GetItems();
    YQL_ENSURE(operationItems.size() == 2,
        "Unexpected result tuple size of " << operation.Ref().Content());

    const auto* outputListType = operationItems[1];
    YQL_ENSURE(outputListType->GetKind() == ETypeAnnotationKind::List,
        "Unexpected output type of " << operation.Ref().Content());

    const auto* outputItemType = outputListType->Cast<TListExprType>()->GetItemType();
    if (outputItemType->GetKind() != ETypeAnnotationKind::Variant) {
        return 1;
    }

    const auto* underlyingType = outputItemType->Cast<TVariantExprType>()->GetUnderlyingType();
    YQL_ENSURE(underlyingType->GetKind() == ETypeAnnotationKind::Tuple,
        "Unexpected variant type of " << operation.Ref().Content());
    return underlyingType->Cast<TTupleExprType>()->GetSize();
}

} // anonymous namespace

class TYtflowPhysicalFinalizingTransformer : public TSyncTransformerBase {
private:
    struct TLambdaWithInputIndex {
        TExprBase Lambda;
        size_t InputIndex;
    };

    struct TOuterConsumersInfo {
        TVector<const TExprNode*> GenericMapNodes;
        TVector<const TExprNode*> NonGenericConsumers;
        bool InjectInputMessageId = false;
    };

    using TOuterConsumersByYtflowOutputIndex = TMap<ui32, TOuterConsumersInfo>;
    using TOuterConsumersByInnerMap = THashMap<TExprNode::TPtr, TOuterConsumersByYtflowOutputIndex>;
    using TYtflowOutputIndexRemaps = THashMap<const TExprNode*, ui32>;

    struct TBuildLambdasResult {
        TVector<TLambdaWithInputIndex> LambdasWithInputIndex;
        TYtflowOutputIndexRemaps InnerMapIndexRemaps;
        THashMap<const TExprNode*, TYtflowOutputIndexRemaps> IndexRemapsByOuterMap;
    };

    struct TSourceMapGroupKey {
        const TExprNode* ReadWrap;
        TWorldKey WorldKey;
        TString SettingsCacheKey;

        bool operator==(const TSourceMapGroupKey& other) const = default;
    };

    struct TSourceMapGroupKeyHash {
        size_t operator()(const TSourceMapGroupKey& key) const {
            auto hash = MultiHash(
                key.ReadWrap,
                key.WorldKey.IsSync,
                key.WorldKey.Inputs.size(),
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

        if (auto status = UpdateHasPersistentSink(output, output, parentsMap, ctx); status != TStatus::Ok) {
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

        if (auto status = MapWithUnusedOutputs(output, output, parentsMap, ctx); status != TStatus::Ok) {
            return status;
        }

        return TStatus::Ok;
    }

    TStatus UpdateHasPersistentSink(
        TExprNode::TPtr input,
        TExprNode::TPtr& output,
        const TParentsMap& parentsMap,
        TExprContext& ctx)
    {
        output = input;

        TNodeOnNodeOwnedMap remaps;
        VisitExpr(input, [&] (const TExprNode::TPtr& node) {
            auto maybeOperation = TMaybeNode<TYtflowOpBase>(node);
            if (!maybeOperation) {
                return true;
            }

            auto operation = maybeOperation.Cast();
            const auto consumers = GetConsumersUnordered(operation, parentsMap);
            if (!consumers) {
                return true;
            }

            const bool hasPersistentSink = std::any_of(
                consumers->begin(), consumers->end(), [] (const TYtflowConsumer& consumer) {
                    return TYtflowPersistentSink::Match(consumer.first);
                });

            if (HasSetting(operation.Settings().Ref(), HAS_PERSISTENT_SINK_SETTING) == hasPersistentSink) {
                return true;
            }

            auto settings = RemoveSetting(
                operation.Settings().Ref(), HAS_PERSISTENT_SINK_SETTING, ctx);
            if (hasPersistentSink) {
                settings = AddSetting(
                    *settings,
                    operation.Pos(),
                    TString(HAS_PERSISTENT_SINK_SETTING),
                    nullptr,
                    ctx);
            }

            remaps.emplace(operation.Raw(), ctx.ChangeChild(
                operation.Ref(), TYtflowOpBase::idx_Settings, std::move(settings)));
            return true;
        });

        if (remaps.empty()) {
            return TStatus::Ok;
        }

        YQL_CLOG(INFO, ProviderYtflow) << "PhysicalFinalizing-UpdateHasPersistentSink";

        auto remapSettings = TOptimizeExprSettings(State_->Types);
        remapSettings.VisitChanges = true;
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

                auto identityMap = Build<TYtflowMap>(ctx, originalOutput.Pos())
                    .World<TCoSync>()
                        .Add(operation.World())
                        .Build()
                    .Sources()
                        .Add(originalOutput)
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

            auto settings = RemoveSettings(
                sourceMap.Settings().Ref(),
                SOURCE_MAP_FUSION_COMPATIBLE_SETTINGS,
                ctx);
            TSourceMapGroupKey key{
                .ReadWrap = persistentSource.Input().Raw(),
                .WorldKey = GetWorldKey(sourceMap.World()),
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

            TVector<TLambdaWithInputIndex> switchLambdas;
            TVector<TYtflowOutputIndexRemaps> sourceMapIndexRemaps;
            ui32 currentOutputIndex = 0;
            bool injectInputMessageId = false;
            for (const auto& sourceMapPtr : sourceMaps) {
                auto sourceMap = TYtflowMapBase(sourceMapPtr);
                sourceMapIndexRemaps.push_back(AddOutputMapBranch(
                    sourceMap,
                    0,
                    parentsMap,
                    currentOutputIndex,
                    switchLambdas));

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
                    sourceMap.Get(),
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

        // inner map -> (YtflowOutput index, outer consumers info)
        TOuterConsumersByInnerMap outerMapsByInnerMap;
        VisitExpr(input, [
            &outerMapsByInnerMap,
            &parentsMap
        ](const TExprNode::TPtr& node) {
            auto maybeInnerMap = TMaybeNode<TYtflowMapBase>(node);
            if (!maybeInnerMap || maybeInnerMap.Maybe<TYtflowMap>()) {
                return true;
            }

            auto innerMap = maybeInnerMap.Cast();
            const auto consumers = GetConsumersOrderedStrict(innerMap, parentsMap);

            TOuterConsumersByYtflowOutputIndex outerMapsByYtflowOutputIndex;
            THashSet<const TExprNode*> nonGenericOutputs;
            bool hasGenericMap = false;
            for (const auto& [consumerNode, consumerYtflowOutput] : consumers) {
                auto outputIndex = FromString<ui32>(consumerYtflowOutput.OutputIndex().Value());
                auto& outputMapsInfo = outerMapsByYtflowOutputIndex[outputIndex];

                auto maybeConsumerMap = TMaybeNode<TYtflowMapBase>(consumerNode);
                if (maybeConsumerMap.Maybe<TYtflowMap>()) {
                    outputMapsInfo.GenericMapNodes.push_back(consumerNode);
                    hasGenericMap = true;
                } else if (nonGenericOutputs.insert(consumerYtflowOutput.Raw()).second) {
                    outputMapsInfo.NonGenericConsumers.push_back(consumerYtflowOutput.Raw());
                }

                if (maybeConsumerMap && GetSetting(
                    maybeConsumerMap.Cast().Settings().Ref(),
                    INJECT_INPUT_MESSAGE_ID_SETTING)
                ) {
                    outputMapsInfo.InjectInputMessageId = true;
                }
            }

            if (hasGenericMap) {
                outerMapsByInnerMap.emplace(innerMap.Ptr(), std::move(outerMapsByYtflowOutputIndex));
            }

            return true;
        });

        if (outerMapsByInnerMap.empty()) {
            return TStatus::Ok;
        }

        YQL_CLOG(INFO, ProviderYtflow) << "PhysicalFinalizing-YtflowMapOverMap";

        TNodeOnNodeOwnedMap remaps;
        for (const auto& [innerMap, outerMapsByYtflowOutputIndex] : outerMapsByInnerMap) {
            auto buildResult = BuildLambdas(innerMap, outerMapsByYtflowOutputIndex, parentsMap, ctx);
            auto parentMap = TYtflowMapBase(innerMap);
            auto newLambda = BuildSwitchLambda(
                parentMap.Lambda(),
                buildResult.LambdasWithInputIndex,
                ctx);

            bool injectInputMessageId = false;
            for (const auto& [outputIndex, outerMapsInfo] : outerMapsByYtflowOutputIndex) {
                if (outerMapsInfo.InjectInputMessageId) {
                    injectInputMessageId = true;
                    break;
                }
            }

            auto newMap = ctx.ChangeChild(
                *innerMap,
                TYtflowMapBase::idx_Lambda,
                std::move(newLambda));

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
                innerMap.Get(),
                buildResult.InnerMapIndexRemaps,
                newMap,
                parentsMap,
                remaps,
                ctx);

            for (const auto& [_, outerMaps] : outerMapsByYtflowOutputIndex) {
                for (const auto& map : outerMaps.GenericMapNodes) {
                    const auto outerMapIndexRemapsIterator = buildResult.IndexRemapsByOuterMap.find(map);
                    YQL_ENSURE(outerMapIndexRemapsIterator != buildResult.IndexRemapsByOuterMap.end(),
                        "Unknown index remaps for " << map->Content());

                    FillRemaps(
                        map,
                        outerMapIndexRemapsIterator->second,
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

    TStatus MapWithUnusedOutputs(
        TExprNode::TPtr input,
        TExprNode::TPtr& output,
        const TParentsMap& parentsMap,
        TExprContext& ctx)
    {
        output = input;

        TNodeOnNodeOwnedMap remaps;
        VisitExpr(input, [&] (const TExprNode::TPtr& node) {
            auto maybeMap = TMaybeNode<TYtflowMapBase>(node);
            if (!maybeMap) {
                return true;
            }

            const auto map = maybeMap.Cast();
            const auto outputCount = GetOutputCount(map);

            const auto consumers = GetConsumersUnorderedStrict(map, parentsMap);
            TMap<ui32, TVector<const TExprNode*>> ytflowOutputsByIndex;
            for (const auto& [_, consumerYtflowOutput] : consumers) {
                const auto index = FromString<ui32>(consumerYtflowOutput.OutputIndex().Value());
                YQL_ENSURE(index < outputCount,
                    "Unexpected output index " << index << " of " << map.Ref().Content());

                ytflowOutputsByIndex[index].push_back(consumerYtflowOutput.Raw());
            }

            YQL_ENSURE(!ytflowOutputsByIndex.empty(),
                "No used outputs of " << map.Ref().Content());
            if (ytflowOutputsByIndex.size() == outputCount) {
                return true;
            }

            TVector<TLambdaWithInputIndex> switchLambdas;
            TYtflowOutputIndexRemaps indexRemaps;
            ui32 newIndex = 0;
            for (const auto& [index, outputs] : ytflowOutputsByIndex) {
                switchLambdas.push_back(TLambdaWithInputIndex{
                    .Lambda = Build<TCoLambda>(ctx, map.Pos())
                        .Args({"stream"})
                        .Body("stream")
                        .Done(),
                    .InputIndex = index,
                });
                for (const auto* consumerOutput : outputs) {
                    indexRemaps.emplace(consumerOutput, newIndex);
                }
                ++newIndex;
            }

            auto newMap = ctx.ChangeChild(
                map.Ref(),
                TYtflowMapBase::idx_Lambda,
                BuildSwitchLambda(map.Lambda(), switchLambdas, ctx));

            FillRemaps(map.Raw(), indexRemaps, newMap, parentsMap, remaps, ctx);

            return true;
        });

        if (remaps.empty()) {
            return TStatus::Ok;
        }

        YQL_CLOG(INFO, ProviderYtflow) << "PhysicalFinalizing-MapWithUnusedOutputs";

        auto remapSettings = TOptimizeExprSettings(State_->Types);
        remapSettings.VisitChanges = true;
        auto status = RemapExpr(output, output, remaps, ctx, remapSettings);
        if (status != TStatus::Ok) {
            return status;
        }

        return TStatus(TStatus::Repeat, /*hasRestart*/ true);
    }

    void Rewind() final {
    }

private:
    void FillRemaps(
        const TExprNode* node,
        const TYtflowOutputIndexRemaps& ytflowOutputIndexRemaps,
        const TExprNode::TPtr& newMap,
        const TParentsMap& parentsMap,
        TNodeOnNodeOwnedMap& remaps,
        TExprContext& ctx
    ) {
        const auto consumers = GetConsumersUnorderedStrict(TYtflowOpBase(node), parentsMap);
        for (const auto& [_, consumerYtflowOutput] : consumers) {
            const auto indexRemapsIterator = ytflowOutputIndexRemaps.find(consumerYtflowOutput.Raw());
            if (indexRemapsIterator == ytflowOutputIndexRemaps.end() ||
                remaps.contains(consumerYtflowOutput.Raw()))
            {
                continue;
            }

            remaps.emplace(consumerYtflowOutput.Raw(), Build<TYtflowOutput>(ctx, consumerYtflowOutput.Pos())
                .InitFrom(consumerYtflowOutput)
                .OutputIndex()
                    .Value(indexRemapsIterator->second)
                    .Build()
                .Operation(newMap)
                .Done().Ptr());
        }

        for (const auto& left : GetLeftNodesFromParents(TYtflowOpBase(node), parentsMap)) {
            remaps.emplace(left.Raw(), Build<TCoLeft>(ctx, left.Pos())
                .InitFrom(left)
                .Input(newMap)
                .Done().Ptr());
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

    TYtflowOutputIndexRemaps AddOutputMapBranch(
        const TYtflowMapBase& outerMap,
        ui32 switchInputIndex,
        const TParentsMap& parentsMap,
        ui32& nextOutputIndex,
        TVector<TLambdaWithInputIndex>& switchLambdas
    ) {
        const auto consumers = GetConsumersOrderedStrict(outerMap, parentsMap);
        YQL_ENSURE(!consumers.empty(), "Map must have at least one consumer");

        TYtflowOutputIndexRemaps ytflowOutputIndexRemaps;
        THashSet<const TExprNode*> seenOutputs;
        const auto outputCount = GetOutputCount(outerMap);
        const auto outputIndexOffset = nextOutputIndex;
        nextOutputIndex += outputCount;
        for (const auto& [_, consumerYtflowOutput] : consumers) {
            if (!seenOutputs.insert(consumerYtflowOutput.Raw()).second) {
                continue;
            }

            auto outputIndex = FromString<ui32>(consumerYtflowOutput.OutputIndex().Value());
            YQL_ENSURE(outputIndex < outputCount,
                "Unexpected output index " << outputIndex << " of " << outerMap.Ref().Content());
            const auto emplaced = ytflowOutputIndexRemaps.emplace(
                consumerYtflowOutput.Raw(), outputIndexOffset + outputIndex).second;
            YQL_ENSURE(emplaced, "Duplicate remap for " << consumerYtflowOutput.Ref().Content());
        }

        switchLambdas.push_back(TLambdaWithInputIndex{
            .Lambda = outerMap.Lambda(),
            .InputIndex = switchInputIndex
        });

        return ytflowOutputIndexRemaps;
    }

    TBuildLambdasResult BuildLambdas(
        const TExprNode::TPtr& inputMap,
        const TOuterConsumersByYtflowOutputIndex& outerMapsByYtflowOutputIndex,
        const TParentsMap& parentsMap,
        TExprContext& ctx
    ) {
        TBuildLambdasResult result;
        ui32 nextRemappedYtflowOutputIndex = 0;
        for (const auto& [ytflowOutputIndex, outerMaps] : outerMapsByYtflowOutputIndex) {
            if (!outerMaps.NonGenericConsumers.empty()) {
                result.LambdasWithInputIndex.push_back(TLambdaWithInputIndex{
                    .Lambda = Build<TCoLambda>(ctx, inputMap->Pos())
                        .Args({"arg"})
                        .Body("arg")
                        .Done(),
                    .InputIndex = ytflowOutputIndex
                });
                for (const auto& nonGenericConsumer : outerMaps.NonGenericConsumers) {
                    const auto emplaced = result.InnerMapIndexRemaps.emplace(
                        nonGenericConsumer, nextRemappedYtflowOutputIndex).second;
                    YQL_ENSURE(emplaced, "Duplicate remap for " << nonGenericConsumer->Content());
                }
                ++nextRemappedYtflowOutputIndex;
            }

            for (const auto& outerMap : outerMaps.GenericMapNodes) {
                const auto emplaced = result.IndexRemapsByOuterMap.emplace(
                    outerMap,
                    AddOutputMapBranch(
                        TYtflowMapBase(outerMap),
                        ytflowOutputIndex,
                        parentsMap,
                        nextRemappedYtflowOutputIndex,
                        result.LambdasWithInputIndex)).second;
                YQL_ENSURE(emplaced, "Duplicate remaps for " << outerMap->Content());
            }
        }

        return result;
    }

private:
    TYtflowState::TPtr State_;
};

THolder<IGraphTransformer> CreateYtflowPhysicalFinalizingTransformer(TYtflowState::TPtr state) {
    return THolder(new TYtflowPhysicalFinalizingTransformer(state));
}

} // namespace
