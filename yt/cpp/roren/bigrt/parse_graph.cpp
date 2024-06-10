#include "parse_graph.h"

#include "bigrt.h"
#include "execution_block.h"
#include "concurrency_transforms.h"
#include "stateful_impl/stateful_par_do.h"

#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/interface/private/par_do_tree.h>
#include <yt/cpp/roren/interface/private/raw_transform.h>

#include <yt/cpp/roren/bigrt/graph/parser.h>

#include <util/generic/hash_multi_map.h>
#include <util/generic/guid.h>

#include <deque>

namespace NRoren::NPrivate {
namespace {

////////////////////////////////////////////////////////////////////////////////

static TSerializedBlockConfig DefaultBlockConfig;

////////////////////////////////////////////////////////////////////////////////

IExecutionBlockPtr ParseSingleExecutionBlock(
    const TExecutionBlockConfig& blockConfig,
    const TTransformNodePtr& rootTransform,
    std::vector<std::pair<TString, TCreateBaseStateManagerFunction>>* createStateManagerFunctionList,
    std::vector<TRegisterWriterFunction>* registerWriterFunctionList,
    const TRegisterExecutionBlockTimers registerExecutionBlockTimers,
    TVCpuMetricsPtr vcpuMetrics
) {
    std::vector<IExecutionBlockPtr> outputBlockList;
    std::vector<int> outputIdList;

    std::deque<std::pair<int, TVector<TTransformNode*>>> queue;
    queue.push_back({TParDoTreeBuilder::RootNodeId, rootTransform->GetSink(0)->GetSourceFor()});

    TParDoTreeBuilder parDoTreeBuilder;
    std::vector<TWriterRegistrator> writerRegistratorList;
    THashMap<TString, NPrivate::TStatefulTimerParDoWrapperPtr> timersCallbacks;

    int parDoCount = 0;
    while (!queue.empty()) {
        auto [treeBuilderNodeId, transforms] = queue.front();
        queue.pop_front();

        auto addParDo = [&, nodeId=treeBuilderNodeId] (const IRawParDoPtr& rawParDo, const std::vector<TPCollectionNodePtr>& sinkNodeList) {
            ++parDoCount;
            auto outputNodeIds = parDoTreeBuilder.AddParDo(rawParDo, nodeId);
            Y_ABORT_UNLESS(std::ssize(outputNodeIds) == std::ssize(sinkNodeList));
            for (ssize_t i = 0; i < std::ssize(sinkNodeList); ++i) {
                queue.emplace_back(outputNodeIds[i], sinkNodeList[i]->GetSourceFor());
            }
        };

        for (const auto& transform : transforms) {
            if (const auto* registrator = NPrivate::GetAttribute(*transform->GetRawTransform(), WriterRegistratorTag)) {
                registerWriterFunctionList->push_back(*registrator);
            }

            auto type = transform->GetRawTransform()->GetType();
            if (type == ERawTransformType::ParDo) {
                auto rawParDo =  transform->GetRawTransform()->AsRawParDo()->Clone();
                addParDo(rawParDo, transform->GetSinkList());
            } else if (type == ERawTransformType::StatefulParDo) {
                if (!std::holds_alternative<TSerializedBlockConfig>(blockConfig)) {
                    ythrow yexception() << "StatefulParDo MUST be used inside serialized concurrency block";
                }
                const auto rawStatefulParDo = transform->GetRawTransform()->AsRawStatefulParDo()->Clone();
                const auto pState = transform->GetPStateNode();
                YT_VERIFY(pState);
                const auto stateVtable = NPrivate::GetRequiredAttribute(*pState, NPrivate::BigRtStateManagerVtableTag);
                const auto stateConfig = NPrivate::GetRequiredAttribute(*pState, NPrivate::BigRtStateConfigTag);

                auto id = TString{"state-manager-"} + TGUID::Create().AsGuidString();

                auto wrappedParDo = CreateStatefulParDo(rawStatefulParDo, id, stateVtable, stateConfig);
                addParDo(wrappedParDo, transform->GetSinkList());

                const auto createStateManagerFunction = NPrivate::GetRequiredAttribute(*pState, NPrivate::CreateBaseStateManagerFunctionTag);
                createStateManagerFunctionList->emplace_back(id, createStateManagerFunction);
            } else if (type == ERawTransformType::StatefulTimerParDo) {
                if (!std::holds_alternative<TSerializedBlockConfig>(blockConfig)) {
                    ythrow yexception() << "StatefulParDo MUST be used inside serialized concurrency block";
                }
                const auto rawStatefulTimerParDo = transform->GetRawTransform()->AsRawStatefulTimerParDo()->Clone();
                const auto pState = transform->GetPStateNode();
                YT_VERIFY(pState);
                const auto stateVtable = NPrivate::GetRequiredAttribute(*pState, NPrivate::BigRtStateManagerVtableTag);
                const auto stateConfig = NPrivate::GetRequiredAttribute(*pState, NPrivate::BigRtStateConfigTag);

                auto id = TString{"state-manager-"} + TGUID::Create().AsGuidString();

                auto wrappedParDo = CreateStatefulTimerParDo(rawStatefulTimerParDo, id, stateVtable, stateConfig);
                timersCallbacks[rawStatefulTimerParDo->GetFnId()] = wrappedParDo;
                addParDo(wrappedParDo, transform->GetSinkList());

                const auto createStateManagerFunction = NPrivate::GetRequiredAttribute(*pState, NPrivate::CreateBaseStateManagerFunctionTag);
                createStateManagerFunctionList->emplace_back(id, createStateManagerFunction);
            } else if (type == ERawTransformType::Flatten) {
                const auto* blockConfig = GetAttribute(*transform->GetRawTransform(), ExecutionBlockConfigTag);

                Y_ABORT_UNLESS(blockConfig, "Transform is not supported");
                // Double check, there should not be such transforms.
                Y_ABORT_UNLESS(transform->GetSourceCount() == 1, "Transform is not supported");

                auto block = ParseSingleExecutionBlock(*blockConfig, transform, createStateManagerFunctionList, registerWriterFunctionList, registerExecutionBlockTimers, vcpuMetrics);
                Y_ABORT_UNLESS(block);

                outputIdList.push_back(treeBuilderNodeId);
                outputBlockList.push_back(block);
            } else {
                ythrow yexception() << "Unexpected transform type: " << type;
            }
        }
    }
    Y_ABORT_UNLESS(outputBlockList.size() == outputIdList.size());

    static const auto trivialOutputIdList = std::vector<int>{TParDoTreeBuilder::RootNodeId};
    if (outputIdList == trivialOutputIdList) {
        // This is the case where switching parallel mode happens just after ReadTransform.
        // Throw away current block and return following as it was the input.
        return outputBlockList[0];
    } else {
        for (auto id : outputIdList) {
            parDoTreeBuilder.MarkAsOutput(id);
        }
    }

    // TODO(ermolovd): ParDoTree could be able to handle this case
    Y_ABORT_UNLESS(parDoCount > 0, "no ParDos for block: %s", rootTransform->SlowlyGetDebugDescription().c_str());

    auto parDo = parDoTreeBuilder.Build();

    return std::visit(
        [&] (auto&& config) ->IExecutionBlockPtr {
            using TConfig = std::decay_t<decltype(config)>;
            if constexpr (std::is_same_v<TConfig, TSerializedBlockConfig>) {
                auto executionBlock = CreateSerializedExecutionBlock(config, parDo, std::move(timersCallbacks), outputBlockList, vcpuMetrics);
                registerExecutionBlockTimers(executionBlock);
                return executionBlock;
            } else if constexpr (std::is_same_v<TConfig, TConcurrentBlockConfig>) {
                Y_ABORT_UNLESS(timersCallbacks.empty());
                return CreateConcurrentExecutionBlock(config, parDo, outputBlockList, vcpuMetrics);
            } else {
                static_assert(TDependentFalse<TConfig>);
            }
        }, blockConfig);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace // anonymous

////////////////////////////////////////////////////////////////////////////////

std::vector<TParseResult> ParseBigRtPipeline(const TPipeline& pipeline, const TRegisterExecutionBlockTimers registerExecutionBlockTimers, TVCpuMetricsPtr vcpuMetrics)
{
    std::vector<TParseResult> parseResultList;
    for (TTransformNodePtr transformNode : GetRawPipeline(pipeline)->GetTransformList()) {
        if (const auto* inputTag = GetAttribute(*transformNode->GetRawTransform(), InputTag)) {
            auto& parseResult = parseResultList.emplace_back();
            parseResult.InputTag = *inputTag;

            auto executionBlock = ParseSingleExecutionBlock(
                DefaultBlockConfig,
                transformNode,
                &parseResult.CreateStateManagerFunctionList,
                &parseResult.RegisterWriterFunctionList,
                registerExecutionBlockTimers,
                vcpuMetrics
            );
            parseResult.ExecutionBlock = executionBlock;
        }
    }

    return parseResultList;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
