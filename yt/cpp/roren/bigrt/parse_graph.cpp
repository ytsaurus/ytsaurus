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

TPCollectionNodePtr VerifiedGetSingleOutput(const TTransformNodePtr& transform)
{
    Y_VERIFY(transform->GetSinkCount() == 1);
    return transform->GetSink(0);
}

////////////////////////////////////////////////////////////////////////////////

class TPipelineReader
{
public:
    TPipelineReader(const TPipeline& pipeline)
    {
        const auto& rawTransformList = GetRawPipeline(pipeline)->GetTransformList();

        for (const auto& rawTransform : rawTransformList) {
            const auto type = rawTransform->GetRawTransform()->GetType();
            switch (rawTransform->GetSourceCount()) {
                case 0:
                    NodeToTransforms_.emplace(nullptr, rawTransform);
                    break;
                case 1:
                    NodeToTransforms_.emplace(rawTransform->GetSource(0), rawTransform);
                    break;
                default:
                    Y_VERIFY(type != ERawTransformType::Read && type != ERawTransformType::ParDo);
                    ythrow yexception() << "Unexpected transform type in BigRt resharder: " << type;
            }
        }
    }

    TTransformNodePtr PopReadTransform()
    {
        while (true) {
            auto&& [begin, end] = NodeToTransforms_.equal_range(nullptr);
            if (begin == end) {
                return nullptr;
            }

            TTransformNodePtr result = begin->second;
            NodeToTransforms_.erase(begin);

            if (!GetAttribute(*result->GetRawTransform(), BindToDictTag)) {
                return result;
            }
        }
        Y_FAIL("unreachable");
    }

    std::vector<TTransformNodePtr> PopTransformsForPCollection(const TPCollectionNodePtr& node)
    {
        auto&& [begin, end] = NodeToTransforms_.equal_range(node);

        auto result = std::vector<TTransformNodePtr>{};
        for (auto it = begin; it != end; ++it) {
            result.push_back(it->second);
        }
        NodeToTransforms_.erase(begin, end);
        return result;
    }

    bool IsExhausted() const
    {
        return NodeToTransforms_.empty();
    }

    TTransformNodePtr GetAnyRemaining() const
    {
        if (NodeToTransforms_.empty()) {
            return nullptr;
        } else {
            return NodeToTransforms_.begin()->second;
        }

    }

private:
    THashMultiMap<TPCollectionNodePtr, TTransformNodePtr> NodeToTransforms_;
};

////////////////////////////////////////////////////////////////////////////////

IExecutionBlockPtr ParseSingleExecutionBlock(
    const TExecutionBlockConfig& blockConfig,
    const TTransformNodePtr& rootTransform,
    TPipelineReader* reader,
    std::vector<std::pair<TString, TCreateBaseStateManagerFunction>>* createStateManagerFunctionList
) {
    std::vector<IExecutionBlockPtr> outputBlockList;
    std::vector<int> outputIdList;

    std::deque<std::pair<int, TPCollectionNodePtr>> queue;
    queue.push_back({TParDoTreeBuilder::RootNodeId, VerifiedGetSingleOutput(rootTransform)});

    TParDoTreeBuilder parDoTreeBuilder;
    std::vector<TWriterRegistrator> writerRegistratorList;

    int parDoCount = 0;
    while (!queue.empty()) {
        auto [treeBuilderNodeId, pCollection] = queue.front();
        queue.pop_front();

        auto transformList = reader->PopTransformsForPCollection(pCollection);

        auto addParDo = [&, nodeId=treeBuilderNodeId] (const IRawParDoPtr& rawParDo, const std::vector<TPCollectionNodePtr>& sinkNodeList) {
            ++parDoCount;
            auto outputNodeIds = parDoTreeBuilder.AddParDo(rawParDo, nodeId);
            Y_VERIFY(std::ssize(outputNodeIds) == std::ssize(sinkNodeList));
            for (ssize_t i = 0; i < std::ssize(sinkNodeList); ++i) {
                queue.emplace_back(outputNodeIds[i], sinkNodeList[i]);
            }
        };

        for (const auto& transform : transformList) {
            auto type = transform->GetRawTransform()->GetType();
            if (type == ERawTransformType::ParDo) {
                auto rawParDo =  transform->GetRawTransform()->AsRawParDo();
                addParDo(rawParDo, transform->GetSinkList());
            } else if (type == ERawTransformType::StatefulParDo) {
                const auto rawStatefulParDo = transform->GetRawTransform()->AsRawStatefulParDo();
                const auto pState = transform->GetPStateNode();
                YT_VERIFY(pState);
                const auto stateVtable = NPrivate::GetRequiredAttribute(*pState, NPrivate::BigRtStateManagerVtableTag);
                const auto stateConfig = NPrivate::GetRequiredAttribute(*pState, NPrivate::BigRtStateConfigTag);

                auto id = TString{"state-manager-"} + TGUID::Create().AsGuidString();

                auto wrappedParDo = CreateStatefulParDo(rawStatefulParDo, id, stateVtable, stateConfig);
                addParDo(wrappedParDo, transform->GetSinkList());

                const auto createStateManagerFunction = NPrivate::GetRequiredAttribute(*pState, NPrivate::CreateBaseStateManagerFunctionTag);
                createStateManagerFunctionList->emplace_back(id, createStateManagerFunction);
            } else if (type == ERawTransformType::Flatten) {
                const auto* blockConfig = GetAttribute(*transform->GetRawTransform(), ExecutionBlockConfigTag);

                Y_VERIFY(blockConfig, "Transform is not supported");
                // Double check, there should not be such transforms.
                Y_VERIFY(transform->GetSourceCount() == 1, "Transform is not supported");

                auto block = ParseSingleExecutionBlock(*blockConfig, transform, reader, createStateManagerFunctionList);
                Y_VERIFY(block);

                outputIdList.push_back(treeBuilderNodeId);
                outputBlockList.push_back(block);
            } else {
                ythrow yexception() << "Unexpected transform type: " << type;
            }
        }
    }
    Y_VERIFY(outputBlockList.size() == outputIdList.size());

    for (auto id : outputIdList) {
        parDoTreeBuilder.MarkAsOutput(id);
    }

    // TODO(ermolovd): ParDoTree could be able to handle this case
    Y_VERIFY(parDoCount > 0, "no ParDos for block: %s", rootTransform->SlowlyGetDebugDescription().c_str());

    auto parDo = parDoTreeBuilder.Build();

    return std::visit(
        [&] (auto&& config) ->IExecutionBlockPtr {
            using TConfig = std::decay_t<decltype(config)>;
            if constexpr (std::is_same_v<TConfig, TSerializedBlockConfig>) {
                return CreateSerializedExecutionBlock(config, parDo, outputBlockList);
            } else if constexpr (std::is_same_v<TConfig, TConcurrentBlockConfig>) {
                return CreateConcurrentExecutionBlock(config, parDo, outputBlockList);
            } else {
                static_assert(TDependentFalse<TConfig>);
            }
        }, blockConfig);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace // anonymous

////////////////////////////////////////////////////////////////////////////////

TParseResult ParseBigRtPipeline(const TPipeline& pipeline)
{
    TParseResult result;
    {
        const auto& transformList = GetRawPipeline(pipeline)->GetTransformList();
        for (const auto& transform : transformList) {
            if (const auto* registrator = NPrivate::GetAttribute(*transform->GetRawTransform(), WriterRegistratorTag)) {
                result.RegisterWriterFunctionList.push_back(*registrator);
            }
        }
    }
    TPipelineReader reader{pipeline};

    auto rootTransform = reader.PopReadTransform();
    Y_VERIFY(rootTransform, "Pipeline is empty");

    auto executionBlock = ParseSingleExecutionBlock(DefaultBlockConfig, rootTransform, &reader, &result.CreateStateManagerFunctionList);
    result.ExecutionBlock = executionBlock;
    if (const auto* inputTag = GetAttribute(*rootTransform->GetRawTransform(), InputTag)) {
        result.InputTag = *inputTag;
    }

    Y_VERIFY(reader.IsExhausted(), "Pipeline with multiple inputs are not supported yet.");

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
