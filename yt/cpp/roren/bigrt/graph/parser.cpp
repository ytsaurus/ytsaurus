#include "parser.h"

#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/interface/private/par_do_tree.h>
#include <yt/cpp/roren/interface/private/raw_multi_write.h>
#include <yt/cpp/roren/interface/private/raw_transform.h>

#include <contrib/libs/protobuf/src/google/protobuf/message.h>

#include <util/generic/hash.h>
#include <util/generic/hash_multi_map.h>

#include <queue>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

const TTypeTag<TWriterRegistrator> WriterRegistratorTag{"writer_registrator"};
const TTypeTag<bool> BindToDictTag{"bind_to_dict"};
const TTypeTag<TString> InputTag{"input_tag"};

////////////////////////////////////////////////////////////////////////////////

TPCollectionNodePtr VerifiedGetSingleOutput(const TTransformNodePtr& transform)
{
    Y_VERIFY(transform->GetSinkCount() == 1);
    return transform->GetSink(0);
}

class TPipelineParser
{
public:
    TPipelineParser(const TPipeline& pipeline)
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

private:
    THashMultiMap<TPCollectionNodePtr, TTransformNodePtr> NodeToTransforms_;
};

std::optional<TBigRtResharderDescription> ParseResharderImpl(TPipelineParser& parser)
{
    auto readTransform = parser.PopReadTransform();
    if (!readTransform) {
        return {};
    }

    THashMap<int, std::vector<TTransformNodePtr>> writeTransforms;
    std::deque<std::pair<int, TPCollectionNodePtr>> queue;
    queue.push_back({TParDoTreeBuilder::RootNodeId, VerifiedGetSingleOutput(readTransform)});

    TParDoTreeBuilder parDoTreeBuilder;
    std::vector<TWriterRegistrator> writerRegistratorList;

    int parDoCount = 0;
    while (!queue.empty()) {
        auto&& [treeBuilderNodeId, pCollection] = queue.front();
        queue.pop_front();

        auto transformList = parser.PopTransformsForPCollection(pCollection);

        for (const auto& transform : transformList) {
            auto type = transform->GetRawTransform()->GetType();
            if (type == ERawTransformType::Write) {
                writeTransforms[treeBuilderNodeId].push_back(transform);
            } else if (type == ERawTransformType::ParDo) {
                ++parDoCount;
                auto outputNodeIds = parDoTreeBuilder.AddParDo(transform->GetRawTransform()->AsRawParDo(), treeBuilderNodeId);
                Y_VERIFY(std::ssize(outputNodeIds) == transform->GetSinkCount());
                for (ssize_t i = 0; i < std::ssize(outputNodeIds); ++i) {
                    queue.emplace_back(outputNodeIds[i], transform->GetSink(i));
                }
            } else {
                ythrow yexception() << "Unexpected transform type: " << type;
            }

            if (const auto* registrator = NPrivate::GetAttribute(*transform->GetRawTransform(), WriterRegistratorTag)) {
                writerRegistratorList.push_back(*registrator);
            }
        }
    }

    // TODO(ermolovd): ParDoTree could be able to handle this case
    Y_VERIFY(parDoCount > 0, "pipeline doesn't have any ParDo");

    TBigRtResharderDescription result;
    for (const auto& [nodeId, transforms] : writeTransforms) {
        std::vector<IRawWritePtr> writes;
        for (const auto& t : transforms) {
            writes.push_back(t->GetRawTransform()->AsRawWrite());
        }

        IRawWritePtr currentResultWrite;
        if (writes.size() == 1) {
            currentResultWrite = writes.front();
        } else {
            currentResultWrite = ::MakeIntrusive<TRawMultiWrite>(writes);
        }

        parDoTreeBuilder.MarkAsOutput(nodeId);
        result.Writers.push_back(currentResultWrite);
    }

    result.Resharder = parDoTreeBuilder.Build();
    result.WriterRegistratorList = std::move(writerRegistratorList);
    if (const auto* inputTag = GetAttribute(*readTransform->GetRawTransform(), InputTag)) {
        result.InputTag = *inputTag;
    }

    return result;
}

std::vector<TBigRtResharderDescription> ParseBigRtResharder(const TPipeline& pipeline)
{
    TPipelineParser parser(pipeline);

    std::vector<TBigRtResharderDescription> resultList;
    while (auto current = ParseResharderImpl(parser)) {
        resultList.emplace_back(*current);
    }

    return resultList;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
