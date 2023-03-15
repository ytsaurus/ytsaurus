#include "raw_pipeline.h"
#include "raw_transform.h"

#include "../executor.h" // IWYU pragma: keep for IExecutorPtr destructor
#include "../roren.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

static const TTypeTag<TBackTrace> TransformBacktraceTag{"transform-backtrace"};

////////////////////////////////////////////////////////////////////////////////

const TRawPipelinePtr& GetRawPipeline(const TMultiPCollection& multiPCollection)
{
    return multiPCollection.RawPipeline_;
}

const std::vector<std::pair<TDynamicTypeTag, TPCollectionNodePtr>>& GetTaggedNodeList(const TMultiPCollection& multiPCollection)
{
    return multiPCollection.NodeList_;
}

TMultiPCollection MakeMultiPCollection(const std::vector<std::pair<TDynamicTypeTag, TPCollectionNodePtr>>& taggedNodes, TRawPipelinePtr rawPipeline)
{
    return {std::move(taggedNodes), std::move(rawPipeline)};
}


////////////////////////////////////////////////////////////////////////////////

TTransformNode::TTransformNode(IRawTransformPtr transform)
    : Transform_(std::move(transform))
{ }

TTransformNodePtr TTransformNode::Allocate(
    const TRawPipelinePtr& rawPipeline,
    IRawTransformPtr transform,
    const std::vector<TPCollectionNode*>& inputs,
    const TRawPStateNodePtr& pState)
{
    const auto& outputTags = transform->GetOutputTags();
    TTransformNodePtr result = new TTransformNode(std::move(transform));

    for (auto* input : inputs) {
        input->SourceFor_.insert(result.Get());
        result->SourceList_.emplace_back(input);
    }

    for (const auto& tag : outputTags) {
        auto pCollectionNode = rawPipeline->AllocatePCollectionNode(tag.GetRowVtable(), result.Get());
        result->SinkList_.emplace_back(pCollectionNode);
    }

    result->PState_ = pState;

    // Saving backtrace of transform node creation for debug purposes.
    TBackTrace backtrace;
    backtrace.Capture();
    NRoren::NPrivate::SetAttribute(*result, TransformBacktraceTag, backtrace);

    return result;
}

TString TTransformNode::SlowlyGetDebugDescription() const
{
    TStringStream result;
    SlowlyPrintDebugDescription(&result);
    return result.Str();
}

void TTransformNode::SlowlyPrintDebugDescription() const
{
    SlowlyPrintDebugDescription(&Cerr);
}

void TTransformNode::SlowlyPrintDebugDescription(IOutputStream* out) const
{
    (*out) << "Transform node created at:\n";
    if (auto backtrace = NRoren::NPrivate::GetAttribute(*this, TransformBacktraceTag)) {
        backtrace->PrintTo(*out);
    } else {
        (*out) << "<unknown>";
    }
}

////////////////////////////////////////////////////////////////////////////////

TTransformNodePtr TRawPipeline::AddTransform(IRawTransformPtr transform, const std::vector<TPCollectionNode*>& inputs, const TRawPStateNodePtr& pState)
{
    Y_VERIFY(inputs.size() == transform->GetInputTags().size(),
        "inputs.size() == %d; transform->GetInputTags().size() == %d",
        static_cast<int>(inputs.size()),
        static_cast<int>(transform->GetInputTags().size()));

    auto transformNode = TTransformNode::Allocate(this, std::move(transform), inputs, pState);
    TransformList_.push_back(transformNode);
    return transformNode;
}

////////////////////////////////////////////////////////////////////////////////

void TraverseInTopologicalOrder(const TRawPipelinePtr& rawPipeline, IRawPipelineVisitor* visitor)
{
    THashSet<TPCollectionNode*> visited;

    // Transforms inside transform list are already ordered in topological order.
    for (const auto& transformNode : rawPipeline->GetTransformList()) {
        visitor->OnTransform(transformNode.Get());
        for (const auto& sink : transformNode->GetSinkList()) {
            if (!visited.contains(sink.Get())) {
                visited.insert(sink.Get());
                visitor->OnPCollection(sink.Get());
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

std::vector<std::pair<TDynamicTypeTag, TPCollectionNodePtr>> TTransformNode::GetTaggedSinkNodeList() const
{
    const auto& tagList = GetRawTransform()->GetOutputTags();
    const auto& sinkNodeList = GetSinkList();
    Y_VERIFY(ssize(tagList) == ssize(sinkNodeList));

    std::vector<std::pair<TDynamicTypeTag, TPCollectionNodePtr>> result;
    for (ssize_t i = 0; i < ssize(tagList); ++i) {
        result.emplace_back(tagList[i], sinkNodeList[i]);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

TPipeline MakePipeline(IExecutorPtr executor)
{
    return TPipeline{executor};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
