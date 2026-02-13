#include "roren.h"

#include "executor.h"
#include "private/merge_par_dos.h"


namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

TTypeTag<TString> TransformNameTag("TransformName");
TTypeTag<TString> StreamIdTag("StreamId");

NPrivate::TAttributeSetter Name(TString transformName)
{
    return NPrivate::TAttributeSetter(TransformNameTag, std::move(transformName));
}

NPrivate::TAttributeSetter SetStreamId(TString streamId)
{
    return NPrivate::TAttributeSetter(StreamIdTag, std::move(streamId));
}

////////////////////////////////////////////////////////////////////////////////

TPipeline::TPipeline(IExecutorPtr executor)
    : TPCollection<void>(
        NYT::New<NPrivate::TPCollectionNode>(NPrivate::MakeRowVtable<void>(), 0, 0, nullptr),
        NYT::New<NPrivate::TRawPipeline>())
    , Executor_(std::move(executor))
{
    if (!Executor_) {
        ythrow yexception() << "Pipeline executor is not specified";
    }
}

TPipeline::TPipeline(const TPipeline& pipeline) = default;

TPipeline::TPipeline(TPipeline&& pipeline) = default;

TPipeline::~TPipeline() = default;

void TPipeline::Run()
{
    if (Executor_->EnableDefaultPipelineOptimization()) {
        Optimize();
    }

    Executor_->Run(*this);
}

TString TPipeline::DumpDot() const
{
    return RawPipeline_->DumpDot();
}

void TPipeline::Dump(NYT::NYson::IYsonConsumer* consumer) const
{
    RawPipeline_->Dump(consumer);
}

void TPipeline::Optimize()
{
    RawPipeline_ = NPrivate::MergeParDos(RawPipeline_);
}

////////////////////////////////////////////////////////////////////////////////

TMultiPCollection::TMultiPCollection(const TPipeline& pipeline)
    : RawPipeline_(NPrivate::GetRawPipeline(pipeline))
{ }

TMultiPCollection::TMultiPCollection(
    const std::vector<std::pair<TDynamicTypeTag, NPrivate::TPCollectionNodePtr>>& nodes,
    NPrivate::TRawPipelinePtr pipeline)
    : RawPipeline_(std::move(pipeline))
{
    for (const auto& [tag, node] : nodes) {
        auto [it, inserted] = NodeMap_.emplace(tag.GetKey(), node);
        Y_ABORT_UNLESS(inserted);
    }
}

////////////////////////////////////////////////////////////////////////////////

const NPrivate::TRawPipelinePtr& NPrivate::GetRawPipeline(const TPipeline &pipeline)
{
    return pipeline.RawPipeline_;
}
////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
