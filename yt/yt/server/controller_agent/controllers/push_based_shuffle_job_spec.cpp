#include "push_based_shuffle_job_spec.h"

#include <yt/yt/server/controller_agent/config.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/distributed_chunk_session_client/helpers.h>

#include <yt/yt/ytlib/push_based_shuffle_client/config.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

namespace NYT::NControllerAgent::NControllers {

using namespace NControllerAgent::NProto;
using namespace NDistributedChunkSessionClient;
using namespace NPushBasedShuffleClient;
using namespace NTableClient;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

TPushBasedShuffleJobSpecBuilder::TPushBasedShuffleJobSpecBuilder(
    TPushBasedShuffleOptionsPtr options,
    TTableSchemaPtr intermediateStreamSchema,
    NCompression::ECodec codec,
    int replicationFactor,
    int partitionCount,
    i64 writerMaxBufferSize)
    : Options_(std::move(options))
    , IntermediateStreamSchema_(std::move(intermediateStreamSchema))
    , Codec_(codec)
    , ReplicationFactor_(replicationFactor)
    , PartitionCount_(partitionCount)
    , WriterMaxBufferSize_(writerMaxBufferSize)
{ }

void TPushBasedShuffleJobSpecBuilder::ValidateWriterMemoryBudget() const
{
    i64 metadataSize = ComputeWriterMetadataSize();
    i64 availableSize = WriterMaxBufferSize_ - metadataSize;

    THROW_ERROR_EXCEPTION_IF(
        availableSize < MinShuffleWriterMemoryBudget,
        "Partition job writer buffer is too small for push-based shuffle")
        .With("max_buffer_size", WriterMaxBufferSize_)
        .With("metadata_size", metadataSize)
        .With("minimum_memory_budget", MinShuffleWriterMemoryBudget)
        .With("partition_count", PartitionCount_);
}

i64 TPushBasedShuffleJobSpecBuilder::ComputeWriterJobMemorySize() const
{
    return ComputeWriterMemoryBudget() + ComputeWriterMetadataSize();
}

void TPushBasedShuffleJobSpecBuilder::FillWriterSpecTemplate(TJobSpec* jobSpecTemplate) const
{
    auto* writerSpec = jobSpecTemplate->MutableExtension(TPartitionJobSpecExt::partition_job_spec_ext)
        ->mutable_push_based_shuffle_writer();
    ToProto(writerSpec->mutable_intermediate_stream_schema(), IntermediateStreamSchema_);
    writerSpec->set_codec(ToProto(Codec_));

    auto writerConfig = CloneYsonStruct(Options_->ShuffleWriterConfig);
    writerConfig->MemoryBudget = ComputeWriterMemoryBudget();
    writerSpec->set_writer_config(ToProto(ConvertToYsonString(writerConfig)));
}

void TPushBasedShuffleJobSpecBuilder::FillWriterSpec(
    TJobSpec* jobSpec,
    int taskJobIndex,
    const std::vector<TReadySession>& readySessions)
{
    auto* writerSpec = jobSpec->MutableExtension(TPartitionJobSpecExt::partition_job_spec_ext)
        ->mutable_push_based_shuffle_writer();
    writerSpec->set_task_job_index(taskJobIndex);

    for (const auto& session : readySessions) {
        auto* sessionSpec = writerSpec->add_seeded_sessions();
        sessionSpec->set_partition_index(session.SlotCookie);
        ToProto(sessionSpec->mutable_session_id(), session.Descriptor.SessionId);
        ToProto(sessionSpec->mutable_sequencer_node(), session.Descriptor.SequencerNode);
    }
}

void TPushBasedShuffleJobSpecBuilder::FillSortReaderSpec(TPushBasedShuffleSortReaderSpec* readerSpec) const
{
    readerSpec->set_partition_reader_config(ToProto(ConvertToYsonString(Options_->PartitionReaderConfig)));
    readerSpec->set_sort_reader_config(ToProto(ConvertToYsonString(Options_->SortReaderConfig)));
    readerSpec->set_read_quorum(ComputeDefaultJournalQuorums(ReplicationFactor_).ReadQuorum);
    ToProto(
        readerSpec->mutable_intermediate_stream_name_table(),
        TNameTable::FromSchema(*IntermediateStreamSchema_));
    readerSpec->set_codec(ToProto(Codec_));
    readerSpec->set_sort_thread_count(Options_->SortThreadCount);
}

void TPushBasedShuffleJobSpecBuilder::FillValidTaskJobIndexes(TJobSpec* jobSpec, const std::vector<int>& taskJobIndexes)
{
    auto getSortReaderIndexes = [&] (const auto& extension) {
        return jobSpec->MutableExtension(extension)
            ->mutable_push_based_shuffle_sort_reader()
            ->mutable_valid_task_job_indexes();
    };
    auto getMergeIndexes = [&] (const auto& extension) {
        return jobSpec->MutableExtension(extension)->mutable_push_based_shuffle_valid_task_job_indexes();
    };

    auto* validTaskJobIndexes = [&] {
        switch (FromProto<EJobType>(jobSpec->type())) {
            case EJobType::FinalSort:
                return getSortReaderIndexes(TSortJobSpecExt::sort_job_spec_ext);
            case EJobType::PartitionReduce:
                return getSortReaderIndexes(TReduceJobSpecExt::reduce_job_spec_ext);
            case EJobType::SortedMerge:
                return getMergeIndexes(TMergeJobSpecExt::merge_job_spec_ext);
            case EJobType::SortedReduce:
                return getMergeIndexes(TReduceJobSpecExt::reduce_job_spec_ext);
            default:
                YT_ABORT();
        }
    }();

    ToProto(validTaskJobIndexes->mutable_task_job_indexes(), taskJobIndexes);
}

i64 TPushBasedShuffleJobSpecBuilder::ComputeWriterMetadataSize() const
{
    return PartitionCount_ *
        (Options_->PerPartitionMetadataEstimate + Options_->PerSequencerConnectionEstimate);
}

i64 TPushBasedShuffleJobSpecBuilder::ComputeWriterMemoryBudget() const
{
    i64 availableSize = WriterMaxBufferSize_ - ComputeWriterMetadataSize();

    i64 desiredSize = std::max<i64>(
        MinShuffleWriterMemoryBudget,
        static_cast<i64>(PartitionCount_ * Options_->TargetUncompressedRecordSize / Options_->ShuffleWriterConfig->BuildersBudgetFraction));

    return std::min(availableSize, desiredSize);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
