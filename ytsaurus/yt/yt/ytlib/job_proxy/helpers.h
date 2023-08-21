#pragma once

#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>

#include <yt/yt/client/table_client/schemaful_reader_adapter.h>

#include <yt/yt/ytlib/scheduler/proto/job.pb.h>
#include <yt/yt/ytlib/table_client/partitioner.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

using TSchemalessMultiChunkReaderFactory = std::function<NTableClient::ISchemalessMultiChunkReaderPtr(
    NTableClient::TNameTablePtr,
    const NTableClient::TColumnFilter&)>;

using TSchemalessMultiChunkWriterFactory = std::function<NTableClient::ISchemalessMultiChunkWriterPtr(
    NTableClient::TNameTablePtr,
    NTableClient::TTableSchemaPtr)>;

////////////////////////////////////////////////////////////////////////////////

void RunQuery(
    const NScheduler::NProto::TQuerySpec& querySpec,
    const NTableClient::TSchemalessReaderFactory& readerFactory,
    const NTableClient::TSchemalessWriterFactory& writerFactory,
    const std::optional<TString>& udfDirectory);

std::vector<NChunkClient::TDataSliceDescriptor> UnpackDataSliceDescriptors(const NScheduler::NProto::TTableInputSpec& inputTableSpec);

NTableClient::IPartitionerPtr CreatePartitioner(const NScheduler::NProto::TPartitionJobSpecExt& partitionJobSpecExt);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
