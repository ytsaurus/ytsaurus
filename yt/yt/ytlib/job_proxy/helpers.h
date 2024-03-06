#pragma once

#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/controller_agent/public.h>

#include <yt/yt/ytlib/table_client/partitioner.h>
#include <yt/yt/ytlib/table_client/schemaful_reader_adapter.h>

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
    const NControllerAgent::NProto::TQuerySpec& querySpec,
    const NTableClient::TSchemalessReaderFactory& readerFactory,
    const NTableClient::TSchemalessWriterFactory& writerFactory,
    const std::optional<TString>& udfDirectory);

std::vector<NChunkClient::TDataSliceDescriptor> UnpackDataSliceDescriptors(const NControllerAgent::NProto::TTableInputSpec& inputTableSpec);

NTableClient::IPartitionerPtr CreatePartitioner(const NControllerAgent::NProto::TPartitionJobSpecExt& partitionJobSpecExt);

////////////////////////////////////////////////////////////////////////////////

int GetJobFirstOutputTableFdFromSpec(const NControllerAgent::NProto::TUserJobSpec& spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
