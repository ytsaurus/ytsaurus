#pragma once

#include "public.h"
#include "schema.h"

#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/table_client/data_slice_descriptor.pb.h>

#include <yt/ytlib/transaction_client/public.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EDataSliceDescriptorType,
    ((File)                 (0))
    ((UnversionedTable)     (1))
    ((VersionedTable)       (2))
);

struct TDataSliceDescriptor
{
    EDataSliceDescriptorType Type;
    std::vector<NChunkClient::NProto::TChunkSpec> ChunkSpecs;
    TTableSchema Schema;
    NTransactionClient::TTimestamp Timestamp = 0;

    TDataSliceDescriptor() = default;
    TDataSliceDescriptor(
        EDataSliceDescriptorType type,
        std::vector<NChunkClient::NProto::TChunkSpec> chunkSpecs);
};

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TDataSliceDescriptor* protoDataSliceDescriptor,
    const TDataSliceDescriptor& dataSliceDescriptor);
void FromProto(
    TDataSliceDescriptor* dataSliceDescriptor,
    const NProto::TDataSliceDescriptor& protoDataSliceDescriptor);

////////////////////////////////////////////////////////////////////////////////

i64 GetCumulativeRowCount(const std::vector<TDataSliceDescriptor>& dataSliceDescriptors);
i64 GetDataSliceDescriptorReaderMemoryEstimate(
    const TDataSliceDescriptor& dataSliceDescriptor,
    NChunkClient::TMultiChunkReaderConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
