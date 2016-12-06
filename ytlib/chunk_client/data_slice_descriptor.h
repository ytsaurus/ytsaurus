#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/data_slice_descriptor.pb.h>

#include <yt/ytlib/table_client/schema.h>

#include <yt/ytlib/transaction_client/public.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EDataSliceDescriptorType,
    ((File)                 (0))
    ((UnversionedTable)     (1))
    ((VersionedTable)       (2))
);

struct TDataSliceDescriptor
{
    EDataSliceDescriptorType Type;
    std::vector<NProto::TChunkSpec> ChunkSpecs;
    NTableClient::TTableSchema Schema;
    NTransactionClient::TTimestamp Timestamp = 0;

    TDataSliceDescriptor() = default;
    TDataSliceDescriptor(
        EDataSliceDescriptorType type,
        std::vector<NProto::TChunkSpec> chunkSpecs,
        const NTableClient::TTableSchema& = NTableClient::TTableSchema(),
        NTransactionClient::TTimestamp timestamp = 0);

    const NProto::TChunkSpec& GetSingleUnversionedChunk() const;
    const NProto::TChunkSpec& GetSingleFileChunk() const;
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
    TMultiChunkReaderConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

TDataSliceDescriptor MakeFileDataSliceDescriptor(NProto::TChunkSpec chunkSpec);
TDataSliceDescriptor MakeUnversionedDataSliceDescriptor(NProto::TChunkSpec chunkSpec);
TDataSliceDescriptor MakeVersionedDataSliceDescriptor(
    std::vector<NProto::TChunkSpec> chunkSpecs,
    const NTableClient::TTableSchema& schema,
    NTransactionClient::TTimestamp timestam);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
