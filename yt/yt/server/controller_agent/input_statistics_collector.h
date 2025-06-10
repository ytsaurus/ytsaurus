#pragma once

#include <yt/yt/ytlib/controller_agent/persistence.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/core/phoenix/type_decl.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

struct TInputStatistics
{
    i64 ChunkCount = 0;
    i64 RowCount = 0;
    i64 ValueCount = 0;
    i64 DataWeight = 0;
    i64 CompressedDataSize = 0;
    i64 UncompressedDataSize = 0;

    i64 PrimaryDataWeight = 0;
    i64 ForeignDataWeight = 0;

    i64 PrimaryCompressedDataSize = 0;
    i64 ForeignCompressedDataSize = 0;

    // Ratio CompressedDataSize / DataWeight for input data.
    double CompressionRatio = 0.0;

    // Ratio TotalDataWeight / UncompressedDataSize for input data.
    double DataWeightRatio = 0.0;

    bool operator==(const TInputStatistics& other) const noexcept = default;

    PHOENIX_DECLARE_TYPE(TInputStatistics, 0xba61a842);
};

class TInputStatisticsCollector
{
public:
    void AddChunk(const NChunkClient::TInputChunkPtr& inputChunk, bool isPrimary) noexcept;
    void AddChunk(const NChunkClient::TLegacyDataSlicePtr& dataSlice, bool isPrimary) noexcept;

    TInputStatistics Finish() && noexcept;

private:
    TInputStatistics Statistics_;

    // Used to calculate compression ratio.
    i64 TotalInputDataWeight_ = 0;
};

void FormatValue(TStringBuilderBase* builder, const TInputStatistics& statistics, TStringBuf spec);
void Serialize(const TInputStatistics& statistics, NYTree::TFluentMap& fluent);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
