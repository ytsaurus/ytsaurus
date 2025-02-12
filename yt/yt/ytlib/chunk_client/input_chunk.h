#pragma once

#include "public.h"
#include "chunk_spec.h"
#include "data_source.h"

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>
#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_spec.pb.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>
#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/library/erasure/public.h>

#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/phoenix/type_decl.h>

#include <array>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

//! Compact representation of some fields from NProto::TChunkSpec.
//! Used inside scheduler to reduce memory footprint.
//! The content of TInputChunkBase is stored in a scheduler snapshot as a POD.
class TInputChunkBase
{
public:
    // TODO(babenko): this could also be a store id.
    DEFINE_BYVAL_RW_PROPERTY(TChunkId, ChunkId);

    // TODO(babenko): store replicas with media.
    using TInputChunkReplicas = std::array<TChunkReplica, MaxInputChunkReplicaCount>;
    DEFINE_BYREF_RO_PROPERTY(TInputChunkReplicas, Replicas);

    DEFINE_BYVAL_RW_PROPERTY(int, TableIndex, -1);
    DEFINE_BYVAL_RO_PROPERTY(NErasure::ECodec, ErasureCodec, NErasure::ECodec::None);
    DEFINE_BYVAL_RW_PROPERTY(i64, TableRowIndex);
    DEFINE_BYVAL_RW_PROPERTY(int, RangeIndex, 0);
    DEFINE_BYVAL_RO_PROPERTY(EChunkFormat, ChunkFormat);
    DEFINE_BYVAL_RW_PROPERTY(i64, ChunkIndex, -1);
    DEFINE_BYVAL_RW_PROPERTY(i64, TabletIndex, -1);
    DEFINE_BYVAL_RW_PROPERTY(NTabletClient::TTabletId, TabletId);
    // TODO(babenko): See YT-14339: Add CellId and pass it around.
    // NB: This will require controller agent snapshot version promotion!
    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, OverrideTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, MaxClipTimestamp);

    DEFINE_BYVAL_RW_PROPERTY(i64, TotalUncompressedDataSize, -1);
    DEFINE_BYVAL_RW_PROPERTY(i64, TotalRowCount, -1);
    DEFINE_BYVAL_RW_PROPERTY(i64, CompressedDataSize, -1); // for TSortControllerBase
    DEFINE_BYVAL_RW_PROPERTY(i64, TotalDataWeight, -1);
    DEFINE_BYVAL_RO_PROPERTY(i64, MaxBlockSize, -1); // for TChunkStripeStatistics
    // NB(psushin): this property is used to distinguish very sparse tables.
    DEFINE_BYVAL_RW_PROPERTY(int, ValuesPerRow, 1);

    DEFINE_BYVAL_RO_PROPERTY(bool, UniqueKeys, false); // for TChunkStripeStatistics

    //! Factor providing a ratio of selected columns data weight to the total data weight.
    //! It is used to propagate the reduction of a chunk total weight to chunk and data slices
    //! that are formed from it.
    DEFINE_BYVAL_RW_PROPERTY(double, ColumnSelectivityFactor, 1.0);

    DEFINE_BYVAL_RO_PROPERTY(bool, StripedErasure, false);

    //! Factor providing the ratio of the sum of sizes of chunk blocks to be read
    //! to the sum of sizes of all blocks.
    DEFINE_BYVAL_RW_PROPERTY(double, ReadSizeSelectivityFactor, 1.0);

public:
    TInputChunkBase() = default;
    TInputChunkBase(TInputChunkBase&& other) = default;
    explicit TInputChunkBase(const NProto::TChunkSpec& chunkSpec);

    // TODO(babenko): this currently always returns GenericMediumIndex.
    TChunkReplicaWithMediumList GetReplicaList() const;
    // TODO(babenko): this currently just drops medium indices.
    void SetReplicaList(const TChunkReplicaWithMediumList& replicas);

    bool IsDynamicStore() const;
    bool IsSortedDynamicStore() const;
    bool IsOrderedDynamicStore() const;
    bool IsFile() const;
    bool IsHunk() const;

private:
    void CheckOffsets();
};

////////////////////////////////////////////////////////////////////////////////

//! Compact representation of NProto::TChunkSpec.
//! Used inside scheduler to reduce memory footprint.
class TInputChunk
    : public TRefCounted
    , public TInputChunkBase
{
public:
    // Here are read limits. They are not read-only because of chunk pool unittests.
    using TReadLimitHolder = std::unique_ptr<TLegacyReadLimit>;
    DEFINE_BYREF_RW_PROPERTY(TReadLimitHolder, LowerLimit);
    DEFINE_BYREF_RW_PROPERTY(TReadLimitHolder, UpperLimit);

    // Here are boundary keys. They are not read-only because of chunk pool unittests.
    using TInputChunkBoundaryKeys = std::unique_ptr<NTableClient::TOwningBoundaryKeys>;
    DEFINE_BYREF_RW_PROPERTY(TInputChunkBoundaryKeys, BoundaryKeys);

    // These fields are not used directly by scheduler.
    using TInputChunkPartitionsExt = std::unique_ptr<NTableClient::NProto::TPartitionsExt>;
    DEFINE_BYREF_RO_PROPERTY(TInputChunkPartitionsExt, PartitionsExt);

    using TInputChunkHeavyColumnarStatisticsExt = std::unique_ptr<NTableClient::NProto::THeavyColumnStatisticsExt>;
    DEFINE_BYREF_RO_PROPERTY(TInputChunkHeavyColumnarStatisticsExt, HeavyColumnarStatisticsExt);

    using TInputChunkHunkChunkRefsExt = std::unique_ptr<NTableClient::NProto::THunkChunkRefsExt>;
    DEFINE_BYREF_RO_PROPERTY(TInputChunkHunkChunkRefsExt, HunkChunkRefsExt);

    //! Factor providing a ratio of data weight inferred from limits to the total data weight.
    //! It is used to propagate the reduction of a chunk total weight to chunk and data slices that are formed from it.
    //! NB: Setting this factor overrides generic logic which computes a row selectivity factor based on a limit-inferred row count.
    //! NB: This factor overrides the value of the column selectivity factor.
    DEFINE_BYVAL_RW_PROPERTY(std::optional<double>, BlockSelectivityFactor);

public:
    TInputChunk() = default;
    TInputChunk(TInputChunk&& other) = default;
    explicit TInputChunk(
        const NProto::TChunkSpec& chunkSpec,
        std::optional<int> keyColumnCount = std::nullopt);

    size_t SpaceUsed() const;

    //! Returns |false| iff the chunk has nontrivial limits.
    bool IsCompleteChunk() const;
    //! Returns |true| iff the chunk is complete and is large enough.
    bool IsLargeCompleteChunk(i64 desiredChunkSize) const;

    //! Releases memory occupied by BoundaryKeys.
    void ReleaseBoundaryKeys();
    //! Releases memory occupied by PartitionsExt.
    void ReleasePartitionsExt();
    //! Releases memory occupied by HeavyColumnarStatisticsExt.
    void ReleaseHeavyColumnarStatisticsExt();

    //! Returns the approximate row count based on the row indexes specified in lower/upper limit.
    i64 GetRowCount() const;

    //! Returns the approximate data weight based on the exact total chunk data weight, the row indexes specified
    //! in lower/upper limits and the input chunks selectivity factors (see next paragraph).
    //! Columnar factors are considered for all formats, i.e. we approximate the logical data weight bounded by all selectors.
    //!
    //! The data weight reduction logic is performed as follows:
    //!   1. If `BlockSelectivityFactor` is set, we assume that it single-handedly accounts for the data weight that will be read.
    //!   2. Otherwise, we compute the row selectivity factor as the proportion of rows bounded by limits out of all the rows in the chunk
    //!      and use it alongisde `ColumnSelectivityFactor`.
    i64 GetDataWeight() const;
    //! Same as above, but for uncompressed data size.
    //! Columnar factors are only considered for columnar formats, i.e. we approximate the uncompressed size of data that is needed to be read from disk.
    i64 GetUncompressedDataSize() const;
    //! Same as above, but for uncompressed data size.
    //! Columnar factors are only considered for columnar formats, i.e. we approximate the compressed data size that is needed to be read from disk.
    i64 GetCompressedDataSize() const;

    //! Returns the combined selectivity factor, which is used to propogate reductions based on external knowledge about the chunk slice referencing this input chunk.
    //! The combined selectivity factor is equal to `BlockSelectivityFactor`, if it is set, and to `ColumnSelectivityFactor` otherwise.
    double GetDataWeightSelectivityFactor() const;

    friend void ToProto(NProto::TChunkSpec* chunkSpec, const TInputChunkPtr& inputChunk);

private:
    i64 ApplySelectivityFactors(i64 dataSize, bool applyReadSizeSelectivityFactors) const;

    PHOENIX_DECLARE_TYPE(TInputChunk, 0x7502ed18);
};

DEFINE_REFCOUNTED_TYPE(TInputChunk)

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TChunkSpec* chunkSpec, const TInputChunkPtr& inputChunk);
void FromProto(TInputChunkPtr* inputChunk, const NProto::TChunkSpec& chunkSpec);
void FormatValue(TStringBuilderBase* builder, const TInputChunkPtr& inputChunk, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

bool IsUnavailable(const TInputChunkPtr& inputChunk, EChunkAvailabilityPolicy policy);

TChunkId EncodeChunkId(const TInputChunkPtr& inputChunk, NNodeTrackerClient::TNodeId nodeId);

////////////////////////////////////////////////////////////////////////////////

class TWeightedInputChunk
    : public TRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(i64, DataWeight);
    DEFINE_BYVAL_RW_PROPERTY(TInputChunkPtr, InputChunk);

public:
    TWeightedInputChunk() = default;
    TWeightedInputChunk(TWeightedInputChunk&& other) = default;

    TWeightedInputChunk(
        TInputChunkPtr inputChunk,
        i64 dataWeight);
};

DEFINE_REFCOUNTED_TYPE(TWeightedInputChunk)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

Y_DECLARE_PODTYPE(NYT::NChunkClient::TInputChunkBase);
