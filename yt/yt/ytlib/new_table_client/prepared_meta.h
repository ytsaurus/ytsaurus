#pragma once

#include "public.h"

#include <yt/yt/client/table_client/row_base.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NNewTableClient {

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE bool IsDirect(int type);

Y_FORCE_INLINE bool IsDense(int type);


struct TMetaBase
{
    ui64 DataOffset;
    // RowCount can be evaluated from ChunkRowCount of adjacent segments.
    ui32 RowCount;
    ui32 ChunkRowCount;
    ui8 Type;

    void Init(const NProto::TSegmentMeta& meta);
};

struct TTimestampMeta
    : public TMetaBase
{
    ui64 BaseTimestamp;
    ui32 ExpectedDeletesPerRow;
    ui32 ExpectedWritesPerRow;

    void Init(const NProto::TSegmentMeta& meta);
};

struct TIntegerMeta
    : public TMetaBase
{
    ui64 BaseValue;

    void Init(const NProto::TSegmentMeta& meta);
};

struct TBlobMeta
    : public TMetaBase
{
    ui32 ExpectedLength;

    void Init(const NProto::TSegmentMeta& meta);
};

template <EValueType Type>
struct TMeta;

template <>
struct TMeta<EValueType::Int64>
    : public TIntegerMeta
{ };

template <>
struct TMeta<EValueType::Uint64>
    : public TIntegerMeta
{ };

template <>
struct TMeta<EValueType::Boolean>
    : public TMetaBase
{ };

template <>
struct TMeta<EValueType::Double>
    : public TMetaBase
{ };

template <>
struct TMeta<EValueType::String>
    : public TBlobMeta
{ };

template <>
struct TMeta<EValueType::Any>
    : public TBlobMeta
{ };

template <>
struct TMeta<EValueType::Composite>
    : public TBlobMeta
{ };

struct TDenseMeta
{
    ui32 ExpectedPerRow;

    void Init(const NProto::TSegmentMeta& meta);
};

template <EValueType Type>
struct TValueMeta
    : public TMeta<Type>
    , public TDenseMeta
{
    void Init(const NProto::TSegmentMeta& meta);
};

template <EValueType Type>
struct TKeyMeta
    : public TMeta<Type>
{
    void Init(const NProto::TSegmentMeta& meta);
};

struct TPreparedChunkMeta final
{
    struct TColumnGroup
    {
        TColumnGroup() = default;
        TColumnGroup(const TColumnGroup&) = delete;
        TColumnGroup(TColumnGroup&&) = default;

        std::vector<ui32> BlockIds;
        std::vector<ui16> ColumnIds;
        // Per block segment metas for each column in group.
        // Contains mapping from column index in group to offsets and serialized segment metas.
        std::vector<TSharedRef> MergedMetas;
    };

    std::vector<TColumnGroup> ColumnGroups;
    std::vector<ui16> ColumnIdToGroupId;
    std::vector<ui16> ColumnIndexInGroup;

    size_t Prepare(
        const NTableClient::TTableSchemaPtr& chunkSchema,
        const NTableClient::TRefCountedColumnMetaPtr& columnMetas);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient

#define PREPARED_META_INL_H_
#include "prepared_meta-inl.h"
#undef PREPARED_META_INL_H_
