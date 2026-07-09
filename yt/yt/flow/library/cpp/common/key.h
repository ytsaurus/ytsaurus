#pragma once

#include "public.h"

#include <yt/yt/library/query/engine_api/public.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/logging/public.h>

#include <yt/yt/core/yson/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class... Ts>
TKey MakeKey(Ts&&... values);

TKey MinKey();
TKey MaxKey();

////////////////////////////////////////////////////////////////////////////////

// Lower is inclusive, upper is exclusive.
struct TKeyRange
{
    TKey Lower;
    TKey Upper;

    bool Contains(const TKey& key) const;
};

bool operator==(const TKeyRange& left, const TKeyRange& right);
bool operator<(const TKeyRange& left, const TKeyRange& right);

void FormatValue(TStringBuilderBase* builder, const TKeyRange& keyRange, TStringBuf /*spec*/);

////////////////////////////////////////////////////////////////////////////////

TKeyRange UniversalKeyRange();

bool TestRangeOverlaps(std::vector<TKeyRange> ranges);
std::vector<TKeyRange> UniteRanges(std::vector<TKeyRange> ranges);
std::vector<TKeyRange> SubtractRanges(std::vector<TKeyRange> minuend, std::vector<TKeyRange> subtrahend);

////////////////////////////////////////////////////////////////////////////////

// optional is used to mark 1<<64
// 0  =>  EValueType::Min
// 1 <= n < 1<<64  =>  EValueType::Uint64
// {}  =>  EValueType::Max
TKey MakeUintKey(std::optional<ui64> key);
std::optional<ui64> ExtractUintFromKey(const TKey& key);

//! Like ExtractUintFromKey but tolerates multi-column keys: looks at the
//! first column only. Returns nullopt for `EValueType::Max`, `0` for
//! `EValueType::Min`, the value for `EValueType::Uint64`, and throws
//! for any other type.
std::optional<ui64> ExtractFirstUintFromKey(const TKey& key);

TKeyRange MakeUintKeyRange(std::optional<ui64> lower, std::optional<ui64> upper);
std::pair<std::optional<ui64>, std::optional<ui64>> ExtractUintsFromKeyRange(const TKeyRange& range);

std::vector<TKeyRange> SplitUintKeyRange(const TKeyRange& range, int count);

////////////////////////////////////////////////////////////////////////////////

// Splits a partition range into |bucketCount| buckets along the first
// Uint64 column; the multi-column tails of `range.Lower` / `range.Upper`
// are preserved on the outermost buckets.
// TODO(YTFLOW-500): merge into SplitUintKeyRange. Blocked on endpoint semantics:
// SplitUintKeyRange normalizes outer bounds to numeric [0u]/Max (universal_controller
// relies on the first partition lower being [0u], not MinKey), whereas this one keeps
// the original Min/Max sentinels and multi-column tails. Unifying changes partition
// ranges, so it needs its own review.
std::vector<TKeyRange> SplitPartitionRangeIntoBuckets(const TKeyRange& range, int bucketCount);

////////////////////////////////////////////////////////////////////////////////

TKey ConcatenateKeys(const TKey& key, const TKey& subKey);

////////////////////////////////////////////////////////////////////////////////

//! Build a TKey from user-supplied YSON. Two accepted forms:
//!   * positional list ``[v0, v1, ...]`` — values laid out in schema column order, including
//!     expression columns; the caller has already computed the full key.
//!   * named map ``{col: value, ...}`` — only non-expression columns; missing expression
//!     columns are filled by running the column evaluator over the row.
//! Throws if the YSON is neither, if a non-expression column is missing in the map form, or if
//! a column name does not exist in the schema. When |ignoreUnknownColumns| is set, map-form
//! entries whose names are not in the schema are silently dropped (still throws on the list form
//! if its length disagrees with the schema).
TKey BuildKeyFromYson(
    const NYson::TYsonString& yson,
    const NTableClient::TTableSchemaPtr& schema,
    const NQueryClient::IColumnEvaluatorCachePtr& evaluatorCache,
    const NLogging::TLogger& logger,
    bool ignoreUnknownColumns = false);

//! Re-layout a TKey from |sourceSchema| to |targetSchema|. Columns are matched by name;
//! the underlying conversion is the same as #ConvertPayloadToNewSchema().
TKey ConvertKeyToSchema(
    const TKey& key,
    const NTableClient::TTableSchemaPtr& sourceSchema,
    const NTableClient::TTableSchemaPtr& targetSchema,
    const IPayloadConverterCachePtr& converterCache);

//! Throws if |key| does not match |schema|.
void ValidateKey(
    const TKey& key,
    const NTableClient::TTableSchemaPtr& schema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

//! A hasher for TKey.
template <>
struct THash<NYT::NFlow::TKey>
{
    inline size_t operator()(const NYT::NFlow::TKey& key) const
    {
        return NYT::NTableClient::TDefaultUnversionedRowHash()(key.Underlying().Get());
    }
};

#define KEY_INL_H_
#include "key-inl.h"
#undef KEY_INL_H_
