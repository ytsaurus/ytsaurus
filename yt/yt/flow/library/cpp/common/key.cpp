#include "key.h"

#include "payload_converter.h"
#include "payload_validation.h"
#include "schema.h"

#include <yt/yt/library/formats/yson_map_to_unversioned_value.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/value_consumer.h>

#include <yt/yt/core/yson/parser.h>
#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/node.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TKey MinKey()
{
    static const TKey minKey = TKey(TKey::TUnderlying(NTableClient::MinKey()));
    return minKey;
}

TKey MaxKey()
{
    static const TKey maxKey = TKey(TKey::TUnderlying(NTableClient::MaxKey()));
    return maxKey;
}

////////////////////////////////////////////////////////////////////////////////

bool TKeyRange::Contains(const TKey& key) const
{
    return key >= Lower && key < Upper;
}

bool operator==(const TKeyRange& left, const TKeyRange& right)
{
    return left.Lower == right.Lower && left.Upper == right.Upper;
}

bool operator<(const TKeyRange& left, const TKeyRange& right)
{
    return std::tie(left.Lower, left.Upper) < std::tie(right.Lower, right.Upper);
}

void FormatValue(TStringBuilderBase* builder, const TKeyRange& keyRange, TStringBuf /*spec*/)
{
    builder->AppendFormat("[%v; %v)",
        keyRange.Lower,
        keyRange.Upper);
}

////////////////////////////////////////////////////////////////////////////////

TKeyRange UniversalKeyRange()
{
    return {MinKey(), MaxKey()};
};

bool TestRangeOverlaps(std::vector<TKeyRange> ranges)
{
    if (ranges.empty()) {
        return {};
    }
    Sort(ranges);
    for (int i = 1; i < std::ssize(ranges); ++i) {
        const auto& prev = ranges[i - 1];
        const auto& current = ranges[i];
        if (prev.Upper > current.Lower) {
            return true;
        }
    }
    return false;
}

std::vector<TKeyRange> UniteRanges(std::vector<TKeyRange> ranges)
{
    std::erase_if(ranges, [] (const TKeyRange& range) {
        return range.Lower == range.Upper;
    });
    if (ranges.empty()) {
        return {};
    }
    Sort(ranges);
    std::vector<TKeyRange> result;
    TKeyRange current = ranges[0];
    for (int i = 1; i < std::ssize(ranges); ++i) {
        if (current.Upper >= ranges[i].Lower) {
            current.Upper = std::max(current.Upper, ranges[i].Upper);
        } else {
            result.push_back(std::move(current));
            current = ranges[i];
        }
    }
    result.push_back(std::move(current));
    return result;
}

std::vector<TKeyRange> SubtractRanges(std::vector<TKeyRange> minuend, std::vector<TKeyRange> subtrahend)
{
    minuend = UniteRanges(minuend);
    subtrahend = UniteRanges(subtrahend);

    if (subtrahend.empty()) {
        return minuend;
    }
    if (minuend.empty()) {
        return {};
    }

    std::vector<TKeyRange> result;

    int minuendIndex = 0;
    int subtrahendIndex = 0;
    std::optional<TKeyRange> current;
    while (minuendIndex < std::ssize(minuend) && subtrahendIndex < std::ssize(subtrahend)) {
        if (!current) {
            current = minuend[minuendIndex];
        }
        const auto& range = subtrahend[subtrahendIndex];
        if (range.Lower == range.Upper) {
            subtrahendIndex += 1;
            continue;
        }

        if (range.Lower >= current->Upper) {
            result.push_back(*current);
            current = {};
            minuendIndex += 1;
            continue;
        }
        if (range.Lower > current->Lower) {
            result.push_back({current->Lower, range.Lower});
        }
        if (range.Upper >= current->Upper) {
            current = {};
            minuendIndex += 1;
        } else {
            if (range.Upper > current->Lower) {
                current = TKeyRange{range.Upper, current->Upper};
            }
            subtrahendIndex += 1;
        }
    }
    if (current) {
        result.push_back(*current);
        minuendIndex += 1;
    }
    while (minuendIndex < std::ssize(minuend)) {
        result.push_back(minuend[minuendIndex]);
        minuendIndex += 1;
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TKey MakeUintKey(std::optional<ui64> key)
{
    if (!key) {
        return MaxKey();
    }
    if (*key == std::numeric_limits<ui64>::min()) {
        return MinKey();
    }
    return MakeKey(*key);
}

std::optional<ui64> ExtractUintFromKey(const TKey& key)
{
    const auto& underlying = key.Underlying();
    THROW_ERROR_EXCEPTION_UNLESS(underlying.GetCount() == 1, "Wrong key size: expected 1, actual %v", underlying.GetCount());

    if (underlying[0].Type == NTableClient::EValueType::Uint64) {
        return NTableClient::FromUnversionedValue<ui64>(underlying[0]);
    } else if (underlying[0].Type == NTableClient::EValueType::Max) {
        return {};
    } else if (underlying[0].Type == NTableClient::EValueType::Min) {
        return std::numeric_limits<ui64>::min();
    }
    THROW_ERROR_EXCEPTION("Unexpected type %Qlv of first element",
        underlying[0].Type);
}

TKeyRange MakeUintKeyRange(std::optional<ui64> lower, std::optional<ui64> upper)
{
    return {MakeUintKey(lower), MakeUintKey(upper)};
}

std::pair<std::optional<ui64>, std::optional<ui64>> ExtractUintsFromKeyRange(const TKeyRange& range)
{
    return {ExtractUintFromKey(range.Lower), ExtractUintFromKey(range.Upper)};
}

std::vector<TKeyRange> SplitUintKeyRange(const TKeyRange& range, int count)
{
    // [lower; upper)
    auto [lower, upper] = ExtractUintsFromKeyRange(range);
    // We use [minIncluded, maxIncluded] to not worry about optional.
    ui64 minIncluded = lower ? *lower : std::numeric_limits<ui64>::max();
    if (upper && *upper <= minIncluded) {
        THROW_ERROR_EXCEPTION("Cannot split empty key range");
    }
    ui64 maxIncluded = upper ? (*upper - 1) : std::numeric_limits<ui64>::max();

    YT_VERIFY(minIncluded <= maxIncluded);
    YT_VERIFY(count >= 1);

    ui64 decreasedLength = maxIncluded - minIncluded;
    ui64 baseBucketSize = decreasedLength / count;
    ui64 leftIncrements = decreasedLength % count + 1;

    std::vector<TKeyRange> ranges;
    ranges.reserve(count);

    ui64 current = minIncluded;
    for (int index = 0; index < count; ++index) {
        auto size = baseBucketSize;
        if (leftIncrements > 0) {
            size += 1;
            leftIncrements -= 1;
        }
        ui64 upper = current + size;
        ranges.push_back(MakeUintKeyRange(current, upper == 0 ? std::optional<ui64>() : upper));
        current = upper;
    }
    YT_VERIFY(current == maxIncluded + 1);
    return ranges;
}

////////////////////////////////////////////////////////////////////////////////

std::optional<ui64> ExtractFirstUintFromKey(const TKey& key)
{
    const auto& underlying = key.Underlying();
    YT_VERIFY(underlying.GetCount() >= 1);
    const auto& v = underlying[0];
    if (v.Type == NTableClient::EValueType::Uint64) {
        return v.Data.Uint64;
    } else if (v.Type == NTableClient::EValueType::Max) {
        return std::nullopt;
    } else if (v.Type == NTableClient::EValueType::Min) {
        return std::numeric_limits<ui64>::min();
    }
    THROW_ERROR_EXCEPTION("Unexpected first-column type %Qlv", v.Type);
}

std::vector<TKeyRange> SplitPartitionRangeIntoBuckets(const TKeyRange& range, int bucketCount)
{
    YT_VERIFY(bucketCount >= 1);
    if (bucketCount == 1) {
        return {range};
    }
    auto lowerUint = ExtractFirstUintFromKey(range.Lower);
    auto upperUint = ExtractFirstUintFromKey(range.Upper);
    // Degenerate first-column range: cannot split by hash. Return a single bucket
    // covering the original range so a multi-column tail is preserved verbatim.
    const ui64 effectiveLower = lowerUint.value_or(std::numeric_limits<ui64>::max());
    const ui64 effectiveUpper = upperUint.value_or(std::numeric_limits<ui64>::max());
    if (effectiveUpper <= effectiveLower) {
        return {range};
    }
    // Never make more buckets than there are distinct hash values in the range,
    // otherwise SplitUintKeyRange would produce empty buckets.
    const ui64 span = effectiveUpper - effectiveLower;
    const int effectiveBucketCount = static_cast<int>(std::min<ui64>(bucketCount, span));
    auto buckets = SplitUintKeyRange(MakeUintKeyRange(lowerUint, upperUint), effectiveBucketCount);
    buckets.front().Lower = range.Lower;
    buckets.back().Upper = range.Upper;
    return buckets;
}

////////////////////////////////////////////////////////////////////////////////

TKey ConcatenateKeys(const TKey& key, const TKey& subKey)
{
    int index = 0;
    NTableClient::TUnversionedOwningRowBuilder builder;
    for (auto value : key.Underlying()) {
        value.Id = index;
        index += 1;
        builder.AddValue(value);
    }
    for (auto value : subKey.Underlying()) {
        value.Id = index;
        index += 1;
        builder.AddValue(value);
    }
    return TKey(TKey::TUnderlying(builder.FinishRow()));
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

// Same as TBuildingValueConsumer, but flips #GetAllowUnknownColumns() to true so that
// the upstream YSON-to-row converter accepts (and skips) map entries whose names are
// not in the schema instead of throwing.
class TForgivingBuildingValueConsumer
    : public NTableClient::TBuildingValueConsumer
{
public:
    using TBuildingValueConsumer::TBuildingValueConsumer;

private:
    bool GetAllowUnknownColumns() const final
    {
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

TKey BuildKeyFromYson(
    const NYson::TYsonString& yson,
    const NTableClient::TTableSchemaPtr& schema,
    const NQueryClient::IColumnEvaluatorCachePtr& evaluatorCache,
    const NLogging::TLogger& logger,
    bool ignoreUnknownColumns)
{
    auto node = NYTree::ConvertToNode(yson);
    if (node->GetType() == NYTree::ENodeType::List) {
        return NYTree::ConvertTo<TKey>(yson);
    }
    if (node->GetType() != NYTree::ENodeType::Map) {
        THROW_ERROR_EXCEPTION("\"key\" must be a list or a map, got %Qlv", node->GetType());
    }

    auto buildingConsumer = ignoreUnknownColumns
        ? std::unique_ptr<NTableClient::TBuildingValueConsumer>(
            new TForgivingBuildingValueConsumer(schema, logger, /*convertNullToEntity*/ false))
        : std::make_unique<NTableClient::TBuildingValueConsumer>(
            schema,
            logger,
            /*convertNullToEntity*/ false);
    buildingConsumer->SetTreatMissingAsNull(true);
    buildingConsumer->SetAllowMissingKeyColumns(true);

    NComplexTypes::TYsonConverterConfig converterConfig;
    NFormats::TYsonMapToUnversionedValueConverter mapConverter(converterConfig, buildingConsumer.get());

    NTableClient::IValueConsumer* consumer = buildingConsumer.get();
    consumer->OnBeginRow();
    NYson::ParseYsonStringBuffer(yson.AsStringBuf(), NYson::EYsonType::Node, &mapConverter);
    consumer->OnEndRow();

    auto rows = buildingConsumer->GetRows();
    YT_VERIFY(rows.size() == 1);

    // Re-lay out into schema order; the consumer's row only has user-provided columns.
    std::vector<std::optional<NTableClient::TUnversionedValue>> bySchemaId(schema->GetColumnCount());
    for (const auto& v : rows[0]) {
        bySchemaId[v.Id] = v;
    }
    for (int i = 0; i < schema->GetColumnCount(); ++i) {
        if (!bySchemaId[i] && !schema->Columns()[i].Expression()) {
            THROW_ERROR_EXCEPTION("Missing column %Qv in key", schema->Columns()[i].Name());
        }
    }

    NTableClient::TUnversionedRowBuilder builder(schema->GetColumnCount());
    for (int i = 0; i < schema->GetColumnCount(); ++i) {
        if (bySchemaId[i]) {
            builder.AddValue(*bySchemaId[i]);
        } else {
            builder.AddValue(NTableClient::MakeUnversionedSentinelValue(NTableClient::EValueType::Null, i));
        }
    }
    auto row = builder.GetRow();

    auto buffer = New<NTableClient::TRowBuffer>();
    auto evaluator = evaluatorCache->Find(schema);
    YT_VERIFY(evaluator);
    evaluator->EvaluateKeys(row, buffer, /*preserveColumnsIds*/ false);

    return TKey(TKey::TUnderlying(row));
}

void ValidateKey(
    const TKey& key,
    const NTableClient::TTableSchemaPtr& schema)
{
    auto error = DoValidatePayload(
        /*rowName*/ "key",
        key,
        /*schemaName*/ "key_schema",
        schema,
        {.ValidateValues = false});
    if (!error.IsOK()) {
        THROW_ERROR_EXCEPTION("Key does not match key schema")
            << TErrorAttribute("key", key)
            << error;
    }
}

////////////////////////////////////////////////////////////////////////////////

TKey ConvertKeyToSchema(
    const TKey& key,
    const NTableClient::TTableSchemaPtr& sourceSchema,
    const NTableClient::TTableSchemaPtr& targetSchema,
    const IPayloadConverterCachePtr& converterCache)
{
    if (sourceSchema == targetSchema) {
        return key;
    }
    return converterCache->Convert(key, sourceSchema, targetSchema);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
