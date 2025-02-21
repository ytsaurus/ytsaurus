#include "query_common.h"

#include <yt/yt/library/query/proto/query.pb.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/wire_protocol.h>

namespace NYT::NQueryClient {

using namespace NTableClient;

using NYT::ToProto;
using NYT::FromProto;

using NCodegen::EExecutionBackend;

////////////////////////////////////////////////////////////////////////////////

const char* GetUnaryOpcodeLexeme(EUnaryOp opcode)
{
    switch (opcode) {
        case EUnaryOp::Plus:  return "+";
        case EUnaryOp::Minus: return "-";
        case EUnaryOp::Not:   return "NOT";
        case EUnaryOp::BitNot:return "~";
        default:              YT_ABORT();
    }
}

const char* GetBinaryOpcodeLexeme(EBinaryOp opcode)
{
    switch (opcode) {
        case EBinaryOp::Plus:           return "+";
        case EBinaryOp::Minus:          return "-";
        case EBinaryOp::Multiply:       return "*";
        case EBinaryOp::Divide:         return "/";
        case EBinaryOp::Modulo:         return "%";
        case EBinaryOp::LeftShift:      return "<<";
        case EBinaryOp::RightShift:     return ">>";
        case EBinaryOp::BitAnd:         return "&";
        case EBinaryOp::BitOr:          return "|";
        case EBinaryOp::And:            return "AND";
        case EBinaryOp::Or:             return "OR";
        case EBinaryOp::Equal:          return "=";
        case EBinaryOp::NotEqual:       return "!=";
        case EBinaryOp::Less:           return "<";
        case EBinaryOp::LessOrEqual:    return "<=";
        case EBinaryOp::Greater:        return ">";
        case EBinaryOp::GreaterOrEqual: return ">=";
        case EBinaryOp::Concatenate:    return "||";
        default:                        YT_ABORT();
    }
}

const char* GetStringMatchOpcodeLexeme(EStringMatchOp opcode)
{
    switch (opcode) {
        case EStringMatchOp::Like:                return "LIKE";
        case EStringMatchOp::CaseInsensitiveLike: return "ILIKE";
        case EStringMatchOp::Regex:               return "REGEX";
        default:                                  YT_ABORT();
    }
}

EBinaryOp GetReversedBinaryOpcode(EBinaryOp opcode)
{
    switch (opcode) {
        case EBinaryOp::Less:           return EBinaryOp::Greater;
        case EBinaryOp::LessOrEqual:    return EBinaryOp::GreaterOrEqual;
        case EBinaryOp::Greater:        return EBinaryOp::Less;
        case EBinaryOp::GreaterOrEqual: return EBinaryOp::LessOrEqual;
        default:                        return opcode;
    }
}

EBinaryOp GetInversedBinaryOpcode(EBinaryOp opcode)
{
    switch (opcode) {
        case EBinaryOp::Equal:          return EBinaryOp::NotEqual;
        case EBinaryOp::NotEqual:       return EBinaryOp::Equal;
        case EBinaryOp::Less:           return EBinaryOp::GreaterOrEqual;
        case EBinaryOp::LessOrEqual:    return EBinaryOp::Greater;
        case EBinaryOp::Greater:        return EBinaryOp::LessOrEqual;
        case EBinaryOp::GreaterOrEqual: return EBinaryOp::Less;
        default:                        YT_ABORT();
    }
}

bool IsArithmeticalBinaryOp(EBinaryOp opcode)
{
    switch (opcode) {
        case EBinaryOp::Plus:
        case EBinaryOp::Minus:
        case EBinaryOp::Multiply:
        case EBinaryOp::Divide:
            return true;
        default:
            return false;
    }
}

bool IsIntegralBinaryOp(EBinaryOp opcode)
{
    switch (opcode) {
        case EBinaryOp::Modulo:
        case EBinaryOp::LeftShift:
        case EBinaryOp::RightShift:
        case EBinaryOp::BitOr:
        case EBinaryOp::BitAnd:
            return true;
        default:
            return false;
    }
}

bool IsLogicalBinaryOp(EBinaryOp opcode)
{
    switch (opcode) {
        case EBinaryOp::And:
        case EBinaryOp::Or:
            return true;
        default:
            return false;
    }
}

bool IsRelationalBinaryOp(EBinaryOp opcode)
{
    switch (opcode) {
        case EBinaryOp::Equal:
        case EBinaryOp::NotEqual:
        case EBinaryOp::Less:
        case EBinaryOp::LessOrEqual:
        case EBinaryOp::Greater:
        case EBinaryOp::GreaterOrEqual:
            return true;
        default:
            return false;
    }
}

bool IsStringBinaryOp(EBinaryOp opcode)
{
    switch (opcode) {
        case EBinaryOp::Concatenate:
            return true;
        default:
            return false;
    }
}

TValue CastValueWithCheck(TValue value, EValueType targetType)
{
    if (value.Type == targetType || value.Type == EValueType::Null) {
        return value;
    }

    if (value.Type == EValueType::Int64) {
        if (targetType == EValueType::Double) {
            auto int64Value = value.Data.Int64;
            if (i64(double(int64Value)) != int64Value) {
                THROW_ERROR_EXCEPTION("Failed to cast %v to double: inaccurate conversion", int64Value);
            }
            value.Data.Double = int64Value;
        } else {
            YT_VERIFY(targetType == EValueType::Uint64);
        }
    } else if (value.Type == EValueType::Uint64) {
        if (targetType == EValueType::Int64) {
            if (value.Data.Uint64 > std::numeric_limits<i64>::max()) {
                THROW_ERROR_EXCEPTION(
                    "Failed to cast %vu to int64: value is greater than maximum", value.Data.Uint64);
            }
        } else if (targetType == EValueType::Double) {
            auto uint64Value = value.Data.Uint64;
            if (ui64(double(uint64Value)) != uint64Value) {
                THROW_ERROR_EXCEPTION("Failed to cast %vu to double: inaccurate conversion", uint64Value);
            }
            value.Data.Double = uint64Value;
        } else {
            YT_ABORT();
        }
    } else if (value.Type == EValueType::Double) {
        auto doubleValue = value.Data.Double;
        if (targetType == EValueType::Uint64) {
            if (double(ui64(doubleValue)) != doubleValue) {
                THROW_ERROR_EXCEPTION("Failed to cast %v to uint64: inaccurate conversion", doubleValue);
            }
            value.Data.Uint64 = doubleValue;
        } else if (targetType == EValueType::Int64) {
            if (double(i64(doubleValue)) != doubleValue) {
                THROW_ERROR_EXCEPTION("Failed to cast %v to int64: inaccurate conversion", doubleValue);
            }
            value.Data.Int64 = doubleValue;
        } else {
            YT_ABORT();
        }
    } else {
        YT_ABORT();
    }

    value.Type = targetType;
    return value;
}

////////////////////////////////////////////////////////////////////////////////

TFeatureFlags MostFreshFeatureFlags()
{
    return {
        .WithTotalsFinalizesAggregatedOnCoordinator = true,
        .GroupByWithLimitIsUnordered = true,
    };
}

TFeatureFlags MostArchaicFeatureFlags()
{
    return {
        .WithTotalsFinalizesAggregatedOnCoordinator = false,
        .GroupByWithLimitIsUnordered = false,
    };
}

TString ToString(const TFeatureFlags& featureFlags)
{
    return Format(
        "{WithTotalsFinalizesAggregatedOnCoordinator: %v, GroupByWithLimitIsUnordered: %v}",
        featureFlags.WithTotalsFinalizesAggregatedOnCoordinator,
        featureFlags.GroupByWithLimitIsUnordered);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TDataSource* serialized,
    const TDataSource& original,
    TRange<TLogicalTypePtr> schema,
    bool lookupSupported,
    size_t keyWidth)
{
    ToProto(serialized->mutable_object_id(), original.ObjectId);
    ToProto(serialized->mutable_cell_id(), original.CellId);
    serialized->set_mount_revision(ToProto(original.MountRevision));

    auto rangesWriter = CreateWireProtocolWriter();
    for (const auto& range : original.Ranges) {
        rangesWriter->WriteUnversionedRow(range.first);
        rangesWriter->WriteUnversionedRow(range.second);
    }
    ToProto(serialized->mutable_ranges(), MergeRefsToString(rangesWriter->Finish()));

    if (original.Keys) {
        std::vector<TColumnSchema> columns;
        for (auto type : schema) {
            columns.emplace_back("", type);
        }

        TTableSchema schema(columns);
        auto keysWriter = CreateWireProtocolWriter();
        keysWriter->WriteTableSchema(schema);
        keysWriter->WriteSchemafulRowset(original.Keys);
        ToProto(serialized->mutable_keys(), MergeRefsToString(keysWriter->Finish()));
    }

    // COMPAT(lukyan)
    serialized->set_lookup_supported(lookupSupported);
    serialized->set_key_width(keyWidth);
}

void FromProto(
    TDataSource* original,
    const NProto::TDataSource& serialized,
    const IMemoryChunkProviderPtr& memoryChunkProvider)
{
    FromProto(&original->ObjectId, serialized.object_id());
    FromProto(&original->CellId, serialized.cell_id());
    FromProto(&original->MountRevision, serialized.mount_revision());

    struct TDataSourceBufferTag
    { };

    TRowRanges ranges;
    auto rowBuffer = New<TRowBuffer>(TDataSourceBufferTag(), memoryChunkProvider);
    auto rangesReader = CreateWireProtocolReader(
        TSharedRef::FromString<TDataSourceBufferTag>(serialized.ranges()),
        rowBuffer);
    while (!rangesReader->IsFinished()) {
        auto lowerBound = rangesReader->ReadUnversionedRow(true);
        auto upperBound = rangesReader->ReadUnversionedRow(true);
        ranges.emplace_back(lowerBound, upperBound);
    }
    original->Ranges = MakeSharedRange(std::move(ranges), rowBuffer);

    if (serialized.has_keys()) {
        auto keysReader = CreateWireProtocolReader(
            TSharedRef::FromString<TDataSourceBufferTag>(serialized.keys()),
            rowBuffer);

        auto schema = keysReader->ReadTableSchema();
        auto schemaData = keysReader->GetSchemaData(schema, NTableClient::TColumnFilter());
        original->Keys = keysReader->ReadSchemafulRowset(schemaData, true);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TFeatureFlags* serialized, const TFeatureFlags& original)
{
    serialized->set_with_totals_finalizes_aggregated_on_coordinator(original.WithTotalsFinalizesAggregatedOnCoordinator);
    serialized->set_group_by_with_limit_is_unordered(original.GroupByWithLimitIsUnordered);
}

void FromProto(TFeatureFlags* original, const NProto::TFeatureFlags& serialized)
{
    original->WithTotalsFinalizesAggregatedOnCoordinator = serialized.with_totals_finalizes_aggregated_on_coordinator();
    if (serialized.has_group_by_with_limit_is_unordered()) {
        original->GroupByWithLimitIsUnordered = serialized.group_by_with_limit_is_unordered();
    }
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TQueryOptions* serialized, const TQueryOptions& original)
{
    serialized->set_timestamp(original.TimestampRange.Timestamp);
    serialized->set_retention_timestamp(original.TimestampRange.RetentionTimestamp);
    serialized->set_verbose_logging(original.VerboseLogging);
    serialized->set_new_range_inference(original.NewRangeInference);
    serialized->set_use_canonical_null_relations(original.UseCanonicalNullRelations);
    serialized->set_execution_backend(ToProto(original.ExecutionBackend));
    serialized->set_max_subqueries(original.MaxSubqueries);
    serialized->set_enable_code_cache(original.EnableCodeCache);
    ToProto(serialized->mutable_workload_descriptor(), original.WorkloadDescriptor);
    serialized->set_allow_full_scan(original.AllowFullScan);
    ToProto(serialized->mutable_read_session_id(), original.ReadSessionId);
    serialized->set_deadline(ToProto(original.Deadline));
    serialized->set_memory_limit_per_node(original.MemoryLimitPerNode);
    if (original.ExecutionPool) {
        serialized->set_execution_pool(*original.ExecutionPool);
    }
    serialized->set_suppress_access_tracking(original.SuppressAccessTracking);
    serialized->set_range_expansion_limit(original.RangeExpansionLimit);
    serialized->set_merge_versioned_rows(original.MergeVersionedRows);
    if (original.UseLookupCache) {
        serialized->set_use_lookup_cache(*original.UseLookupCache);
    }
    serialized->set_min_row_count_per_subquery(original.MinRowCountPerSubquery);
    serialized->set_allow_unordered_group_by_with_limit(original.AllowUnorderedGroupByWithLimit);
}

void FromProto(TQueryOptions* original, const NProto::TQueryOptions& serialized)
{
    original->TimestampRange.Timestamp = serialized.timestamp();
    original->TimestampRange.RetentionTimestamp = serialized.retention_timestamp();
    original->VerboseLogging = serialized.verbose_logging();
    original->MaxSubqueries = serialized.max_subqueries();
    original->EnableCodeCache = serialized.enable_code_cache();
    original->WorkloadDescriptor = serialized.has_workload_descriptor()
        ? FromProto<TWorkloadDescriptor>(serialized.workload_descriptor())
        : TWorkloadDescriptor();
    original->AllowFullScan = serialized.allow_full_scan();
    original->ReadSessionId  = serialized.has_read_session_id()
        ? FromProto<TReadSessionId>(serialized.read_session_id())
        : TReadSessionId::Create();

    if (serialized.has_memory_limit_per_node()) {
        original->MemoryLimitPerNode = serialized.memory_limit_per_node();
    }

    if (serialized.has_execution_pool()) {
        original->ExecutionPool = serialized.execution_pool();
    }

    original->Deadline = serialized.has_deadline()
        ? FromProto<TInstant>(serialized.deadline())
        : TInstant::Max();

    if (serialized.has_suppress_access_tracking()) {
        original->SuppressAccessTracking = serialized.suppress_access_tracking();
    }

    if (serialized.has_range_expansion_limit()) {
        original->RangeExpansionLimit = serialized.range_expansion_limit();
    }
    if (serialized.has_new_range_inference()) {
        original->NewRangeInference = serialized.new_range_inference();
    }
    if (serialized.has_use_canonical_null_relations()) {
        original->UseCanonicalNullRelations = serialized.use_canonical_null_relations();
    }
    if (serialized.has_merge_versioned_rows()) {
        original->MergeVersionedRows = serialized.merge_versioned_rows();
    }
    if (serialized.has_execution_backend()) {
        original->ExecutionBackend = static_cast<EExecutionBackend>(serialized.execution_backend());
    }
    if (serialized.has_use_lookup_cache()) {
        original->UseLookupCache = serialized.use_lookup_cache();
    }
    if (serialized.has_min_row_count_per_subquery()) {
        original->MinRowCountPerSubquery = serialized.min_row_count_per_subquery();
    }
    if (serialized.has_allow_unordered_group_by_with_limit()) {
        original->AllowUnorderedGroupByWithLimit = serialized.allow_unordered_group_by_with_limit();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
