#pragma once

#include "public.h"

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/transaction_client/helpers.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/library/codegen_api/execution_backend.h>

namespace NYT::NQueryClient {

using NTransactionClient::TReadTimestampRange;

////////////////////////////////////////////////////////////////////////////////

struct TDataSplit
{
    TGuid ObjectId;

    TTableSchemaPtr TableSchema;

    NHydra::TRevision MountRevision;
};

////////////////////////////////////////////////////////////////////////////////

using TSourceLocation = std::pair<int, int>;
static const TSourceLocation NullSourceLocation(0, 0);

DEFINE_ENUM(EUnaryOp,
    // Arithmetical operations.
    (Plus)
    (Minus)
    // Integral operations.
    (BitNot)
    // Logical operations.
    (Not)
);

DEFINE_ENUM(EBinaryOp,
    // Arithmetical operations.
    (Plus)
    (Minus)
    (Multiply)
    (Divide)
    // Integral operations.
    (Modulo)
    (LeftShift)
    (RightShift)
    (BitOr)
    (BitAnd)
    // Logical operations.
    (And)
    (Or)
    // Relational operations.
    (Equal)
    (NotEqual)
    (Less)
    (LessOrEqual)
    (Greater)
    (GreaterOrEqual)
    // String operations.
    (Concatenate)
);

DEFINE_ENUM(EStringMatchOp,
    (Like)
    (CaseInsensitiveLike)
    (Regex)
);

DEFINE_ENUM(ETotalsMode,
    (None)
    (BeforeHaving)
    (AfterHaving)
);

DEFINE_ENUM(EAggregateFunction,
    (Sum)
    (Min)
    (Max)
);

DEFINE_ENUM(EStreamTag,
    (Aggregated)
    (Intermediate)
    (Totals)
);

const char* GetUnaryOpcodeLexeme(EUnaryOp opcode);
const char* GetBinaryOpcodeLexeme(EBinaryOp opcode);
const char* GetStringMatchOpcodeLexeme(EStringMatchOp opcode);

//! Reverse binary opcode for comparison operations (for swapping arguments).
EBinaryOp GetReversedBinaryOpcode(EBinaryOp opcode);

//! Inverse binary opcode for comparison operations (for inverting the operation).
EBinaryOp GetInversedBinaryOpcode(EBinaryOp opcode);

//! Classifies binary opcode according to classification above.
bool IsArithmeticalBinaryOp(EBinaryOp opcode);

//! Classifies binary opcode according to classification above.
bool IsIntegralBinaryOp(EBinaryOp opcode);

//! Classifies binary opcode according to classification above.
bool IsLogicalBinaryOp(EBinaryOp opcode);

//! Classifies binary opcode according to classification above.
bool IsRelationalBinaryOp(EBinaryOp opcode);

//! Classifies binary opcode according to classification above.
bool IsStringBinaryOp(EBinaryOp opcode);

//! Cast numeric values.
TValue CastValueWithCheck(TValue value, EValueType targetType);

////////////////////////////////////////////////////////////////////////////////

// TODO(lukyan): Use opaque data descriptor instead of ObjectId, CellId and MountRevision.
struct TDataSource
{
    // Could be:
    // * a table id;
    // * a tablet id.
    NObjectClient::TObjectId ObjectId;
    // If #ObjectId is a tablet id then this is the id of the cell hosting this tablet.
    // COMPAT(babenko): legacy clients may omit this field.
    NObjectClient::TCellId CellId;

    NHydra::TRevision MountRevision;

    TSharedRange<TRowRange> Ranges;
    TSharedRange<TRow> Keys;
};

void ToProto(
    NProto::TDataSource* serialized,
    const TDataSource& original,
    TRange<NTableClient::TLogicalTypePtr> schema,
    bool lookupSupported,
    size_t keyWidth);
void FromProto(
    TDataSource* original,
    const NProto::TDataSource& serialized,
    const IMemoryChunkProviderPtr& memoryChunkProvider = GetDefaultMemoryChunkProvider());

////////////////////////////////////////////////////////////////////////////////

struct TQueryBaseOptions
{
    TReadSessionId ReadSessionId;

    i64 InputRowLimit = std::numeric_limits<i64>::max();
    i64 OutputRowLimit = std::numeric_limits<i64>::max();
    size_t MemoryLimitPerNode = std::numeric_limits<size_t>::max();

    NCodegen::EExecutionBackend ExecutionBackend = NCodegen::EExecutionBackend::Native;

    bool EnableCodeCache = true;
    bool UseCanonicalNullRelations = false;
    bool MergeVersionedRows = true;
    // COMPAT(sabdenovch)
    bool AllowUnorderedGroupByWithLimit = false;
};

struct TQueryOptions
    : public TQueryBaseOptions
{
    TReadTimestampRange TimestampRange{
        .Timestamp = NTransactionClient::SyncLastCommittedTimestamp,
        .RetentionTimestamp = NTransactionClient::NullTimestamp,
    };

    std::optional<TString> ExecutionPool;
    TWorkloadDescriptor WorkloadDescriptor;

    std::optional<bool> UseLookupCache;

    ui64 RangeExpansionLimit = 0;

    TInstant Deadline = TInstant::Max();

    i64 MinRowCountPerSubquery = 100'000;
    int MaxSubqueries = std::numeric_limits<int>::max();

    bool VerboseLogging = false;
    bool AllowFullScan = true;
    bool SuppressAccessTracking = false;
    // COMPAT(lukyan)
    bool NewRangeInference = true;
};

void ToProto(NProto::TQueryOptions* serialized, const TQueryOptions& original);
void FromProto(TQueryOptions* original, const NProto::TQueryOptions& serialized);

////////////////////////////////////////////////////////////////////////////////

struct TFeatureFlags
{
    bool WithTotalsFinalizesAggregatedOnCoordinator = false;
    bool GroupByWithLimitIsUnordered = false;
};

TFeatureFlags MostFreshFeatureFlags();
TFeatureFlags MostArchaicFeatureFlags();

TString ToString(const TFeatureFlags& featureFlags);

void ToProto(NProto::TFeatureFlags* serialized, const TFeatureFlags& original);
void FromProto(TFeatureFlags* original, const NProto::TFeatureFlags& serialized);

////////////////////////////////////////////////////////////////////////////////

struct TShuffleNavigator
{
    THashMap<TString, TSharedRange<TKeyRange>> DestinationMap;
    int PrefixHint;
};

using TJoinLayerDataSourceSet = std::vector<NQueryClient::TDataSource>;

////////////////////////////////////////////////////////////////////////////////

using TPlanFragmentPtr = TIntrusivePtr<TPlanFragment>;

struct TPlanFragment final
{
    TQueryPtr Query;
    TDataSource DataSource;
    TPlanFragmentPtr SubqueryFragment;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
