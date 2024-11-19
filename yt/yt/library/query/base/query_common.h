#pragma once

#include "public.h"

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/transaction_client/helpers.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/library/codegen/execution_backend.h>

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

struct TQueryBaseOptions
{
    i64 InputRowLimit = std::numeric_limits<i64>::max();
    i64 OutputRowLimit = std::numeric_limits<i64>::max();

    bool EnableCodeCache = true;
    bool UseCanonicalNullRelations = false;
    NCodegen::EExecutionBackend ExecutionBackend = NCodegen::EExecutionBackend::Native;
    TReadSessionId ReadSessionId;
    size_t MemoryLimitPerNode = std::numeric_limits<size_t>::max();
    bool MergeVersionedRows = true;
};

struct TQueryOptions
    : public TQueryBaseOptions
{
    TReadTimestampRange TimestampRange{
        .Timestamp = NTransactionClient::SyncLastCommittedTimestamp,
        .RetentionTimestamp = NTransactionClient::NullTimestamp,
    };
    bool VerboseLogging = false;
    bool AllowFullScan = true;
    bool SuppressAccessTracking = false;
    // COMPAT(lukyan)
    bool NewRangeInference = true;
    int MaxSubqueries = std::numeric_limits<int>::max();
    i64 MinRowCountPerSubquery = 100'000;
    std::optional<bool> UseLookupCache;
    ui64 RangeExpansionLimit = 0;
    TWorkloadDescriptor WorkloadDescriptor;
    TInstant Deadline = TInstant::Max();
    std::optional<TString> ExecutionPool;
};

////////////////////////////////////////////////////////////////////////////////

struct TFeatureFlags
{
    bool WithTotalsFinalizesAggregatedOnCoordinator = false;
};

TFeatureFlags MostFreshFeatureFlags();
TFeatureFlags MostArchaicFeatureFlags();

TString ToString(const TFeatureFlags& featureFlags);

////////////////////////////////////////////////////////////////////////////////

struct TShuffleNavigator
{
    THashMap<TString, TSharedRange<TKeyRange>> DestinationMap;
    int PrefixHint;
};

using TJoinLayerDataSourceSet = std::vector<NQueryClient::TDataSource>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
