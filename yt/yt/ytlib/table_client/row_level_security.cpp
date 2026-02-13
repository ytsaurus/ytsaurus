#include "row_level_security.h"

#include <yt/yt/core/phoenix/type_def.h>

#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/library/query/engine/folding_profiler.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>

#include <library/cpp/iterator/enumerate.h>

namespace NYT::NTableClient {

using namespace NLogging;
using namespace NQueryClient;
using namespace NSecurityClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

bool IsTypeBoolean(const TLogicalType& logicalType)
{
    if (logicalType.GetMetatype() == ELogicalMetatype::Simple) {
        if (logicalType.UncheckedAsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Boolean) {
            return true;
        }
    } else if (logicalType.GetMetatype() == ELogicalMetatype::Optional) {
        return IsTypeBoolean(*logicalType.UncheckedAsOptionalTypeRef().GetElement());
    }

    return false;
}

bool ValidatePredicateApplicability(
    TRowLevelAccessControlEntry rowLevelAce,
    const TTableSchemaPtr& schema,
    const TLogger& Logger)
{
    try {
        THashSet<std::string> references;
        auto preparedExpression = PrepareExpression(rowLevelAce.RowAccessPredicate, *schema, GetBuiltinTypeInferrers(), &references);
        THROW_ERROR_EXCEPTION_IF(
            !IsTypeBoolean(*preparedExpression->LogicalType),
            "Expected row access predicate's result type to be boolean, got %Qlv",
            *preparedExpression->LogicalType);
        return true;
    } catch (const std::exception& ex) {
        switch (rowLevelAce.InapplicableRowAccessPredicateMode) {
            case EInapplicableRowAccessPredicateMode::Ignore: {
                YT_LOG_INFO(ex, "Ignored row access predicate (RowAccessPredicate: %v)", rowLevelAce.RowAccessPredicate);
                return false;
            }
            case EInapplicableRowAccessPredicateMode::Fail: {
                auto error = TError(
                    "One of row-level ACE's predicate is inapplicable to the table schema "
                    "and ACE has %v=%lv",
                    TSerializableAccessControlEntry::InapplicableRowAccessPredicateModeKey,
                    EInapplicableRowAccessPredicateMode::Fail)
                    << ex;

                THROW_ERROR std::move(error);
            }
        }
    }
}

//! Builds a single expression, which is a disjunction of all Row-Level ACE's predicates.
//!
//! When all rl aces are inapplicable and inapplicable_row_access_predicate_mode=ignore,
//! return nullopt.
std::optional<std::string> ValidateAndBuildPredicate(
    const TTableSchemaPtr& schema,
    const std::vector<TRowLevelAccessControlEntry>& rowLevelAcl,
    const TLogger& Logger)
{
    TStringBuilder builder;
    bool first = true;
    for (const auto& rowLevelAce : rowLevelAcl) {
        YT_VERIFY(!rowLevelAce.RowAccessPredicate.empty());

        if (!ValidatePredicateApplicability(rowLevelAce, schema, Logger)) {
            continue;
        }

        // NB(coteeq): |ValidateRowLevelAceApplicability| checks that the |rowLevelAce.RowAccessPredicate| is a valid expression.
        // That means that we can just copy-paste the predicate into the builder and not care about
        // SQL-injection-like things (e.g. it is not possible to have one of predicates be like
        // `) || true || (` and break the logic of disjunction†). And since we enclose predicates
        // in parens, the logic should not be affected by operators precedence either.
        //
        // † For now, all these expressions are controlled by the table's admins anyway,
        // so SQL-injection is not an attack vector here, but it is still nice to know that
        // the resulting expression will always be valid in the sense of syntax and work as
        // intuitively expected.
        if (!first) {
            builder.AppendString(" or ");
        }
        first = false;
        builder.AppendChar('(');
        builder.AppendString(rowLevelAce.RowAccessPredicate);
        builder.AppendChar(')');
    }

    auto predicate = builder.Flush();
    if (predicate.empty()) {
        if (rowLevelAcl.empty()) {
            YT_LOG_INFO("RL ACL is empty; denying to read any rows");
        } else {
            YT_LOG_INFO("All RL ACEs for a data source were ignored; no rows will be read");
        }
        return std::nullopt;
    }

    return predicate;
}

////////////////////////////////////////////////////////////////////////////////

struct TCGInstanceHolder final
{
    TCGExpressionInstance Instance;
    TCGVariables Variables;
    TCGExpressionImage Image;

    void Run(
        TUnversionedValue* value,
        TUnversionedRow schemafulRow,
        const TRowBufferPtr& rowBuffer)
    {
        Instance.Run(
            Variables.GetLiteralValues(),
            Variables.GetOpaqueData(),
            Variables.GetOpaqueDataSizes(),
            value,
            schemafulRow.Elements(),
            rowBuffer);
    }
};

using TCGInstanceHolderPtr = TIntrusivePtr<TCGInstanceHolder>;

////////////////////////////////////////////////////////////////////////////////

//! A thing that knows chunk schema and is able to remap incoming unversioned
//! rows to the order expected by the codegened instance.
class TRlsChecker
    : public IRlsChecker
{
public:
    TRlsChecker(
        TCGInstanceHolderPtr instance,
        TNameTableToSchemaIdMapping chunkToExpressionIdMapping,
        int valueCount)
        : Instance_(std::move(instance))
        , ValueCount_(valueCount)
        , RemappedValueCount_(
            std::ranges::count_if(
                chunkToExpressionIdMapping,
                [] (int value) {
                    return value != -1;
                }))
        , ChunkToExpressionIdMapping_(std::move(chunkToExpressionIdMapping))
    {
        YT_VERIFY(Instance_);
        YT_VERIFY(
            AllOf(
                ChunkToExpressionIdMapping_,
                [&] (int index) {
                    return index < ValueCount_;
                }));
    }

    ESecurityAction Check(TUnversionedRow row, const TRowBufferPtr& rowBuffer, int chunkValuesPrefix) const override
    {
        // NB(coteeq): Although RLS only acts on schemaful rows,
        // this checker is created per-datasource, not per-chunk (so there is
        // no guarantee that value indices will be the same for every chunk).
        // As codegen may be slow, reordering rows probably will be faster than
        // per-chunk expression compilation, but YMMV. Maybe, I should add
        // an option to compile per-chunk and some heuristic to choose between
        // per-chunk and per-datasource compilation.
        auto reorderedRow = ReorderRow(row, rowBuffer, chunkValuesPrefix);

        auto value = MakeUnversionedSentinelValue(EValueType::Null);

        Instance_->Run(
            &value,
            reorderedRow,
            rowBuffer);

        switch (value.Type) {
            case EValueType::Null:
                return ESecurityAction::Deny;
            case EValueType::Boolean:
                return value.Data.Boolean
                    ? ESecurityAction::Allow
                    : ESecurityAction::Deny;
            default:
                YT_ABORT();
        }
    }

    bool IsColumnNeeded(int indexInChunkNameTable) const override
    {
        return ChunkToExpressionIdMapping_[indexInChunkNameTable] != -1;
    }

private:
    const TCGInstanceHolderPtr Instance_;
    //! Width of the row expected by the |Instance_|.
    //! Note that this number may be greater than |RemappedValueCount_| when
    //! a chunk physically does not have enough columns (we must assume
    //! corresponding values to be nulls).
    const int ValueCount_;
    //! Number of columns that must be present in a row.
    const int RemappedValueCount_;
    const TNameTableToSchemaIdMapping ChunkToExpressionIdMapping_;

    TUnversionedRow ReorderRow(TUnversionedRow row, const TRowBufferPtr& rowBuffer, int chunkValuesPrefix) const
    {
        auto reorderedRow = rowBuffer->AllocateUnversioned(ValueCount_);
        for (int index = 0; index < ValueCount_; ++index) {
            reorderedRow[index] = MakeUnversionedNullValue(index);
        }

        int remappedValueCount = 0;
        for (const auto& [index, value] : SEnumerate(row)) {
            if (index >= chunkValuesPrefix) {
                break;
            }
            if (value.Id >= std::ssize(ChunkToExpressionIdMapping_)) {
                continue;
            }
            auto newId = ChunkToExpressionIdMapping_[value.Id];
            if (newId != -1) {
                reorderedRow[newId] = value;
                // Just for sanity.
                reorderedRow[newId].Id = newId;
                ++remappedValueCount;
            }
        }
        YT_VERIFY(
            remappedValueCount == RemappedValueCount_,
            Format("%v != %v", remappedValueCount, RemappedValueCount_));
        return reorderedRow;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRlsCheckerFactory
    : public IRlsCheckerFactory
{
public:
    TRlsCheckerFactory(
        TTableSchemaPtr adjustedSchema,
        TCGInstanceHolderPtr instanceHolder)
        : AdjustedSchema_(std::move(adjustedSchema))
        , CGInstance_(std::move(instanceHolder))
    { }

    IRlsCheckerPtr CreateCheckerForChunk(const TNameTablePtr& chunkNameTable) const override
    {
        TNameTableToSchemaIdMapping idMapping(static_cast<size_t>(chunkNameTable->GetSize()), -1);
        for (const auto& [index, column] : Enumerate(AdjustedSchema_->Columns())) {
            // NB(coteeq): RLS references non-stable names, so we refer to stable name inside the chunk.
            auto inChunkId = chunkNameTable->FindId(column.StableName().Underlying());
            if (inChunkId) {
                idMapping[*inChunkId] = index;
            } else {
                // This is fine. We will fill this column with null,
                // as is intended with missing-in-chunk columns.
            }
        }

        return New<TRlsChecker>(
            CGInstance_,
            std::move(idMapping),
            std::ssize(AdjustedSchema_->Columns()));
    }

private:
    const TTableSchemaPtr AdjustedSchema_;
    const TCGInstanceHolderPtr CGInstance_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::optional<TRlsReadSpec> TRlsReadSpec::BuildFromRowLevelAclAndTableSchema(
    const TTableSchemaPtr& tableSchema,
    const std::optional<std::vector<TRowLevelAccessControlEntry>>& rowLevelAcl,
    const TLogger& logger)
{
    if (!rowLevelAcl) {
        return std::nullopt;
    }
    auto predicate = ValidateAndBuildPredicate(tableSchema, *rowLevelAcl, logger);
    YT_VERIFY(!predicate || !predicate->empty());

    TRlsReadSpec rlsReadSpec;
    rlsReadSpec.TableSchema_ = tableSchema;
    if (predicate) {
        rlsReadSpec.PredicateOrTrivialDeny_ = *predicate;
    } else {
        rlsReadSpec.PredicateOrTrivialDeny_ = TTrivialDeny{};
    }

    return rlsReadSpec;
}

bool TRlsReadSpec::IsTrivialDeny() const
{
    return std::holds_alternative<TTrivialDeny>(PredicateOrTrivialDeny_);
}

const std::string& TRlsReadSpec::GetPredicate() const
{
    YT_VERIFY(!IsTrivialDeny());
    return std::get<std::string>(PredicateOrTrivialDeny_);
}

const TTableSchemaPtr& TRlsReadSpec::GetTableSchema() const
{
    return TableSchema_;
}

void TRlsReadSpec::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, TableSchema_,
        .template Serializer<TNonNullableIntrusivePtrSerializer<>>());
    PHOENIX_REGISTER_FIELD(2, PredicateOrTrivialDeny_);
}

PHOENIX_DEFINE_TYPE(TRlsReadSpec);

void ToProto(
    NProto::TRlsReadSpec* protoRlsReadSpec,
    const TRlsReadSpec& rlsReadSpec)
{
    Visit(
        rlsReadSpec.PredicateOrTrivialDeny_,
        [&] (const std::string& predicate) {
            protoRlsReadSpec->set_predicate(predicate);
        },
        [&] (const TRlsReadSpec::TTrivialDeny& /*trivialDeny*/) {
            protoRlsReadSpec->mutable_trivial_deny();
        });

    if (rlsReadSpec.TableSchema_) {
        ToProto(protoRlsReadSpec->mutable_table_schema(), *rlsReadSpec.TableSchema_);
    }
}

void FromProto(
    TRlsReadSpec* rlsReadSpec,
    const NProto::TRlsReadSpec& protoRlsReadSpec)
{
    switch (protoRlsReadSpec.predicate_or_trivial_deny_case()) {
        case NProto::TRlsReadSpec::kPredicate:
            rlsReadSpec->PredicateOrTrivialDeny_ = NYT::FromProto<std::string>(protoRlsReadSpec.predicate());
            break;
        case NProto::TRlsReadSpec::kTrivialDeny:
            rlsReadSpec->PredicateOrTrivialDeny_ = TRlsReadSpec::TTrivialDeny{};
            break;
        default:
            YT_ABORT();
    }

    if (protoRlsReadSpec.has_table_schema()) {
        rlsReadSpec->TableSchema_ = NYT::FromProto<TTableSchemaPtr>(protoRlsReadSpec.table_schema());
    }
}

void FormatValue(TStringBuilderBase* builder, const TRlsReadSpec& rlsReadSpec, TStringBuf /*spec*/)
{
    if (rlsReadSpec.IsTrivialDeny()) {
        Format(builder, "{TrivialDeny}");
    } else {
        Format(
            builder,
            "{Predicate: %v}",
            rlsReadSpec.GetPredicate());
    }
}

////////////////////////////////////////////////////////////////////////////////

IRlsCheckerFactoryPtr CreateRlsCheckerFactory(
    const TRlsReadSpec& rlsReadSpec)
{
    YT_VERIFY(!rlsReadSpec.IsTrivialDeny());
    const auto& schema = rlsReadSpec.GetTableSchema();
    YT_VERIFY(schema);

    THashSet<std::string> references;
    auto preparedExpression = PrepareExpression(
        rlsReadSpec.GetPredicate(),
        *schema,
        GetBuiltinTypeInferrers(),
        &references);

    // Drop unused columns from the schema.
    std::vector<TColumnSchema> columns;
    columns.reserve(references.size());
    for (const auto& column : schema->Columns()) {
        if (references.contains(column.Name())) {
            columns.push_back(column);
        }
    }
    auto adjustedSchema = New<TTableSchema>(std::move(columns));

    TCGVariables variables;

    auto profiler = Profile(
        preparedExpression,
        adjustedSchema,
        /*id*/ nullptr,
        &variables,
        /*useCanonicalNullRelations*/ false,
        NYT::NCodegen::EExecutionBackend::Native,
        GetBuiltinFunctionProfilers());

    auto image = profiler();
    auto instance = image.Instantiate();
    auto instanceHolder = New<TCGInstanceHolder>(
        std::move(instance),
        std::move(variables),
        std::move(image));

    return New<TRlsCheckerFactory>(
        adjustedSchema,
        std::move(instanceHolder));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
