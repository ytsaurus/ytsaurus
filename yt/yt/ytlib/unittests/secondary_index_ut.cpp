#include <yt/yt/ytlib/api/native/secondary_index_modification.h>

#include <yt/yt/library/query/secondary_index/schema.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/library/query/unittests/helpers/helpers.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/core/test_framework/framework.h>

#include <util/random/shuffle.h>

namespace NYT::NQueryClient {
namespace {

using namespace NApi;
using namespace NApi::NNative;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NTabletClient;

using NTabletClient::EErrorCode::UniqueIndexConflict;

////////////////////////////////////////////////////////////////////////////////

const int StressIterationCount = 10'000;

const int UnrequiredIsEvaluatedPercentChance = 20;
const int ValueIsAggregatedPercentChance = 20;
const int ValueHasLockPercentChance = 20;
const int ColumnHasGroupPercentChance = 20;
const int TypeFlipPercentChance = 10;

const auto BasicTableSchema = New<TTableSchema>(std::vector{
    TColumnSchema("key", EValueType::Int64, ESortOrder::Ascending),
    TColumnSchema("value", EValueType::Int64),
}, true, true);

const auto BasicIndexSchema = New<TTableSchema>(std::vector{
    TColumnSchema("value", EValueType::Int64, ESortOrder::Ascending),
    TColumnSchema("key", EValueType::Int64, ESortOrder::Ascending),
    TColumnSchema("$empty", EValueType::Int64),
}, true, true);

////////////////////////////////////////////////////////////////////////////////

TTableMountInfoPtr MakeFrom(TTableSchemaPtr primarySchema)
{
    auto mountInfo = New<TTableMountInfo>();
    mountInfo->Schemas[ETableSchemaKind::Write] = primarySchema->ToWrite();
    mountInfo->Schemas[ETableSchemaKind::Lookup] = primarySchema->ToLookup();
    mountInfo->Schemas[ETableSchemaKind::Primary] = std::move(primarySchema);

    return mountInfo;
}

////////////////////////////////////////////////////////////////////////////////

const std::vector<ESimpleLogicalValueType> TestedTypes = {
    ESimpleLogicalValueType::Int64,
    ESimpleLogicalValueType::Uint64,
    ESimpleLogicalValueType::String,
    ESimpleLogicalValueType::Double,
    ESimpleLogicalValueType::Boolean,
};

const std::vector<TColumnSchema> AllColumns{
    {"Hera", MakeLogicalType(ESimpleLogicalValueType::Int64, /*required*/ false)},
    {"Zeus", MakeLogicalType(ESimpleLogicalValueType::Int64, /*required*/ true)},
    {"Apollo", MakeLogicalType(ESimpleLogicalValueType::Uint64, /*required*/ false)},
    {"Ares", MakeLogicalType(ESimpleLogicalValueType::Uint64, /*required*/ true)},
    {"Dionysus", MakeLogicalType(ESimpleLogicalValueType::Double, /*required*/ false)},
    {"Chaos", MakeLogicalType(ESimpleLogicalValueType::Double, /*required*/ true)},
    // {"Chronos", MakeLogicalType(ESimpleLogicalValueType::Any, /*required*/ false)},
    // {"Athena", MakeLogicalType(ESimpleLogicalValueType::Any, /*required*/ true)},
    {"Eros", MakeLogicalType(ESimpleLogicalValueType::Boolean, /*required*/ false)},
    {"Artemis", MakeLogicalType(ESimpleLogicalValueType::Boolean, /*required*/ true)},
    {"Aether", MakeLogicalType(ESimpleLogicalValueType::String, /*required*/ false)},
    {"Aphrodite", MakeLogicalType(ESimpleLogicalValueType::String, /*required*/ true)},
};

////////////////////////////////////////////////////////////////////////////////

class TSecondaryIndexTest
    : public ::testing::Test
{
public:
    void RunWithSingleRow(
        TRandomExpressionGenerator& generator,
        TTableSchemaPtr tableSchema,
        TTableSchemaPtr indexSchema,
        ESecondaryIndexKind kind)
    {
        auto columnCount = tableSchema->GetColumnCount();
        auto row = generator.GenerateRandomRow(columnCount);
        auto modification = TUnversionedSubmittedRow{
            .Command = EWireProtocolCommand::WriteRow,
            .Row = row,
            .Locks = {},
            .SequentialId = 0,
        };

        auto modifications = Run(std::move(tableSchema), std::move(indexSchema), TRange(&modification, 1), {}, kind);

        THROW_ERROR_EXCEPTION_IF(modifications.Size() != 1,
            "Expected a single index modification, got %v",
            modifications.Size());

        auto indexModification = modifications[0];
        THROW_ERROR_EXCEPTION_IF(indexModification.Type != ERowModificationType::Write,
            "Expected a write modification, got %Qlv", indexModification.Type);

        for (auto val : TUnversionedRow(indexModification.Row)) {
            auto primaryIndex = val.Id;
            if (primaryIndex >= columnCount) {
                continue;
            }
            EXPECT_EQ(TUnversionedRow(row)[primaryIndex], val);
        }
    }

    TSharedRange<TRowModification> Run(
        TTableSchemaPtr tableSchema,
        TTableSchemaPtr indexSchema,
        TRange<TUnversionedSubmittedRow> tableModifications = {},
        TRange<TUnversionedRow> initRows = {},
        ESecondaryIndexKind kind = ESecondaryIndexKind::FullSync)
    {
        auto keyCC = tableSchema->GetKeyColumnCount();

        auto tableMountInfo = MakeFrom(std::move(tableSchema));
        tableMountInfo->Indices = {{.Kind=kind}};

        auto indexMountInfo = MakeFrom(std::move(indexSchema));

        auto modifier = CreateSecondaryIndexModifier(
            MakeMockLookuper(initRows, keyCC),
            std::move(tableMountInfo),
            {indexMountInfo},
            tableModifications);

        WaitForFast(modifier->LookupRows())
            .ThrowOnError();

        TSharedRange<TRowModification> indexModifications;

        WaitForFast(modifier->OnIndexModifications([&] (
                NYPath::TYPath,
                TNameTablePtr,
                TSharedRange<TRowModification> modifications)
            {
                indexModifications = std::move(modifications);
            }))
            .ThrowOnError();

        return indexModifications;
    }

    std::function<TLookupSignature> MakeMockLookuper(TRange<TUnversionedRow> initRows, int keyWidth)
    {
        THashMap<TLegacyKey, TUnversionedRow> allRows;
        for (auto row : initRows) {
            EmplaceOrCrash(
                allRows,
                RowBuffer_->CaptureRow(row.FirstNElements(keyWidth), /*captureValues*/ false),
                row);
        }

        return [allRows = std::move(allRows)] (
            NYPath::TYPath /* path */,
            NTableClient::TNameTablePtr /* nameTable */,
            TSharedRange<TLegacyKey> keys,
            TLookupRowsOptions options)
        {
            std::vector<TUnversionedRow> response;
            response.reserve(keys.size());
            for (auto key : keys) {
                auto it = allRows.find(key);
                if (it != allRows.end()) {
                    response.push_back(it->second);
                } else if (options.KeepMissingRows) {
                    response.push_back({});
                }
            }
            return MakeFuture(MakeSharedRange(std::move(response)));
        };
    }

    void ExpectError(TError error, const TString& substring)
    {
        THROW_ERROR_EXCEPTION_IF(error.IsOK(), "Expected error");

        THROW_ERROR_EXCEPTION_IF(!error.GetMessage().Contains(substring),
            "Expected error message to contain %Qv, got %Qv",
            substring,
            error.GetMessage());
    }

protected:
    const TRowBufferPtr RowBuffer_ = New<TRowBuffer>();
    TFastRng64 Rng_{4};

    void ApplyRandomModifiers(TColumnSchema& column, bool key)
    {
        static const std::vector<TString> Locks{"L1", "L2"};
        static const std::vector<TString> Groups{"G1", "G2"};
        static const std::vector<TString> Aggregates{"max", "min"};

        if (Rng_.Uniform(100) < TypeFlipPercentChance) {
            auto newType = TestedTypes[Rng_.Uniform(TestedTypes.size())];
            auto required = Rng_.Uniform(2) == 1;
            column.SetLogicalType(MakeLogicalType(newType, required));
        }

        if (key) {
            if (!column.Required() && Rng_.Uniform(100) < UnrequiredIsEvaluatedPercentChance) {
                column.SetExpression("#");
            }
        } else {
            if (Rng_.Uniform(100) < ValueIsAggregatedPercentChance) {
                column.SetAggregate(Aggregates[Rng_.Uniform(Aggregates.size())]);
            }
            if (Rng_.Uniform(100) < ValueHasLockPercentChance) {
                column.SetLock(Locks[Rng_.Uniform(Locks.size())]);
            }
        }

        if (Rng_.Uniform(100) < ColumnHasGroupPercentChance) {
            column.SetGroup(Groups[Rng_.Uniform(Groups.size())]);
        }
    }

    std::vector<TColumnSchema> MakeRandomSchema()
    {
        auto columns = AllColumns;
        Shuffle(columns.begin(), columns.end());
        columns.resize(2 + Rng_.Uniform(columns.size() - 1));

        auto keyColumnCount = 1 + Rng_.Uniform(columns.size() - 1);

        for (size_t index = 0; index < columns.size(); ++index) {
            ApplyRandomModifiers(columns[index], index < keyColumnCount);
            if (index < keyColumnCount) {
                columns[index].SetSortOrder(ESortOrder::Ascending);
                continue;
            }
        }

        return columns;
    }

    std::pair<TTableSchemaPtr, TTableSchemaPtr> MakeRandomTableAndIndexSchemas()
    {
        auto tableColumns = MakeRandomSchema();

        auto indexColumns = tableColumns;
        Shuffle(indexColumns.begin(), indexColumns.end());
        indexColumns.resize(1 + Rng_.Uniform(indexColumns.size()));

        auto indexKeyColumnCount = 1 + Rng_.Uniform(indexColumns.size());
        for (size_t index = 0; index < indexColumns.size(); ++index) {
            auto& column = indexColumns[index];
            if (index < indexKeyColumnCount) {
                column.SetSortOrder(ESortOrder::Ascending);
            } else {
                column.SetSortOrder(std::nullopt);
            }
            column.SetAggregate(std::nullopt);
            column.SetLock(std::nullopt);
            column.SetGroup(std::nullopt);
            ApplyRandomModifiers(column, index < indexKeyColumnCount);
        }

        if (indexKeyColumnCount == indexColumns.size()) {
            indexColumns.push_back({"$empty", EValueType::Int64});
        }

        auto tableSchema = New<TTableSchema>(std::move(tableColumns), /*strict*/ true, true);
        auto indexSchema = New<TTableSchema>(std::move(indexColumns), true, /*unique keys*/ true);

        ValidateTableSchema(*tableSchema, true);
        ValidateTableSchema(*indexSchema, true);

        return {tableSchema, indexSchema};
    }
};

TEST_F(TSecondaryIndexTest, Basic)
{
    TRandomExpressionGenerator generator{
        .Schema = BasicTableSchema,
        .RowBuffer = RowBuffer_,
    };

    RunWithSingleRow(generator, BasicTableSchema, BasicIndexSchema, ESecondaryIndexKind::FullSync);
}

TEST_F(TSecondaryIndexTest, Sorted)
{
    auto unsortedSchema = New<TTableSchema>(std::vector{
        TColumnSchema("value1", EValueType::Int64),
        TColumnSchema("value2", EValueType::Int64),
    }, true, true);

    auto sortedSchema = New<TTableSchema>(std::vector{
        TColumnSchema("value2", EValueType::Int64, ESortOrder::Ascending),
        TColumnSchema("value1", EValueType::Int64),
    }, true, true);

    EXPECT_THROW_THAT(
        Run(unsortedSchema, sortedSchema, {}),
        testing::HasSubstr("must be sorted"));

    EXPECT_THROW_THAT(
        Run(sortedSchema, unsortedSchema, {}),
        testing::HasSubstr("must be sorted"));
}

TEST_F(TSecondaryIndexTest, Unique)
{
    auto nonUniqueSchema = New<TTableSchema>(std::vector{
        TColumnSchema("key1", EValueType::Int64, ESortOrder::Ascending),
        TColumnSchema("key2", EValueType::Int64, ESortOrder::Ascending),
        TColumnSchema("value1", EValueType::Int64),
        TColumnSchema("value2", EValueType::Int64),
    }, true, false);

    auto uniqueSchema = New<TTableSchema>(std::vector{
        TColumnSchema("key2", EValueType::Int64, ESortOrder::Ascending),
        TColumnSchema("key1", EValueType::Int64, ESortOrder::Ascending),
        TColumnSchema("value1", EValueType::Int64),
        TColumnSchema("value2", EValueType::Int64),
        TColumnSchema()
    }, true, true);

    EXPECT_THROW_THAT(
        Run(nonUniqueSchema, uniqueSchema, {}),
        testing::HasSubstr("must have unique keys"));

    EXPECT_THROW_THAT(
        Run(uniqueSchema, nonUniqueSchema, {}),
        testing::HasSubstr("must have unique keys"));
}

TEST_F(TSecondaryIndexTest, IndexMustHaveAllTableKeys)
{
    auto tableSchema = New<TTableSchema>(std::vector{
        TColumnSchema("key1", EValueType::Int64, ESortOrder::Ascending),
        TColumnSchema("key2", EValueType::Int64, ESortOrder::Ascending),
        TColumnSchema("key3", EValueType::Int64, ESortOrder::Ascending),
        TColumnSchema("value", EValueType::Int64),
    }, true, true);

    auto indexSchema = New<TTableSchema>(std::vector{
        TColumnSchema("value", EValueType::Int64, ESortOrder::Ascending),
        TColumnSchema("key1", EValueType::Int64, ESortOrder::Ascending),
        TColumnSchema("key2", EValueType::Int64, ESortOrder::Ascending),
        TColumnSchema("$empty", EValueType::Int64),
    }, true, true);

    EXPECT_THROW_THAT(
        Run(std::move(tableSchema), std::move(indexSchema), {}),
        testing::HasSubstr("missing in the index"));
}

TEST_F(TSecondaryIndexTest, IndexCannotHaveExtraValues)
{
    auto indexSchema = New<TTableSchema>(std::vector{
        TColumnSchema("value", EValueType::Int64, ESortOrder::Ascending),
        TColumnSchema("key", EValueType::Int64, ESortOrder::Ascending),
        TColumnSchema("value2", EValueType::Int64),
    }, true, true);

    EXPECT_THROW_THAT(
        Run(BasicTableSchema, std::move(indexSchema), {}),
        testing::HasSubstr("missing in the table schema"));
}

TEST_F(TSecondaryIndexTest, TypeMismatch)
{
    auto indexSchema = New<TTableSchema>(std::vector{
        TColumnSchema(
            "value",
            MakeLogicalType(ESimpleLogicalValueType::Int64, /*required*/ true),
            ESortOrder::Ascending),
        TColumnSchema("key", EValueType::Int64, ESortOrder::Ascending),
        TColumnSchema("$empty", EValueType::Int64),
    }, true, true);

    EXPECT_THROW_THAT(
        Run(BasicTableSchema, std::move(indexSchema), {}),
        testing::HasSubstr("Type mismatch"));
}

TEST_F(TSecondaryIndexTest, IndexOnAggregate)
{
    auto tableSchema = New<TTableSchema>(std::vector{
        TColumnSchema("key1", EValueType::Int64, ESortOrder::Ascending),
        TColumnSchema("value", EValueType::Int64)
            .SetAggregate("max"),
        TColumnSchema("value2", EValueType::Int64),
    }, true, true);

    auto indexSchema = New<TTableSchema>(std::vector{
        TColumnSchema("value", EValueType::Int64, ESortOrder::Ascending),
        TColumnSchema("key1", EValueType::Int64, ESortOrder::Ascending),
        TColumnSchema("$empty", EValueType::Int64),
    }, true, true);

    EXPECT_THROW_THAT(
        Run(std::move(tableSchema), std::move(indexSchema), {}),
        testing::HasSubstr("Cannot create index on an aggregate column"));
}

TEST_F(TSecondaryIndexTest, SingleIndexLock)
{
    auto tableSchema = New<TTableSchema>(std::vector{
        TColumnSchema("key1", EValueType::Int64, ESortOrder::Ascending),
        TColumnSchema("key2", EValueType::String, ESortOrder::Ascending),
        TColumnSchema("value1", EValueType::Int64)
            .SetLock("alpha"),
        TColumnSchema("value2", EValueType::Int64)
            .SetLock("beta"),
    }, true, true);

    auto indexSchema = New<TTableSchema>(std::vector{
        TColumnSchema("value1", EValueType::Int64, ESortOrder::Ascending),
        TColumnSchema("key1", EValueType::Int64, ESortOrder::Ascending),
        TColumnSchema("key2", EValueType::String, ESortOrder::Ascending),
        TColumnSchema("value2", EValueType::Int64),
    }, true, true);

    EXPECT_THROW_THAT(
        Run(std::move(tableSchema), std::move(indexSchema)),
        testing::HasSubstr("All indexed table columns must have same lock group"));
}

TEST_F(TSecondaryIndexTest, OverwriteRow)
{
    auto mutableInitRow = RowBuffer_->AllocateUnversioned(2);
    mutableInitRow[0] = MakeUnversionedInt64Value(0, 0);
    mutableInitRow[1] = MakeUnversionedInt64Value(0, 1);
    auto initRow = TUnversionedRow(mutableInitRow);

    auto mutableModificationRow = RowBuffer_->AllocateUnversioned(2);
    mutableModificationRow[0] = MakeUnversionedInt64Value(0, 0);
    mutableModificationRow[1] = MakeUnversionedInt64Value(1, 1);

    auto modification = TUnversionedSubmittedRow{
        .Command=EWireProtocolCommand::WriteRow,
        .Row=mutableModificationRow,
        .Locks={},
        .SequentialId=0,
    };

    auto indexModifications = Run(BasicTableSchema, BasicIndexSchema, TRange(&modification, 1), TRange(&initRow, 1));

    ASSERT_EQ(indexModifications.Size(), 2ul);
    ASSERT_TRUE(
        (indexModifications[0].Type == ERowModificationType::Delete) !=
        (indexModifications[1].Type == ERowModificationType::Delete));
    ASSERT_TRUE(
        (indexModifications[0].Type == ERowModificationType::Write) !=
        (indexModifications[1].Type == ERowModificationType::Write));

    for (auto& im : indexModifications) {
        if (im.Type == ERowModificationType::Write) {
            EXPECT_EQ(TUnversionedRow(im.Row)[0].Data.Int64, 1);
            EXPECT_EQ(TUnversionedRow(im.Row)[1].Data.Int64, 0);
        } else {
            EXPECT_EQ(TUnversionedRow(im.Row)[0].Data.Int64, 0);
            EXPECT_EQ(TUnversionedRow(im.Row)[1].Data.Int64, 0);
        }
    }
}

TEST_F(TSecondaryIndexTest, Stress)
{
    static const std::vector<ESecondaryIndexKind> TestedKinds{
        ESecondaryIndexKind::FullSync,
        ESecondaryIndexKind::Unique,
    };

    int iterations = StressIterationCount;
    int passedValidation = 0;
    while (iterations--) {
        auto kind = TestedKinds[Rng_.Uniform(TestedKinds.size())];

        TTableSchemaPtr tableSchema, indexSchema;
        bool validationPassed = false;

        try {
            std::tie(tableSchema, indexSchema) = MakeRandomTableAndIndexSchemas();
            switch (kind) {
                case ESecondaryIndexKind::FullSync:
                    ValidateFullSyncIndexSchema(*tableSchema, *indexSchema);
                    break;
                case ESecondaryIndexKind::Unique:
                    ValidateUniqueIndexSchema(*tableSchema, *indexSchema);
                    break;
                default:
                    YT_ABORT();
            }
            validationPassed = true;
            passedValidation++;
        } catch (const std::exception& ex) { }

        if (validationPassed) {
            TRandomExpressionGenerator generator{
                .Schema = tableSchema,
                .RowBuffer = RowBuffer_,
            };
            RunWithSingleRow(generator, tableSchema, indexSchema, kind);
        }
    }

    YT_VERIFY(double(passedValidation) / StressIterationCount > 0.1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient
