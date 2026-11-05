#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/connectors/servicelog/joiner.h>
#include <yt/yt/flow/library/cpp/connectors/servicelog/provider.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

static constexpr ui64 MaxIndex = 1000;

NLogging::TLogger Logger("TableJoinerTest");
NProfiling::TProfiler Profiler("TableJoinerTest");

NTableClient::TTableSchemaPtr GetCommonSchema()
{
    NTableClient::TColumnSchema indexColumn1("index1", NTableClient::EValueType::Uint64, NTableClient::ESortOrder::Ascending);
    NTableClient::TColumnSchema indexColumn2("index2", NTableClient::EValueType::Uint64, NTableClient::ESortOrder::Ascending);
    NTableClient::TColumnSchema valueColumn1("value1", NTableClient::EValueType::Uint64);
    NTableClient::TColumnSchema valueColumn2("value2", NTableClient::EValueType::Uint64);
    NTableClient::TColumnSchema valueColumn3("value3", NTableClient::EValueType::Uint64);
    std::vector<NTableClient::TColumnSchema> columns{indexColumn1, indexColumn2, valueColumn1, valueColumn2, valueColumn3};
    return New<NTableClient::TTableSchema>(columns);
}

NTableClient::TUnversionedOwningRow GetKey(int index)
{
    return NTableClient::MakeUnversionedOwningRow(index, index + 1, index + 2, index + 3, index + 4);
}

void ValidateRow(const NTableClient::TTableSchemaPtr& schema, const NTableClient::TUnversionedOwningRow& row, ui64 checkIndex)
{
    ui64 index = NTableClient::FromUnversionedValue<ui64>(row[0]);
    for (int i = 0; i < schema->GetColumnCount(); ++i) {
        const auto& column = schema->Columns()[i];
        const auto& name = column.Name();

        if (name.find("missing") != std::string::npos) {
            if (name.find("ispresent") != std::string::npos) {
                EXPECT_EQ(row[i].Type, NTableClient::EValueType::Boolean);
                EXPECT_EQ(NTableClient::FromUnversionedValue<bool>(row[i]), index % 10 != 0);
                continue;
            }
            if (index % 10 == 0) {
                EXPECT_EQ(row[i].Type, NTableClient::EValueType::Null);
                continue;
            }
        }

        if (name.find("ispresent") != std::string::npos) {
            EXPECT_EQ(row[i].Type, NTableClient::EValueType::Boolean);
            EXPECT_EQ(NTableClient::FromUnversionedValue<bool>(row[i]), true);
            continue;
        }

        ui64 value = NTableClient::FromUnversionedValue<ui64>(row[i]);

        if (name.find("index2") != std::string::npos) {
            EXPECT_EQ(value, index + 1);
        } else if (name.find("index1") != std::string::npos) {
            EXPECT_EQ(value, index);
        } else if (name.find("value3") != std::string::npos) {
            EXPECT_EQ(value, index + 4);
        } else if (name.find("value2") != std::string::npos) {
            EXPECT_EQ(value, index + 3);
        } else if (name.find("value1") != std::string::npos) {
            EXPECT_EQ(value, index + 2);
        } else {
            EXPECT_FALSE(true);
        }
    }

    EXPECT_EQ(index, checkIndex);
}

////////////////////////////////////////////////////////////////////////////////

class TTestProviderBase
    : public IServiceLogRowsProvider
{
    NTableClient::TTableSchemaPtr GetSchema() override
    {
        return GetCommonSchema();
    }

    i64 GetApproximateRowCount() override
    {
        return MaxIndex;
    }
};

class TTestNormalProvider
    : public TTestProviderBase
{
    TFuture<TFetchResult> Fetch(const TServiceLogRangePtr& range, i64 rowLimit) override
    {
        ui64 index = 0;
        if (range->Lower) {
            YT_VERIFY(!range->Lower->Exclusive);
            index = NTableClient::FromUnversionedValue<ui64>(range->Lower->Key.Underlying()[0]);
        }
        ui64 upperExclusiveIndex = std::numeric_limits<ui64>::max();
        if (range->Upper) {
            YT_VERIFY(range->Upper->Exclusive);
            upperExclusiveIndex = NTableClient::FromUnversionedValue<ui64>(range->Upper->Key.Underlying()[0]);
        }
        std::vector<NTableClient::TUnversionedOwningRow> result;
        while (index < upperExclusiveIndex && std::ssize(result) < rowLimit && index <= MaxIndex) {
            result.push_back(GetKey(index));
            index++;
        }
        return MakeFuture<TFetchResult>({result, result.empty()});
    }
};

class TTestMissingProvider
    : public TTestProviderBase
{
    TFuture<TFetchResult> Fetch(const TServiceLogRangePtr& range, i64 rowLimit) override
    {
        ui64 index = 0;
        if (range->Lower) {
            YT_VERIFY(!range->Lower->Exclusive);
            index = NTableClient::FromUnversionedValue<ui64>(range->Lower->Key.Underlying()[0]);
        }
        ui64 upperExclusiveIndex = std::numeric_limits<ui64>::max();
        if (range->Upper) {
            YT_VERIFY(range->Upper->Exclusive);
            upperExclusiveIndex = NTableClient::FromUnversionedValue<ui64>(range->Upper->Key.Underlying()[0]);
        }
        std::vector<NTableClient::TUnversionedOwningRow> result;
        while (index < upperExclusiveIndex && std::ssize(result) < rowLimit && index <= MaxIndex) {
            if (index % 10 != 0) {
                result.push_back(GetKey(index));
            }
            index++;
        }
        return MakeFuture<TFetchResult>({result, result.empty()});
    }
};

//! Allows to fine-tune the outputs of the provider.
class TTestCustomizableProvider
    : public TTestProviderBase
{
public:
    // Does ignore range on purpose.
    TFuture<TFetchResult> Fetch(const TServiceLogRangePtr&, i64) override
    {
        std::vector<NTableClient::TUnversionedOwningRow> result;
        for (i64 i = LowerKey_; i < UpperKeyExclusive_; i++) {
            result.push_back(GetKey(i));
        }
        return MakeFuture<TFetchResult>({result, Finished_});
    }

    void SetNextResult(i64 lowerKey, i64 upperKeyExclusive, bool finished)
    {
        LowerKey_ = lowerKey;
        UpperKeyExclusive_ = upperKeyExclusive;
        Finished_ = finished;
    }

private:
    i64 LowerKey_ = 0;
    i64 UpperKeyExclusive_ = 0;
    bool Finished_ = false;
};

class TTestVeryCustomizableProvider
    : public TTestProviderBase
{
public:
    void SetNextResult(std::vector<i64> indices, bool finished)
    {
        Indices_ = indices;
        Finished_ = finished;
    }

    TFuture<TFetchResult> Fetch(const TServiceLogRangePtr&, i64) override
    {
        std::vector<NTableClient::TUnversionedOwningRow> result;
        for (i64 index : Indices_) {
            result.push_back(GetKey(index));
        }
        return MakeFuture<TFetchResult>({result, Finished_});
    }

private:
    std::vector<i64> Indices_;
    bool Finished_ = false;
};

void CheckLowerUpper(const IServiceLogRowsProviderPtr& tableJoiner, std::optional<ui64> lowerOffset, std::optional<ui64> upperOffset, ui64 rowLimit, const std::vector<ui64>& checkIndices)
{
    auto range = New<TServiceLogRange>();
    if (lowerOffset) {
        range->Lower = TServiceLogEndpoint();
        range->Lower->Key = MakeKey(*lowerOffset);
    }
    if (upperOffset) {
        range->Upper = TServiceLogEndpoint();
        range->Upper->Key = MakeKey(*upperOffset);
        range->Upper->Exclusive = true;
    }

    auto rows = NConcurrency::WaitFor(tableJoiner->Fetch(range, rowLimit)).ValueOrThrow().Rows;
    ui64 i = 0;
    EXPECT_EQ(checkIndices.size(), rows.size());
    for (ui64 index : checkIndices) {
        ValidateRow(tableJoiner->GetSchema(), rows[i], index);
        i++;
    }
}

std::vector<ui64> GetCheckIndices(std::optional<ui64> lowerOffset, std::optional<ui64> upperOffset, ui64 rowLimit = 100000, bool shouldMaybeDiscardLast = false)
{
    std::vector<ui64> checkIndices;
    ui64 i = 0;
    for (ui64 index = lowerOffset.value_or(0); index < upperOffset.value_or(MaxIndex + 1) && i < rowLimit; index++) {
        checkIndices.push_back(index);
        i++;
    }
    if (!checkIndices.empty() && shouldMaybeDiscardLast && checkIndices.back() % 10 == 0) {
        checkIndices.pop_back();
    }
    return checkIndices;
}

void CheckTableJoiner(const IServiceLogRowsProviderPtr& tableJoiner, bool shouldMaybeDiscardLast)
{
    CheckLowerUpper(tableJoiner, std::nullopt, std::nullopt, 100000, GetCheckIndices(std::nullopt, std::nullopt, 100000, shouldMaybeDiscardLast));
    CheckLowerUpper(tableJoiner, std::nullopt, std::nullopt, 300, GetCheckIndices(std::nullopt, std::nullopt, 300, false));

    for (ui64 lowerOffset = 0; lowerOffset < MaxIndex; lowerOffset += 81) {
        for (ui64 upperOffset = lowerOffset + 80; upperOffset < MaxIndex; upperOffset += 81) {
            bool shouldMaybeDiscardLastLong = shouldMaybeDiscardLast && (upperOffset <= lowerOffset + 100000);
            bool shouldMaybeDiscardLastShort = shouldMaybeDiscardLast && (upperOffset <= lowerOffset + 300);

            CheckLowerUpper(tableJoiner, lowerOffset, upperOffset, 100000, GetCheckIndices(lowerOffset, upperOffset, 100000, shouldMaybeDiscardLastLong));
            CheckLowerUpper(tableJoiner, lowerOffset, upperOffset, 300, GetCheckIndices(lowerOffset, upperOffset, 300, shouldMaybeDiscardLastShort));
        }
    }

    for (ui64 lowerOffset = 0; lowerOffset < MaxIndex; lowerOffset += 81) {
        CheckLowerUpper(tableJoiner, lowerOffset, std::nullopt, 300, GetCheckIndices(lowerOffset, std::nullopt, 300, shouldMaybeDiscardLast && ((1001 - lowerOffset) <= 300)));
        CheckLowerUpper(tableJoiner, lowerOffset, std::nullopt, 100000, GetCheckIndices(lowerOffset, std::nullopt, 100000, shouldMaybeDiscardLast));
        CheckLowerUpper(tableJoiner, std::nullopt, lowerOffset, 300, GetCheckIndices(std::nullopt, lowerOffset, 300, shouldMaybeDiscardLast && (lowerOffset <= 300)));
        CheckLowerUpper(tableJoiner, std::nullopt, lowerOffset, 100000, GetCheckIndices(std::nullopt, lowerOffset, 100000, shouldMaybeDiscardLast));
    }
}

TEST(TTableJoinerTest, NoJoin)
{
    auto normalProvider = New<TTestNormalProvider>();

    auto tableJoiner = CreateTableJoiner(Logger, Profiler, {{"", normalProvider}});

    auto columnsRange = tableJoiner->GetSchema()->Columns() | std::views::transform([] (const auto& columnSchema) {
        return columnSchema.Name();
    });
    std::vector<std::string> expectedColumns = {"index1", "index2", "ispresent", "value1", "value2", "value3"};
    EXPECT_EQ(expectedColumns, std::vector<std::string>(columnsRange.begin(), columnsRange.end()));

    CheckTableJoiner(tableJoiner, false);
}

TEST(TTableJoinerTest, TwoJoin)
{
    auto normalProvider = New<TTestNormalProvider>();
    auto missingProvider = New<TTestMissingProvider>();

    auto tableJoiner = CreateTableJoiner(Logger, Profiler, {{"", normalProvider}, {"missing1.", missingProvider}});
    auto columnsRange = tableJoiner->GetSchema()->Columns() | std::views::transform([] (const auto& columnSchema) {
        return columnSchema.Name();
    });
    std::vector<std::string> expectedColumns = {"index1", "index2", "ispresent", "value1", "value2", "value3", "missing1.ispresent", "missing1.value1", "missing1.value2", "missing1.value3"};
    EXPECT_EQ(expectedColumns, std::vector<std::string>(columnsRange.begin(), columnsRange.end()));

    CheckTableJoiner(tableJoiner, true);
}

TEST(TTableJoinerTest, ThreeJoin)
{
    auto normalProvider = New<TTestNormalProvider>();
    auto normalProvider2 = New<TTestNormalProvider>();
    auto missingProvider = New<TTestMissingProvider>();

    auto tableJoiner = CreateTableJoiner(Logger, Profiler, {{"", normalProvider}, {"missing1.", missingProvider}, {"complete1.", normalProvider2}});
    auto columnsRange = tableJoiner->GetSchema()->Columns() | std::views::transform([] (const auto& columnSchema) {
        return columnSchema.Name();
    });
    std::vector<std::string> expectedColumns = {"index1", "index2", "ispresent", "value1", "value2", "value3", "missing1.ispresent", "missing1.value1", "missing1.value2", "missing1.value3", "complete1.ispresent", "complete1.value1", "complete1.value2", "complete1.value3"};
    EXPECT_EQ(expectedColumns, std::vector<std::string>(columnsRange.begin(), columnsRange.end()));

    CheckTableJoiner(tableJoiner, true);
}

TEST(TTableJoinerTest, JoinWithLeftMissing)
{
    auto missingProvider = New<TTestMissingProvider>();
    auto normalProvider = New<TTestNormalProvider>();

    auto tableJoiner = CreateTableJoiner(Logger, Profiler, {{"missing.", missingProvider}, {"complete.", normalProvider}});
    auto schema = tableJoiner->GetSchema();

    CheckTableJoiner(tableJoiner, true);
}

//! Some scenarious where we primarily verify correct "isFinished" value is returned.
TEST(TTableJoinerTest, SpecialCasesWithRightCritical)
{
    auto provider1 = New<TTestCustomizableProvider>();
    auto provider2 = New<TTestCustomizableProvider>();

    auto validateIncompleteRow = [] (auto& row, i64 index) {
        EXPECT_EQ(row, NTableClient::MakeUnversionedOwningRow(index, index + 1, true, index + 2, index + 3, index + 4, false, std::nullopt, std::nullopt, std::nullopt));
    };

    auto validateCompleteRow = [] (auto& row, i64 index) {
        EXPECT_EQ(row, NTableClient::MakeUnversionedOwningRow(index, index + 1, true, index + 2, index + 3, index + 4, true, index + 2, index + 3, index + 4));
    };

    auto joiner = CreateTableJoiner(Logger, Profiler, {{"", provider1}, {"provider2", provider2}});
    [[maybe_unused]] auto schema = joiner->GetSchema();

    {
        // Since second provider has finished we can safely assume there is no key 101 therein and thus we can finish read.
        provider1->SetNextResult(51, 101, true);
        provider2->SetNextResult(51, 100, true);
        auto [rows, completed] = NConcurrency::WaitFor(joiner->Fetch(New<TServiceLogRange>(), 100)).ValueOrThrow();
        EXPECT_EQ(std::ssize(rows), 50);
        EXPECT_EQ(completed, true);
        for (int i = 51; i < 100; ++i) {
            validateCompleteRow(rows[i - 51], i);
        }
        validateIncompleteRow(rows.back(), 100);
    }

    {
        // On the contrary, here key 101 might just as well appear in second provider, then we shouldn't be seeing it now at all.
        provider1->SetNextResult(51, 101, true);
        provider2->SetNextResult(51, 100, false);
        auto [rows, completed] = NConcurrency::WaitFor(joiner->Fetch(New<TServiceLogRange>(), 100)).ValueOrThrow();
        EXPECT_EQ(std::ssize(rows), 49);
        EXPECT_EQ(completed, false);
        for (int i = 51; i < 100; ++i) {
            validateCompleteRow(rows[i - 51], i);
        }
    }

    {
        // Apparently, we should never be completed until the main provider finishes reading.
        provider1->SetNextResult(51, 101, false);
        provider2->SetNextResult(51, 101, true);
        auto [rows, completed] = NConcurrency::WaitFor(joiner->Fetch(New<TServiceLogRange>(), 100)).ValueOrThrow();
        EXPECT_EQ(std::ssize(rows), 50);
        EXPECT_EQ(completed, false);
        for (int i = 51; i < 101; ++i) {
            validateCompleteRow(rows[i - 51], i);
        }
    }

    {
        //! At times the total row count might be more than requested.
        auto provider1 = New<TTestVeryCustomizableProvider>();
        auto provider2 = New<TTestVeryCustomizableProvider>();
        auto joiner = CreateTableJoiner(Logger, Profiler, {{"provider1", provider1}, {"provider2", provider2}});
        [[maybe_unused]] auto schema = joiner->GetSchema();

        std::vector<i64> evenIndices, oddIndices;
        for (int i = 0; i < 100; ++i) {
            if (i % 2 == 0) {
                evenIndices.push_back(i);
            } else {
                oddIndices.push_back(i);
            }
        }

        auto getRowByIndex = [] (i64 index) {
            NTableClient::TUnversionedOwningRowBuilder builder;
            builder.AddValue(NTableClient::MakeUnversionedInt64Value(index));
            builder.AddValue(NTableClient::MakeUnversionedInt64Value(index + 1));
            builder.AddValue(NTableClient::MakeUnversionedBooleanValue(index % 2 == 1));
            if (index % 2 == 1) {
                builder.AddValue(NTableClient::MakeUnversionedInt64Value(index + 2));
                builder.AddValue(NTableClient::MakeUnversionedInt64Value(index + 3));
                builder.AddValue(NTableClient::MakeUnversionedInt64Value(index + 4));
            } else {
                builder.AddValue(NTableClient::MakeUnversionedNullValue());
                builder.AddValue(NTableClient::MakeUnversionedNullValue());
                builder.AddValue(NTableClient::MakeUnversionedNullValue());
            }
            builder.AddValue(NTableClient::MakeUnversionedBooleanValue(index % 2 == 0));
            if (index % 2 == 0) {
                builder.AddValue(NTableClient::MakeUnversionedInt64Value(index + 2));
                builder.AddValue(NTableClient::MakeUnversionedInt64Value(index + 3));
                builder.AddValue(NTableClient::MakeUnversionedInt64Value(index + 4));
            } else {
                builder.AddValue(NTableClient::MakeUnversionedNullValue());
                builder.AddValue(NTableClient::MakeUnversionedNullValue());
                builder.AddValue(NTableClient::MakeUnversionedNullValue());
            }
            return builder.FinishRow();
        };

        [[maybe_unused]] auto generateRows = [&] (i64 start, i64 end) {
            std::vector<NTableClient::TUnversionedOwningRow> rows;
            for (int i = start; i < end; ++i) {
                rows.push_back(getRowByIndex(i));
            }
            return rows;
        };

        {
            provider1->SetNextResult(oddIndices, false);
            provider2->SetNextResult(evenIndices, false);
            auto [rows, completed] = NConcurrency::WaitFor(joiner->Fetch(New<TServiceLogRange>(), 100)).ValueOrThrow();
            EXPECT_EQ(rows, generateRows(0, 99));
            EXPECT_EQ(completed, false);
        }

        {
            provider1->SetNextResult(oddIndices, true);
            provider2->SetNextResult(evenIndices, false);
            auto [rows, completed] = NConcurrency::WaitFor(joiner->Fetch(New<TServiceLogRange>(), 100)).ValueOrThrow();
            EXPECT_EQ(rows, generateRows(0, 99));
            EXPECT_EQ(completed, false);
        }

        {
            provider1->SetNextResult(oddIndices, false);
            provider2->SetNextResult(evenIndices, true);
            auto [rows, completed] = NConcurrency::WaitFor(joiner->Fetch(New<TServiceLogRange>(), 100)).ValueOrThrow();
            EXPECT_EQ(rows, generateRows(0, 100));
            EXPECT_EQ(completed, false);
        }

        {
            provider1->SetNextResult(oddIndices, true);
            provider2->SetNextResult(evenIndices, true);
            auto [rows, completed] = NConcurrency::WaitFor(joiner->Fetch(New<TServiceLogRange>(), 100)).ValueOrThrow();
            EXPECT_EQ(rows, generateRows(0, 100));
            EXPECT_EQ(completed, true);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
