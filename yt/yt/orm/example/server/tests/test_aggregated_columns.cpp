#include "common.h"

#include <yt/yt/orm/example/server/library/autogen/objects.h>

#include <yt/yt/orm/server/objects/transaction_manager.h>

namespace NYT::NOrm::NExample::NServer::NTests {

////////////////////////////////////////////////////////////////////////////////

namespace {

void Check(TCat* cat, int expectedLoadOld, int expectedLoad)
{
    EXPECT_EQ(cat->Spec().FriendCatsCount().LoadOld(), expectedLoadOld);
    EXPECT_EQ(cat->Spec().FriendCatsCount().Load(), expectedLoad);
}

void Store(TCat* cat, EAggregateMode aggregateMode, int storedValue)
{
    cat->Spec().FriendCatsCount().Store(storedValue, aggregateMode);
}

void CheckResult(TObjectKey catKey, int expectedValue)
{
    auto readTransaction = WaitFor(GetBootstrap()->GetTransactionManager()
        ->StartReadOnlyTransaction())
        .ValueOrThrow();
    auto resultedCat = readTransaction->GetTypedObject<TCat>(catKey);
    Check(resultedCat, expectedValue, expectedValue);
}

void UpdateExpectedLoadValue(int& expectedLoadValue, EAggregateMode storedMode, int storedValue) {
    expectedLoadValue = storedMode == EAggregateMode::Override
        ? storedValue
        : expectedLoadValue + storedValue;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TestAggregatedColumnCreateAndUpdateInSameTx
    : public testing::TestWithParam<std::tuple<std::optional<int>, bool, EAggregateMode, bool>>
{ };

TEST_P(TestAggregatedColumnCreateAndUpdateInSameTx, TestAggregateAndOverride)
{
    auto [originalFriendCatsCount, checkBeforeFirstStage, firstStageMode, shouldRemove] = GetParam();
    auto fixedOriginalFriendCatsCount = 0;
    auto expectedLoadValue = originalFriendCatsCount.value_or(0);

    auto transaction = WaitFor(GetBootstrap()->GetTransactionManager()
        ->StartReadWriteTransaction())
        .ValueOrThrow();
    auto* cat = transaction->CreateObjects({{
        .Type = TObjectTypeValues::Cat,
        .Attributes = NYTree::BuildYsonNodeFluently()
            .BeginMap()
                .Item("spec")
                    .BeginMap()
                        .OptionalItem("friend_cats_count", originalFriendCatsCount)
                    .EndMap()
            .EndMap()
            ->AsMap()}},
        /*transactionCallContext*/ {})[0]->As<TCat>();
    auto catKey = cat->GetKey();

    if (checkBeforeFirstStage) {
        Check(cat, fixedOriginalFriendCatsCount, expectedLoadValue);
    }
    auto firstStageStoredValue = -11;
    Store(cat, firstStageMode, firstStageStoredValue);
    UpdateExpectedLoadValue(expectedLoadValue, firstStageMode, firstStageStoredValue);

    Check(cat, fixedOriginalFriendCatsCount, expectedLoadValue);

    if (shouldRemove) {
        transaction->RemoveObject(cat);
    }
    EXPECT_NO_THROW(WaitFor(transaction->Commit())
        .ValueOrThrow());

    if (shouldRemove) {
        auto transaction = WaitFor(GetBootstrap()->GetTransactionManager()
            ->StartReadOnlyTransaction())
            .ValueOrThrow();
        auto* object = transaction->GetObject(TObjectTypeValues::Cat, catKey);
        EXPECT_THROW(object->ValidateExists(), TErrorException);
    } else {
        CheckResult(catKey, expectedLoadValue);
    }
}

INSTANTIATE_TEST_SUITE_P(
    TestAggregatedColumnCreateAndUpdateInSameTx,
    TestAggregatedColumnCreateAndUpdateInSameTx,
     ::testing::Combine(
        ::testing::Values(std::nullopt, 0, 18),
        ::testing::Values(false, true),
        ::testing::Values(EAggregateMode::Aggregate, EAggregateMode::Override),
        ::testing::Values(false, true)));

////////////////////////////////////////////////////////////////////////////////

class TestAggregatedColumnOneStage
    : public testing::TestWithParam<std::tuple<std::optional<int>, bool, EAggregateMode>>
{ };

TEST_P(TestAggregatedColumnOneStage, TestAggregateAndOverride)
{
    auto [originalFriendCatsCount, checkBeforeFirstStage, firstStageMode] = GetParam();
    auto fixedOriginalFriendCatsCount = originalFriendCatsCount.value_or(0);
    auto expectedLoadValue = fixedOriginalFriendCatsCount;

    auto catKey = CreateCat(originalFriendCatsCount);

    auto transaction = WaitFor(GetBootstrap()->GetTransactionManager()
        ->StartReadWriteTransaction())
        .ValueOrThrow();
    auto* cat = transaction->GetTypedObject<TCat>(catKey);

    if (checkBeforeFirstStage) {
        Check(cat, fixedOriginalFriendCatsCount, expectedLoadValue);
    }
    auto firstStageStoredValue = 10;
    Store(cat, firstStageMode, firstStageStoredValue);
    UpdateExpectedLoadValue(expectedLoadValue, firstStageMode, firstStageStoredValue);

    Check(cat, fixedOriginalFriendCatsCount, expectedLoadValue);
    EXPECT_NO_THROW(WaitFor(transaction->Commit())
        .ValueOrThrow());

    CheckResult(catKey, expectedLoadValue);
}

INSTANTIATE_TEST_SUITE_P(
    TestAggregatedColumnOneStage,
    TestAggregatedColumnOneStage,
     ::testing::Combine(
        ::testing::Values(std::nullopt, 0, 18),
        ::testing::Values(false, true),
        ::testing::Values(EAggregateMode::Unspecified, EAggregateMode::Aggregate, EAggregateMode::Override)));

////////////////////////////////////////////////////////////////////////////////

class TestAggregatedColumnThreeStages
    : public testing::TestWithParam<
        std::tuple<std::optional<int>, bool, EAggregateMode, bool, EAggregateMode, bool, EAggregateMode>>
{ };

TEST_P(TestAggregatedColumnThreeStages, TestAggregateAndOverride)
{
    auto [originalFriendCatsCount,
        checkBeforeFirstStage,
        firstStageMode,
        checkBeforeSecondStage,
        secondStageMode,
        checkBeforeThirdStage,
        thirdStageMode] = GetParam();
    auto fixedOriginalFriendCatsCount = originalFriendCatsCount.value_or(0);
    auto expectedLoadValue = fixedOriginalFriendCatsCount;

    auto catKey = CreateCat(originalFriendCatsCount);

    auto transaction = WaitFor(GetBootstrap()->GetTransactionManager()
        ->StartReadWriteTransaction())
        .ValueOrThrow();
    auto* cat = transaction->GetTypedObject<TCat>(catKey);

    for (auto [checkBeforeStage, stageMode, stageStoredValue]: {
        std::tuple{checkBeforeFirstStage, firstStageMode, 10},
        std::tuple{checkBeforeSecondStage, secondStageMode, -5},
        std::tuple{checkBeforeThirdStage, thirdStageMode, 20}})
    {
        if (checkBeforeStage) {
            Check(cat, fixedOriginalFriendCatsCount, expectedLoadValue);
        }
        Store(cat, stageMode, stageStoredValue);
        UpdateExpectedLoadValue(expectedLoadValue, stageMode, stageStoredValue);
    }

    Check(cat, fixedOriginalFriendCatsCount, expectedLoadValue);
    EXPECT_NO_THROW(WaitFor(transaction->Commit())
        .ValueOrThrow());

    CheckResult(catKey, expectedLoadValue);
}

INSTANTIATE_TEST_SUITE_P(
    TestAggregatedColumnThreeStages,
    TestAggregatedColumnThreeStages,
     ::testing::Combine(
        ::testing::Values(std::nullopt, 0, 18),
        ::testing::Values(false, true),
        ::testing::Values(EAggregateMode::Aggregate, EAggregateMode::Override),
        ::testing::Values(false, true),
        ::testing::Values(EAggregateMode::Aggregate, EAggregateMode::Override),
        ::testing::Values(false, true),
        ::testing::Values(EAggregateMode::Aggregate, EAggregateMode::Override)));

////////////////////////////////////////////////////////////////////////////////

// Aggregate/Override + (check?) + Aggregate/Override + (check?) +
// + Store + (check?) + Aggregate/Override + (check?) + Aggregate
class TestAggregatedColumnThreeStagesWithStore
    : public testing::TestWithParam<
        std::tuple<std::optional<int>, EAggregateMode, bool, EAggregateMode, bool, bool, EAggregateMode, bool>>
{ };

TEST_P(TestAggregatedColumnThreeStagesWithStore, TestAggregateOverrideWithStore)
{
    auto [originalFriendCatsCount,
        firstStageMode,
        checkBeforeSecondStage,
        secondStageMode,
        checkBeforeStore,
        checkBeforeThirdStage,
        thirdStageMode,
        checkBeforeFourthStage] = GetParam();
    auto fixedOriginalFriendCatsCount = originalFriendCatsCount.value_or(0);
    auto expectedLoadValue = fixedOriginalFriendCatsCount;

    auto catKey = CreateCat(originalFriendCatsCount);

    auto transaction = WaitFor(GetBootstrap()->GetTransactionManager()
        ->StartReadWriteTransaction())
        .ValueOrThrow();
    auto* cat = transaction->GetTypedObject<TCat>(catKey);

    auto firstStageValue = 10;
    Store(cat, firstStageMode, firstStageValue);
    UpdateExpectedLoadValue(expectedLoadValue, firstStageMode, firstStageValue);

    if (checkBeforeSecondStage) {
        Check(cat, fixedOriginalFriendCatsCount, expectedLoadValue);
    }
    auto secondStageValue = -5;
    Store(cat, secondStageMode, secondStageValue);
    UpdateExpectedLoadValue(expectedLoadValue, secondStageMode, secondStageValue);

    if (checkBeforeStore) {
        Check(cat, fixedOriginalFriendCatsCount, expectedLoadValue);
    }
    transaction->GetSession()->ScheduleStore([&] (IStoreContext* /*context*/) {
        transaction->GetSession()->ScheduleStore([&] (IStoreContext* /*context*/) {
            if (checkBeforeThirdStage) {
                Check(cat, fixedOriginalFriendCatsCount, expectedLoadValue);
            }
            auto thirdStageValue = 20;
            Store(cat, thirdStageMode, thirdStageValue);
            UpdateExpectedLoadValue(expectedLoadValue, thirdStageMode, thirdStageValue);

            if (checkBeforeFourthStage) {
                Check(cat, fixedOriginalFriendCatsCount, expectedLoadValue);
            }
            auto fourthStageValue = -11;
            auto fourthStageMode = EAggregateMode::Aggregate;
            Store(cat, fourthStageMode, fourthStageValue);
            UpdateExpectedLoadValue(expectedLoadValue, fourthStageMode, fourthStageValue);

            Check(cat, fixedOriginalFriendCatsCount, expectedLoadValue);
        });
    });

    EXPECT_NO_THROW(WaitFor(transaction->Commit())
        .ValueOrThrow());
    CheckResult(catKey, expectedLoadValue);
}

INSTANTIATE_TEST_SUITE_P(
    TestAggregatedColumnThreeStagesWithStore,
    TestAggregatedColumnThreeStagesWithStore,
     ::testing::Combine(
        ::testing::Values(std::nullopt, 0, 18),
        ::testing::Values(EAggregateMode::Aggregate, EAggregateMode::Override),
        ::testing::Values(false, true),
        ::testing::Values(EAggregateMode::Aggregate, EAggregateMode::Override),
        ::testing::Values(false, true),
        ::testing::Values(false, true),
        ::testing::Values(EAggregateMode::Aggregate, EAggregateMode::Override),
        ::testing::Values(false, true)));

////////////////////////////////////////////////////////////////////////////////

} // NYT::NOrm::NExample::NServer::NTests
