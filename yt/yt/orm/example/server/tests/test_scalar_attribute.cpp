#include "common.h"

#include <yt/yt/orm/example/server/library/autogen/objects.h>

#include <yt/yt/orm/server/objects/helpers.h>
#include <yt/yt/orm/server/objects/transaction.h>
#include <yt/yt/orm/server/objects/transaction_manager.h>

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NOrm::NExample::NServer::NTests {

using namespace NYT::NYTree;
using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

class TScalarAttributeTestSuiteBase
    : public ::testing::Test
{
protected:
    TObjectKey Key_;

    void SetUp() override
    {
        Key_ = CreateObject(
            TObjectTypeValues::Employer,
            BuildYsonNodeFluently()
                .BeginMap()
                    .Item("meta")
                        .BeginMap()
                            .Item("id").Value("sarah-kerrigan")
                            .Item("email").Value("kerrigan@yandex.ru")
                            .Item("passport").Value("777-for-the-swarm-777")
                        .EndMap()
                .EndMap()
                ->AsMap());
    }
};

class TScalarAttributeDisplayName
    : public TScalarAttributeTestSuiteBase
{
    void TearDown() override
    {
        auto tx = WaitFor(GetBootstrap()->GetTransactionManager()
            ->StartReadWriteTransaction())
            .ValueOrThrow();
        tx->RemoveObject(tx->GetObject(TObjectTypeValues::Employer, Key_));
        WaitFor(tx->Commit())
            .ValueOrThrow();
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TScalarAttributeDisplayName, ObjectDisplayNameLoadErrorSuppression)
{
    {
        auto transaction = WaitFor(GetBootstrap()->GetTransactionManager()
            ->StartReadWriteTransaction())
            .ValueOrThrow();

        auto* employer = transaction->GetObject(
            TObjectTypeValues::Employer,
            Key_);

        auto nameNode = GetEphemeralNodeFactory()->CreateString();
        nameNode->SetValue(TString("Sarah Kerrigan"));

        transaction->UpdateObject(
            employer,
            /*requests*/ {
                TSetUpdateRequest{
                    .Path = "/meta/name",
                    .Value = std::move(nameNode),
                },
            });

        WaitFor(transaction->Commit())
            .ValueOrThrow();
    }
    {
        auto transaction = WaitFor(GetBootstrap()->GetTransactionManager()
            ->StartReadOnlyTransaction())
            .ValueOrThrow();
        transaction->GetSession()->SetTestingStorageOptions(TTestingStorageOptions{
            .FailSelects = true,
            .FailLookups = true,
        });
        auto* employer = transaction->GetObject(
            TObjectTypeValues::Employer,
            Key_);
        auto employerDisplayName = employer->GetDisplayName();
        EXPECT_TRUE(employerDisplayName);
    }
    {
        auto transaction = WaitFor(GetBootstrap()->GetTransactionManager()
            ->StartReadOnlyTransaction())
            .ValueOrThrow();
        auto* employer = transaction->GetObject(
            TObjectTypeValues::Employer,
            Key_);
        auto employerDisplayName = employer->GetDisplayName();
        EXPECT_NE(Key_.ToString(), employerDisplayName);
        EXPECT_TRUE(employerDisplayName);
    }
}

class TScalarAttributeLocks
    : public TScalarAttributeTestSuiteBase
{};

TEST_F(TScalarAttributeLocks, TestLocks)
{
    auto tx1 = WaitFor(GetBootstrap()->GetTransactionManager()
        ->StartReadWriteTransaction())
        .ValueOrThrow();
    tx1->RemoveObject(tx1->GetObject(TObjectTypeValues::Employer, Key_));

    auto tx2 = WaitFor(GetBootstrap()->GetTransactionManager()
        ->StartReadWriteTransaction())
        .ValueOrThrow();
    auto* employer2 = tx2->GetObject(TObjectTypeValues::Employer, Key_);
    employer2->ExistenceLock().Lock(NTableClient::ELockType::SharedWeak);
    // Check double lock.
    tx2->GetSession()->ScheduleStore([&] (IStoreContext* /*context*/) {
        employer2->ExistenceLock().Lock(NTableClient::ELockType::SharedStrong);
        tx2->GetSession()->ScheduleStore([&] (IStoreContext* /*context*/) {
            employer2->ExistenceLock().Lock(NTableClient::ELockType::SharedStrong);
        });
    });
    WaitFor(tx2->Commit())
        .ValueOrThrow();

    EXPECT_THROW_WITH_SUBSTRING(
        WaitFor(tx1->Commit())
            .ValueOrThrow(),
        "Write failed due to concurrent read lock");
}

TEST(TScalarAttributeLoads, TestLoads)
{
    auto key = CreateObject(
        TObjectTypeValues::Cat,
        BuildYsonNodeFluently()
            .BeginMap()
                .Item("spec")
                    .BeginMap()
                        .OptionalItem("mood", "playful")
                        .OptionalItem("health_condition", "healthy")
                    .EndMap()
            .EndMap()
            ->AsMap());

    auto transaction = WaitFor(GetBootstrap()->GetTransactionManager()
        ->StartReadOnlyTransaction())
        .ValueOrThrow();
    auto* cat = transaction->GetTypedObject<TCat>(key);

    auto mood = cat->Spec().Mood().Load();
    auto moodTimestamp = cat->Spec().Mood().LoadTimestamp();
    auto statistics = transaction->FlushPerformanceStatistics();
    EXPECT_EQ(mood, EMood::Playful);
    // +2 due to loads from TObjectExistenceChecker and TFinalizationChecker.
    EXPECT_EQ(statistics.ReadPhaseCount, 2 + 2);

    auto healthConditionTimestamp = cat->Spec().HealthCondition().LoadTimestamp();
    auto healthCondition = cat->Spec().HealthCondition().Load();
    statistics = transaction->FlushPerformanceStatistics();
    EXPECT_EQ(healthCondition, EHealthCondition::Healthy);
    EXPECT_EQ(moodTimestamp, healthConditionTimestamp);
    EXPECT_EQ(statistics.ReadPhaseCount, 1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NTests
