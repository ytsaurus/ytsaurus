#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/tests/cpp/api_test_base.h>

#include <yt/yt/server/lib/hydra/config.h>
#include <yt/yt/server/lib/hydra/changelog.h>
#include <yt/yt/server/lib/hydra/remote_changelog_store.h>

#include <yt/yt/server/lib/security_server/resource_limits_manager.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/ytree/attributes.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace {

using namespace NConcurrency;
using namespace NCppTests;
using namespace NHydra;
using namespace NObjectClient;
using namespace NSecurityServer;
using namespace NTransactionClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TDummyResourceLimitsManager
    : public IResourceLimitsManager
{
public:
    virtual void ValidateResourceLimits(
        const TString& /*account*/,
        const TString& /*mediumName*/,
        const std::optional<TString>& /*tabletCellBundle*/,
        NTabletClient::EInMemoryMode /*inMemoryMode*/)
    { }
};

////////////////////////////////////////////////////////////////////////////////

class TRemoteChangelogStoreTest
    : public TApiTestBase
{
public:
    TRemoteChangelogStoreTest()
    {
        constexpr static int RecordCount = 10;
        Records_.reserve(RecordCount);
        for (int recordIndex = 0; recordIndex < RecordCount; ++recordIndex) {
            Records_.push_back(TSharedRef::FromString(ToString(recordIndex)));
        }

        Options_->ChangelogAccount = "sys";
        Options_->ChangelogErasureCodec = NErasure::ECodec::None;
        Options_->ChangelogReplicationFactor = 3;
        Options_->ChangelogReadQuorum = 2;
        Options_->ChangelogWriteQuorum = 2;
    }

protected:
    std::vector<TSharedRef> Records_;

    TRemoteChangelogStoreConfigPtr Config_ = New<TRemoteChangelogStoreConfig>();
    TRemoteChangelogStoreOptionsPtr Options_ = New<TRemoteChangelogStoreOptions>();

    void SetUp() override
    {
        WaitFor(Client_->CreateNode(GetChangelogsPath(), EObjectType::MapNode))
            .ThrowOnError();
    }

    void TearDown() override
    {
        auto tryRemove = [&] {
            return WaitFor(Client_->RemoveNode(GetChangelogsPath())).IsOK();
        };
        WaitForPredicate(tryRemove, /*iteartionCount*/ 100, /*period*/ TDuration::MilliSeconds(200));
    }

    IChangelogStoreFactoryPtr CreateChangelogStoreFactory(TTransactionId prerequisiteTransactionId)
    {
        return CreateRemoteChangelogStoreFactory(
            Config_,
            Options_,
            GetChangelogsPath(),
            Client_,
            New<TDummyResourceLimitsManager>(),
            prerequisiteTransactionId);
    }

    IChangelogStorePtr LockStoreFactory(const IChangelogStoreFactoryPtr& factory)
    {
        IChangelogStorePtr store;

        auto tryLock = [&] {
            auto storeOrError = WaitFor(factory->Lock());
            if (storeOrError.IsOK()) {
                store = storeOrError.Value();
                return true;
            } else {
                return false;
            }
        };
        WaitForPredicate(tryLock, /*iteartionCount*/ 100, /*period*/ TDuration::MilliSeconds(200));

        YT_VERIFY(store);
        return store;
    }

    void CreateChangelog(int changelogIndex, int recordCount)
    {
        auto prerequisiteTransaction = WaitFor(Client_->StartTransaction(ETransactionType::Master))
            .ValueOrThrow();
        auto changelogStoreFactory = CreateChangelogStoreFactory(prerequisiteTransaction->GetId());
        auto changelogStore = LockStoreFactory(changelogStoreFactory);
        auto changelog = WaitFor(changelogStore->CreateChangelog(changelogIndex))
            .ValueOrThrow();

        WaitFor(changelog->Append(MakeRange(Records_.begin(), Records_.begin() + recordCount)))
            .ThrowOnError();
        WaitFor(changelog->Close())
            .ThrowOnError();
    }

    static TYPath GetChangelogsPath()
    {
        return "//changelogs";
    }

    TYPath GetChangelogPath(int changelogIndex) const
    {
        return Format("%v/%09d",
            GetChangelogsPath(),
            changelogIndex);
    }

    void WaitUntilSealed(int changelogIndex)
    {
        auto checkSealed = [&] {
            NApi::TGetNodeOptions options{
                .Attributes = std::vector<TString>{"sealed"},
            };
            auto path = GetChangelogPath(changelogIndex);
            auto rspOrError = WaitFor(Client_->GetNode(path, options));
            if (!rspOrError.IsOK()) {
                return false;
            }

            auto response = ConvertTo<INodePtr>(rspOrError.Value());
            return response->Attributes().Get<bool>("sealed");
        };
        WaitForPredicate(checkSealed, /*iteartionCount*/ 100, /*period*/ TDuration::MilliSeconds(200));
    }

    void CheckChangelog(const IChangelogPtr& changelog, int recordCount)
    {
        std::vector<TSharedRef> records;
        while (true) {
            auto newRecords = WaitFor(changelog->Read(/*firstRecordId*/ records.size(), /*maxRecords*/ 100'000, /*maxBytes*/ 100'000))
                .ValueOrThrow();
            if (newRecords.empty()) {
                break;
            }
            records.insert(records.end(), newRecords.begin(), newRecords.end());
        }

        EXPECT_EQ(std::ssize(records), recordCount);
        for (auto recordIndex = 0; recordIndex < recordCount; ++recordIndex) {
            EXPECT_TRUE(TRef::AreBitwiseEqual(records[recordIndex], Records_[recordIndex]));
        }
    }
};

TEST_F(TRemoteChangelogStoreTest, TestReadWrite)
{
    auto prerequisiteTransaction = WaitFor(Client_->StartTransaction(ETransactionType::Master))
        .ValueOrThrow();

    auto changelogStoreFactory = CreateChangelogStoreFactory(prerequisiteTransaction->GetId());
    auto changelogStore = LockStoreFactory(changelogStoreFactory);
    auto changelog = WaitFor(changelogStore->CreateChangelog(/*id*/ 1))
        .ValueOrThrow();

    WaitFor(changelog->Append(MakeRange(Records_.begin(), Records_.begin() + 2)))
        .ThrowOnError();
    WaitFor(changelog->Close())
        .ThrowOnError();

    WaitFor(prerequisiteTransaction->Abort())
        .ThrowOnError();

    prerequisiteTransaction = WaitFor(Client_->StartTransaction(ETransactionType::Master))
        .ValueOrThrow();

    changelogStoreFactory = CreateChangelogStoreFactory(prerequisiteTransaction->GetId());
    changelogStore = LockStoreFactory(changelogStoreFactory);
    EXPECT_EQ(changelogStore->GetReachableVersion(), TVersion(1, 2));
    changelog = WaitFor(changelogStore->OpenChangelog(/*id*/ 1))
        .ValueOrThrow();
    CheckChangelog(changelog, /*recordCount*/ 2);
}

TEST_F(TRemoteChangelogStoreTest, TestTwoConcurrentWritersAreForbidden)
{
    auto createAndLockStore = [this] (TTransactionId prerequisiteTransactionId) {
        auto changelogStoreFactory = CreateChangelogStoreFactory(prerequisiteTransactionId);
        return WaitFor(changelogStoreFactory->Lock())
            .ValueOrThrow();
    };

    auto prerequisiteTransaction1 = WaitFor(Client_->StartTransaction(ETransactionType::Master))
        .ValueOrThrow();
    auto store1 = createAndLockStore(prerequisiteTransaction1->GetId());
    WaitFor(prerequisiteTransaction1->Abort())
        .ThrowOnError();

    auto prerequisiteTransaction2 = WaitFor(Client_->StartTransaction(ETransactionType::Master))
        .ValueOrThrow();
    auto store2 = createAndLockStore(prerequisiteTransaction2->GetId());
    auto changelog2 = WaitFor(store2->CreateChangelog(/*id*/ 2))
        .ValueOrThrow();

    // Prerequistie transaction of #store1 is aborted, no more changelogs
    // can be allocated.
    EXPECT_FALSE(WaitFor(store1->CreateChangelog(/*id*/ 2)).IsOK());
    EXPECT_FALSE(WaitFor(store1->CreateChangelog(/*id*/ 3)).IsOK());
}

TEST_F(TRemoteChangelogStoreTest, TestTruncate)
{
    CreateChangelog(/*changelogIndex*/ 42, /*recordCount*/ 7);
    WaitUntilSealed(/*changelogIndex*/ 42);

    auto prerequisiteTransaction = WaitFor(Client_->StartTransaction(ETransactionType::Master))
        .ValueOrThrow();
    auto changelogStoreFactory = CreateChangelogStoreFactory(prerequisiteTransaction->GetId());
    auto changelogStore = LockStoreFactory(changelogStoreFactory);
    auto changelog = WaitFor(changelogStore->OpenChangelog(/*id*/ 42))
        .ValueOrThrow();

    CheckChangelog(changelog, 7);

    WaitFor(changelog->Truncate(/*recordCount*/ 5))
        .ThrowOnError();

    CheckChangelog(changelog, 5);

    WaitFor(changelog->Truncate(/*recordCount*/ 6))
        .ThrowOnError();

    CheckChangelog(changelog, 5);
}

TEST_F(TRemoteChangelogStoreTest, TestAppend)
{
    CreateChangelog(/*changelogIndex*/ 25, /*recordCount*/ 6);
    WaitUntilSealed(/*changelogIndex*/ 25);

    auto prerequisiteTransaction = WaitFor(Client_->StartTransaction(ETransactionType::Master))
        .ValueOrThrow();
    auto changelogStoreFactory = CreateChangelogStoreFactory(prerequisiteTransaction->GetId());
    auto changelogStore = LockStoreFactory(changelogStoreFactory);
    auto changelog = WaitFor(changelogStore->OpenChangelog(/*id*/ 25))
        .ValueOrThrow();
    WaitFor(changelog->Append(MakeRange(Records_.begin() + 6, Records_.begin() + 8)))
        .ThrowOnError();
    WaitFor(changelog->Flush())
        .ThrowOnError();

    CheckChangelog(changelog, 8);
}

TEST_F(TRemoteChangelogStoreTest, TestAppendPrerequisiteCheck)
{
    CreateChangelog(/*changelogIndex*/ 28, /*recordCount*/ 6);
    WaitUntilSealed(/*changelogIndex*/ 28);

    auto prerequisiteTransaction = WaitFor(Client_->StartTransaction(ETransactionType::Master))
        .ValueOrThrow();
    auto changelogStoreFactory = CreateChangelogStoreFactory(prerequisiteTransaction->GetId());
    auto changelogStore = LockStoreFactory(changelogStoreFactory);
    auto changelog = WaitFor(changelogStore->OpenChangelog(/*id*/ 28))
        .ValueOrThrow();
    WaitFor(prerequisiteTransaction->Abort())
        .ThrowOnError();
    EXPECT_FALSE(WaitFor(changelog->Append(MakeRange(Records_.begin() + 6, Records_.begin() + 8))).IsOK());

    CheckChangelog(changelog, 6);
}

TEST_F(TRemoteChangelogStoreTest, TestReadOnlyStore)
{
    CreateChangelog(/*changelogIndex*/ 42, /*recordCount*/ 6);
    WaitUntilSealed(/*changelogIndex*/ 42);

    auto changelogStoreFactory = CreateChangelogStoreFactory(NullTransactionId);
    auto changelogStore = LockStoreFactory(changelogStoreFactory);
    auto changelog = WaitFor(changelogStore->OpenChangelog(/*id*/ 42))
        .ValueOrThrow();

    EXPECT_FALSE(WaitFor(changelog->Truncate(/*recordCount*/ 5)).IsOK());
    EXPECT_FALSE(WaitFor(changelog->Append(MakeRange(Records_.begin() + 6, Records_.begin() + 8))).IsOK());

    CheckChangelog(changelog, 6);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
