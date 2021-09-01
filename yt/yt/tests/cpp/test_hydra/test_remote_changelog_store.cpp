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

////////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace {

using namespace NConcurrency;
using namespace NCppTests;
using namespace NHydra;
using namespace NObjectClient;
using namespace NSecurityServer;
using namespace NTransactionClient;

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
        WaitFor(Client_->CreateNode("//changelogs", EObjectType::MapNode))
            .ThrowOnError();
    }

    void TearDown() override
    {
        auto tryRemove = [&] {
            return WaitFor(Client_->RemoveNode("//changelogs")).IsOK();
        };
        WaitForPredicate(tryRemove, /*iteartionCount*/ 100, /*period*/ TDuration::MilliSeconds(200));
    }

    IChangelogStoreFactoryPtr CreateChangelogStoreFactory(TTransactionId prerequisiteTransactionId)
    {
        return CreateRemoteChangelogStoreFactory(
            Config_,
            Options_,
            "//changelogs",
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
    auto records = WaitFor(changelog->Read(/*firstRecordId*/ 0, /*maxRecords*/ 1000, /*maxBytes*/ 1000))
        .ValueOrThrow();
    EXPECT_EQ(std::ssize(records), 2);
    EXPECT_TRUE(TRef::AreBitwiseEqual(records[0], Records_[0]));
    EXPECT_TRUE(TRef::AreBitwiseEqual(records[1], Records_[1]));
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

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
