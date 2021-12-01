#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/server/lib/hydra/changelog.h>

#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/node/data_node/bootstrap.h>
#include <yt/yt/server/node/data_node/journal_manager.h>
#include <yt/yt/server/node/data_node/config.h>
#include <yt/yt/server/node/data_node/location.h>
#include <yt/yt/server/node/data_node/chunk_store.h>

#include <library/cpp/testing/common/env.h>

namespace NYT::NDataNode {
namespace {

using namespace NClusterNode;

////////////////////////////////////////////////////////////////////////////////

struct TMultiplexedChangelogTest
    : public testing::Test
    , public NClusterNode::TBootstrap
    , public NDataNode::TBootstrap
{
    TStoreLocationPtr Location;
    TJournalManagerPtr JournalManager;

    TMultiplexedChangelogTest()
        : NClusterNode::TBootstrap(
            New<TClusterNodeConfig>(),
            NYTree::ConvertTo<NYTree::INodePtr>(New<TClusterNodeConfig>()))
        , NDataNode::TBootstrap(this)
    {
        auto config = NDataNode::TBootstrap::GetConfig();

        auto locationConfig = New<TStoreLocationConfig>();
        locationConfig->Path = GetOutputPath() / ::testing::UnitTest::GetInstance()->current_test_info()->name() / "store";

        config->ClusterConnection = New<TClusterNodeConnectionConfig>();
        config->ClusterConnection->PrimaryMaster = New<NApi::NNative::TMasterConnectionConfig>();
        config->DataNode->StoreLocations.push_back(locationConfig);

        ControlActionQueue_ = New<NConcurrency::TActionQueue>("Control");
        ChunkStore_ = New<TChunkStore>(config->DataNode, this);

        BIND([this] {
            GetChunkStore()->Initialize();
        })
            .AsyncVia(NDataNode::TBootstrap::GetControlInvoker())
            .Run()
            .Get()
            .ThrowOnError();

        Location = GetChunkStore()->Locations()[0];
        JournalManager = Location->GetJournalManager();
    }

    ~TMultiplexedChangelogTest()
    {
        ControlActionQueue_->Shutdown();
    }
};

TEST_F(TMultiplexedChangelogTest, OpenWriteClose)
{
    for (bool multiplexed : {true, false}) {
        auto journalId = TGuid::Create();

        auto changelog = JournalManager->CreateChangelog(journalId, multiplexed, TWorkloadDescriptor{})
            .Get()
            .ValueOrThrow();

        auto r0 = TSharedRef::FromString("r0");
        auto r1 = TSharedRef::FromString("r1");

        changelog->Append({r0, r1})
            .Get()
            .ThrowOnError();

        changelog->Close()
            .Get()
            .ThrowOnError();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NDataNode
