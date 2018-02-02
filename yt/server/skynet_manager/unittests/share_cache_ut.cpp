#include <yt/core/test_framework/framework.h>

#include <yt/core/actions/invoker_util.h>

#include <yt/core/concurrency/action_queue.h>

#include <yt/server/skynet_manager/share_cache.h>
#include <yt/server/skynet_manager/rb_torrent.h>

using namespace NYT;
using namespace NYT::NConcurrency;
using namespace NYT::NSkynetManager;

using namespace testing;

struct TShareHostMock : public IShareHost
{
    IInvokerPtr Invoker;

    virtual IInvokerPtr GetInvoker() override
    {
        return Invoker;
    }

    MOCK_METHOD2(ReadMeta, std::pair<TSkynetShareMeta, std::vector<TFileOffset>>(const TString&, const NYPath::TRichYPath&));

    MOCK_METHOD2(CheckTableAttributes, i64(const TString&, const NYPath::TRichYPath&));

    MOCK_METHOD4(AddShareToCypress, void(const TString&, const TString&, const NYPath::TRichYPath&, i64));
    MOCK_METHOD2(RemoveShareFromCypress, void(const TString&, const TString&));

    MOCK_METHOD2(AddResourceToSkynet, void(const TString&, const TString&));
    MOCK_METHOD1(RemoveResourceFromSkynet, void(const TString&));
};

DECLARE_REFCOUNTED_STRUCT(TShareHostMock)
DEFINE_REFCOUNTED_TYPE(TShareHostMock)

struct TShareCacheTest : public ::testing::Test
{
    TShareHostMockPtr HostMock;
    TTombstoneCacheConfigPtr Config;

    TShareKey TestTable42 = {"local", "//tmp/test_table", 42};
    TShareKey TestTable43 = {"local", "//tmp/test_table", 43};

    TShareKey DuplicateTable1234 = {"local", "//tmp/duplicate", 1234};

    TShareCacheTest()
    {
        HostMock = New<TShareHostMock>();
        HostMock->Invoker = GetSyncInvoker();

        Config = New<TTombstoneCacheConfig>();
        Config->SetDefaults();
    }

    std::pair<TSkynetShareMeta, std::vector<TFileOffset>> MakeMeta(const TString& filename, const TString& content)
    {
        TFileMeta fileMeta;
        fileMeta.FileSize = content.Size();
        fileMeta.MD5 = TMD5Hasher().Append(content).GetDigest();
        fileMeta.SHA1.emplace_back(TSHA1Hasher().Append(content).GetDigest());

        TSkynetShareMeta meta;
        meta.Files[filename] = fileMeta;

        TFileOffset offset{filename, 0, 1, content.size()};
        return {meta, std::vector<TFileOffset>{offset}};
    }
};

TEST_F(TShareCacheTest, Share)
{
    auto cache = New<TShareCache>(HostMock, Config);

    TString rbTorrentId;

    EXPECT_CALL(*HostMock, ReadMeta(_, _))
        .WillOnce(InvokeWithoutArgs([this] { return MakeMeta("test.txt", "abc"); }));
    EXPECT_CALL(*HostMock, AddShareToCypress("local", _, _, 42));
    EXPECT_CALL(*HostMock, AddResourceToSkynet(_, _))
        .WillOnce(SaveArg<0>(&rbTorrentId));

    auto result = cache->TryShare(TestTable42, true);
    EXPECT_FALSE(result);

    result = cache->TryShare(TestTable42, true);
    ASSERT_TRUE(result);
    ASSERT_NE("", *result);

    auto discoverInfo = cache->TryDiscover(rbTorrentId);
    ASSERT_TRUE(discoverInfo);
    EXPECT_TRUE(discoverInfo->Offsets);
    EXPECT_EQ("local", discoverInfo->Cluster);
}

TEST_F(TShareCacheTest, FailedShare)
{
    auto cache = New<TShareCache>(HostMock, Config);

    EXPECT_CALL(*HostMock, ReadMeta(_, _))
        .WillRepeatedly(Throw(std::runtime_error("Table not found")));

    auto result = cache->TryShare(TestTable42, true);
    EXPECT_FALSE(result);

    EXPECT_FALSE(cache->CheckTombstone(TestTable42).IsOK());
}

TEST_F(TShareCacheTest, ContentConflict)
{
    auto meta = MakeMeta("test.txt", "abc");
    auto resource = GenerateResource(meta.first);

    auto cache = New<TShareCache>(HostMock, Config);

    EXPECT_CALL(*HostMock, ReadMeta(_, _))
        .Times(3)
        .WillRepeatedly(Return(meta));
    EXPECT_CALL(*HostMock, AddShareToCypress("local", _, _, _))
        .Times(3);
    EXPECT_CALL(*HostMock, AddResourceToSkynet(_, _))
        .Times(3);

    cache->TryShare(TestTable42, true);
    cache->TryShare(DuplicateTable1234, true);

    EXPECT_FALSE(cache->TryShare(TestTable42, true));
}

TEST_F(TShareCacheTest, ShareRecoverRace)
{
    for (bool failure : {true, false}) {
        auto meta = MakeMeta("test.txt", "abc");
        auto resource = GenerateResource(meta.first);
    
        auto cache = New<TShareCache>(HostMock, Config);
        EXPECT_CALL(*HostMock, ReadMeta(_, _))
            .WillRepeatedly(Return(meta));
        EXPECT_CALL(*HostMock, AddShareToCypress("local", _, _, 42))
            .Times(AtLeast(1));
        EXPECT_CALL(*HostMock, AddResourceToSkynet(_, _))
            .WillRepeatedly(InvokeWithoutArgs([&] {
                cache->TryShare(TestTable42, true);
                if (failure) {
                    throw std::runtime_error("Skynet manager is not avaliable");
                }
            }));

        auto result = cache->TryShare(TestTable42, true);
        result = cache->TryShare(TestTable42, true);

        if (failure) {
            EXPECT_FALSE(result);
        } else {
            EXPECT_TRUE(result);
        }
    }
}

TEST_F(TShareCacheTest, UnshareAfterShare)
{
    auto cache = New<TShareCache>(HostMock, Config);

    TString rbTorrentId;

    EXPECT_CALL(*HostMock, ReadMeta(_, _))
        .WillOnce(InvokeWithoutArgs([this] { return MakeMeta("test.txt", "abc"); }));
    EXPECT_CALL(*HostMock, AddShareToCypress("local", _, _, 42));
    EXPECT_CALL(*HostMock, AddResourceToSkynet(_, _))
        .WillOnce(SaveArg<0>(&rbTorrentId));

    cache->TryShare(TestTable42, true);

    EXPECT_CALL(*HostMock, RemoveShareFromCypress("local", rbTorrentId));
    EXPECT_CALL(*HostMock, RemoveResourceFromSkynet(rbTorrentId));

    cache->Unshare(TestTable42);

    EXPECT_FALSE(cache->TryDiscover(rbTorrentId));
}

TEST_F(TShareCacheTest, UnshareInTheMiddleOfShare)
{
    auto cache = New<TShareCache>(HostMock, Config);

    TString rbTorrentId;

    EXPECT_CALL(*HostMock, ReadMeta(_, _))
        .WillOnce(InvokeWithoutArgs([this] { return MakeMeta("test.txt", "abc"); }));
    EXPECT_CALL(*HostMock, AddShareToCypress("local", _, _, 42));
    EXPECT_CALL(*HostMock, AddResourceToSkynet(_, _))
        .WillOnce(DoAll(
            SaveArg<0>(&rbTorrentId),
            InvokeWithoutArgs([&] {
                cache->Unshare(TestTable42);
            })));

    EXPECT_CALL(*HostMock, RemoveShareFromCypress("local", _));
    EXPECT_CALL(*HostMock, RemoveResourceFromSkynet(_));

    cache->TryShare(TestTable42, true);

    EXPECT_FALSE(cache->TryDiscover(rbTorrentId));
}

