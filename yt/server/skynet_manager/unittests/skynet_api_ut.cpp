#include <yt/core/test_framework/framework.h>

#include <yt/server/skynet_manager/skynet_api.h>
#include <yt/server/skynet_manager/rb_torrent.h>
#include <yt/server/skynet_manager/config.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/thread_pool.h>
#include <yt/core/concurrency/scheduler.h>

using namespace NYT;
using namespace NYT::NConcurrency;
using namespace NYT::NSkynetManager;

class TTestSkynetApi
    : public ::testing::Test
{
public:
    TActionQueuePtr ActionQueue;
    ISkynetApiPtr Api;

    TThreadPoolPtr Pool;

    virtual void SetUp() override
    {
        ActionQueue = New<TActionQueue>();

        Pool = New<TThreadPool>(16, "SkynetApi");

        auto config = New<TSkynetManagerConfig>();
        config->SetDefaults();

        Api = CreateShellSkynetApi(Pool->GetInvoker(),
            config->SkynetPythonInterpreterPath,
            config->SkynetMdsToolPath);
    }

    virtual void TearDown() override
    {
        ActionQueue->Shutdown();
    }
};

TSkynetRbTorrent CreateSampleTorrent(const TString& smallContent)
{
    TString file1 = smallContent;
    TFileMeta file1Meta;
    file1Meta.FileSize = file1.Size();
    file1Meta.MD5 = TMD5Hasher().Append(file1).GetDigest();
    file1Meta.SHA1.emplace_back(TSHA1Hasher().Append(file1).GetDigest());

    TSkynetShareMeta meta;
    meta.Files["file1"] = file1Meta;

    return GenerateResource(meta);
}

TEST_F(TTestSkynetApi, DISABLED_SimpleAddRemove)
{
    auto sampleTorrent = CreateSampleTorrent("some data");

    auto asyncResult = Api->AddResource(
        sampleTorrent.RbTorrentId,
        "http://localhost:5000/",
        sampleTorrent.BencodedTorrentMeta);

    WaitFor(asyncResult).ThrowOnError();

    WaitFor(Api->RemoveResource(sampleTorrent.RbTorrentId)).ThrowOnError();
}

TEST_F(TTestSkynetApi, DISABLED_MillionAdds)
{
    const int N = 10000;
    const int BatchSize = 100;
    std::vector<TFuture<void>> requests;

    auto wait = [&] (auto result) {
        requests.emplace_back(result);
        if (requests.size() > BatchSize) {
            Combine(requests).Get();
            requests.clear();
        }
    };

    auto listed = WaitFor(Api->ListResources()).ValueOrThrow();
    for (auto torrent : listed) {
        wait(Api->RemoveResource(torrent));
    }

    for (size_t i = 0; i < N; ++i) {
        auto torrent = CreateSampleTorrent(Format("test%v", i));

        wait(Api->AddResource(
            torrent.RbTorrentId,
            "http://localhost:5000/",
            torrent.BencodedTorrentMeta));
    }
}

TEST_F(TTestSkynetApi, DISABLED_ManyAddsAndList)
{
    std::vector<TSkynetRbTorrent> torrents = {
        CreateSampleTorrent("aaa"),
        CreateSampleTorrent("bbb"),
        CreateSampleTorrent("ccc")
    };

    for (auto torrent : torrents) {
        WaitFor(Api->AddResource(
            torrent.RbTorrentId,
            "http://localhost:5000/",
            torrent.BencodedTorrentMeta))
            .ThrowOnError();
    }

    auto listed = WaitFor(Api->ListResources()).ValueOrThrow();

    for (auto torrent : torrents) {
        ASSERT_NE(std::find(listed.begin(), listed.end(), torrent.RbTorrentId),
            listed.end());
    }

    for (auto torrent : torrents) {
        WaitFor(Api->RemoveResource(torrent.RbTorrentId))
            .ThrowOnError();
    }
}
