#include <yt/core/test_framework/framework.h>

#include <yt/server/skynet_manager/bootstrap.h>
#include <yt/server/skynet_manager/config.h>
#include <yt/server/skynet_manager/skynet_manager.h>
#include <yt/server/skynet_manager/skynet_api.h>

#include <yt/ytlib/ypath/rich.h>

#include <yt/core/http/http.h>
#include <yt/core/http/server.h>
#include <yt/core/http/client.h>
#include <yt/core/http/config.h>

#include <yt/core/yson/writer.h>

#include <yt/core/ytree/fluent.h>

#include <yt/core/net/listener.h>

#include <yt/core/concurrency/thread_pool_poller.h>
#include <yt/core/concurrency/poller.h>
#include <yt/core/concurrency/action_queue.h>

namespace NYT {
namespace NSkynetManager {

using namespace NNet;
using namespace NHttp;
using namespace NYson;
using namespace NYTree;
using namespace NYPath;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TMockReadTable
    : public IHttpHandler
{
public:
    virtual void HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp) override
    {
        rsp->WriteHeaders(EStatusCode::Ok);

        auto output = CreateBufferedSyncAdapter(rsp);
        auto consumer = CreateYsonWriter(
            output.get(),
            EYsonFormat::Binary,
            EYsonType::ListFragment,
            false,
            false);

        rsp->GetHeaders()->Add(
            "X-Yt-Response-Parameters",
            "{start_row_index=3;}");

        auto yson = BuildYsonListFluently(consumer.get());
        for (int i = 0; i < 10; ++i) {
            yson
                .Item()
                    .BeginMap()
                        .Item("filename").Value("file1")
                        .Item("sha1").Value(TString(20, 'f'))
                        .Item("md5").Value(TString(16, 'x'))
                        .Item("data_size").Value(i == 9 ? 54 : 4_MB)
                    .EndMap();
        }

        for (int i = 0; i < 10; ++i) {
            yson
                .Item()
                    .BeginMap()
                        .Item("filename").Value(Format("small_file%d", i))
                        .Item("sha1").Value(TString(20, 'a'))
                        .Item("md5").Value(TString(16, 'b'))
                        .Item("data_size").Value(1234)
                    .EndMap();
        }

        for (int i = 0; i < 10; ++i) {
            yson
                .Item()
                    .BeginMap()
                        .Item("filename").Value("file2")
                        .Item("sha1").Value(TString(20, '1'))
                        .Item("md5").Value(TString(16, '2'))
                        .Item("data_size").Value(4_MB)
                    .EndMap();
        }

        rsp->GetTrailers()->Add("X-Yt-Response-Code", "0");

        consumer->Flush();
        output->Flush();
        WaitFor(rsp->Close())
            .ThrowOnError();
    }
};

class TMockLocateSkynetShare
    : public IHttpHandler
{
public:
    virtual void HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp) override
    {
        rsp->WriteHeaders(EStatusCode::Ok);
        rsp->GetHeaders()->Add("X-Yt-Response-Code", "0");
        auto asyncWrite = rsp->WriteBody(TSharedRef::FromString(R"EOF(
    {
        nodes=[
            {node_id=3724;addresses={
                 default="n0232-man.hume.yt.yandex.net:10012";
                 fastbone="n0232-man-fb.hume.yt.yandex.net:10012";
            };};
            {node_id=1234;addresses={
                 default="n01-man.hume.yt.yandex.net:10012";
                 fastbone="n01-man-fb.hume.yt.yandex.net:10012";
            };};
            {node_id=23;addresses={
                 default="n02-man.hume.yt.yandex.net:10012";
                 fastbone="n02-man-fb.hume.yt.yandex.net:10012";
            };};
        ];
        chunk_specs=[
            {
                chunk_id="2f31-201fb4-bcf0064-30abe0c8";
                row_index=3;
                row_count=7;
                lower_limit={row_index=2;};
                range_index=0;
                replicas=[3724;1234;23;];
            };
            {
                chunk_id="2f31-201fb4-bcf0064-30abe0c9";
                row_index=10;
                row_count=20;
                range_index=0;
                replicas=[1234;];
            };
            {
                chunk_id="2f31-201fb4-bcf0064-30abe0c0";
                row_index=30;
                row_count=3;
                range_index=0;
                replicas=[23;];
            };
        ];
    }
            )EOF"));

        WaitFor(asyncWrite)
            .ThrowOnError();
    }
};

class TTestSkynetManager
    : public ::testing::Test
{
public:
    IPollerPtr Poller;

    IListenerPtr MockProxyListener;
    IServerPtr MockProxy;

    TString ProxyUrl;

    IClientPtr TestClient;

    virtual void SetUp() override
    {
        Poller = CreateThreadPoolPoller(4, "Test");
        MockProxyListener = CreateListener(
            TNetworkAddress::CreateIPv6Loopback(0),
            Poller);
        MockProxy = CreateServer(
            New<NHttp::TServerConfig>(),
            MockProxyListener,
            Poller);

        MockProxy->AddHandler("/api/v3/read_table", New<TMockReadTable>());
        MockProxy->AddHandler("/api/v3/locate_skynet_share", New<TMockLocateSkynetShare>());

        MockProxy->Start();

        ProxyUrl = "http://localhost:" + ToString(MockProxyListener->Address().GetPort());

        TestClient = CreateClient(New<NHttp::TClientConfig>(), Poller);
    }

    virtual void TearDown() override
    {
        MockProxy->Stop();
        Sleep(TDuration::MilliSeconds(100));
        
        Poller->Shutdown();
    }
};

TEST_F(TTestSkynetManager, ProxyInteraction)
{
    TSkynetShareMeta meta;
    std::vector<TFileOffset> fileOffsets;

    TRichYPath path = "//home/prime/test";
    
    std::tie(meta, fileOffsets) = ReadSkynetMetaFromTable(
        TestClient,
        ProxyUrl,
        "token",
        path);

    ASSERT_EQ(12, meta.Files.size());

    ASSERT_EQ(12, fileOffsets.size());
    ASSERT_EQ("file1", fileOffsets[0].FilePath);
    ASSERT_EQ(3, fileOffsets[0].StartRow);
    ASSERT_EQ("small_file0", fileOffsets[1].FilePath);
    ASSERT_EQ(13, fileOffsets[1].StartRow);

    auto locations = FetchSkynetPartsLocations(
        TestClient,
        ProxyUrl,
        "token",
        path,
        fileOffsets);

    ASSERT_EQ(14, locations.size());
}

class TMockSkynetApi
    : public ISkynetApi
{
public:
    MOCK_METHOD3(AddResource, TFuture<void>(const TString&, const TString&, const TString&));
    MOCK_METHOD1(RemoveResource, TFuture<void>(const TString&));
    MOCK_METHOD0(ListResources, TFuture<std::vector<TString>>());
};

TEST_F(TTestSkynetManager, FullCycle)
{
    using namespace ::testing;

    auto hume = New<TClusterConnectionConfig>();
    hume->Name = "hume";
    hume->ProxyUrl = ProxyUrl;
    
    auto config = New<TSkynetManagerConfig>();
    config->SetDefaults();
    config->Clusters = {hume};
    
    auto bootstrap = New<TBootstrap>(config);

    config->SelfUrl = Format("http://localhost:%v",
        bootstrap->HttpListener->Address().GetPort());

    auto skynetMock = New<TMockSkynetApi>();
    bootstrap->SkynetApi = skynetMock;

    auto callbackUrlPromise = NewPromise<TString>();
    EXPECT_CALL(*skynetMock, AddResource(_, _, _))
        .WillOnce(Invoke([&] (TString , TString callbackUrl, TString){
            callbackUrlPromise.Set(callbackUrl);
            return VoidFuture;
        }));

    bootstrap->Start();

    auto params = New<THeaders>();
    params->Set("X-Yt-Parameters", "{cluster=hume; path=\"//foo/bar\"}");

    auto shareReq = WaitFor(bootstrap->HttpClient->Post(
        config->SelfUrl + "/api/v1/share", EmptySharedRef, params))
        .ValueOrThrow();
    EXPECT_EQ(shareReq->GetStatusCode(), EStatusCode::Ok);

    auto callbackUrl = WaitFor(callbackUrlPromise.ToFuture())
        .ValueOrThrow();

    auto discoverReq = WaitFor(bootstrap->HttpClient->Get(callbackUrl))
        .ValueOrThrow();
   
    EXPECT_EQ(shareReq->GetStatusCode(), EStatusCode::Ok);

    bootstrap->Stop();
    Sleep(TDuration::MilliSeconds(10));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT
