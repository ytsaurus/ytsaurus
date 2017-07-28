#include <yt/core/test_framework/framework.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/service_combiner.h>
#include <yt/core/ytree/ypath_client.h>

namespace NYT {
namespace NYTree {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TEST(TYPathServiceCombinerTest, Simple)
{
    IYPathServicePtr service1 = IYPathService::FromProducer(BIND([] (IYsonConsumer* consumer) {
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("key1").Value(42)
                    .Item("key2").BeginAttributes()
                        .Item("opaque").Value(true)
                    .EndAttributes()
                    .BeginMap()
                        .Item("subkey1").Value("abc")
                        .Item("subkey2").Value(3.1415926)
                    .EndMap()
               .EndMap();
        }));
    IYPathServicePtr service2 = IYPathService::FromProducer(BIND([] (IYsonConsumer* consumer) {
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("key3").Value(-1)
                    .Item("key4").BeginAttributes()
                        .Item("attribute1").Value(-1)
                    .EndAttributes().Entity()
                .EndMap();
        }));
    auto combinedService = New<TServiceCombiner>(std::vector<IYPathServicePtr> { service1, service2 });

    // Give service time to build key mapping.
    Sleep(TDuration::MilliSeconds(100));

    EXPECT_EQ(true, SyncYPathExists(combinedService, ""));
    EXPECT_THROW(SyncYPathExists(combinedService, "/"), std::exception);
    EXPECT_EQ(false, SyncYPathExists(combinedService, "/keyNonExistent"));
    EXPECT_EQ(true, SyncYPathExists(combinedService, "/key1"));
    EXPECT_EQ(true, SyncYPathExists(combinedService, "/key3"));
    EXPECT_EQ(false, SyncYPathExists(combinedService, "/key2/subkeyNonExistent"));
    EXPECT_EQ(true, SyncYPathExists(combinedService, "/key2/subkey1"));
    EXPECT_EQ((std::vector<TString> { "key1", "key2", "key3", "key4" }), SyncYPathList(combinedService, ""));
    EXPECT_EQ((std::vector<TString> { "subkey1", "subkey2" }), SyncYPathList(combinedService, "/key2"));
    EXPECT_THROW(SyncYPathList(combinedService, "/keyNonExistent"), std::exception);
    EXPECT_EQ(ConvertToYsonString(-1, EYsonFormat::Binary), SyncYPathGet(combinedService, "/key4/@attribute1"));
    EXPECT_EQ(ConvertToYsonString("abc", EYsonFormat::Binary), SyncYPathGet(combinedService, "/key2/subkey1"));
    EXPECT_THROW(SyncYPathGet(combinedService, "/"), std::exception);
}

TEST(TYPathServiceCombinerTest, DynamicAndStatic)
{
    IYPathServicePtr dynamicService = GetEphemeralNodeFactory()->CreateMap();
    IYPathServicePtr staticService = IYPathService::FromProducer(BIND([] (IYsonConsumer* consumer) {
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("static_key1").Value(-1)
                    .Item("static_key2").Value(false)
                    .Item("error_key").Value("this key will be shared leading to an error")
                .EndMap();
        }));

    auto combinedService = New<TServiceCombiner>(std::vector<IYPathServicePtr> { staticService, dynamicService }, TDuration::MilliSeconds(100));

    // Give service time to build key mapping.
    Sleep(TDuration::MilliSeconds(200));

    EXPECT_EQ(true, SyncYPathExists(combinedService, "/static_key1"));
    EXPECT_EQ(false, SyncYPathExists(combinedService, "/dynamic_key1"));
    EXPECT_EQ(true, SyncYPathExists(combinedService, "/error_key"));
    EXPECT_EQ((std::vector<TString> { "static_key1", "static_key2", "error_key" }), SyncYPathList(combinedService, ""));

    SyncYPathSet(dynamicService, "/dynamic_key1", ConvertToYsonString(3.1415926));
    SyncYPathSet(dynamicService, "/dynamic_key2", TYsonString("#"));

    // Give service time to rebuild key mapping.
    Sleep(TDuration::MilliSeconds(200));

    EXPECT_EQ(true, SyncYPathExists(combinedService, "/static_key1"));
    EXPECT_EQ(true, SyncYPathExists(combinedService, "/dynamic_key1"));
    EXPECT_EQ(true, SyncYPathExists(combinedService, "/error_key"));
    EXPECT_EQ((std::vector<TString> { "static_key1", "static_key2", "error_key", "dynamic_key1", "dynamic_key2" }), SyncYPathList(combinedService, ""));
    EXPECT_EQ(TYsonString("#"), SyncYPathGet(combinedService, "/dynamic_key2"));

    SyncYPathSet(dynamicService, "/error_key", ConvertToYsonString(42));

    // Give service time to rebuild key mapping and notice two services sharing the same key.
    Sleep(TDuration::MilliSeconds(200));

    EXPECT_THROW(SyncYPathExists(combinedService, "/static_key1"), std::exception);
    EXPECT_THROW(SyncYPathExists(combinedService, "/dynamic_key1"), std::exception);
    EXPECT_THROW(SyncYPathExists(combinedService, "/error_key"), std::exception);
    EXPECT_THROW(SyncYPathGet(combinedService, ""), std::exception);
    EXPECT_THROW(SyncYPathList(combinedService, ""), std::exception);
    EXPECT_THROW(SyncYPathGet(combinedService, "/static_key1"), std::exception);

    SyncYPathRemove(dynamicService, "/error_key");

    // Give service time to return to the normal state.
    Sleep(TDuration::MilliSeconds(200));

    EXPECT_EQ(true, SyncYPathExists(combinedService, "/static_key1"));
    EXPECT_EQ(true, SyncYPathExists(combinedService, "/dynamic_key1"));
    EXPECT_EQ(true, SyncYPathExists(combinedService, "/error_key"));
    EXPECT_EQ((std::vector<TString> { "static_key1", "static_key2", "error_key", "dynamic_key1", "dynamic_key2" }), SyncYPathList(combinedService, ""));
    EXPECT_EQ(TYsonString("#"), SyncYPathGet(combinedService, "/dynamic_key2"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

