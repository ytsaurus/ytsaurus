#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <yt/cpp/mapreduce/interface/patchable_field.h>

using namespace NYT;
using namespace NYT::NTesting;

////////////////////////////////////////////////////////////////////////////////

TEST(TPatchableFieldTest, HasField) {
    auto config = MakeIntrusive<TConfig>();
    TTestFixture fixture(TCreateClientOptions()
        .Config(config));

    TYPath configPath = fixture.GetWorkingDir() + "/client_config";
    TYPath configProfilePath = configPath + "/" + TPatchableField<i64>::ConfigProfile;

    auto client = fixture.GetClient();

    client->Create(configPath, ENodeType::NT_MAP);
    client->Create(configProfilePath, ENodeType::NT_DOCUMENT);
    client->Set(configProfilePath, TNode()("some_option", 43));

    TPatchableField<i64> field("some_option", 42);

    config->ConfigRemotePatchPath = configPath;

    ASSERT_EQ(field.Get(client), 43);
}

TEST(TPatchableFieldTest, NoField) {
    auto config = MakeIntrusive<TConfig>();
    TTestFixture fixture(TCreateClientOptions()
        .Config(config));

    TYPath configPath = fixture.GetWorkingDir() + "/client_config";
    TYPath configProfilePath = configPath + "/" + TPatchableField<i64>::ConfigProfile;

    auto client = fixture.GetClient();

    client->Create(configPath, ENodeType::NT_MAP);
    client->Create(configProfilePath, ENodeType::NT_DOCUMENT);
    client->Set(configProfilePath, TNode()("some_other_option", true));

    TPatchableField<i64> field("some_option", 42);

    config->ConfigRemotePatchPath = configPath;

    ASSERT_EQ(field.Get(client), 42);
}

TEST(TPatchableFieldTest, NoConfig) {
    auto config = MakeIntrusive<TConfig>();
    TTestFixture fixture(TCreateClientOptions()
        .Config(config));

    TYPath configPath = fixture.GetWorkingDir() + "/client_config";
    TYPath configProfilePath = configPath + "/" + TPatchableField<i64>::ConfigProfile;

    auto client = fixture.GetClient();

    TPatchableField<i64> field("some_option", 42);

    config->ConfigRemotePatchPath = configPath;

    ASSERT_EQ(field.Get(client), 42);
}

TEST(TPatchableFieldTest, NoProfile) {
    auto config = MakeIntrusive<TConfig>();
    TTestFixture fixture(TCreateClientOptions()
        .Config(config));

    TYPath configPath = fixture.GetWorkingDir() + "/client_config";
    TYPath configProfilePath = configPath + "/" + TPatchableField<i64>::ConfigProfile;

    auto client = fixture.GetClient();

    client->Create(configPath, ENodeType::NT_MAP);

    TPatchableField<i64> field("some_option", 42);

    config->ConfigRemotePatchPath = configPath;

    ASSERT_EQ(field.Get(client), 42);
}

////////////////////////////////////////////////////////////////////////////////
