#include <yt/core/test_framework/framework.h>

#include <yt/server/clickhouse_server/helpers/poco_config.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NClickHouseServer {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TPocoConfig, Simple)
{
    auto config = ConvertToPocoConfig(BuildYsonNodeFluently()
        .BeginMap()
            .Item("key1").Value(42)
            .Item("key2").Value(0.5)
            .Item("key3").BeginMap()
                .Item("subkey1").Value(false)
                .Item("subkey2").Value("asd")
                .Item("subkey3").Value(23u)
            .EndMap()
            .Item("key4").BeginList()
                .Item().Value(42)
                .Item().Value("qwe")
            .EndList()
        .EndMap()
    );

    EXPECT_EQ(42, config->getInt("key1"));
    EXPECT_EQ(42u, config->getUInt("key1"));
    EXPECT_EQ("42", config->getString("key1"));

    EXPECT_EQ(23, config->getInt("key3.subkey3"));
    EXPECT_EQ(23u, config->getUInt("key3.subkey3"));
    EXPECT_EQ("23", config->getString("key3.subkey3"));

    EXPECT_EQ(0.5, config->getDouble("key2"));

    EXPECT_EQ(false, config->getBool("key3.subkey1"));

    EXPECT_EQ("asd", config->getString("key3.subkey2"));

    EXPECT_THROW(config->getInt("key3.subkey2"), std::exception);
    EXPECT_THROW(config->getInt("key2.subkey1"), std::exception);
    EXPECT_THROW(config->getInt("key7"), std::exception);
    EXPECT_NO_THROW(config->getString("key4"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

