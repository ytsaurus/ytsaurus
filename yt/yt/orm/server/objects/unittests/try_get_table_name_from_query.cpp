#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/orm/server/objects/helpers.h>

namespace NYT::NOrm::NServer::NObjects::NTests {

using namespace NYT::NOrm::NClient::NObjects;

////////////////////////////////////////////////////////////////////////////////

TEST(TTryGetTableNameFromQueryTest, Simple)
{
    EXPECT_EQ("table_name", TryGetTableNameFromQuery("[column] from [table_name]"));
    EXPECT_EQ("table_name", TryGetTableNameFromQuery("[column] from `table_name`"));
    EXPECT_EQ("table_name[[]]", TryGetTableNameFromQuery("[column] from [table_name[[]]]"));
    EXPECT_EQ("table_name[[]]", TryGetTableNameFromQuery("[column] from `table_name[[]]`"));
    EXPECT_EQ("table_name`", TryGetTableNameFromQuery("[column] from [table_name`]"));
    EXPECT_EQ("table_name\\`", TryGetTableNameFromQuery("[column] from `table_name\\``"));

    EXPECT_EQ("", TryGetTableNameFromQuery(""));
    EXPECT_EQ("", TryGetTableNameFromQuery("[column]"));
    EXPECT_EQ("", TryGetTableNameFromQuery("[column] from"));
    EXPECT_EQ("", TryGetTableNameFromQuery("[column] from "));
    EXPECT_EQ("", TryGetTableNameFromQuery("[column] from ["));
    EXPECT_EQ("", TryGetTableNameFromQuery("[column] from `"));
    EXPECT_EQ("", TryGetTableNameFromQuery("[column] from [table_name"));
    EXPECT_EQ("", TryGetTableNameFromQuery("[column] from [table_name[[[]"));
    EXPECT_EQ("", TryGetTableNameFromQuery("[column] from `table_name"));
    EXPECT_EQ("", TryGetTableNameFromQuery("[column] from `table_name\\"));
    EXPECT_EQ("", TryGetTableNameFromQuery("[column] from `table_name\\`"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects::NTests
