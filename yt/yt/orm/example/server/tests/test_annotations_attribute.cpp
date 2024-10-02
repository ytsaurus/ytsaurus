#include "common.h"

#include <yt/yt/orm/server/objects/object.h>
#include <yt/yt/orm/server/objects/transaction.h>
#include <yt/yt/orm/server/objects/transaction_manager.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>

namespace NYT::NOrm::NExample::NServer::NTests {

using namespace NYT::NYTree;
using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

class TAnnotationsAttributeTestSuite
    : public ::testing::Test
{ };

////////////////////////////////////////////////////////////////////////////////

TEST_F(TAnnotationsAttributeTestSuite, LoadAllReturnsNewKeys)
{
    auto userKey = CreateUser();
    {
        auto transaction = WaitFor(GetBootstrap()->GetTransactionManager()
            ->StartReadWriteTransaction())
            .ValueOrThrow();
        auto* user = transaction->GetObject(TObjectTypeValues::User, userKey);

        EXPECT_EQ(0u, user->Annotations().LoadAll().size());
        user->Annotations().Store("k1", TYsonString(TStringBuf("value1")));
        WaitFor(transaction->Commit())
            .ValueOrThrow();
    }
    {
        auto transaction = WaitFor(GetBootstrap()->GetTransactionManager()
            ->StartReadWriteTransaction())
            .ValueOrThrow();
        auto* user = transaction->GetObject(TObjectTypeValues::User, userKey);

        std::vector<std::pair<std::string, TYsonString>> expected;

        expected.emplace_back("k1", TYsonString(TStringBuf("value1")));
        EXPECT_EQ(expected, user->Annotations().LoadAll());

        user->Annotations().Store("k2", TYsonString(TStringBuf("value2")));
        expected.emplace_back("k2", TYsonString(TStringBuf("value2")));
        auto actual = user->Annotations().LoadAll();
        std::sort(actual.begin(), actual.end(), [] (const auto& lhs, const auto& rhs) {
            return lhs.first < rhs.first || (lhs.first == rhs.first && lhs.second.AsStringBuf() < rhs.second.AsStringBuf());
        });
        EXPECT_EQ(expected, actual);
    }
}

TEST_F(TAnnotationsAttributeTestSuite, LoadTimestampNoCrashAfterRemove)
{
    auto userKey = CreateUser();
    {
        auto transaction = WaitFor(GetBootstrap()->GetTransactionManager()
            ->StartReadWriteTransaction())
            .ValueOrThrow();
        transaction->GetObject(TObjectTypeValues::User, userKey)
            ->Annotations()
            .Store("k1", TYsonString(TStringBuf("value1")));
        WaitFor(transaction->Commit())
            .ValueOrThrow();
    }
    {
        auto transaction = WaitFor(GetBootstrap()->GetTransactionManager()
            ->StartReadWriteTransaction())
            .ValueOrThrow();
        auto* user = transaction->GetObject(TObjectTypeValues::User, userKey);
        auto oldTimestamp = user->Annotations().LoadTimestamp("k1");
        EXPECT_TRUE(oldTimestamp > 0);
        transaction->RemoveObject(user);
        // Validate for crash. Any return value is OK.
        EXPECT_EQ(oldTimestamp, user->Annotations().LoadTimestamp("k1"));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NTests
