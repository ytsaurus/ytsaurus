#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/orm/server/objects/batch_size_backoff.h>
#include <yt/yt/orm/server/objects/config.h>

using namespace NYT::NOrm::NClient::NObjects;

namespace NYT::NOrm::NServer::NObjects::NTests {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger Logger("BatchSizeBackoffTest");

////////////////////////////////////////////////////////////////////////////////

TEST(TBatchSizeBackoffTest, StepAndLimit)
{
    auto config = ConvertTo<TBatchSizeBackoffConfigPtr>(NYTree::BuildYsonNodeFluently()
        .BeginMap()
            .Item("batch_size_limit").Value(5)
            .Item("batch_size_multiplier").Value(2)
            .Item("batch_size_additive").Value(1)
        .EndMap());

    TBatchSizeBackoff batchSize(Logger, config, /*initialSize*/ 1);
    ASSERT_EQ(*batchSize, 1);

    batchSize.Next();
    ASSERT_EQ(*batchSize, 2);

    batchSize.Next();
    ASSERT_EQ(*batchSize, 4);

    batchSize.Next();
    ASSERT_EQ(*batchSize, 5);

    batchSize.RestrictLimit(3);
    ASSERT_EQ(*batchSize, 3);

    batchSize.Next();
    ASSERT_EQ(*batchSize, 3);
}

TEST(TBatchSizeBackoffTest, AdditiveThenExponential)
{
    auto config = ConvertTo<TBatchSizeBackoffConfigPtr>(NYTree::BuildYsonNodeFluently()
        .BeginMap()
            .Item("batch_size_multiplier").Value(2)
            .Item("batch_size_additive").Value(100)
        .EndMap());

    TBatchSizeBackoff batchSize(Logger, config, /*initialSize*/ 50);
    ASSERT_EQ(*batchSize, 50);

    batchSize.Next();
    ASSERT_EQ(*batchSize, 150);

    batchSize.Next();
    ASSERT_EQ(*batchSize, 300);
}

TEST(TBatchSizeBackoffTest, Rollback)
{
    auto config = ConvertTo<TBatchSizeBackoffConfigPtr>(NYTree::BuildYsonNodeFluently()
        .BeginMap()
            .Item("batch_size_multiplier").Value(2)
            .Item("batch_size_additive").Value(100)
            .Item("batch_size_additive_relative_to_max").Value(0.2)
        .EndMap());

    TBatchSizeBackoff batchSize(Logger, config, /*initialSize*/ 50);
    ASSERT_EQ(*batchSize, 50);

    batchSize.Next();
    ASSERT_EQ(*batchSize, 150);

    batchSize.Rollback();
    ASSERT_EQ(*batchSize, 75);

    // Since rollback the backoff is linear.
    batchSize.Next();
    ASSERT_EQ(*batchSize, 105);

    batchSize.Next();
    ASSERT_EQ(*batchSize, 135);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects::NTests
