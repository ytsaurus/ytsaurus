#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/extensions/clickhouse/cpp/spec.h>

#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TClickHouseRegistryTest, SinksAreRegistered)
{
    const auto typeNames = TRegistry::Get()->GetSinkTypeNames();

    EXPECT_THAT(typeNames, testing::Contains("NYT::NFlow::TClickHouseBatchingSink"));
    EXPECT_THAT(typeNames, testing::Contains("NYT::NFlow::TShardedClickHouseBatchingSink"));
    EXPECT_THAT(typeNames, testing::Contains("NYT::NFlow::TAtLeastOnceClickHouseSink"));
    EXPECT_THAT(typeNames, testing::Contains("NYT::NFlow::TAtMostOnceClickHouseSink"));
}

TEST(TClickHouseRegistryTest, LegacyBatchingRejectsShardHosts)
{
    EXPECT_THROW(NYTree::ConvertTo<TClickHouseBatchingSinkParametersPtr>(
        NYson::TYsonStringBuf("{shard_hosts={a=[h]};table=t}")),
        TErrorException);
}

TEST(TClickHouseRegistryTest, ShardedBatchingRequiresShardHosts)
{
    EXPECT_NO_THROW(NYTree::ConvertTo<TShardedClickHouseBatchingSinkParametersPtr>(
        NYson::TYsonStringBuf("{shard_hosts={a=[h]};table=t}")));
    EXPECT_THROW(NYTree::ConvertTo<TShardedClickHouseBatchingSinkParametersPtr>(
        NYson::TYsonStringBuf("{host=h;table=t}")),
        TErrorException);
    EXPECT_THROW(NYTree::ConvertTo<TShardedClickHouseBatchingSinkParametersPtr>(
        NYson::TYsonStringBuf("{hosts=[h1;h2];table=t}")),
        TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
