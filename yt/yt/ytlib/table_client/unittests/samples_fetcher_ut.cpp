#include <yt/yt/ytlib/table_client/samples_fetcher.h>

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NTableClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

TLegacyOwningKey MakeKey(i64 value)
{
    TUnversionedOwningRowBuilder builder;
    builder.AddValue(MakeUnversionedInt64Value(value));
    return builder.FinishRow();
}

TEST(TTableSampleRequestTest, WritesNewAndLegacyKeyBounds)
{
    NChunkClient::NProto::TReqGetTableSamples::TSampleRequest request;
    auto lowerBound = TOwningKeyBound::FromRow() > MakeKey(10);
    auto upperBound = TOwningKeyBound::FromRow() <= MakeKey(20);

    NDetail::SetTableSampleKeyBound(&request, lowerBound);
    NDetail::SetTableSampleKeyBound(&request, upperBound);

    ASSERT_TRUE(request.has_lower_key());
    ASSERT_TRUE(request.has_upper_key());
    EXPECT_EQ(KeyBoundToLegacyRow(lowerBound), NYT::FromProto<TLegacyOwningKey>(request.lower_key()));
    EXPECT_EQ(KeyBoundToLegacyRow(upperBound), NYT::FromProto<TLegacyOwningKey>(request.upper_key()));

    ASSERT_TRUE(request.has_lower_limit());
    ASSERT_TRUE(request.has_upper_limit());
    EXPECT_FALSE(request.lower_limit().has_legacy_key());
    EXPECT_FALSE(request.upper_limit().has_legacy_key());

    NChunkClient::TReadLimit lowerLimit;
    NChunkClient::FromProto(&lowerLimit, request.lower_limit(), /*isUpper*/ false, /*keyLength*/ 1);
    EXPECT_EQ(lowerBound, lowerLimit.KeyBound());

    NChunkClient::TReadLimit upperLimit;
    NChunkClient::FromProto(&upperLimit, request.upper_limit(), /*isUpper*/ true, /*keyLength*/ 1);
    EXPECT_EQ(upperBound, upperLimit.KeyBound());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
