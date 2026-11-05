#include <yt/yt/ytlib/queue_client/config.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NQueueClient {

namespace {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TQueueConsumerRegistrationManagerConfigPtr ParseConfig(TStringBuf yson)
{
    return ConvertTo<TQueueConsumerRegistrationManagerConfigPtr>(TYsonString(yson));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TRegistrationManagerConfigSuccessfulLookupsTest, OmittedByDefault)
{
    auto config = ParseConfig("{}");

    EXPECT_TRUE(config->Cache);

    for (auto cacheKind : TEnumTraits<EQueueConsumerRegistrationManagerCacheKind>::GetDomainValues()) {
        EXPECT_FALSE(config->Cache->CacheKindToSuccessfulLookupsRequired[cacheKind].has_value())
            << Format("CacheKind: %v", cacheKind);
    }

    EXPECT_NO_THROW(ParseConfig("{cache={cache_kind_to_successful_lookups_required={}}}"));
}

TEST(TRegistrationManagerConfigSuccessfulLookupsTest, ParsedPerCacheKind)
{
    auto config = ParseConfig("{cache={cache_kind_to_successful_lookups_required={registration_lookup=1}}}");

    EXPECT_EQ(config->Cache->CacheKindToSuccessfulLookupsRequired[EQueueConsumerRegistrationManagerCacheKind::RegistrationLookup], 1);
    EXPECT_FALSE(config->Cache->CacheKindToSuccessfulLookupsRequired[EQueueConsumerRegistrationManagerCacheKind::ListRegistrations].has_value());
    EXPECT_FALSE(config->Cache->CacheKindToSuccessfulLookupsRequired[EQueueConsumerRegistrationManagerCacheKind::ReplicaMappingLookup].has_value());
}

TEST(TRegistrationManagerConfigSuccessfulLookupsTest, MustBePositive)
{
    EXPECT_THROW_WITH_SUBSTRING(
        ParseConfig("{cache={cache_kind_to_successful_lookups_required={registration_lookup=0}}}"),
        "Successful lookup count requirement must be positive");
    EXPECT_THROW_WITH_SUBSTRING(
        ParseConfig("{cache={cache_kind_to_successful_lookups_required={list_registrations=-1}}}"),
        "Successful lookup count requirement must be positive");

    EXPECT_NO_THROW(
        ParseConfig("{cache={cache_kind_to_successful_lookups_required={registration_lookup=1}}}"));
}

TEST(TRegistrationManagerConfigSuccessfulLookupsTest, MustNotExceedReplicaCount)
{
    EXPECT_THROW_WITH_SUBSTRING(
        ParseConfig("{cache={cache_kind_to_successful_lookups_required={registration_lookup=2}}}"),
        "Successful lookup count requirement cannot exceed replica count");

    EXPECT_NO_THROW(ParseConfig(
        "{state_read_path=<clusters=[\"a\";\"b\";\"c\"]>\"//path\";"
        "cache={cache_kind_to_successful_lookups_required={registration_lookup=3;list_registrations=2}}}"));

    EXPECT_THROW_WITH_SUBSTRING(
        ParseConfig(
            "{state_read_path=<clusters=[\"a\";\"b\";\"c\"]>\"//path\";"
            "cache={cache_kind_to_successful_lookups_required={registration_lookup=4}}}"),
        "Successful lookup count requirement cannot exceed replica count");
}

TEST(TRegistrationManagerConfigSuccessfulLookupsTest, PathSelectedPerCacheKind)
{
    EXPECT_NO_THROW(ParseConfig(
        "{replica_mapping_read_path=<clusters=[\"a\";\"b\"]>\"//path\";"
        "cache={cache_kind_to_successful_lookups_required={replica_mapping_lookup=2}}}"));

    EXPECT_THROW_WITH_SUBSTRING(
        ParseConfig(
            "{state_read_path=<clusters=[\"a\";\"b\";\"c\"]>\"//path\";"
            "cache={cache_kind_to_successful_lookups_required={replica_mapping_lookup=2}}}"),
        "Successful lookup count requirement cannot exceed replica count");

    EXPECT_THROW_WITH_SUBSTRING(
        ParseConfig(
            "{replica_mapping_read_path=<clusters=[\"a\";\"b\";\"c\"]>\"//path\";"
            "cache={cache_kind_to_successful_lookups_required={registration_lookup=2}}}"),
        "Successful lookup count requirement cannot exceed replica count");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

} // namespace NYT::NQueueClient
