#include <gtest/gtest.h>

#include <yt/yt/ytlib/security_client/config.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/yt/yson_string/string.h>

namespace NYT::NSecurityClient {
namespace {

using namespace NApi;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TPermissionCacheConfigTest, Simple)
{
    auto config = New<TPermissionCacheConfig>();
    EXPECT_EQ(config->MasterReadOptions->ReadFrom, EMasterChannelKind::Cache);

    auto ysonConfig = TYsonString(TYsonStringBuf("{}"));
    config = ConvertTo<TPermissionCacheConfigPtr>(ysonConfig);
    EXPECT_EQ(config->MasterReadOptions->ReadFrom, EMasterChannelKind::Cache);

    ysonConfig = TYsonString(TYsonStringBuf("{master_read_options={}}"));
    config = ConvertTo<TPermissionCacheConfigPtr>(ysonConfig);
    EXPECT_EQ(config->MasterReadOptions->ReadFrom, EMasterChannelKind::Cache);

    ysonConfig = TYsonString(TYsonStringBuf("{master_read_options={read_from=follower}}"));
    config = ConvertTo<TPermissionCacheConfigPtr>(ysonConfig);
    EXPECT_EQ(config->MasterReadOptions->ReadFrom, EMasterChannelKind::Follower);

    // COMPAT(dakovalkov)
    ysonConfig = TYsonString(TYsonStringBuf("{master_read_options={read_from=follower};read_from=leader}"));
    config = ConvertTo<TPermissionCacheConfigPtr>(ysonConfig);
    EXPECT_EQ(config->MasterReadOptions->ReadFrom, EMasterChannelKind::Leader);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NSecurityClient
