#include <yt/core/test_framework/framework.h>

#include <yt/server/http_proxy/helpers.h>

#include <yt/ytlib/auth/cookie_authenticator.h>

#include <yt/core/misc/error.h>

namespace NYT::NHttpProxy {
namespace {

using namespace NAuth;

////////////////////////////////////////////////////////////////////////////////

TEST(TTestParseQuery, Sample)
{
    ParseQueryString("path[10]=0");

    EXPECT_THROW(ParseQueryString("path[10]=0&path[a]=1"), TErrorException);

    ParseQueryString("path=//tmp/home/comdep-analytics/chaos-ad/adhoc/advertanalytics/4468/round_geo_detailed2&output_format[$value]=schemaful_dsv&output_format[$attributes][enable_column_names_header]=true&output_format[$attributes][missing_value_mode]=print_sentinel&output_format[$attributes][missing_value_sentinel]=&output_format[$attributes][columns][0]=auctions_media&output_format[$attributes][columns][1]=auctions_perf&output_format[$attributes][columns][2]=auctions_unsold&output_format[$attributes][columns][3]=block_id&output_format[$attributes][columns][4]=block_shows_media&output_format[$attributes][columns][5]=block_shows_perf&output_format[$attributes][columns][6]=block_shows_unsold&output_format[$attributes][columns][7]=cpm_media&output_format[$attributes][columns][8]=cpm_perf&output_format[$attributes][columns][9]=domain_root&output_format[$attributes][columns][10]=has_geo_cpms");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TTestUserAgentDetection, Wrapper)
{
    auto version = DetectPythonWrapper("");
    EXPECT_FALSE(version);

    version = DetectPythonWrapper("1.2.3");
    EXPECT_FALSE(version);

    version = DetectPythonWrapper("Python wrapper 1.8.43");
    EXPECT_TRUE(version);
    EXPECT_EQ(version->Major, 1);
    EXPECT_EQ(version->Minor, 8);
    EXPECT_EQ(version->Patch, 43);

    version = DetectPythonWrapper("Python wrapper 1.8.43a");
    EXPECT_TRUE(version);
    EXPECT_EQ(version->Major, 1);
    EXPECT_EQ(version->Minor, 8);
    EXPECT_EQ(version->Patch, 43);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TTestCsrfToken, Sample)
{
    auto now = TInstant::Now();
    auto token = SignCsrfToken("prime", "abcd", now);
    CheckCsrfToken(token, "prime", "abcd", now - TDuration::Minutes(1))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NHttpProxy
