#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/http_proxy/helpers.h>

#include <yt/yt/library/auth_server/cookie_authenticator.h>
#include <yt/yt/library/auth_server/helpers.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/node.h>

namespace NYT::NHttpProxy {
namespace {

using namespace NAuth;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TTestParseQueryTest, Sample)
{
    ParseQueryString("path[10]=0");

    EXPECT_THROW(ParseQueryString("path[10]=0&path[a]=1"), TErrorException);

    ParseQueryString("path=//tmp/home/comdep-analytics/chaos-ad/adhoc/advertanalytics/4468/round_geo_detailed2&output_format[$value]=schemaful_dsv&output_format[$attributes][enable_column_names_header]=true&output_format[$attributes][missing_value_mode]=print_sentinel&output_format[$attributes][missing_value_sentinel]=&output_format[$attributes][columns][0]=auctions_media&output_format[$attributes][columns][1]=auctions_perf&output_format[$attributes][columns][2]=auctions_unsold&output_format[$attributes][columns][3]=block_id&output_format[$attributes][columns][4]=block_shows_media&output_format[$attributes][columns][5]=block_shows_perf&output_format[$attributes][columns][6]=block_shows_unsold&output_format[$attributes][columns][7]=cpm_media&output_format[$attributes][columns][8]=cpm_perf&output_format[$attributes][columns][9]=domain_root&output_format[$attributes][columns][10]=has_geo_cpms");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TTestUserAgentDetectionTest, Wrapper)
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

TEST(TTestCsrfTokenTest, Sample)
{
    auto now = TInstant::Now();
    auto token = SignCsrfToken("prime", "abcd", now);
    CheckCsrfToken(token, "prime", "abcd", now - TDuration::Minutes(1))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TEST(THideSecretParametersTest, CreateCommandMasksValueAndDoesNotMutateOriginal)
{
    // Test that /attributes/value is masked when commandName == "create"
    // and that the original parameters node is not mutated
    auto original = BuildYsonNodeFluently()
        .BeginMap()
            .Item("type").Value("document")
            .Item("attributes").BeginMap()
                .Item("value").Value("secret_content_here")
                .Item("public_field").Value("public_data")
            .EndMap()
        .EndMap();

    auto originalPtr = original->AsMap().Get();
    auto result = HideSecretParameters("create", original->AsMap());

    // Verify original still has the secret value (not mutated)
    auto originalValue = FindNodeByYPath(original->AsMap(), "/attributes/value");
    ASSERT_TRUE(originalValue);
    EXPECT_EQ(ConvertTo<TString>(originalValue), "secret_content_here");

    // Verify the result has the value masked
    auto maskedValue = FindNodeByYPath(result, "/attributes/value");
    ASSERT_TRUE(maskedValue);
    EXPECT_EQ(ConvertTo<TString>(maskedValue), "***");

    // Verify other fields remain unchanged in result
    auto publicField = FindNodeByYPath(result, "/attributes/public_field");
    ASSERT_TRUE(publicField);
    EXPECT_EQ(ConvertTo<TString>(publicField), "public_data");

    auto typeField = FindNodeByYPath(result, "/type");
    ASSERT_TRUE(typeField);
    EXPECT_EQ(ConvertTo<TString>(typeField), "document");

    // Verify result is a different object
    EXPECT_NE(result.Get(), originalPtr);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NHttpProxy
