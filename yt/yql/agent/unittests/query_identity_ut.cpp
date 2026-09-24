#include <yt/yql/agent/query_identity.h>

#include <yt/yt/ytlib/yql_client/proto/token_service.pb.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NYqlAgent {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TQueryIdentityAuthorityTest, IssuesValidToken)
{
    TQueryIdentityAuthority authority;
    auto executionId = TExecutionId::Create();

    EXPECT_EQ(authority.ValidateToken(authority.IssueToken(executionId)), executionId);
}

TEST(TQueryIdentityAuthorityTest, RejectsMalformedToken)
{
    TQueryIdentityAuthority authority;

    EXPECT_THROW_WITH_SUBSTRING(
        authority.ValidateToken("malformed"),
        "Malformed query identity token");
}

TEST(TQueryIdentityAuthorityTest, RejectsTokenWithoutRequiredField)
{
    TQueryIdentityAuthority authority;
    NYqlClient::NProto::TQueryIdentityToken token;
    ASSERT_TRUE(token.ParseFromString(authority.IssueToken(TExecutionId::Create())));
    token.clear_signature();

    EXPECT_THROW_WITH_SUBSTRING(
        authority.ValidateToken(token.SerializePartialAsString()),
        "Malformed query identity token");
}

TEST(TQueryIdentityAuthorityTest, RejectsTamperedToken)
{
    TQueryIdentityAuthority authority;
    auto token = authority.IssueToken(TExecutionId::Create());
    char lastByte = token.back();
    token.back() = lastByte ^ 1;

    EXPECT_THROW_WITH_SUBSTRING(
        authority.ValidateToken(token),
        "Invalid query identity token signature");
}

TEST(TQueryIdentityAuthorityTest, RejectsTokenIssuedByAnotherAuthority)
{
    TQueryIdentityAuthority firstAuthority;
    TQueryIdentityAuthority secondAuthority;
    auto token = firstAuthority.IssueToken(TExecutionId::Create());

    EXPECT_THROW_WITH_SUBSTRING(
        secondAuthority.ValidateToken(token),
        "Invalid query identity token signature");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYqlAgent
