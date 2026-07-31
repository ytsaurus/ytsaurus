#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/auth_server/ldap_helpers.h>

namespace NYT::NAuth {
namespace {

using namespace NDetail;

////////////////////////////////////////////////////////////////////////////////

TEST(TLdapFilterTest, NoEscapingNeeded)
{
    EXPECT_EQ(LdapEscapeFilterValue("alice"), "alice");
    EXPECT_EQ(LdapEscapeFilterValue("user123"), "user123");
    EXPECT_EQ(LdapEscapeFilterValue(""), "");
}

TEST(TLdapFilterTest, EscapesSpecialChars)
{
    EXPECT_EQ(LdapEscapeFilterValue("*"), "\\2a");
    EXPECT_EQ(LdapEscapeFilterValue("("), "\\28");
    EXPECT_EQ(LdapEscapeFilterValue(")"), "\\29");
    EXPECT_EQ(LdapEscapeFilterValue("\\"), "\\5c");
    EXPECT_EQ(LdapEscapeFilterValue(std::string("\0", 1)), "\\00");
}

TEST(TLdapFilterTest, EscapesMixed)
{
    EXPECT_EQ(LdapEscapeFilterValue("foo*bar(baz)"), "foo\\2abar\\28baz\\29");
}

TEST(TLdapFilterTest, EscapesInjectionAttempt)
{
    EXPECT_EQ(
        LdapEscapeFilterValue("*)(uid=*))(|(uid=*"),
        "\\2a\\29\\28uid=\\2a\\29\\29\\28|\\28uid=\\2a");
}

TEST(TLdapFilterTest, BuildSearchFilterSubstitutes)
{
    EXPECT_EQ(BuildSearchFilter("(uid={login})", "alice"), "(uid=alice)");
}

TEST(TLdapFilterTest, BuildSearchFilterEscapes)
{
    EXPECT_EQ(BuildSearchFilter("(uid={login})", "al*ce"), "(uid=al\\2ace)");
}

TEST(TLdapFilterTest, BuildSearchFilterMultiplePlaceholders)
{
    EXPECT_EQ(
        BuildSearchFilter("(|(uid={login})(mail={login}@example.com))", "bob"),
        "(|(uid=bob)(mail=bob@example.com))");
}

TEST(TLdapFilterTest, BuildSearchFilterNoPlaceholder)
{
    EXPECT_EQ(BuildSearchFilter("(objectClass=person)", "alice"), "(objectClass=person)");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NAuth
