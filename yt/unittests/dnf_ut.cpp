#include "framework.h"

#include <yt/core/misc/dnf.h>

#include <yt/core/yson/string.h>

#include <yt/core/ytree/convert.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;
using namespace NYson;

TEST(TDnfTest, Conjunctions)
{
    TConjunctiveClause clause;
    EXPECT_TRUE(clause.IsSatisfiedBy(std::vector<Stroka>()));
    EXPECT_TRUE(clause.IsSatisfiedBy(std::vector<Stroka>({Stroka("aaa")})));

    clause = TConjunctiveClause({"aaa"}, {});
    EXPECT_FALSE(clause.IsSatisfiedBy(std::vector<Stroka>()));
    EXPECT_TRUE(clause.IsSatisfiedBy(std::vector<Stroka>({Stroka("aaa")})));

    clause = TConjunctiveClause({"aaa", "bbb"}, {});
    EXPECT_FALSE(clause.IsSatisfiedBy(std::vector<Stroka>()));
    EXPECT_FALSE(clause.IsSatisfiedBy(std::vector<Stroka>({Stroka("aaa")})));
    EXPECT_TRUE(clause.IsSatisfiedBy(std::vector<Stroka>({Stroka("aaa"), Stroka("bbb")})));

    clause = TConjunctiveClause({"aaa", "bbb"}, {"ccc"});
    EXPECT_FALSE(clause.IsSatisfiedBy(std::vector<Stroka>()));
    EXPECT_FALSE(clause.IsSatisfiedBy(std::vector<Stroka>({Stroka("aaa")})));
    EXPECT_TRUE(clause.IsSatisfiedBy(std::vector<Stroka>({Stroka("aaa"), Stroka("bbb")})));
    EXPECT_FALSE(clause.IsSatisfiedBy(std::vector<Stroka>({Stroka("aaa"), Stroka("bbb"), Stroka("ccc")})));
}

TEST(TDnfTest, Dnf)
{
    TDnfFormula dnf;

    EXPECT_FALSE(dnf.IsSatisfiedBy(std::vector<Stroka>()));
    EXPECT_FALSE(dnf.IsSatisfiedBy(std::vector<Stroka>({Stroka("aaa")})));

    dnf.Clauses().push_back(TConjunctiveClause({"aaa", "bbb"}, {"ccc"}));

    EXPECT_FALSE(dnf.IsSatisfiedBy(std::vector<Stroka>()));
    EXPECT_FALSE(dnf.IsSatisfiedBy(std::vector<Stroka>({Stroka("aaa")})));
    EXPECT_TRUE(dnf.IsSatisfiedBy(std::vector<Stroka>({Stroka("aaa"), Stroka("bbb")})));
    EXPECT_FALSE(dnf.IsSatisfiedBy(std::vector<Stroka>({Stroka("aaa"), Stroka("bbb"), Stroka("ccc")})));

    dnf.Clauses().push_back(TConjunctiveClause({"ccc"}, {}));

    EXPECT_FALSE(dnf.IsSatisfiedBy(std::vector<Stroka>()));
    EXPECT_FALSE(dnf.IsSatisfiedBy(std::vector<Stroka>({Stroka("aaa")})));
    EXPECT_TRUE(dnf.IsSatisfiedBy(std::vector<Stroka>({Stroka("aaa"), Stroka("bbb")})));
    EXPECT_TRUE(dnf.IsSatisfiedBy(std::vector<Stroka>({Stroka("ccc")})));
    EXPECT_FALSE(dnf.IsSatisfiedBy(std::vector<Stroka>({Stroka("bbb")})));
}

TEST(TDnfTest, Serialization)
{
    auto clause = TConjunctiveClause({"aaa", "bbb"}, {"ccc"});
    auto conjunctionString = ConvertToYsonString(clause, EYsonFormat::Text);
    EXPECT_EQ("{\"include\"=[\"aaa\";\"bbb\";];\"exclude\"=[\"ccc\";];}", conjunctionString.Data());
    EXPECT_EQ(clause, ConvertTo<TConjunctiveClause>(conjunctionString));

    auto dnf = TDnfFormula({
        TConjunctiveClause({"aaa", "bbb"}, {"ccc"}),
        TConjunctiveClause({"ccc"}, {})});
    auto dnfString = ConvertToYsonString(dnf, EYsonFormat::Text);
    EXPECT_EQ("[{\"include\"=[\"aaa\";\"bbb\";];\"exclude\"=[\"ccc\";];};{\"include\"=[\"ccc\";];\"exclude\"=[];};]", dnfString.Data());
    EXPECT_EQ(dnf, ConvertTo<TDnfFormula>(dnfString));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
