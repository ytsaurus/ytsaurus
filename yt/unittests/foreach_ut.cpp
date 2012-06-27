#include "stdafx.h"

#include <ytlib/misc/foreach.h>
#include <ytlib/misc/common.h>

#include <util/generic/yexception.h>
#include <util/generic/vector.h>

#include <contrib/testing/framework.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TEST(TForeachTest, CommonCase)
{
    std::vector<int> a;
    for (int i = 0; i < 10; ++i) {
        a.push_back(i);
    }

    std::vector<int> b;
    FOREACH (int x, a) {
        b.push_back(x);
    }

    EXPECT_EQ(a, b);
}

TEST(TForeachTest, CommonCaseReference)
{
    std::vector<int> a;
    for (int i = 0; i < 10; ++i) {
        a.push_back(i);
    }

    std::vector<int> b = a;
    FOREACH (int& x, b) {
        x += 10;
    }

    for (int i = 0; i < 10; ++i) {
        a[i] += 10;
    }

    EXPECT_EQ(a, b);
}

TEST(TForeachTest, Break)
{
    std::vector<int> a;
    for (int i = 0; i < 10; ++i) {
        a.push_back(i);
    }

    std::vector<int> b;
    FOREACH (int x, a) {
        if (x > 5) break;
        b.push_back(x);
    }

    std::vector<int> aEtalon;
    for (int i = 0; i < 10; ++i) {
        int x = a[i];
        if (x > 5) break;
        aEtalon.push_back(x);
    }

    EXPECT_EQ(aEtalon, b);
}

TEST(TForeachTest, Continue)
{
    std::vector<int> a;
    for (int i = 0; i < 10; ++i) {
        a.push_back(i);
    }

    std::vector<int> b;
    FOREACH (int x, a) {
        if (x % 2 == 0) continue;
        b.push_back(x);
    }

    std::vector<int> aEtalon;
    for (int i = 0; i < 10; ++i) {
        int x = a[i];
        if (x % 2 == 0) continue;
        aEtalon.push_back(x);
    }

    EXPECT_EQ(aEtalon, b);
}

TEST(TForeachTest, NestedLoops)
{
    std::vector< std::vector<int> > a;
    for (int i = 0; i < a.size(); ++i) {
        a.push_back(std::vector<int>());
        for (int j = 0; j < i; ++j) {
            a[i].push_back(100 * j + i);
        }
    }

    std::vector< std::vector<int> > b;
    FOREACH (auto& v, a) {
        b.push_back(std::vector<int>());
        FOREACH (int x, v) {
            b.back().push_back(x);
        }
    }

    EXPECT_EQ(a, b);
}

std::vector<int> GetVector(std::vector <int>& a)
{
    return std::vector<int>(a);
}

TEST(TForeachTest, CollectionGivenByResultOfFunction)
{
    std::vector<int> a;
    for (int i = 0; i < 10; ++i) {
        a.push_back(i);
    }

    std::vector<int> b;
    FOREACH (int x, GetVector(a)) {
        b.push_back(x);
    }

    EXPECT_EQ(a, b);
}

TEST(TForeachTest, CollectionGivenByReference)
{
    std::vector<int> a;
    for (int i = 0; i < 10; ++i) {
        a.push_back(i);
    }
    std::vector<int>& referenceToA = a;

    std::vector<int> b;
    FOREACH (int x, referenceToA) {
        b.push_back(x);
    }

    EXPECT_EQ(a, b);
}

TEST(TForeachTest, CollectionGivenByConstReference)
{
    std::vector<int> a;
    for (int i = 0; i < 10; ++i) {
        a.push_back(i);
    }
    const std::vector<int>& referenceToA = a;

    std::vector<int> b;
    FOREACH (int x, referenceToA) {
        b.push_back(x);
    }

    EXPECT_EQ(a, b);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

