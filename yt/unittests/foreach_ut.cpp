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
    yvector<int> a;
    for (int i = 0; i < 10; ++i) {
        a.push_back(i);
    }

    yvector<int> b;
    FOREACH (int x, a) {
        b.push_back(x);
    }

    EXPECT_EQ(a, b);
}

TEST(TForeachTest, CommonCaseReference)
{
    yvector<int> a;
    for (int i = 0; i < 10; ++i) {
        a.push_back(i);
    }

    yvector<int> b = a;
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
    yvector<int> a;
    for (int i = 0; i < 10; ++i) {
        a.push_back(i);
    }

    yvector<int> b;
    FOREACH (int x, a) {
        if (x > 5) break;
        b.push_back(x);
    }

    yvector<int> aEtalon;
    for (int i = 0; i < 10; ++i) {
        int x = a[i];
        if (x > 5) break;
        aEtalon.push_back(x);
    }

    EXPECT_EQ(aEtalon, b);
}

TEST(TForeachTest, Continue)
{
    yvector<int> a;
    for (int i = 0; i < 10; ++i) {
        a.push_back(i);
    }

    yvector<int> b;
    FOREACH (int x, a) {
        if (x % 2 == 0) continue;
        b.push_back(x);
    }

    yvector<int> aEtalon;
    for (int i = 0; i < 10; ++i) {
        int x = a[i];
        if (x % 2 == 0) continue;
        aEtalon.push_back(x);
    }

    EXPECT_EQ(aEtalon, b);
}

TEST(TForeachTest, NestedLoops)
{
    yvector< yvector<int> > a;
    for (int i = 0; i < a.ysize(); ++i) {
        a.push_back(yvector<int>());
        for (int j = 0; j < i; ++j) {
            a[i].push_back(100 * j + i);
        }
    }

    yvector< yvector<int> > b;
    FOREACH (auto& v, a) {
        b.push_back(yvector<int>());
        FOREACH (int x, v) {
            b.back().push_back(x);
        }
    }

    EXPECT_EQ(a, b);
}

yvector<int> GetVector(yvector <int>& a)
{
    return yvector<int>(a);
}

TEST(TForeachTest, CollectionGivenByResultOfFunction)
{
    yvector<int> a;
    for (int i = 0; i < 10; ++i) {
        a.push_back(i);
    }

    yvector<int> b;
    FOREACH (int x, GetVector(a)) {
        b.push_back(x);
    }

    EXPECT_EQ(a, b);
}

TEST(TForeachTest, CollectionGivenByReference)
{
    yvector<int> a;
    for (int i = 0; i < 10; ++i) {
        a.push_back(i);
    }
    yvector<int>& referenceToA = a;

    yvector<int> b;
    FOREACH (int x, referenceToA) {
        b.push_back(x);
    }

    EXPECT_EQ(a, b);
}

TEST(TForeachTest, CollectionGivenByConstReference)
{
    yvector<int> a;
    for (int i = 0; i < 10; ++i) {
        a.push_back(i);
    }
    const yvector<int>& referenceToA = a;

    yvector<int> b;
    FOREACH (int x, referenceToA) {
        b.push_back(x);
    }

    EXPECT_EQ(a, b);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

