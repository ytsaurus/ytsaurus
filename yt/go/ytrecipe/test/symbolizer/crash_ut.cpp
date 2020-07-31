#include <gtest/gtest.h>

TEST(TCrash, UseAfterFree)
{
    auto p = new int;
    delete p;
    *p = 0;
}
