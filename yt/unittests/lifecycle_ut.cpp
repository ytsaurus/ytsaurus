#include "stdafx.h"
#include "framework.h"

#include <core/misc/lifecycle.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TLifecycleTest, LifecycleManual)
{
    std::string s;

    TShadowingLifecycle manager;

    manager.RegisterAtExit([&s] () { s.push_back('A'); }, 10);
    manager.RegisterAtExit([&s] () { s.push_back('B'); }, 20);
    manager.RegisterAtExit([&s] () { s.push_back('C'); });
    manager.RegisterAtExit([&s] () { s.push_back('D'); }, 10);

    EXPECT_EQ("", s);

    manager.FireAtExit();
    EXPECT_EQ("CADB", s);

    manager.FireAtExit();
    EXPECT_EQ("CADB", s);
}

TEST(TLifecycleTest, AtExitAutomatic)
{
    std::string s;

    {
        TShadowingLifecycle manager;

        manager.RegisterAtExit([&s] () { s.push_back('A'); }, 10);
        manager.RegisterAtExit([&s] () { s.push_back('B'); }, 20);
        manager.RegisterAtExit([&s] () { s.push_back('C'); });
        manager.RegisterAtExit([&s] () { s.push_back('D'); }, 10);

        EXPECT_EQ("", s);
    }

    EXPECT_EQ("CADB", s);
}

#ifndef _win_

TEST(TLifecycleTest, AtFork)
{
    std::string s;

    TShadowingLifecycle manager;
    manager.RegisterAtFork(
        [&s] () { s.push_back('.'); },
        [&s] () { s.push_back('P'); },
        [&s] () { s.push_back('C'); });

    EXPECT_EQ("", s);

    int pid = fork();
    if (pid > 0) {
        YCHECK(waitpid(pid, nullptr, 0) == pid);
        EXPECT_EQ(".P", s);
    } else {
        EXPECT_EQ(".C", s);
        _exit(0);
    }
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
