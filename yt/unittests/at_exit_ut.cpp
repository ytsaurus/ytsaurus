#include "stdafx.h"
#include "framework.h"

#include <core/misc/at_exit_manager.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TAtExitTest, AtExitManual)
{
    std::string s;

    TShadowingAtExitManager manager;

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

TEST(TAtExitTest, AtExitAutomatic)
{
    std::string s;

    {
        TShadowingAtExitManager manager;

        manager.RegisterAtExit([&s] () { s.push_back('A'); }, 10);
        manager.RegisterAtExit([&s] () { s.push_back('B'); }, 20);
        manager.RegisterAtExit([&s] () { s.push_back('C'); });
        manager.RegisterAtExit([&s] () { s.push_back('D'); }, 10);

        EXPECT_EQ("", s);
    }

    EXPECT_EQ("CADB", s);
}

TEST(TAtExitTest, AtExitAutomatic)
{
    std::string s;

    {
        TShadowingAtExitManager manager;

        manager.RegisterAtExit([&s] () { s.push_back('A'); }, 10);
        manager.RegisterAtExit([&s] () { s.push_back('B'); }, 20);
        manager.RegisterAtExit([&s] () { s.push_back('C'); });
        manager.RegisterAtExit([&s] () { s.push_back('D'); }, 10);

        EXPECT_EQ("", s);
    }

    EXPECT_EQ("ADBC", s);
}

#ifndef _win_

TEST(TAtExitTest, AtFork)
{
    std::string s;

    TShadowingAtExitManager manager;
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
