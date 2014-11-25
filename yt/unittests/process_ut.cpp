#include "stdafx.h"
#include "framework.h"

#include <core/misc/process.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

#ifndef _win_

TEST(TProcessTest, Basic)
{
    TProcess p("/bin/ls");
    ASSERT_NO_THROW(p.Spawn());
    p.Wait();
}

TEST(TProcessTest, InvalidPath)
{
    TProcess p("/some/bad/path/binary");
    ASSERT_THROW(p.Spawn(), std::exception);
}

TEST(TProcessTest, BadDup)
{
    TProcess p("/bin/date");
    p.AddDup2FileAction(1000, 1);
    ASSERT_THROW(p.Spawn(), std::exception);
}

TEST(TProcessTest, GoodDup)
{
    TProcess p("/bin/date");
    p.AddDup2FileAction(2, 3);
    ASSERT_NO_THROW(p.Spawn());

    auto error = p.Wait();
    ASSERT_TRUE(error.IsOK()) << ToString(error);
}

TEST(TProcess, IgnoreCloseInvalidFd)
{
    TProcess p("/bin/sh");
    p.AddArgument("-c");
    p.AddArgument("exit 0");
    p.AddCloseFileAction(74);

    ASSERT_NO_THROW(p.Spawn());
    ASSERT_NO_THROW(p.Wait());
}

TEST(TProcessTest, ProcessReturnCode0)
{
    TProcess p("/bin/sh");
    p.AddArgument("-c");
    p.AddArgument("exit 0");

    ASSERT_NO_THROW(p.Spawn());

    auto error = p.Wait();
    ASSERT_TRUE(error.IsOK()) << ToString(error);
}

TEST(TProcessTest, ProcessReturnCode1)
{
    TProcess p("/bin/sh");
    p.AddArgument("-c");
    p.AddArgument("exit 1");

    ASSERT_NO_THROW(p.Spawn());

    auto error = p.Wait();
    ASSERT_FALSE(error.IsOK()) << ToString(error);
}

TEST(TProcessTest, Params1)
{
    TProcess p("/bin/bash");
    p.AddArgument("-c");
    p.AddArgument("if test 3 -gt 1; then exit 7; fi");

    ASSERT_NO_THROW(p.Spawn());

    auto error = p.Wait();
    EXPECT_FALSE(error.IsOK());
}

TEST(TProcessTest, Params2)
{
    TProcess p("/bin/bash");
    p.AddArgument("-c");
    p.AddArgument("if test 1 -gt 3; then exit 7; fi");

    ASSERT_NO_THROW(p.Spawn());

    auto error = p.Wait();
    EXPECT_TRUE(error.IsOK()) << ToString(error);
}

TEST(TProcessTest, InheritEnvironment)
{
    const char* name = "SPAWN_TEST_ENV_VAR";
    const char* value = "42";
    setenv(name, value, 1);

    TProcess p("/bin/bash");
    p.AddArgument("-c");
    p.AddArgument("if test $SPAWN_TEST_ENV_VAR = 42; then exit 7; fi");

    ASSERT_NO_THROW(p.Spawn());

    auto error = p.Wait();
    EXPECT_FALSE(error.IsOK());

    unsetenv(name);
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
