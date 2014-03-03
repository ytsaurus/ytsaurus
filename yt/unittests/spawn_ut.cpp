#include "stdafx.h"
#include "framework.h"

#include <core/misc/proc.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

#ifndef _win_

TEST(TSpawnTest, Basic)
{
    std::vector<Stroka> args;
    args.push_back("ls");

    int pid = Spawn("/bin/ls", args);
    EXPECT_NE(0, pid);
}

TEST(TSpawnTest, InvalidPath)
{
    std::vector<Stroka> args;
    args.push_back("binary");

#ifdef __darwin__
    EXPECT_THROW(Spawn("/some/bad/path/binary", args), std::exception);
#endif

#ifdef __linux__
    int pid = Spawn("/some/bad/path/binary", args);
    ASSERT_GT(pid, 0);

    int status;
    int result = waitpid(pid, &status, 0);
    ASSERT_EQ(pid, result);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_NE(WEXITSTATUS(status), 0);
    EXPECT_EQ(ENOENT, GetErrNoFromExitCode(WEXITSTATUS(status)));
#endif
}

TEST(TSpawnTest, BasicUsePATH)
{
    std::vector<Stroka> args;
    args.push_back("ls");
    int pid = Spawn("ls", args);
    EXPECT_NE(0, pid);
}

TEST(TSpawnTest, ProcessReturnCode0)
{
    std::vector<Stroka> args;
    args.push_back("true");

    int pid = Spawn("true", args);
    ASSERT_GT(pid, 0);

    int status;
    int result = waitpid(pid, &status, 0);
    ASSERT_EQ(result, pid);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(0, WEXITSTATUS(status));
}

TEST(TSpawnTest, ProcessReturnCode1)
{
    std::vector<Stroka> args;
    args.push_back("false");

    int pid = Spawn("false", args);
    ASSERT_GT(pid, 0);

    int status;
    int result = waitpid(pid, &status, 0);
    ASSERT_EQ(result, pid);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_NE(0, WEXITSTATUS(status));
}

TEST(TSpawnTest, Params1)
{
    std::vector<Stroka> args;
    args.push_back("bash");
    args.push_back("-c");
    args.push_back("if test 3 -gt 1; then exit 7; fi");

    int pid = Spawn("bash", args);
    ASSERT_GT(pid, 0);

    int status;
    int result = waitpid(pid, &status, 0);
    ASSERT_EQ(result, pid);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(7, WEXITSTATUS(status));
}

TEST(TSpawnTest, Params2)
{
    std::vector<Stroka> args;
    args.push_back("bash");
    args.push_back("-c");
    args.push_back("if test 1 -gt 3; then exit 7; fi");

    int pid = Spawn("bash", args);
    ASSERT_GT(pid, 0);

    int status;
    int result = waitpid(pid, &status, 0);
    ASSERT_EQ(result, pid);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(0, WEXITSTATUS(status));
}

TEST(TSpawnTest, InheritEnvironment)
{
    const char* name = "SPAWN_TEST_ENV_VAR";
    const char* value = "42";
    setenv(name, value, 1);

    std::vector<Stroka> args;
    args.push_back("bash");
    args.push_back("-c");
    args.push_back("if test $SPAWN_TEST_ENV_VAR = 42; then exit 7; fi");

    int pid = Spawn("bash", args);
    ASSERT_GT(pid, 0);

    int status;
    int result = waitpid(pid, &status, 0);
    ASSERT_EQ(result, pid);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(7, WEXITSTATUS(status));

    unsetenv(name);
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
