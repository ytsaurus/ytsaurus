#include "stdafx.h"

#include <ytlib/misc/proc.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace {

using std::vector;

TEST(TSpawnTest, Basic)
{
    vector<Stroka> args;
    args.push_back("ls");

    int pid = Spawn("/bin/ls", args);
    EXPECT_NE(pid, 0);
}


TEST(TSpawnTest, InvalidPath)
{
    vector<Stroka> args;
    args.push_back("binary");

#ifdef __darwin__
    EXPECT_THROW(Spawn("/some/bad/path/binary", args), std::exception);
#endif

#ifdef __linux__
    int pid = Spawn("/some/bad/path/binary", args);
    ASSERT_GT(pid, 0);

    int status;
    int result = waitpid(pid, &status, 0);
    ASSERT_EQ(result, pid);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 127);
#endif
}

TEST(TSpawnTest, BasicUsePATH)
{
    vector<Stroka> args;
    args.push_back("ls");
    int pid = Spawn("ls", args);
    EXPECT_NE(pid, 0);
}

TEST(TSpawnTest, ProcessReturnCode0)
{
    vector<Stroka> args;
    args.push_back("true");

    int pid = Spawn("true", args);
    ASSERT_GT(pid, 0);

    int status;
    int result = waitpid(pid, &status, 0);
    ASSERT_EQ(result, pid);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
}

TEST(TSpawnTest, ProcessReturnCode1)
{
    vector<Stroka> args;
    args.push_back("false");

    int pid = Spawn("false", args);
    ASSERT_GT(pid, 0);

    int status;
    int result = waitpid(pid, &status, 0);
    ASSERT_EQ(result, pid);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_NE(WEXITSTATUS(status), 0);
}

TEST(TSpawnTest, Params1)
{
    vector<Stroka> args;
    args.push_back("bash");
    args.push_back("-c");
    args.push_back("if test 3 -gt 1; then exit 7; fi");

    int pid = Spawn("bash", args);
    ASSERT_GT(pid, 0);

    int status;
    int result = waitpid(pid, &status, 0);
    ASSERT_EQ(result, pid);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 7);
}

TEST(TSpawnTest, Params2)
{
    vector<Stroka> args;
    args.push_back("bash");
    args.push_back("-c");
    args.push_back("if test 1 -gt 3; then exit 7; fi");

    int pid = Spawn("bash", args);
    ASSERT_GT(pid, 0);

    int status;
    int result = waitpid(pid, &status, 0);
    ASSERT_EQ(result, pid);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
}

TEST(TSpawnTest, CloseFd1)
{
    vector<Stroka> args;
    args.push_back("bash");
    args.push_back("-c");
    args.push_back("echo hello >&42");

    int pid = Spawn("bash", args);
    ASSERT_GT(pid, 0);

    int status;
    int result = waitpid(pid, &status, 0);
    ASSERT_EQ(result, pid);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_NE(WEXITSTATUS(status), 0);
}

TEST(TSpawnTest, CloseFd2)
{
    vector<Stroka> args;
    args.push_back("bash");
    args.push_back("-c");
    args.push_back("echo hello >&42");

    int newFileId = dup2(1, 42);
    ASSERT_EQ(newFileId, 42);

    vector<int> fds;
    fds.push_back(42);

    int pid = Spawn("bash", args, fds);
    ASSERT_GT(pid, 0);

    int status;
    int result = waitpid(pid, &status, 0);
    ASSERT_EQ(result, pid);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_NE(WEXITSTATUS(status), 0);
}

} // namespace
} // namespace NYT
