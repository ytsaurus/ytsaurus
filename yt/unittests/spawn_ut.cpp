#include "stdafx.h"

#include <ytlib/misc/proc.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace {

TEST(TSpawnTest, Basic)
{
    int pid = Spawn("/bin/ls", {"ls"}, std::vector<int>());
    EXPECT_NE(pid, 0);
}


TEST(TSpawnTest, InvalidPath)
{
#ifdef __darwin__
    EXPECT_THROW(Spawn("/some/bad/path/binary", {"binary"}, std::vector<int>()), std::exception);
#endif

#ifdef __linux__
    int pid = Spawn("/some/bad/path/binary", {"binary"}, std::vector<int>());
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
    int pid = Spawn("ls", {"ls"}, std::vector<int>());
    EXPECT_NE(pid, 0);
}

TEST(TSpawnTest, ProcessReturnCode0)
{
    int pid = Spawn("true", {"true"}, std::vector<int>());
    ASSERT_GT(pid, 0);

    int status;
    int result = waitpid(pid, &status, 0);
    ASSERT_EQ(result, pid);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
}

TEST(TSpawnTest, ProcessReturnCode1)
{
    int pid = Spawn("false", {"false"}, std::vector<int>());
    ASSERT_GT(pid, 0);

    int status;
    int result = waitpid(pid, &status, 0);
    ASSERT_EQ(result, pid);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_NE(WEXITSTATUS(status), 0);
}

TEST(TSpawnTest, Params1)
{
    int pid = Spawn("bash", {"bash", "-c", "if test 3 -gt 1; then exit 7; fi" }, std::vector<int>());
    ASSERT_GT(pid, 0);

    int status;
    int result = waitpid(pid, &status, 0);
    ASSERT_EQ(result, pid);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 7);
}

TEST(TSpawnTest, Params2)
{
    int pid = Spawn("bash", {"bash", "-c", "if test 1 -gt 3; then exit 7; fi" }, std::vector<int>());
    ASSERT_GT(pid, 0);

    int status;
    int result = waitpid(pid, &status, 0);
    ASSERT_EQ(result, pid);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
}

TEST(TSpawnTest, CloseFd1)
{
    int pid = Spawn("bash", {"bash", "-c", "echo hello >&42" }, std::vector<int>());
    ASSERT_GT(pid, 0);

    int status;
    int result = waitpid(pid, &status, 0);
    ASSERT_EQ(result, pid);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_NE(WEXITSTATUS(status), 0);
}

TEST(TSpawnTest, CloseFd2)
{
    int newFileId = dup2(1, 42);
    ASSERT_EQ(newFileId, 42);

    int pid = Spawn("bash", {"bash", "-c", "echo hello >&42" }, { 42 });
    ASSERT_GT(pid, 0);

    int status;
    int result = waitpid(pid, &status, 0);
    ASSERT_EQ(result, pid);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_NE(WEXITSTATUS(status), 0);
}

} // namespace
} // namespace NYT
