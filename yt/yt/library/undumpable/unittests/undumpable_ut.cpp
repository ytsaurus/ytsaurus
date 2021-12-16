#include <gtest/gtest.h>

#include <yt/yt/library/undumpable/undumpable.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(UndumpableMemory, Mark)
{
    std::vector<int> bigVector;
    bigVector.resize(1024 * 1024);

    auto mark = MarkUndumpable(&bigVector[0], bigVector.size() * sizeof(bigVector[0]));
    ASSERT_GT(GetUndumpableBytesCount(), 0u);
    ASSERT_GT(GetUndumpableMemoryFootprint(), 0u);

    CutUndumpableFromCoredump();

    UnmarkUndumpable(mark);

    ASSERT_EQ(GetUndumpableBytesCount(), 0u);
    ASSERT_GT(GetUndumpableMemoryFootprint(), 0u);
}

TEST(UndumpableMemory, MarkOOB)
{
    std::vector<int> bigVector;
    bigVector.resize(1024 * 1024);

    MarkUndumpableOOB(&bigVector[0], bigVector.size() * sizeof(bigVector[0]));
    ASSERT_GT(GetUndumpableBytesCount(), 0u);
    ASSERT_GT(GetUndumpableMemoryFootprint(), 0u);

    CutUndumpableFromCoredump();

    UnmarkUndumpableOOB(&bigVector[0]);

    ASSERT_EQ(GetUndumpableBytesCount(), 0u);
    ASSERT_GT(GetUndumpableMemoryFootprint(), 0u);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
