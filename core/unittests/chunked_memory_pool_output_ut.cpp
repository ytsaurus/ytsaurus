#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/chunked_memory_pool.h>
#include <yt/core/misc/chunked_memory_pool_output.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TChunkedMemoryPool, TestBasic)
{
    constexpr size_t PoolChunkSize = 10;
    constexpr size_t PoolOutputChunkSize = 7;
    TChunkedMemoryPool pool(NullRefCountedTypeCookie, PoolChunkSize);
    TChunkedMemoryPoolOutput output(&pool, PoolOutputChunkSize);
    output.Write("Short."); // 1 chunk.
    output.Write("Quite a long string."); // 3 chunks.
    output.Write('.'); // 1 chunk.

    char* buf;
    auto len = output.Next(&buf);
    ASSERT_EQ(len, PoolOutputChunkSize);
    const auto Foo = AsStringBuf("foo");
    ::memcpy(buf, Foo.data(), Foo.length());
    output.Undo(7 - Foo.length());

    auto chunks = output.FinishAndGetRefs();
    ASSERT_EQ(chunks.size(), 6);

    auto toString = [] (TRef ref) {
        return TString(ref.Begin(), ref.End());
    };
    EXPECT_EQ(toString(chunks[0]), "Short.");
    EXPECT_EQ(toString(chunks[1]), "Quite a");
    EXPECT_EQ(toString(chunks[2]), " long s");
    EXPECT_EQ(toString(chunks[3]), "tring.");
    EXPECT_EQ(toString(chunks[4]), ".");
    EXPECT_EQ(toString(chunks[5]), "foo");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
