#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/chunked_memory_pool.h>
#include <yt/core/misc/chunked_memory_pool_output.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TChunkedMemoryPoolOutputTest, Basic)
{
    constexpr size_t PoolChunkSize = 10;
    constexpr size_t PoolOutputChunkSize = 7;
    TChunkedMemoryPool pool(NullRefCountedTypeCookie, PoolChunkSize);
    TChunkedMemoryPoolOutput output(&pool, PoolOutputChunkSize);

    output.Write("Short.");
    output.Write("Quite a long string.");

    char* buf;
    auto len = output.Next(&buf);
    ASSERT_EQ(4, len);
    output.Undo(len);
    
    auto chunks = output.FinishAndGetRefs();
    ASSERT_EQ(chunks.size(), 5);

    auto toString = [] (TRef ref) {
        return TString(ref.Begin(), ref.End());
    };
    EXPECT_EQ("Short.Q", toString(chunks[0]));
    EXPECT_EQ("uite a ", toString(chunks[1]));
    EXPECT_EQ("lo",      toString(chunks[2]));
    EXPECT_EQ("ng stri", toString(chunks[3]));
    EXPECT_EQ("ng.",     toString(chunks[4]));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
