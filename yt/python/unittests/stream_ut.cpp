#include <yt/core/test_framework/framework.h>

#include <yt/python/common/stream.h>

#include <util/stream/mem.h>

namespace NYT::NPython {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TStreamReaderTest, Simple)
{
    TMemoryInput input("Hello world!");
    TStreamReader reader(&input, /*blockSize*/ 2);
    
    EXPECT_FALSE(reader.IsFinished()); 
    EXPECT_EQ(reader.Current(), reader.Begin());

    reader.Advance(2);
    EXPECT_EQ(reader.Current(), reader.End());
    
    reader.RefreshBlock();
    reader.Advance(2);
    EXPECT_EQ(reader.Current(), reader.End());

    EXPECT_EQ("H", ToString(reader.ExtractPrefix(1)));
    EXPECT_EQ("el", ToString(reader.ExtractPrefix(2)));
    
    reader.RefreshBlock();
    reader.Advance(2);
    reader.RefreshBlock();
    reader.Advance(2);
    reader.RefreshBlock();
    reader.Advance(2);
    EXPECT_EQ("lo worl", ToString(reader.ExtractPrefix(reader.End())));
    
    reader.RefreshBlock();
    reader.Advance(2);
    EXPECT_EQ("d!", ToString(reader.ExtractPrefix(2)));

    EXPECT_TRUE(reader.IsFinished());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NControllerAgent

