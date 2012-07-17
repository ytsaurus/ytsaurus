#include <ytlib/misc/foreach.h>
#include <ytlib/misc/codec.h>

#include <contrib/testing/framework.h>

using NYT::TSharedRef;
using NYT::ECodecId;
using NYT::GetCodec;

class TCodecTest:
    public ::testing::Test
{
};

TEST_F(TCodecTest, Compression)
{
    FOREACH (const auto& codecId, ECodecId::GetDomainValues()) {
        auto codec = GetCodec(static_cast<ECodecId>(codecId));

        Stroka data = "hello world";

        TSharedRef compressed = codec->Compress(TSharedRef::FromString(data));
        TSharedRef decompressed = codec->Decompress(compressed);

        EXPECT_EQ(
            Stroka(decompressed.Begin(), decompressed.End()),
            data);
    }
}

TEST_F(TCodecTest, VectorCompression)
{
    FOREACH (const auto& codecId, ECodecId::GetDomainValues()) {
        auto codec = GetCodec(static_cast<ECodecId>(codecId));

        Stroka data[] = {"", "", "hello", "", " ", "world", ""};
        size_t count = sizeof(data) / sizeof(data[0]);
        
        std::vector<TSharedRef> refs(count);
        for (size_t i = 0; i < count; ++i) {
            refs[i] = TSharedRef::FromString(data[i]);
        }

        TSharedRef compressed = codec->Compress(refs);
        TSharedRef decompressed = codec->Decompress(compressed);

        EXPECT_EQ(
            Stroka(decompressed.Begin(), decompressed.End()),
            "hello world");
    }
}
