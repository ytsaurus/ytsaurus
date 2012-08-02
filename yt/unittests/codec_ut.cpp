#include <ytlib/misc/foreach.h>
#include <ytlib/codecs/codec.h>

#include <contrib/testing/framework.h>


#include <contrib/libs/snappy/snappy.h>
#include <contrib/libs/snappy/snappy-sinksource.h>

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
        auto codec = GetCodec(codecId);

        Stroka data = "hello world";

        TSharedRef compressed = codec->Compress(TSharedRef::FromString(data));
        TSharedRef decompressed = codec->Decompress(compressed);

        EXPECT_EQ(
            data,
            Stroka(decompressed.Begin(), decompressed.End()));
    }
}

TEST_F(TCodecTest, VectorCompression)
{
    FOREACH (const auto& codecId, ECodecId::GetDomainValues()) {
        auto codec = GetCodec(codecId);

        {
            Stroka a = Stroka('a', 10000);
            Stroka b = Stroka('b', 50000);
            Stroka data[] = {"", "", "hello", "", " ", "world", "", a, b, "", ""};
            size_t count = sizeof(data) / sizeof(data[0]);

            std::vector<TSharedRef> refs(count);
            for (size_t i = 0; i < count; ++i) {
                refs[i] = TSharedRef::FromString(data[i]);
            }

            TSharedRef compressed = codec->Compress(refs);
            TSharedRef decompressed = codec->Decompress(compressed);

            EXPECT_EQ(
                Stroka(decompressed.Begin(), decompressed.End()),
                Stroka("hello world") + a + b);
        }

        {
            std::vector<TSharedRef> emptyRefs(10, TSharedRef());
            TSharedRef compressed = codec->Compress(emptyRefs);
            TSharedRef decompressed = codec->Decompress(compressed);

            EXPECT_EQ(Stroka(decompressed.Begin(), decompressed.End()), "");
        }

    }
}


TEST_F(TCodecTest, LargeTest) {
    FOREACH (const auto& codecId, ECodecId::GetDomainValues()) {
        auto codec = GetCodec(codecId);

        Stroka data(static_cast<int>(1e7), 'a');

        TSharedRef compressed = codec->Compress(TSharedRef::FromString(data));
        TSharedRef decompressed = codec->Decompress(compressed);

        EXPECT_EQ(
            data,
            Stroka(decompressed.Begin(), decompressed.End()));
    }
}
