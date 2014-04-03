#include "stdafx.h"
#include "framework.h"

#include <core/compression/codec.h>

#include <contrib/libs/snappy/snappy.h>
#include <contrib/libs/snappy/snappy-sinksource.h>

namespace NYT {
namespace NCompression {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TCodecTest:
    public ::testing::Test
{ };

TEST_F(TCodecTest, Compression)
{
    for (auto codecId : ECodec::GetDomainValues()) {
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
    for (auto codecId : ECodec::GetDomainValues()) {
        auto codec = GetCodec(codecId);

        {
            Stroka a = Stroka(10000, 'a');
            Stroka b = Stroka(50000, 'b');
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
    for (auto codecId : ECodec::GetDomainValues()) {
        auto codec = GetCodec(codecId);

        Stroka data(static_cast<int>(1e7), 'a');

        TSharedRef compressed = codec->Compress(TSharedRef::FromString(data));
        TSharedRef decompressed = codec->Decompress(compressed);

        EXPECT_EQ(
            data,
            Stroka(decompressed.Begin(), decompressed.End()));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NCompression
} // namespace NYT
