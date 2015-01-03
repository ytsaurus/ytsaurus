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
    for (auto codecId : TEnumTraits<ECodec>::GetDomainValues()) {
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
    for (auto codecId : TEnumTraits<ECodec>::GetDomainValues()) {
        auto codec = GetCodec(codecId);

        {
            std::vector<Stroka> data = {"", "", "hello", "", " ", "world", "", Stroka(10000, 'a'), Stroka(50000, 'b'), "", ""};
            std::vector<TSharedRef> refs;
            for (const auto& str : data) {
                refs.push_back(TSharedRef::FromString(str));
            }

            TSharedRef compressed = codec->Compress(refs);
            TSharedRef decompressed = codec->Decompress(compressed);

            EXPECT_EQ(
                Stroka(decompressed.Begin(), decompressed.End()),
                Stroka("hello world") + Stroka(10000, 'a') + Stroka(50000, 'b'));
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
    for (auto codecId : TEnumTraits<ECodec>::GetDomainValues()) {
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
