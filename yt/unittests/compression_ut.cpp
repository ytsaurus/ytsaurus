#include "stdafx.h"
#include "framework.h"

#include <core/compression/codec.h>

#include <contrib/libs/snappy/snappy.h>
#include <contrib/libs/snappy/snappy-sinksource.h>

namespace NYT {
namespace NCompression {
namespace {

////////////////////////////////////////////////////////////////////////////////

Stroka Join(const std::vector<Stroka>& data)
{
    Stroka result;
    for (const auto& str : data) {
        result += str;
    }
    return result;
}

std::vector<TSharedRef> ConvertToSharedRefs(const std::vector<Stroka>& data)
{
    std::vector<TSharedRef> refs;
    for (const auto& str : data) {
        refs.push_back(TSharedRef::FromString(str));
    }
    return refs;
}

////////////////////////////////////////////////////////////////////////////////

class TCodecTest:
    public ::testing::Test
{ };

TEST_F(TCodecTest, Compression)
{
    for (auto codecId : TEnumTraits<ECodec>::GetDomainValues()) {
        auto codec = GetCodec(codecId);

        Stroka data = "hello world";
        auto compressed = codec->Compress(TSharedRef::FromString(data));
        auto decompressed = codec->Decompress(compressed);
        EXPECT_EQ(data, Stroka(decompressed.Begin(), decompressed.End()));
    }
}

TEST_F(TCodecTest, VectorCompression)
{
    for (auto codecId : TEnumTraits<ECodec>::GetDomainValues()) {
        auto codec = GetCodec(codecId);

        {
            std::vector<Stroka> data = {"", "", "hello", "", " ", "world", "", Stroka(10000, 'a'), Stroka(50000, 'b'), "", ""};
            auto refs = ConvertToSharedRefs(data);
            auto compressed = codec->Compress(refs);
            auto decompressed = codec->Decompress(compressed);
            EXPECT_EQ(Join(data), Stroka(decompressed.Begin(), decompressed.End()));
        }

        {
            std::vector<TSharedRef> emptyRefs(10, TSharedRef());
            auto compressed = codec->Compress(emptyRefs);
            auto decompressed = codec->Decompress(compressed);
            EXPECT_EQ("", Stroka(decompressed.Begin(), decompressed.End()));
        }

        {
            std::vector<Stroka> data(10000, "a");
            auto refs = ConvertToSharedRefs(data);
            auto compressed = codec->Compress(refs);
            auto decompressed = codec->Decompress(compressed);
            EXPECT_EQ(Join(data), Stroka(decompressed.Begin(), decompressed.End()));
        }

        {
            std::vector<Stroka> data;
            for (int i = 0; i < 20; ++i) {
                data.push_back(Stroka(1 << i, i));
            }
            auto refs = ConvertToSharedRefs(data);
            auto compressed = codec->Compress(refs);
            auto decompressed = codec->Decompress(compressed);
            EXPECT_EQ(Join(data), Stroka(decompressed.Begin(), decompressed.End()));
        }
    }
}

TEST_F(TCodecTest, LargeTest)
{
    for (auto codecId : TEnumTraits<ECodec>::GetDomainValues()) {
        auto codec = GetCodec(codecId);

        Stroka data(static_cast<int>(1e7), 'a');
        auto compressed = codec->Compress(TSharedRef::FromString(data));
        auto decompressed = codec->Decompress(compressed);
        EXPECT_EQ(data, Stroka(decompressed.Begin(), decompressed.End()));
    }
}

TEST_F(TCodecTest, VectorDecompression)
{
    for (auto codecId : TEnumTraits<ECodec>::GetDomainValues()) {
        auto codec = GetCodec(codecId);

        {
            Stroka data = "hello world";
            auto dataRef = TSharedRef::FromString(data).Split(2);
            EXPECT_TRUE(dataRef.size() > 1);
            auto compressed = codec->Compress(dataRef).Split(2);
            EXPECT_TRUE(compressed.size() > 1);
            auto decompressed = codec->Decompress(compressed);
            EXPECT_EQ(data, Stroka(decompressed.Begin(), decompressed.End()));
        }

        {
            std::vector<Stroka> data(10000, "a");
            auto refs = ConvertToSharedRefs(data);
            auto compressed = codec->Compress(refs).Split(4);
            EXPECT_TRUE(compressed.size() > 1);
            auto decompressed = codec->Decompress(compressed);
            EXPECT_EQ(Join(data), Stroka(decompressed.Begin(), decompressed.End()));
        }

        {
            std::vector<Stroka> data;
            for (int i = 0; i < 20; ++i) {
                data.push_back(Stroka(1 << i, i));
            }
            auto refs = ConvertToSharedRefs(data);
            auto compressed = codec->Compress(refs).Split(5);
            EXPECT_TRUE(compressed.size() > 1);
            auto decompressed = codec->Decompress(compressed);
            EXPECT_EQ(Join(data), Stroka(decompressed.Begin(), decompressed.End()));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NCompression
} // namespace NYT
