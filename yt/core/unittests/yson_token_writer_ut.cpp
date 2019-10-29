#include <yt/core/test_framework/framework.h>

#include <yt/core/yson/token_writer.h>
#include <yt/core/yson/writer.h>

namespace NYT::NYson {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TSimpleStringOutput
    : public IZeroCopyOutput
{
public:
    TSimpleStringOutput(TString& s, size_t bufferSize) noexcept
        : S_(s)
        , NextBufferSize_(bufferSize)
    { }

private:
    size_t DoNext(void** ptr) override
    {
        size_t previousSize = S_.size();
        S_.resize(S_.size() + NextBufferSize_);
        *ptr = S_.begin() + previousSize;
        return S_.size() - previousSize;
    }

    void DoUndo(size_t len) override
    {
        EXPECT_LE(len, S_.size());
        S_.resize(S_.size() - len);
    }

private:
    TString& S_;
    size_t NextBufferSize_;
};

////////////////////////////////////////////////////////////////////////////////

void IntListTest(EYsonFormat format, size_t stringBufferSize)
{
    TString out1, out2;
    std::vector<i64> ints = {1LL << 62, 12345678, 1, -12345678, 12, -(1LL << 62), 12345678910121LL, -12345678910121LL, 1, -1, 2, -2, 0};
    std::vector<ui64> uints = {1ULL << 63, 1, 10, 100, 1000000000000, 0, 1ULL << 31};

    {
        TSimpleStringOutput outStream(out1, stringBufferSize);
        TCheckedYsonTokenWriter writer(&outStream);

        writer.WriteBeginList();
        for (i64 val : ints) {
            if (format == EYsonFormat::Binary) {
                writer.WriteBinaryInt64(val);
            } else {
                writer.WriteTextInt64(val);
            }
            writer.WriteItemSeparator();
        }
        for (ui64 val : uints) {
            if (format == EYsonFormat::Binary) {
                writer.WriteBinaryUint64(val);
            } else {
                writer.WriteTextUint64(val);
            }
            writer.WriteItemSeparator();
        }
        writer.WriteEndList();

        writer.Finish();
    }

    {
        TStringOutput outStream(out2);
        TYsonWriter writer(&outStream, format);

        writer.OnBeginList();
        for (i64 val : ints) {
            writer.OnListItem();
            writer.OnInt64Scalar(val);
        }
        for (ui64 val : uints) {
            writer.OnListItem();
            writer.OnUint64Scalar(val);
        }
        writer.OnEndList();

        writer.Flush();
    }

    EXPECT_EQ(out1, out2);
}

TEST(TYsonTokenWriterTest, BinaryIntList)
{
    for (size_t bufferSize = 1; bufferSize <= 20; ++bufferSize) {
        IntListTest(EYsonFormat::Binary, bufferSize);
    }
}

TEST(TYsonTokenWriterTest, TextIntList)
{
    for (size_t bufferSize = 1; bufferSize <= 20; ++bufferSize) {
        IntListTest(EYsonFormat::Text, bufferSize);
    }
}

TEST(TYsonTokenWriterTest, BinaryString)
{
    for (size_t bufferSize = 1; bufferSize <= 20; ++bufferSize) {
        TString out;
        TSimpleStringOutput outStream(out, bufferSize);
        TCheckedYsonTokenWriter writer(&outStream);
        writer.WriteBinaryString("Hello, world!");
        writer.Finish();

        EXPECT_EQ(out, "\1\x1AHello, world!");
    }
}

TEST(TYsonTokenWriterTest, TextString)
{
    for (size_t bufferSize = 1; bufferSize <= 20; ++bufferSize) {
        TString out;
        TSimpleStringOutput outStream(out, bufferSize);
        TCheckedYsonTokenWriter writer(&outStream);
        writer.WriteTextString("Hello, world!");
        writer.Finish();

        EXPECT_EQ(out, "\"Hello, world!\"");
    }
}

TEST(TYsonTokenWriterTest, DifferentTypesBinaryMap)
{
    for (size_t bufferSize = 1; bufferSize <= 20; ++bufferSize) {
        TString out;
        TSimpleStringOutput outStream(out, bufferSize);
        TCheckedYsonTokenWriter writer(&outStream);

        writer.WriteBeginAttributes();
        writer.WriteBinaryString("type");
        writer.WriteKeyValueSeparator();
        writer.WriteBinaryString("map");
        writer.WriteItemSeparator();
        writer.WriteEndAttributes();

        writer.WriteBeginMap();
        writer.WriteBinaryString("double");
        writer.WriteKeyValueSeparator();
        writer.WriteBinaryDouble(2.71828);
        writer.WriteItemSeparator();
        writer.WriteBinaryString("boolean");
        writer.WriteKeyValueSeparator();
        writer.WriteBinaryBoolean(true);
        writer.WriteItemSeparator();
        writer.WriteBinaryString("entity");
        writer.WriteKeyValueSeparator();
        writer.WriteEntity();
        writer.WriteItemSeparator();
        writer.WriteEndMap();

        writer.Finish();

        EXPECT_EQ(out, "<\1\x08type=\1\6map;>{\1\014double=\3\x90\xF7\xAA\x95\t\xBF\5@;\1\016boolean=\5;\1\014entity=#;}");
    }
}

TEST(TYsonTokenWriterTest, DifferentTypesTextMap)
{
    for (size_t bufferSize = 1; bufferSize <= 20; ++bufferSize) {
        TString out;
        TSimpleStringOutput outStream(out, bufferSize);
        TCheckedYsonTokenWriter writer(&outStream);

        writer.WriteBeginAttributes();
        writer.WriteTextString("type");
        writer.WriteKeyValueSeparator();
        writer.WriteTextString("map");
        writer.WriteItemSeparator();
        writer.WriteEndAttributes();

        writer.WriteBeginMap();
        writer.WriteTextString("double");
        writer.WriteKeyValueSeparator();
        writer.WriteTextDouble(2.71828);
        writer.WriteItemSeparator();
        writer.WriteTextString("boolean");
        writer.WriteKeyValueSeparator();
        writer.WriteTextBoolean(true);
        writer.WriteItemSeparator();
        writer.WriteTextString("entity");
        writer.WriteKeyValueSeparator();
        writer.WriteEntity();
        writer.WriteItemSeparator();
        writer.WriteEndMap();

        writer.Finish();

        EXPECT_EQ(out, "<\"type\"=\"map\";>{\"double\"=2.71828;\"boolean\"=%true;\"entity\"=#;}");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYson
