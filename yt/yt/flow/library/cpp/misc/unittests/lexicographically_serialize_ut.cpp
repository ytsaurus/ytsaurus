#include <yt/yt/flow/library/cpp/misc/lexicographically_serialize.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

using namespace std::literals::string_view_literals;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

// input -> {parsedValue, excessBytes}
template <typename T>
std::pair<T, i64> LexicographicallyReadTest(TStringBuf input)
{
    T destination = {};
    LexicographicallyRead(input, destination);
    return {destination, input.size()};
}

TEST(TLexicographicallySerializeTest, Ui64)
{
    auto serialize = [] (ui64 value) {
        return LexicographicallySerialize(value);
    };

    EXPECT_EQ(serialize(0), "00");
    EXPECT_EQ(serialize(10), "0a");
    EXPECT_EQ(serialize(16), "110");
    for (ui64 i = 0; i < 1000; ++i) {
        EXPECT_EQ(LexicographicallyParse<ui64>(serialize(i)), i);

        auto [parsedI, excessBytes] = LexicographicallyReadTest<ui64>(serialize(i) + " ");
        EXPECT_EQ(parsedI, i);
        EXPECT_EQ(excessBytes, 1);
    }
    for (const ui64 i : {ui64{1ULL << 60}, std::numeric_limits<ui64>::max()}) {
        EXPECT_EQ(LexicographicallyParse<ui64>(serialize(i)), i);

        auto [parsedI, excessBytes] = LexicographicallyReadTest<ui64>(serialize(i) + "  ");
        EXPECT_EQ(parsedI, i);
        EXPECT_EQ(excessBytes, 2);
    }
    for (ui64 i = 0; i < 100; ++i) {
        for (ui64 j = i + 1; j < 100; ++j) {
            EXPECT_FALSE(serialize(i).starts_with(serialize(j)));
            EXPECT_FALSE(serialize(j).starts_with(serialize(i)));
            EXPECT_LT(serialize(i), serialize(j));
        }
    }
}

TEST(TLexicographicallySerializeTest, I64)
{
    auto serialize = [] (i64 value) {
        return LexicographicallySerialize(value);
    };

    EXPECT_EQ(serialize(0), "00");
    EXPECT_EQ(serialize(10), "0a");
    EXPECT_EQ(serialize(-10), "-f5");
    for (i64 i = -1000; i < 1000; ++i) {
        EXPECT_EQ(LexicographicallyParse<i64>(serialize(i)), i);

        auto [parsedI, excessBytes] = LexicographicallyReadTest<i64>(serialize(i) + "  ");
        EXPECT_EQ(parsedI, i);
        EXPECT_EQ(excessBytes, 2);
    }
    std::vector<i64> bigNumbers = {
        i64{1LL << 60},
        -i64{1LL << 60},
        std::numeric_limits<i64>::max(),
        std::numeric_limits<i64>::min(),
    };
    for (const i64 i : bigNumbers) {
        EXPECT_EQ(LexicographicallyParse<i64>(serialize(i)), i);

        auto [parsedI, excessBytes] = LexicographicallyReadTest<i64>(serialize(i) + " ");
        EXPECT_EQ(parsedI, i);
        EXPECT_EQ(excessBytes, 1);
    }
    for (i64 i = -100; i < 100; ++i) {
        for (i64 j = i + 1; j < 100; ++j) {
            EXPECT_FALSE(serialize(i).starts_with(serialize(j)));
            EXPECT_FALSE(serialize(j).starts_with(serialize(i)));
            EXPECT_LT(serialize(i), serialize(j));
        }
    }
}

TEST(TLexicographicallySerializeTest, String)
{
    auto serialize = [] (TStringBuf value) {
        return LexicographicallySerialize(value);
    };

    EXPECT_EQ(serialize(""), " ");
    EXPECT_EQ(serialize("a"), "a ");
    EXPECT_EQ(serialize("aa"), "aa ");
    EXPECT_EQ(serialize("\n"), "!H ");
    EXPECT_EQ(serialize("\0\1"sv), "!>!? ");

    for (ui64 i = 0; i < 1000; ++i) {
        EXPECT_EQ(LexicographicallyParse<std::string>(serialize(ToString(i))), ToString(i));
    }
    for (ui64 i = 0; i < 100; ++i) {
        for (ui64 j = 0; j < 100; ++j) {
            if (ToString(i) < ToString(j)) {
                EXPECT_FALSE(serialize(ToString(i)).starts_with(serialize(ToString(j))));
                EXPECT_FALSE(serialize(ToString(j)).starts_with(serialize(ToString(i))));
                EXPECT_LT(serialize(ToString(i)), serialize(ToString(j)));
            }
        }
    }

    // Fill with empty and all one-char strings.
    std::vector<std::string> charStrings = {""};
    for (ui64 i = 0; i < 256; ++i) {
        char s[2] = {static_cast<char>(i), 0};
        charStrings.push_back(std::string(s, 1));
    }
    std::vector<std::string> charStringsShort = {""};
    for (const auto& c : "\x0\x1\n\t !^{}[]Aa1\x7E\x7F\x80\x81\xfe\xff"sv) {
        char s[2] = {c, 0};
        charStringsShort.push_back(std::string(s, 1));
    }

#if !defined(__OPTIMIZE__) || defined(_san_enabled_)
    // Debug and sanitizer builds works too slow.
    charStrings = charStringsShort;
#endif

    // i and j iterates all strings of length 0 and 1. And many strings of length 2 (it consumes too much time to iterate all strings of length 2).
    const auto serializedEmpty = serialize("");
    for (const auto& i1 : charStrings) {
        for (const auto& i2 : charStringsShort) {
            const std::string i = i1 + i2;
            const auto serializedI = serialize(i);
            EXPECT_EQ(LexicographicallyParse<std::string>(serializedI), i);
            if (!i.empty()) {
                EXPECT_LT(serializedEmpty, serializedI);
            }
            for (const auto& j1 : charStrings) {
                for (const auto& j2 : charStringsShort) {
                    const std::string j = j1 + j2;
                    if (i >= j) {
                        continue;
                    }
                    const auto serializedJ = serialize(j);
                    EXPECT_LT(serializedI, serializedJ);
                    EXPECT_FALSE(serializedI.starts_with(serializedJ));
                    EXPECT_FALSE(serializedJ.starts_with(serializedI));
                }
            }
        }
    }
}

TEST(TLexicographicallySerializeTest, Vector)
{
    auto serialize = [] (const std::vector<std::string>& value) {
        return LexicographicallySerialize(value);
    };

    EXPECT_EQ(serialize({}), " "sv);
    EXPECT_EQ(serialize({"A"}), "A!^ "sv);
    EXPECT_EQ(serialize({"A", "B"}), "A!^B!^ "sv);
    EXPECT_LT(serialize({}), serialize({""}));
    EXPECT_LT(serialize({""}), serialize({"A"}));
    EXPECT_LT(serialize({"A"}), serialize({"B"}));
    EXPECT_LT(serialize({"A", "B"}), serialize({"A", "C"}));
    EXPECT_LT(serialize({"A", "B"}), serialize({"A", "B", ""}));
}

TEST(TLexicographicallySerializeTest, UnversionedRowV1)
{
    auto serialize = [] (const TUnversionedOwningRow& value) {
        return LexicographicallySerializeUnversionedRowV1(value.Get());
    };

    auto makeRow = [] (auto... args) {
        return MakeUnversionedOwningRow(args...);
    };

    EXPECT_EQ(serialize(makeRow()), LexicographicallySerialize(""));
    EXPECT_EQ(serialize(makeRow("A", 25u)), LexicographicallySerialize(LexicographicallySerialize("A") + LexicographicallySerialize(static_cast<ui64>(25))));

    EXPECT_LT(serialize(makeRow("A", 25u)), serialize(makeRow("B", 25u)));
    EXPECT_LT(serialize(makeRow("A", 25u)), serialize(makeRow("A", 26u)));
    EXPECT_LT(serialize(makeRow("A", 25u)), serialize(makeRow("A", 25u, 0u)));
    EXPECT_LT(serialize(makeRow()), serialize(makeRow("")));
    EXPECT_LT(serialize(makeRow()), serialize(makeRow(std::nullopt)));
    EXPECT_LT(serialize(makeRow(std::nullopt)), serialize(makeRow(std::nullopt, std::nullopt)));
}

TEST(TLexicographicallySerializeTest, UnversionedRowV2)
{
    auto serialize = [] (const TUnversionedOwningRow& value) {
        return LexicographicallySerializeUnversionedRowV2(value.Get());
    };

    auto makeRow = [] (auto... args) {
        return MakeUnversionedOwningRow(args...);
    };

    // Serialized as expected.
    EXPECT_EQ(serialize(makeRow()), LexicographicallySerialize(""));
    EXPECT_EQ(
        serialize(makeRow("A", 25u)),
        LexicographicallySerialize(
            LexicographicallySerialize(static_cast<ui64>(EValueType::String)) +
            LexicographicallySerialize("A") +
            LexicographicallySerialize(static_cast<ui64>(EValueType::Uint64)) +
            LexicographicallySerialize(static_cast<ui64>(25))));

    // Between types.
    EXPECT_LT(serialize(MinKey()), serialize(makeRow(std::nullopt)));
    EXPECT_LT(serialize(makeRow(std::nullopt)), serialize(makeRow(static_cast<i64>(1))));
    EXPECT_LT(serialize(makeRow(static_cast<i64>(1))), serialize(makeRow((static_cast<ui64>(1)))));
    EXPECT_LT(serialize(makeRow((static_cast<ui64>(1)))), serialize(makeRow("1")));
    EXPECT_LT(serialize(makeRow("1")), serialize(MaxKey()));

    // Between values.
    EXPECT_LT(serialize(makeRow("A", 25u)), serialize(makeRow("B", 25u)));
    EXPECT_LT(serialize(makeRow("A", 25u)), serialize(makeRow("A", 26u)));
    EXPECT_LT(serialize(makeRow("A", 25u)), serialize(makeRow("A", 25u, 0u)));
    EXPECT_LT(serialize(makeRow()), serialize(makeRow("")));
    EXPECT_LT(serialize(makeRow()), serialize(makeRow(std::nullopt)));
    EXPECT_LT(serialize(makeRow(std::nullopt)), serialize(makeRow(std::nullopt, std::nullopt)));
}

TEST(TLexicographicallySerializeTest, Reverse)
{
    EXPECT_EQ(RevertLexicographicallySerialized(LexicographicallySerialize("A")), "]}# ");

    const std::vector<std::string> values = {"", "A", "AA", "B", "!", " ", std::string("\0"sv)};

    for (const auto& i : values) {
        const auto iSerialized = LexicographicallySerialize(i);
        const auto iReverted = RevertLexicographicallySerialized(iSerialized);
        EXPECT_EQ(UndoRevertLexicographicallySerialized(iReverted), iSerialized);

        for (const auto& j : values) {
            if (i >= j) {
                continue;
            }
            const auto jSerialized = LexicographicallySerialize(j);
            const auto jReverted = RevertLexicographicallySerialized(jSerialized);
            ASSERT_LT(iSerialized, jSerialized);
            EXPECT_GT(iReverted, jReverted);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
