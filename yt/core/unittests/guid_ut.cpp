#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/guid.h>
#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/format.h>
#include <yt/core/misc/string.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TGuidTest, SerializationToProto)
{
    auto guid = TGuid::Create();
    auto protoGuid = ToProto<NProto::TGuid>(guid);
    auto deserializedGuid = FromProto<TGuid>(protoGuid);
    EXPECT_EQ(guid, deserializedGuid);
}

TEST(TGuidTest, RandomGuids)
{
    auto guid = TGuid::Create();
    auto otherGuid = TGuid::Create();
    EXPECT_FALSE(guid == otherGuid);
}

TString CanonicalToString(const TGuid& value)
{
   return Sprintf("%x-%x-%x-%x",
        value.Parts32[3],
        value.Parts32[2],
        value.Parts32[1],
        value.Parts32[0]);
}

TEST(TGuidTest, FormatAllTricky)
{
    const ui32 TrickyValues[] = {
        0, 0x1, 0x12, 0x123, 0x1234, 0x12345, 0x123456, 0x1234567, 0x12345678
    };

    for (ui32 a : TrickyValues) {
        for (ui32 b : TrickyValues) {
            for (ui32 c : TrickyValues) {
                for (ui32 d : TrickyValues) {
                    auto value = TGuid(a, b, c, d);
                    EXPECT_EQ(CanonicalToString(value), ToString(value));
                }
            }
        }
    }
}

TEST(TGuidTest, FormatAllSymbols)
{
    const auto Value = TGuid::FromString("12345678-abcdef01-12345678-abcdef01");
    EXPECT_EQ(CanonicalToString(Value), ToString(Value));
}
////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
