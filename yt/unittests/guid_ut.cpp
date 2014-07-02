#include "stdafx.h"
#include "framework.h"

#include <core/misc/guid.h>
#include <core/misc/protobuf_helpers.h>

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

TEST(TGuidTest, DifferentGuids)
{
    auto guid = TGuid::Create();
    auto otherGuid = TGuid::Create();
    EXPECT_FALSE(guid == otherGuid);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
