#include "stdafx.h"

#include <ytlib/misc/guid.h>
#include <ytlib/misc/protobuf_helpers.h>

#include <contrib/testing/framework.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TEST(TGuidTest, SerializationToProto)
{
    auto guid = TGuid::Create();
    auto protoGuid = ToProto<NProto::TGuid>(guid);
    auto deserializedGuid = FromProto<TGuid>(protoGuid);
    EXPECT_EQ(guid, deserializedGuid);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
