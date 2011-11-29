#include "stdafx.h"

#include "../ytlib/ytree/ypath_service.h"
#include "../ytlib/ytree/ypath_client.h"

#include "../ytlib/ytree/tree_builder.h"
#include "../ytlib/ytree/tree_visitor.h"
#include "../ytlib/ytree/yson_writer.h"
#include "../ytlib/ytree/ephemeral.h"

#include <contrib/testing/framework.h>

using ::testing::InSequence;
using ::testing::StrictMock;

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

TEST(TYPathTest, SomeTest)
{
    auto rootService = IYPathService::FromNode(~GetEphemeralNodeFactory()->CreateMap());
    SyncExecuteYPathSet(~rootService, "/a/b/c", "z");
    TYson output = SyncExecuteYPathGet(~rootService, "/");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
