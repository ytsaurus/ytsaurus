#pragma once

#include <yt/client/cypress_client/public.h>

namespace NYT::NCypressClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TReqCreate;
class TRspCreate;

} // namespace NProto

//! Describes the reason for cloning a node.
//! Some node types may allow moving but not copying.
DEFINE_ENUM(ENodeCloneMode,
    ((Copy) (0))
    ((Move) (1))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressClient
