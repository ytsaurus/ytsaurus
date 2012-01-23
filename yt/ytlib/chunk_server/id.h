#pragma once

#include <ytlib/object_server/id.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

typedef i32 THolderId;
const i32 InvalidHolderId = -1;

typedef NObjectServer::TObjectId TChunkId;
extern TChunkId NullChunkId;

typedef NObjectServer::TObjectId TChunkListId;
extern TChunkListId NullChunkListId;

typedef NObjectServer::TObjectId TChunkTreeId;
extern TChunkTreeId NullChunkTreeId;

using NObjectServer::TTransactionId;
using NObjectServer::NullTransactionId;

typedef TGuid TJobId;

DECLARE_ENUM(EJobState,
    (Running)
    (Completed)
    (Failed)
);

DECLARE_ENUM(EJobType,
    (Replicate)
    (Remove)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

