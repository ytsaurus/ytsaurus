#pragma once

#include "../misc/common.h"
#include "../misc/ptr.h"

#include "../logging/log.h"

#include "../chunk_holder/common.h"

#include "../transaction/common.h"
#include "../transaction/transaction.h"
#include "../transaction/transaction_manager.h"

namespace NYT {
namespace NChunkManager {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger ChunkManagerLogger;

////////////////////////////////////////////////////////////////////////////////

using NChunkHolder::TChunkId;
using NChunkHolder::TChunkIdHash;
using NTransaction::TTransactionId;
using NTransaction::TTransaction;
using NTransaction::TTransactionManager;

////////////////////////////////////////////////////////////////////////////////

struct TChunkManagerConfig
{
    TChunkManagerConfig()
        : HolderLeaseTimeout(TDuration::Seconds(60))
    { }

    TDuration HolderLeaseTimeout;    
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT

