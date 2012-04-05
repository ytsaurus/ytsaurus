#include "stdafx.h"
#include "job.h"
#include "chunk.h"

#include <ytlib/cell_master/load_context.h>

namespace NYT {
namespace NChunkServer {

using namespace NCellMaster;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

TJob::TJob(
    EJobType type,
    const TJobId& jobId,
    TChunk* chunk,
    const Stroka& runnerAddress,
    const yvector<Stroka>& targetAddresses,
    TInstant startTime)
    : Type_(type)
    , Id_(jobId)
    , Chunk_(chunk)
    , RunnerAddress_(runnerAddress)
    , TargetAddresses_(targetAddresses)
    , StartTime_(startTime)
{ }

TJob::TJob(const TJobId& jobId)
    : Id_(jobId)
{ }

void TJob::Save(TOutputStream* output) const
{
    ::Save(output, Type_);
    SaveObject(output, Chunk_);
    ::Save(output, RunnerAddress_);
    ::Save(output, TargetAddresses_);
    ::Save(output, StartTime_);
}

void TJob::Load(const TLoadContext& context, TInputStream* input)
{
    UNUSED(context);
    ::Load(input, Type_);
    LoadObject(input, Chunk_, context);
    ::Load(input, RunnerAddress_);
    ::Load(input, TargetAddresses_);
    ::Load(input, StartTime_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
