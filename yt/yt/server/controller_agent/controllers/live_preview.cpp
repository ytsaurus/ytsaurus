#include "live_preview.h"

#include <yt/yt/server/lib/controller_agent/serialize.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

namespace NYT::NControllerAgent::NControllers {

using namespace NNodeTrackerClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TLivePreview::TLivePreview(TTableSchemaPtr schema, TNodeDirectoryPtr nodeDirectory)
    : Schema_(std::move(schema))
    , NodeDirectory_(std::move(nodeDirectory))
{
    Initialize();
}

void TLivePreview::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist<TSetSerializer<TDefaultSerializer, TUnsortedTag>>(context, Chunks_);
    Persist(context, NodeDirectory_);

    // COMPAT(gritukan)
    if (context.GetVersion() >= ESnapshotVersion::VirtualTableSchema) {
        Persist(context, *Schema_);
    }

    if (context.IsLoad()) {
        Initialize();
    }
}

void TLivePreview::Initialize()
{
    Service_ = New<TVirtualStaticTable>(Chunks_, Schema_, NodeDirectory_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
