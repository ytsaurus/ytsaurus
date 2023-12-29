#include "live_preview.h"

#include <yt/yt/server/lib/controller_agent/serialize.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

namespace NYT::NControllerAgent::NControllers {

using namespace NNodeTrackerClient;
using namespace NTableClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TLivePreview::TLivePreview(
    TTableSchemaPtr schema,
    TNodeDirectoryPtr nodeDirectory,
    TOperationId operationId,
    TString name,
    TYPath path)
    : Schema_(std::move(schema))
    , NodeDirectory_(std::move(nodeDirectory))
    , OperationId_(operationId)
    , Name_(std::move(name))
    , Path_(std::move(path))
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

    // COMPAT(galtsev)
    if (context.GetVersion() >= ESnapshotVersion::LivePreviewAnnotation) {
        Persist(context, OperationId_);
        Persist(context, Name_);
        Persist(context, Path_);
    }

    if (context.IsLoad()) {
        Initialize();
    }
}

void TLivePreview::Initialize()
{
    Service_ = New<TVirtualStaticTable>(Chunks_, Schema_, NodeDirectory_, OperationId_, Name_, Path_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
