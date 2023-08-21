#pragma once

#include <yt/yt/client/journal_client/public.h>

namespace NYT::NJournalClient {

////////////////////////////////////////////////////////////////////////////////

struct TChunkReplicaDescriptor;
struct TChunkQuorumInfo;

DECLARE_REFCOUNTED_STRUCT(IJournalChunkWriter)
DECLARE_REFCOUNTED_STRUCT(IJournalHunkChunkWriter)

DECLARE_REFCOUNTED_CLASS(TJournalHunkChunkWriterConfig)
DECLARE_REFCOUNTED_CLASS(TJournalHunkChunkWriterOptions)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient
