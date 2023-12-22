#pragma once

#include "private.h"

#include <yt/yt/server/controller_agent/virtual.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TLivePreview)

class TLivePreview
    : public TRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(NYTree::IYPathServicePtr, Service);
    DEFINE_BYREF_RW_PROPERTY(THashSet<NChunkClient::TInputChunkPtr>, Chunks);
    DEFINE_BYREF_RW_PROPERTY(NTableClient::TTableSchemaPtr, Schema, New<NTableClient::TTableSchema>());

public:
    TLivePreview() = default;

    TLivePreview(NTableClient::TTableSchemaPtr schema, NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory);

    void Persist(const TPersistenceContext& context);

private:
    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;

    void Initialize();
};

DEFINE_REFCOUNTED_TYPE(TLivePreview)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
