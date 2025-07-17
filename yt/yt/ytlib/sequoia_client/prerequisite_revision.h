#pragma once

#include "public.h"

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/ytlib/cypress_server/proto/sequoia_actions.pb.h>

#include <yt/yt/client/hydra/public.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

struct TPrerequisiteRevision
{
    NHydra::TRevision Revision;
    NYPath::TYPath Path;
};

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TPrerequisiteRevision& prerequisiteRevision, TStringBuf spec);

void FromProto(TPrerequisiteRevision* revision, const NObjectClient::NProto::TPrerequisiteRevision& protoRevision);

////////////////////////////////////////////////////////////////////////////////

struct TResolvedPrerequisiteRevision
{
    NCypressClient::TNodeId NodeId;
    NHydra::TRevision Revision;
    NYPath::TYPath NodePath;
};

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TResolvedPrerequisiteRevision& prerequisiteRevision, TStringBuf spec);

void ToProto(NCypressServer::NProto::TResolvedPrerequisiteRevision* protoRevision, const TResolvedPrerequisiteRevision& revision);
void FromProto(TResolvedPrerequisiteRevision* revision, const NCypressServer::NProto::TResolvedPrerequisiteRevision& protoRevision);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
