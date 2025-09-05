#include "prerequisite_revision.h"

#include "helpers.h"

#include <yt/ytlib/object_client/proto/object_ypath.pb.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NSequoiaClient {

using namespace NYPath;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TPrerequisiteRevision& prerequisiteRevision, TStringBuf /*spec*/)
{
    builder->AppendFormat("{Path=%v; Revision=%v}",
        prerequisiteRevision.Path,
        prerequisiteRevision.Revision);
}

void FromProto(TPrerequisiteRevision* revision, const NObjectClient::NProto::TPrerequisiteRevision& protoRevision)
{
    revision->Path = ValidateAndMakeYPath(TRawYPath(protoRevision.path()));
    revision->Revision = FromProto<NHydra::TRevision>(protoRevision.revision());
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TResolvedPrerequisiteRevision& prerequisiteRevision, TStringBuf /*spec*/)
{
    builder->AppendFormat("{NodeId=%v; NodePath=%v; Revision=%v}",
        prerequisiteRevision.NodeId,
        prerequisiteRevision.NodePath,
        prerequisiteRevision.Revision);
}

void ToProto(NCypressServer::NProto::TResolvedPrerequisiteRevision* protoRevision, const TResolvedPrerequisiteRevision& revision)
{
    protoRevision->Clear();

    ToProto(protoRevision->mutable_node_id(), revision.NodeId);
    ToProto(protoRevision->mutable_node_path(), revision.NodePath);
    protoRevision->set_revision(ToProto(revision.Revision));
}

void FromProto(TResolvedPrerequisiteRevision* revision, const NCypressServer::NProto::TResolvedPrerequisiteRevision& protoRevision)
{
    revision->NodeId = FromProto<NCypressClient::TNodeId>(protoRevision.node_id());
    revision->NodePath = FromProto<TYPath>(protoRevision.node_path());
    revision->Revision = FromProto<NHydra::TRevision>(protoRevision.revision());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
