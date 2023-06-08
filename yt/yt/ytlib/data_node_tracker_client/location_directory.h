#pragma once

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt_proto/yt/core/misc/proto/guid.pb.h>

namespace NYT::NDataNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkLocationDirectory
{
    using TChunkLocationList = TCompactVector<
        NChunkClient::TChunkLocationUuid,
        NChunkClient::TypicalChunkLocationCount>;

public:
    TChunkLocationDirectory() = default;
    explicit TChunkLocationDirectory(int sizeHint);

    int GetOrCreateIndex(NChunkClient::TChunkLocationUuid uuid);

    bool IsValid() const &;
    bool IsValid() &&;

    DEFINE_BYREF_RO_PROPERTY(TChunkLocationList, Uuids);

    friend void ToProto(
        google::protobuf::RepeatedPtrField<NYT::NProto::TGuid>* protoDirectory,
        const TChunkLocationDirectory& directory);
    friend void FromProto(
        TChunkLocationDirectory* directory,
        const google::protobuf::RepeatedPtrField<NYT::NProto::TGuid>& protoDirectory);
};

void ToProto(
    google::protobuf::RepeatedPtrField<NYT::NProto::TGuid>* protoDirectory,
    const TChunkLocationDirectory& directory);
void FromProto(
    TChunkLocationDirectory* directory,
    const google::protobuf::RepeatedPtrField<NYT::NProto::TGuid>& protoDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNodeTrackerClient
