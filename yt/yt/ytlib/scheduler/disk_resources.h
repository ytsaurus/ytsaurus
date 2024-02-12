#pragma once

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/core/misc/serialize.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TDiskResources
{
    struct TDiskLocationResources
    {
        i64 Usage = 0;
        i64 Limit = 0;
        int MediumIndex = NChunkClient::DefaultSlotsMediumIndex;

        void Persist(const TStreamPersistenceContext& context);
    };

    std::vector<TDiskLocationResources> DiskLocationResources;
    int DefaultMediumIndex = NChunkClient::DefaultSlotsMediumIndex;

    void Persist(const TStreamPersistenceContext& context);
};

////////////////////////////////////////////////////////////////////////////////

void ToProto(NNodeTrackerClient::NProto::TDiskResources* protoDiskResources, const TDiskResources& diskResources);

void FromProto(TDiskResources* diskResources, const NNodeTrackerClient::NProto::TDiskResources& protoDiskResources);

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TDiskResources& diskResources, TStringBuf /*format*/);

TString ToString(const TDiskResources& diskResources);

TString ToString(
    const TDiskResources& diskResources,
    const NChunkClient::TMediumDirectoryPtr& mediumDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

