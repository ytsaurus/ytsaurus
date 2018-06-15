#pragma once

#include <yt/ytlib/chunk_client/data_statistics.pb.h>

#include <yt/ytlib/job_tracker_client/public.h>

#include <yt/core/compression/public.h>

#include <yt/core/misc/dense_map.h>

#include <yt/core/yson/public.h>

#include <yt/core/ytree/public.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

bool HasInvalidDataWeight(const TDataStatistics& statistics);

TDataStatistics& operator += (TDataStatistics& lhs, const TDataStatistics& rhs);
TDataStatistics  operator +  (const TDataStatistics& lhs, const TDataStatistics& rhs);

bool operator == (const TDataStatistics& lhs, const TDataStatistics& rhs);
bool operator != (const TDataStatistics& lhs, const TDataStatistics& rhs);

void Serialize(const TDataStatistics& statistics, NYson::IYsonConsumer* consumer);

void SetDataStatisticsField(TDataStatistics& statistics, TStringBuf key, i64 value);

TString ToString(const TDataStatistics& statistics);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

struct TCodecDuration
{
    NCompression::ECodec Codec;
    TDuration CpuDuration;
};

class TCodecStatistics
{
public:
    TCodecStatistics& Append(const TCodecDuration& codecTime);

    TCodecStatistics& operator+=(const TCodecStatistics& other);

    void DumpTo(NJobTrackerClient::TStatistics *statistics, const TString& path) const;

    TDuration GetTotalDuration() const;

private:
    SmallDenseMap<
        NCompression::ECodec,
        TDuration,
        1,
        TEnumTraits<NCompression::ECodec>::TDenseMapInfo> Map_;

    TDuration TotalDuration_;

    TCodecStatistics& Append(const std::pair<NCompression::ECodec, TDuration>& codecTime);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

