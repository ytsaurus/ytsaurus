#pragma once

#include <yt/ytlib/chunk_client/data_statistics.pb.h>
#include <yt/ytlib/job_tracker_client/statistics.h>

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
    struct TDenseMapInfo
    {
        static inline NCompression::ECodec getEmptyKey()
        {
            return static_cast<TEnumTraits<NCompression::ECodec>::TType>(~0);
        }

        static inline NCompression::ECodec getTombstoneKey()
        {
            return static_cast<TEnumTraits<NCompression::ECodec>::TType>(~0U - 1);
        }

        static unsigned getHashValue(const NCompression::ECodec& key)
        {
            return static_cast<unsigned>(key);
        }

        static bool isEqual(const NCompression::ECodec& lhs, const NCompression::ECodec& rhs) {
            return lhs == rhs;
        }
    };

    SmallDenseMap<NCompression::ECodec, TDuration, 1, TDenseMapInfo> Map_;
    TDuration TotalDuration_;

    TCodecStatistics& Append(const std::pair<NCompression::ECodec, TDuration>& codecTime);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

