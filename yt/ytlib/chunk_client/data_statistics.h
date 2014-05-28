#pragma once

#include <ytlib/chunk_client/data_statistics.pb.h>
#include <core/yson/public.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

TDataStatistics& operator += (TDataStatistics& lhs, const TDataStatistics& rhs);
TDataStatistics  operator +  (const TDataStatistics& lhs, const TDataStatistics& rhs);

TDataStatistics& operator -= (TDataStatistics& lhs, const TDataStatistics& rhs);
TDataStatistics  operator -  (const TDataStatistics& lhs, const TDataStatistics& rhs);

bool operator==  (const TDataStatistics& lhs, const TDataStatistics& rhs);

void Serialize(const TDataStatistics& statistics, NYson::IYsonConsumer* consumer);

const TDataStatistics& ZeroDataStatistics();

Stroka ToString(const TDataStatistics& statistics);

}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

