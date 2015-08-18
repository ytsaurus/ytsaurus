#pragma once

#include <core/yson/public.h>

#include <core/ytree/public.h>

#include <ytlib/chunk_client/data_statistics.pb.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

TDataStatistics& operator += (TDataStatistics& lhs, const TDataStatistics& rhs);
TDataStatistics  operator +  (const TDataStatistics& lhs, const TDataStatistics& rhs);

TDataStatistics& operator -= (TDataStatistics& lhs, const TDataStatistics& rhs);
TDataStatistics  operator -  (const TDataStatistics& lhs, const TDataStatistics& rhs);

bool operator == (const TDataStatistics& lhs, const TDataStatistics& rhs);
bool operator != (const TDataStatistics& lhs, const TDataStatistics& rhs);

void Serialize(const TDataStatistics& statistics, NYson::IYsonConsumer* consumer);
void Deserialize(TDataStatistics& value, NYTree::INodePtr node);

const TDataStatistics& ZeroDataStatistics();

Stroka ToString(const TDataStatistics& statistics);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

