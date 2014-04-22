#pragma once

#include "public.h"

#include <ytlib/chunk_client/schema.pb.h>

#include <core/logging/log.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

extern const NLog::TLogger TableClientLogger;

int LowerBound(int lowerIndex, int upperIndex, std::function<bool(int)> less);

void ValidateKeyColumns(const TKeyColumns& keyColumns, const TKeyColumns& chunkKeyColumns);

TColumnFilter CreateColumnFilter(const NChunkClient::NProto::TChannel& protoChannel, TNameTablePtr nameTable);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
