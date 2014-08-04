#pragma once

#include <core/logging/log.h>

#include <core/rpc/public.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger ChunkClientLogger;

//! For heavy requests (e.g. PutBlocks).
NRpc::IChannelFactoryPtr GetHeavyNodeChannelFactory();

// For light requests (e.g. SendBlocks, GetBlocks, etc).
NRpc::IChannelFactoryPtr GetLightNodeChannelFactory();

const int MaxPrefetchWindow = 250;

//! Estimated memory overhead per chunk reader.
const i64 ChunkReaderMemorySize = (i64) 16 * 1024;

//! Represents an offset inside a chunk.
typedef i64 TChunkOffset;

//! A suffix to distinguish chunk meta files.
extern const char* const ChunkMetaSuffix;


////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

