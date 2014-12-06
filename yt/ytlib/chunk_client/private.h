#pragma once

#include <core/logging/log.h>

#include <core/rpc/public.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

extern const NLog::TLogger ChunkClientLogger;

//! For heavy requests (e.g. PutBlocks).
extern NRpc::IChannelFactoryPtr HeavyNodeChannelFactory;

// For light requests (e.g. SendBlocks, GetBlocks, etc).
extern NRpc::IChannelFactoryPtr LightNodeChannelFactory;

const int MaxPrefetchWindow = 250;

//! Estimated memory overhead per chunk reader.
const i64 ChunkReaderMemorySize = (i64) 16 * 1024;

//! Represents an offset inside a chunk.
typedef i64 TChunkOffset;

//! A suffix to distinguish chunk meta files.
extern const Stroka ChunkMetaSuffix;

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

