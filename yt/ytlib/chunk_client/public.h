#pragma once

#include <ytlib/misc/common.h>

#include <ytlib/object_client/public.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

typedef TGuid TIncarnationId;

typedef NObjectClient::TObjectId TChunkId;
extern TChunkId NullChunkId;

typedef NObjectClient::TObjectId TChunkListId;
extern TChunkListId NullChunkListId;

typedef NObjectClient::TObjectId TChunkTreeId;
extern TChunkTreeId NullChunkTreeId;

typedef TGuid TJobId;

DECLARE_ENUM(EJobState,
    (Running)
    (Completed)
    (Failed)
);

DECLARE_ENUM(EJobType,
    (Replicate)
    (Remove)
);

//! Represents an offset inside a chunk.
typedef i64 TBlockOffset;

//! A |(chunkId, blockIndex)| pair.
struct TBlockId;

DECLARE_ENUM(EChunkType,
    ((Unknown)(0))
    ((File)(1))
    ((Table)(2))
);

////////////////////////////////////////////////////////////////////////////////

struct TRemoteReaderConfig;
typedef TIntrusivePtr<TRemoteReaderConfig> TRemoteReaderConfigPtr;

struct TClientBlockCacheConfig;
typedef TIntrusivePtr<TClientBlockCacheConfig> TClientBlockCacheConfigPtr;

struct TEncodingWriterConfig;
typedef TIntrusivePtr<TEncodingWriterConfig> TEncodingWriterConfigPtr;

struct TDispatcherConfig;
typedef TIntrusivePtr<TDispatcherConfig> TDispatcherConfigPtr;

class TEncodingWriter;
typedef TIntrusivePtr<TEncodingWriter> TEncodingWriterPtr;

struct IAsyncWriter;
typedef TIntrusivePtr<IAsyncWriter> IAsyncWriterPtr;

struct IAsyncReader;
typedef TIntrusivePtr<IAsyncReader> IAsyncReaderPtr;

class TSequentialReader;
typedef TIntrusivePtr<TSequentialReader> TSequentialReaderPtr;

struct IBlockCache;
typedef TIntrusivePtr<IBlockCache> IBlockCachePtr;

struct TSequentialReaderConfig;
typedef TIntrusivePtr<TSequentialReaderConfig> TSequentialReaderConfigPtr;

struct TRemoteWriterConfig;
typedef TIntrusivePtr<TRemoteWriterConfig> TRemoteWriterConfigPtr;

class TRemoteWriter;
typedef TIntrusivePtr<TRemoteWriter> TRemoteWriterPtr;

class TFileReader;
typedef TIntrusivePtr<TFileReader> TFileReaderPtr;

class TFileWriter;
typedef TIntrusivePtr<TFileWriter> TFileWriterPtr;

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
