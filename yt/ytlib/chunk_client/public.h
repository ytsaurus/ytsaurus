#pragma once

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

struct TRemoteReaderConfig;
typedef TIntrusivePtr<TRemoteReaderConfig> TRemoteReaderConfigPtr;

struct TClientBlockCacheConfig;
typedef TIntrusivePtr<TClientBlockCacheConfig> TClientBlockCacheConfigPtr;

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

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
