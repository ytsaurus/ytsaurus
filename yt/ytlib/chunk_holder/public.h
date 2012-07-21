#pragma once

#include <ytlib/misc/common.h>
#include <ytlib/chunk_server/public.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap;

struct TLocationConfig;
typedef TIntrusivePtr<TLocationConfig> TLocationConfigPtr;

struct TDataNodeConfig;
typedef TIntrusivePtr<TDataNodeConfig> TDataNodeConfigPtr;

class TMasterConnector;
typedef TIntrusivePtr<TMasterConnector> TMasterConnectorPtr;

struct TPeerBlockTableConfig;
typedef TIntrusivePtr<TPeerBlockTableConfig> TPeerBlockTableConfigPtr;

class TChunkStore;
typedef TIntrusivePtr<TChunkStore> TChunkStorePtr;

class TChunkCache;
typedef TIntrusivePtr<TChunkCache> TChunkCachePtr;

class TLocation;
typedef TIntrusivePtr<TLocation> TLocationPtr;

class TReaderCache;
typedef TIntrusivePtr<TReaderCache> TReaderCachePtr;

struct TChunkDescriptor;

class TChunk;
typedef TIntrusivePtr<TChunk> TChunkPtr;

class TStoredChunk;
typedef TIntrusivePtr<TStoredChunk> TStoredChunkPtr;

class TCachedChunk;
typedef TIntrusivePtr<TCachedChunk> TCachedChunkPtr;

class TSessionManager;
typedef TIntrusivePtr<TSessionManager> TSessionManagerPtr;

class TSession;
typedef TIntrusivePtr<TSession> TSessionPtr;

class TJob;
typedef TIntrusivePtr<TJob> TJobPtr;

class TJobExecutor;
typedef TIntrusivePtr<TJobExecutor> TJobExecutorPtr;

class TBlockStore;
typedef TIntrusivePtr<TBlockStore> TBlockStorePtr;

class TCachedBlock;
typedef TIntrusivePtr<TCachedBlock> TCachedBlockPtr;

class TPeerBlockTable;
typedef TIntrusivePtr<TPeerBlockTable> TPeerBlockTablePtr;

class TPeerBlockUpdater;
typedef TIntrusivePtr<TPeerBlockUpdater> TPeerBlockUpdaterPtr;

class TChunkRegistry;
typedef TIntrusivePtr<TChunkRegistry> TChunkRegistryPtr;

class TChunkHolderServiceProxy;

class TChunkHolderService;
typedef TIntrusivePtr<TChunkHolderService> TChunkHolderServicePtr;

using NChunkServer::TChunkId;
using NChunkServer::TJobId;
using NChunkServer::EJobType;
using NChunkServer::EJobState;
using NChunkServer::TBlockId;

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NChunkHolder
} // namespace NYT
