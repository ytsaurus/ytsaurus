#pragma once

#include "chunk_manager.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class THolderExpiration
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<THolderExpiration> TPtr;
    typedef TChunkManagerConfig TConfig;

    THolderExpiration(
        const TConfig& config,
        TChunkManager::TPtr chunkManager);

    void Start(IInvoker::TPtr invoker);
    void Stop();

    void AddHolder(const THolder& holder);
    void RemoveHolder(const THolder& holder);
    void RenewHolder(const THolder& holder);

private:
    struct THolderInfo
    {
        TLeaseManager::TLease Lease;
    };

    typedef yhash_map<THolderId, THolderInfo> THolderInfoMap;
     
    TConfig Config;
    TChunkManager::TPtr ChunkManager;
    THolderInfoMap HolderInfoMap;
    IInvoker::TPtr Invoker;

    THolderInfo* FindHolderInfo(THolderId holderId);
    THolderInfo& GetHolderInfo(THolderId holderId);

    void OnExpired(THolderId holderId);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
