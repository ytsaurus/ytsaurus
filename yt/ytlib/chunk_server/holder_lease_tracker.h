#pragma once

#include "holder.h"

#include <ytlib/misc/lease_manager.h>

// TODO(babenko): get rid
#include "common.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkManager;

//! Controls holder server-side leases.
/*!
 *  Upon receiving a registration request from a holder,
 *  TChunkManager registers its by calling #THolderExpiration::Register.
 *  
 *  It also extends the leases by calling #THolderExpiration::RenewHolderLeases.
 *  
 *  When a lease expires #THolderExpiration triggers holder deregistration
 *  by calling #TChunkManager::InitiateUnregisterHolder.
 *  The latter is a logged operation during which #THolderExpiration::Unregister
 *  gets called.
 */
class THolderLeaseTracker
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<THolderLeaseTracker> TPtr;
    typedef TChunkManagerConfig TConfig;

    //! Initializes an instance.
    /*!
     *  \param config A configuration.
     *  \param chunkManager A chunk manager.
     *  \param invoker An invoker used for lease expiration callbacks.
     */
    THolderLeaseTracker(
        TConfig* config,
        TChunkManager* chunkManager,
        IInvoker* invoker);

    //! Registers the holder and assigns it an initial lease.
    void OnHolderRegistered(const THolder& holder);
    
    //! Unregisters the holder and stop tracking its lease.
    void OnHolderUnregistered(const THolder& holder);

    //! Renews the lease.
    void RenewHolderLease(const THolder& holder);

private:
    struct THolderInfo
    {
        TLeaseManager::TLease Lease;
    };

    typedef yhash_map<THolderId, THolderInfo> THolderInfoMap;
     
    TConfig::TPtr Config;
    TIntrusivePtr<TChunkManager> ChunkManager;
    THolderInfoMap HolderInfoMap;
    IInvoker::TPtr Invoker;

    THolderInfo* FindHolderInfo(THolderId holderId);
    THolderInfo& GetHolderInfo(THolderId holderId);

    void OnExpired(THolderId holderId);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
