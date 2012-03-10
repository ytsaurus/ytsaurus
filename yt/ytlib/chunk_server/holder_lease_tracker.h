#pragma once

#include "config.h"
#include "holder.h"

#include <ytlib/misc/lease_manager.h>
#include <ytlib/cell_master/public.h>

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
    : public TRefCounted
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
        NCellMaster::TBootstrap* bootstrap);

    //! Registers the holder and assigns it an initial lease.
    /*!
     *  Initial lease timeout for registered holders is #TChunkManagerConfig::RegisteredHolderTimeout.
     *  For online holders it is decreased to #TChunkManagerConfig::OnlineHolderTimeout.
     */
    void OnHolderRegistered(const THolder& holder);

    //! Notifies that the holder has become online and hence its lease timeout must be updated.
    void OnHolderOnline(const THolder& holder);

    //! Renews the lease.
    void OnHolderHeartbeat(const THolder& holder);

    //! Unregisters the holder and stop tracking its lease.
    void OnHolderUnregistered(const THolder& holder);


private:
    struct THolderInfo
    {
        TLeaseManager::TLease Lease;
    };

    typedef yhash_map<THolderId, THolderInfo> THolderInfoMap;
     
    TConfig::TPtr Config;
    NCellMaster::TBootstrap* Bootstrap;

    THolderInfoMap HolderInfoMap;

    THolderInfo* FindHolderInfo(THolderId holderId);
    THolderInfo& GetHolderInfo(THolderId holderId);
    void RecreateLease(const THolder& holder);

    void OnExpired(THolderId holderId);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
