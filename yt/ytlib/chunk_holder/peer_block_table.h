#pragma once

#include "public.h"

#include <ytlib/chunk_server/block_id.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

//! Keeps information about a peer possibly holding a block.
struct TPeerInfo
{
    Stroka Address;
    TInstant ExpirationTime;

    TPeerInfo()
    { }

    TPeerInfo(const Stroka& address, TInstant expirationTime)
        : Address(address)
        , ExpirationTime(expirationTime)
    { }
};

//////////////////////////////////////////////////////////////////////////////// 

//! When Chunk Holder sends a block to a certain client
//! its address is remembered to facilitate peer-to-peer transfers.
//! This class maintains an auto-expiring map for this purpose.
/*!
 *  \note
 *  Thread affinity: single-threaded
 */
class TPeerBlockTable
    : public TRefCounted
{
public:
    TPeerBlockTable(TPeerBlockTableConfigPtr config);
    
    //! Gets peers where a particular block was sent to.
    /*!
     *  Also sweeps expired peers.
     */
    const yvector<TPeerInfo>& GetPeers(const TBlockId& blockId);
    
    //! For a given block, registers a new peer or updates the existing one.
    /*!
     *  Also sweeps expired peers.
     */
    void UpdatePeer(const TBlockId& blockId, const TPeerInfo& peer);

private:
    typedef yhash_map<TBlockId, yvector<TPeerInfo> > TTable;

    static void SweepExpiredPeers(yvector<TPeerInfo>& peers);

    void SweepAllExpiredPeers();
    yvector<TPeerInfo>& GetMutablePeers(const TBlockId& blockId);

    TPeerBlockTableConfigPtr Config;

    //! Each vector is sorted by decreasing expiration time.
    TTable Table;

    TInstant LastSwept;
};

//////////////////////////////////////////////////////////////////////////////// 

} // namespace NChunkHolder
} // namespace NYT
