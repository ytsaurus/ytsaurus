#pragma once

#include "config.h"

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
//! it has to remember its address to facilitate peer-to-peer transfers.
//! This class maintains an auto-expiring map for this purpose.
/*!
 *  \note
 *  Thread affinity: single.
 */
class TPeerBlockTable
    : public TRefCountedBase
{
public:
    TPeerBlockTable(TPeerBlockTableConfig* config);
    
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

    TPeerBlockTableConfig::TPtr Config;

    //! Each vector is sorted by decreasing expiration time.
    TTable Table;

    TInstant LastSwept;
};

//////////////////////////////////////////////////////////////////////////////// 

} // namespace NChunkHolder
} // namespace NYT