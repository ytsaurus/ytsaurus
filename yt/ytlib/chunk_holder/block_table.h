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

/*!
 *  Thread affinity: single.
 */
class TBlockTable
    : public TRefCountedBase
{
public:
    TBlockTable(TBlockTableConfig* config);
    
    //! Gets peers possibly holding the block.
    /*!
     *  Also sweeps expired peers.
     */
    const yvector<TPeerInfo>& GetPeers(const NChunkClient::TBlockId& blockId);
    
    //! Registers new peer or updates existing peer expiration time.
    /*!
     *  Also sweeps expired peers.
     */
    void UpdatePeer(const NChunkClient::TBlockId& blockId, const TPeerInfo& peer);

private:
    typedef yhash_map<NChunkClient::TBlockId, yvector<TPeerInfo> > TTable;

    static void SweepExpiredPeers(yvector<TPeerInfo>& peers);

    void SweepAllExpiredPeers();
    yvector<TPeerInfo>& GetMutablePeers(const NChunkClient::TBlockId& blockId);

    TBlockTableConfig::TPtr Config;

    //! Each vector is sorted by decreasing expiration time.
    TTable Table;

    TInstant LastSwept;
};

//////////////////////////////////////////////////////////////////////////////// 

} // namespace NChunkHolder
} // namespace NYT