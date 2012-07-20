#pragma once

#include "private.h"

#include <ytlib/misc/ref.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

//! A simple synchronous interface for caching chunk blocks.
/*!
 *  \note
 *  Thread affinity: any
 */
struct IBlockCache
    : public virtual TRefCounted
{
    //! Puts a block into the cache.
    /*!
     *  If a block with the given id is already present, then the request is ignored.
     *  
     *  #sourceAddress is an address of peer from which the block was downloaded.
     *  If the block was not downloaded from another peer, it must be Null.
     */
    virtual void Put(
        const NChunkServer::TBlockId& id,
        const TSharedRef& data,
        const TNullable<Stroka>& sourceAddress) = 0;

    //! Fetches a block from the cache.
    /*!
     *  If no such block is present, then NULL is returned.
     */
    virtual TSharedRef Find(const NChunkServer::TBlockId& id) = 0;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
