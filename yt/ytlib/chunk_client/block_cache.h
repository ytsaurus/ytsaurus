#pragma once

#include "common.h"

#include "../misc/ref.h"

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

//! A simple synchronous interface for caching chunk blocks.
/*!
 *  \note
 *  Thread affinity: any
 */
struct IBlockCache
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<IBlockCache> TPtr;

    //! Puts a block into the cache.
    /*!
     *  If a block with the given id is already present, then the request is ignored.
     */
    void Put(const TBlockId& id, const TSharedRef& data);

    //! Fetches a block from the cache.
    /*!
     *  If no such block is present, then NULL is returned.
     */
    TSharedRef Find(const TBlockId& id);
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
