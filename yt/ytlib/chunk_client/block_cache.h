#pragma once

#include "common.h"
#include "block_id.h"

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
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<IBlockCache> TPtr;

    //! Puts a block into the cache.
    /*!
     *  If a block with the given id is already present, then the request is ignored.
     */
    virtual void Put(const TBlockId& id, const TSharedRef& data) = 0;

    //! Fetches a block from the cache.
    /*!
     *  If no such block is present, then NULL is returned.
     */
    virtual TSharedRef Find(const TBlockId& id) = 0;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
