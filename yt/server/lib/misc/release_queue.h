#pragma once

#include "yt/core/misc/ring_queue.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A helper class for keeping a list of entities to be released in a checkpointable automaton.
/*!
 *  Usual workflow:
 *  - Entities have an external storage (like chunk ids in master or job ids in scheduler node shards)
 *    that requires an explicit release procedure;
 *  - Entities to be released are put in a release queue;
 *  - When snapshot building process started, queue is checkpointed and token is saved;
 *  - If snapshot is built and saved correctly, all entities up to a saved token are released;
 *  - This ensures that no entities are released until their effect is somehow saved into a snapshot.
 *
 *  Keep in mind that this class uses TRingQueue<T> as a storage that does not support shrinkage.
 */
template <class T>
class TReleaseQueue
{
public:
    using TCookie = i64;

    void Push(T&& value);
    void Push(const T& value);

    TCookie Checkpoint() const;

    std::vector<T> Release(TCookie limit = std::numeric_limits<TCookie>::max());

    TCookie GetHeadCookie() const;

private:
    //! Underlying queue. Default allocator is used as nobody will ever use any other allocator.
    TRingQueue<T> Queue_;
    TCookie HeadCookie_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define RELEASE_QUEUE_H_
#include "release_queue-inl.h"
#undef RELEASE_QUEUE_H_
