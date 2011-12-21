#pragma once

#include "common.h"
#include "ref_counted_tracker.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// TODO: consider making Ref, UnRef, and AfterConstruction private and
// declare appropriate friends.

//! Provides a common base for all reference-counted objects within YT.
class TRefCountedBase
    : private TNonCopyable
{
public:
    //! Constructs an instance.
    TRefCountedBase()
        // Counter is initially set to 1.
        : RefCounter(1)
#ifdef ENABLE_REF_COUNTED_TRACKING
        , Cookie(NULL)
#endif
    { }

    //! Destroys the instance.
    virtual ~TRefCountedBase()
    { }

#ifdef ENABLE_REF_COUNTED_TRACKING
    //! Called from #New functions to initialize the tracking cookie.
    inline void BindToCookie(TRefCountedTracker::TCookie cookie)
    {
        YASSERT(!Cookie);
        Cookie = cookie;
        TRefCountedTracker::Get()->Register(cookie);

        YASSERT(RefCounter >= 1);
    }
#endif

    //! Increments the reference counter.
    inline void Ref() throw()
    {
        // Failure within this line means a zombie is being resurrected.
        YASSERT(RefCounter > 0);
        AtomicIncrement(RefCounter);
    }

    //! Decrements the reference counter.
    /*!
     * When this counter reaches zero, the object also kills itself by calling
     * "delete this". When reference tracking is enabled, this call also
     * unregisters the instance from the tracker.
     */
    inline void UnRef() throw()
    {
        if (AtomicDecrement(RefCounter) == 0) {
#ifdef ENABLE_REF_COUNTED_TRACKING
            YASSERT(Cookie);
            TRefCountedTracker::Get()->Unregister(Cookie);
#endif
            delete this;
        }
    }

    //! Tries to obtain an intrusive pointer for an object that may had
    //! already lost all of its references and, thus, is about to be deleted.
    /*!
     * You may call this method at any time provided that you have a valid
     * raw pointer to an object. The call either returns an intrusive pointer
     * for the object (thus ensuring that the object won't be destroyed until
     * you're holding this pointer) or NULL indicating that the last reference
     * had already been lost and the object is on its way to heavens.
     * All these steps happen atomically.
     *
     * Under all circumstances it is caller's responsibility the make sure that
     * the object is not destroyed during the call to #DangerousGetPtr.
     * Typically this is achieved by keeping a (lock-protected) collection of
     * raw pointers, taking a lock in object's destructor, and unregistering
     * its raw pointer from the collection there.
     */
    template<class T>
    static TIntrusivePtr<T> DangerousGetPtr(T* obj)
    {
        while (true) {
            TAtomic counter = obj->RefCounter;

            YASSERT(counter >= 0);
            if (counter == 0)
                return NULL;

            TAtomic newCounter = counter + 1;
            if (AtomicCas(&obj->RefCounter, newCounter, counter)) {
                TIntrusivePtr<T> ptr(obj);
                AtomicDecrement(obj->RefCounter);
                return ptr;
            }
        }
    }

private:
    TAtomic RefCounter;

#ifdef ENABLE_REF_COUNTED_TRACKING
    TRefCountedTracker::TCookie Cookie;
#endif

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
