#pragma once
/*
$$==============================================================================
$$ The following code is merely an adaptation of Chromium's Binds and Callbacks.
$$ Kudos to Chromium authors.
$$
$$ Original Chromium revision:
$$   - git-treeish: 206a2ae8a1ebd2b040753fff7da61bbca117757f
$$   - git-svn-id:  svn://svn.chromium.org/chrome/trunk/src@115607
$$
$$ See bind.h for an extended commentary.
$$==============================================================================
*/

#include <ytlib/misc/common.h>
#include <ytlib/misc/source_location.h>

namespace NYT {
namespace NDetail {
/*! \internal */
////////////////////////////////////////////////////////////////////////////////

//! An opaque handle representing bound arguments.
/*!
 * #TBindStateBase is used to provide an opaque handle that the #TCallback<> class
 * can use to represent a function object with bound arguments. It behaves as
 * an existential type that is used by a corresponding invoke function
 * to perform the function execution. This allows us to shield the #TCallback<>
 * class from the types of the bound argument via "type erasure."
 */
class TBindStateBase
    : public TIntrinsicRefCounted
{
protected:
    TBindStateBase(const ::NYT::TSourceLocation& location);

    friend class TIntrinsicRefCounted;
    virtual ~TBindStateBase();

    ::NYT::TSourceLocation Location_;
};

//! Holds the TCallback methods that don't require specialization to reduce
//! template bloat.
class TCallbackBase
{
public:
    //! Returns true iff #TCallback<> is null (does not refer to anything).
    bool IsNull() const;

    //! Returns the #TCallback<> into an uninitialized state.
    void Reset();

    //! Returns a magical handle.
    void* GetHandle() const;

protected:
    //! Swaps the state and the invoke function with other callback (without typechecking!).
    void Swap(TCallbackBase& other);
 
    //! Returns true iff this callback equals to the other (which may be null).
    bool Equals(const TCallbackBase& other) const;

    /*!
     * Yup, out-of-line copy constructor. Yup, explicit.
     */
    explicit TCallbackBase(const TCallbackBase& other);
 
    /*!
     * We can efficiently move-construct callbacks avoiding extra interlocks
     * while moving reference counted #TBindStateBase.
     */
    explicit TCallbackBase(TCallbackBase&& other);

    /*!
     * We can construct #TCallback<> from a rvalue reference to the #TBindStateBase
     * since the #TBindStateBase is created at the #Bind() site.
     */
    explicit TCallbackBase(TIntrusivePtr<TBindStateBase>&& bindState);

    /*!
     * Force the destructor to be instantiated inside this translation unit so
     * that our subclasses will not get inlined versions.
     * Avoids more template bloat.
     */
    ~TCallbackBase();

protected:
    /*!
     * In C++, it is safe to cast function pointers to function pointers of
     * another type. It is not okay to use void*.
     * We create a TUntypedInvokeFunction type that can store our
     * function pointer, and then cast it back to the original type on usage.
     */
    typedef void(*TUntypedInvokeFunction)();

    TIntrusivePtr<TBindStateBase> BindState;
    TUntypedInvokeFunction UntypedInvoke;

private:
    TCallbackBase& operator=(const TCallbackBase&);
    TCallbackBase& operator=(TCallbackBase&&);
};

////////////////////////////////////////////////////////////////////////////////
/*! \endinternal */
} // namespace NDetail
} // namespace NY
