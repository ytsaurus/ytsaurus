#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Provides access to indexed invokers.
/*
 *  Underlying invokers are supposed to share common state in the pool
 *  and work in cooperative way (e.g.: pool could share CPU between invokers fairly).
 *
 *  Interface is generic, but user is supposed to work with instantiations for well known
 *  invoker types: see below IInvokerPool, IPrioritizedInvokerPool, ISuspendableInvokerPool, etc.
*/
template <class TInvoker>
class IGenericInvokerPool
    : public virtual TRefCounted
{
public:
    //! Returns number of invokers in the pool.
    virtual int GetSize() const = 0;

    //! Returns reference to the invoker from the underlying storage by the integer #index.
    //! Parameter #index is supposed to take values in the [0, implementation-defined limit) range.
    const TIntrusivePtr<TInvoker>& GetInvoker(int index) const
    {
        return DoGetInvoker(index);
    }

    //! Returns reference to the invoker from the underlying storage by the enum #index.
    //! Parameter #index is supposed to take values in the [0, implementation-defined limit) range.
    template <class TEnum, class = typename TEnumTraits<TEnum>::TType>
    const TIntrusivePtr<TInvoker>& GetInvoker(TEnum index) const
    {
        return DoGetInvoker(static_cast<int>(index));
    }

protected:
    //! Returns reference to the invoker from the underlying storage by the integer #index.
    virtual const TIntrusivePtr<TInvoker>& DoGetInvoker(int index) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(IInvokerPool)
DEFINE_REFCOUNTED_TYPE(IPrioritizedInvokerPool)
DEFINE_REFCOUNTED_TYPE(ISuspendableInvokerPool)

////////////////////////////////////////////////////////////////////////////////

//! For each underlying invoker calls Suspend. Returns combined future.
TFuture<void> SuspendInvokerPool(const ISuspendableInvokerPoolPtr& suspendableInvokerPool);

//! For each underlying invoker calls Resume.
void ResumeInvokerPool(const ISuspendableInvokerPoolPtr& suspendableInvokerPool);

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

//! Helper is provided for step-by-step infering of the TOutputInvoker given template arguments.
template <class TInvokerFunctor, class TInputInvoker>
struct TTransformInvokerPoolHelper
{
    using TInputInvokerPtr = TIntrusivePtr<TInputInvoker>;
    using TOutputInvokerPtr = typename std::result_of<TInvokerFunctor(TInputInvokerPtr)>::type;
    using TOutputInvoker = typename TOutputInvokerPtr::TUnderlying;
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

//! Applies #functor (TInputInvoker is supposed to represent mapping from TInputInvokerPtr to TOutputInvokerPtr)
//! to all invokers in the #inputInvokerPool producing IGenericInvokerPool<TOutputInvoker>.
//! Output invoker pool is guaranteed to capture input invoker pool so feel free to chain TransformInvokerPool calls.
template <
    class TInvokerFunctor,
    class TInputInvoker,
    class TOutputInvoker = typename NDetail::TTransformInvokerPoolHelper<TInvokerFunctor, TInputInvoker>::TOutputInvoker>
TIntrusivePtr<IGenericInvokerPool<TOutputInvoker>> TransformInvokerPool(
    TIntrusivePtr<IGenericInvokerPool<TInputInvoker>> inputInvokerPool,
    TInvokerFunctor&& functor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define INVOKER_POOL_INL_H_
#include "invoker_pool-inl.h"
#undef INVOKER_POOL_INL_H_
