#pragma once

#include "public.h"
#include "permission.h"

#include <yt/core/actions/public.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/property.h>
#include <yt/core/misc/variant.h>

#include <yt/core/rpc/public.h>

#include <yt/core/yson/consumer.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

//! Represents an abstract way of handling YPath requests.
/*!
 *  To handle a given YPath request one must first resolve the target.
 *
 *  We start with some root service and call #Resolve. The latter either replies "here", in which case
 *  the resolution is finished, or "there", in which case a new candidate target is provided.
 *  At each resolution step the current path may be altered by specifying a new one
 *  as a part of the result.
 *
 *  Once the request is resolved, #Invoke is called for the target service.
 *
 *  This interface also provides means for inspecting attributes associated with the service.
 *
 */
struct IYPathService
    : public virtual TRefCounted
{
    //! A result indicating that resolution is finished.
    struct TResolveResultHere
    {
        TYPath Path;
    };

    //! A result indicating that resolution must proceed.
    struct TResolveResultThere
    {
        IYPathServicePtr Service;
        TYPath Path;
    };

    using TResolveResult = TVariant<
        TResolveResultHere,
        TResolveResultThere
    >;

    //! Resolves the given path by either returning "here" or "there" result.
    virtual TResolveResult Resolve(const TYPath& path, const NRpc::IServiceContextPtr& context) = 0;

    //! Executes a given request.
    virtual void Invoke(const NRpc::IServiceContextPtr& context) = 0;

    //! Writes a map fragment consisting of attributes conforming to #filter into #consumer.
    /*!
     *  If #stable is |true| then the implementation must ensure a stable result.
     */
    void WriteAttributesFragment(
        NYson::IAsyncYsonConsumer* consumer,
        const std::optional<std::vector<TString>>& attributeKeys,
        bool stable);

    //! Wraps WriteAttributesFragment by enclosing attributes with angle brackets.
    //! If WriteAttributesFragment writes nothing then this method also does nothing.
    void WriteAttributes(
        NYson::IAsyncYsonConsumer* consumer,
        const std::optional<std::vector<TString>>& attributeKeys,
        bool stable);

    //! Manages strategy of writing attributes if attribute keys are null.
    virtual bool ShouldHideAttributes() = 0;

    // Extension methods

    //! Creates a YPath service from a YSON producer.
    /*!
     *  Each time a request is issued, producer is called, its output is turned in
     *  an ephemeral tree, and the request is forwarded to that tree.
     */
    static IYPathServicePtr FromProducer(NYson::TYsonProducer producer, TDuration cachePeriod = TDuration());

    //! Creates a YPath service from a class method.
    template <class T, class R>
    static IYPathServicePtr FromMethod(
        R (T::*method) () const,
        const TWeakPtr<T>& weak);

    //! Creates a wrapper that handles all requests via the given invoker.
    IYPathServicePtr Via(IInvokerPtr invoker);

    //! Creates a wrapper that makes ephemeral snapshots to cache
    //! the underlying service. 
    //! Building tree from underlying service is performed in #invoker,
    //! HeavyRpc invoker is used by default.
    IYPathServicePtr Cached(TDuration updatePeriod, IInvokerPtr workerInvoker = nullptr);

    //! Creates a wrapper that calls given callback on each invocation
    //! in order to validate user permission to query the ypath service.
    IYPathServicePtr WithPermissionValidator(TCallback<void(const TString&, EPermission)> validationCallback);

protected:
    //! Implementation method for WriteAttributesFragment.
    //! It always write requested attributes and call ShouldHideAttributes.
    virtual void DoWriteAttributesFragment(
        NYson::IAsyncYsonConsumer* consumer,
        const std::optional<std::vector<TString>>& attributeKeys,
        bool stable) = 0;

};

DEFINE_REFCOUNTED_TYPE(IYPathService)

class ICachedYPathService
    : public virtual TRefCounted
{
public:
    virtual void SetCachePeriod(TDuration period) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
