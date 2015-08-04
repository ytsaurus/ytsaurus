#pragma once

#include "public.h"

#include <core/yson/consumer.h>

#include <core/rpc/public.h>

#include <core/misc/property.h>

#include <core/actions/public.h>

#include <core/logging/log.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

//! Describes an attribute filtering mode.
DEFINE_ENUM(EAttributeFilterMode,
    // Accept all attributes.
    (All)
    // Don't accept any attribute.
    (None)
    // Accept only matching attributes.
    (MatchingOnly)
);

//! Describes a filtering criteria for attributes.
/*!
 *  If #Mode is |All| or |None| then act accordingly.
 *  If #Mode is |MatchingOnly| then only accept keys listed in #Keys.
 */
struct TAttributeFilter
{
    TAttributeFilter();
    TAttributeFilter(EAttributeFilterMode mode, const std::vector<Stroka>& keys);
    explicit TAttributeFilter(EAttributeFilterMode mode);

    EAttributeFilterMode Mode;
    std::vector<Stroka> Keys;

    static TAttributeFilter All;
    static TAttributeFilter None;
};

void ToProto(NProto::TAttributeFilter* protoFilter, const TAttributeFilter& filter);
void FromProto(TAttributeFilter* filter, const NProto::TAttributeFilter& protoFilter);

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
    class TResolveResult
    {
    public:
        DEFINE_BYVAL_RO_PROPERTY(IYPathServicePtr, Service);
        DEFINE_BYVAL_RO_PROPERTY(TYPath, Path);

    public:
        //! Creates a result indicating that resolution is finished.
        static TResolveResult Here(const TYPath& path)
        {
            TResolveResult result;
            result.Path_ = path;
            return result;
        }

        //! Creates a result indicating that resolution must proceed.
        static TResolveResult There(IYPathServicePtr service, const TYPath& path)
        {
            YASSERT(service);

            TResolveResult result;
            result.Service_ = service;
            result.Path_ = path;
            return result;
        }

        //! Returns true iff the resolution is finished.
        bool IsHere() const
        {
            return !Service_;
        }
    };

    //! Resolves the given path by either returning "here" or "there" result.
    virtual TResolveResult Resolve(const TYPath& path, NRpc::IServiceContextPtr context) = 0;

    //! Executes a given request.
    virtual void Invoke(NRpc::IServiceContextPtr context) = 0;

    //! Called for the target service and returns the logger that will be used by RPC infrastructure
    //! to log various details about verb invocation (e.g. request and response infos).
    virtual NLogging::TLogger GetLogger() const = 0;

    //! Writes a map fragment consisting of attributes conforming to #filter into #consumer.
    /*!
     *  If #sortKeys is |true| then the implementation must ensure a stable ordering of keys.
     */
    virtual void WriteAttributesFragment(
        NYson::IAsyncYsonConsumer* consumer,
        const TAttributeFilter& filter,
        bool sortKeys) = 0;

    // Extension methods

    //! Creates a YPath service from a YSON producer.
    /*!
     *  Each time a request is issued, producer is called, its output is turned in
     *  an ephemeral tree, and the request is forwarded to that tree.
     */
    static IYPathServicePtr FromProducer(NYson::TYsonProducer producer);

    //! Creates a wrapper that handles all requests via the given invoker.
    IYPathServicePtr Via(IInvokerPtr invoker);

    //! Creates a wrapper that makes ephemeral snapshots to cache
    //! the underlying service.
    IYPathServicePtr Cached(TDuration expirationTime);

    //! Wraps WriteAttributesFragment by enclosing attributes with angle brackets.
    //! If WriteAttributesFragment writes nothing then this method also does nothing.
    void WriteAttributes(
        NYson::IAsyncYsonConsumer* consumer,
        const TAttributeFilter& filter,
        bool sortKeys);

};

DEFINE_REFCOUNTED_TYPE(IYPathService)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
