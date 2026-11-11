#pragma once

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/public.h>

#include <library/cpp/yt/memory/ref_counted.h>

#include <library/cpp/yt/misc/enum.h>

#include <util/generic/hash.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Context passed to describe traits when they are created. Carries the data needed to build links.
struct TDescribeTraitsContext
{
    //! Rich path of the pipeline (with cluster), used to resolve the cluster for navigation links.
    NYPath::TRichYPath PipelinePath;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EDataChannelAccess,
    (Read)
    (Write)
);

//! One logical channel the entity reads or writes. Two channels are the same data iff their alias sets intersect.
struct TDataChannel
{
    EDataChannelAccess Access = EDataChannelAccess::Read;
    //! Normalized alias keys, e.g. "yt:primary://home/q"; the preferred one to display goes first. Never empty.
    std::vector<std::string> Aliases;
};

//! The entity's already-persisted controller states: state name (relative to the entity's own prefix) -> YSON.
//! Empty when there are none (e.g. before the first controller commit).
using TEntityControllerStateMap = THashMap<std::string, NYson::TYsonString>;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IDescribeTraits)

//! Per-entity hooks used by controller/describe to enrich how an entity is presented in the UI.
//! Every computation/source/sink exposes an implementation via a `using TDescribeTraits = ...;` alias
//! (defaulting to TDescribeTraitsBase); the registry creates it with a context and describe pulls it by class name.
struct IDescribeTraits
    : public TRefCounted
{
    //! Rewrites the entity's parameters node in place, turning references into clickable UI links.
    virtual void MakeLinks(const NYTree::IMapNodePtr& parameters) const = 0;

    //! Data channels the entity reads or writes, derived from its parsed static |parameters| and, optionally,
    //! from its persisted controller |states|.
    virtual std::vector<TDataChannel> GetDataChannels(
        const NYTree::TYsonStructPtr& parameters,
        const TEntityControllerStateMap& states) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IDescribeTraits)

////////////////////////////////////////////////////////////////////////////////

//! Default describe traits: turns YT paths (//...) anywhere in the parameters into clickable links,
//! resolving the cluster from the context's pipeline path, and derives data channels from the YT path
//! ownership claims of the parameters.
class TDescribeTraitsBase
    : public IDescribeTraits
{
public:
    explicit TDescribeTraitsBase(TDescribeTraitsContext context);

    void MakeLinks(const NYTree::IMapNodePtr& parameters) const override;

    std::vector<TDataChannel> GetDataChannels(
        const NYTree::TYsonStructPtr& parameters,
        const TEntityControllerStateMap& states) const override;

protected:
    const TDescribeTraitsContext Context_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
