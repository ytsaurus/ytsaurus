#pragma once

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ytree/public.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Context passed to describe traits when they are created. Carries the data needed to build links.
struct TDescribeTraitsContext
{
    //! Rich path of the pipeline (with cluster), used to resolve the cluster for navigation links.
    NYPath::TRichYPath PipelinePath;
};

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
};

DEFINE_REFCOUNTED_TYPE(IDescribeTraits)

////////////////////////////////////////////////////////////////////////////////

//! Default describe traits: turns YT paths (//...) anywhere in the parameters into clickable links,
//! resolving the cluster from the context's pipeline path.
class TDescribeTraitsBase
    : public IDescribeTraits
{
public:
    explicit TDescribeTraitsBase(TDescribeTraitsContext context);

    void MakeLinks(const NYTree::IMapNodePtr& parameters) const override;

protected:
    const TDescribeTraitsContext Context_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
