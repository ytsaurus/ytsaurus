#pragma once

#include "public.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct ICompressionDictionaryBuilder
    : public TRefCounted
{
    virtual void Start() = 0;
};

DEFINE_REFCOUNTED_TYPE(ICompressionDictionaryBuilder)

ICompressionDictionaryBuilderPtr CreateCompressionDictionaryBuilder(IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
