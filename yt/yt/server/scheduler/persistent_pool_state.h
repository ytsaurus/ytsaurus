#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TPersistentPoolState
    : public NYTree::TYsonSerializable  // TODO(renadeen): try make it lite
{
public:
    double IntegralResourceVolume;

    TPersistentPoolState();
};

DEFINE_REFCOUNTED_TYPE(TPersistentPoolState)

TString ToString(const TPersistentPoolStatePtr& state);
void FormatValue(TStringBuilderBase* builder, const TPersistentPoolStatePtr& state, TStringBuf /* format */);

////////////////////////////////////////////////////////////////////////////////

class TPersistentTreeState
    : public NYTree::TYsonSerializable
{
public:
    THashMap<TString, TPersistentPoolStatePtr> PoolStates;

    TPersistentTreeState();
};

DEFINE_REFCOUNTED_TYPE(TPersistentTreeState)

////////////////////////////////////////////////////////////////////////////////

class TPersistentStrategyState
    : public NYTree::TYsonSerializable
{
public:
    THashMap<TString, TPersistentTreeStatePtr> TreeStates;

    TPersistentStrategyState();
};

DEFINE_REFCOUNTED_TYPE(TPersistentStrategyState)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
