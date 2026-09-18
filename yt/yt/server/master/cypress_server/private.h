#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

constexpr double MaxExternalCellBias = 16.0;

DECLARE_REFCOUNTED_STRUCT(ICypressNodeVisitor)

using TCypressNodeExpirationMap = std::multimap<TInstant, NObjectServer::TRawObjectPtr<TCypressNode>>;

template <class T>
class TScalarNode;
using TStringNode  = TScalarNode<std::string>;
using TInt64Node   = TScalarNode<i64>;
using TUint64Node  = TScalarNode<ui64>;
using TDoubleNode  = TScalarNode<double>;
using TBooleanNode = TScalarNode<bool>;

template <class T>
class TScalarNodeTypeHandler;
using TStringNodeTypeHandler  = TScalarNodeTypeHandler<std::string>;
using TInt64NodeTypeHandler   = TScalarNodeTypeHandler<i64>;
using TUint64NodeTypeHandler  = TScalarNodeTypeHandler<ui64>;
using TDoubleNodeTypeHandler  = TScalarNodeTypeHandler<double>;
using TBooleanNodeTypeHandler = TScalarNodeTypeHandler<bool>;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, CypressServerLogger, "Cypress");

YT_DEFINE_LEAKY_GLOBAL(const NProfiling::TProfiler, ExpirationTrackerProfiler, "/expiration_tracker");

DECLARE_REFCOUNTED_CLASS(TAccessTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
