#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/public.h>

#include <yt/core/ytree/yson_serializable.h>

#include <yt/core/compression/public.h>

#include <yt/library/erasure/public.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TCypressManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    int DefaultFileReplicationFactor;
    int DefaultTableReplicationFactor;
    NErasure::ECodec DefaultJournalErasureCodec;
    int DefaultJournalReplicationFactor;
    int DefaultJournalReadQuorum;
    int DefaultJournalWriteQuorum;

    TCypressManagerConfig();
};

DEFINE_REFCOUNTED_TYPE(TCypressManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicCypressManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Period between Cypress access statistics commits.
    TDuration StatisticsFlushPeriod;

    //! Maximum number of children map and list nodes are allowed to contain.
    int MaxNodeChildCount;

    //! Maximum allowed length of string nodes.
    int MaxStringNodeLength;

    //! Maximum allowed size of custom attributes for objects (transactions, Cypress nodes etc).
    //! This limit concerns the binary YSON representation of attributes.
    int MaxAttributeSize;

    //! Maximum allowed length of keys in map nodes.
    int MaxMapNodeKeyLength;

    TDuration ExpirationCheckPeriod;
    int MaxExpiredNodesRemovalsPerCommit;
    TDuration ExpirationBackoffTime;

    NCompression::ECodec TreeSerializationCodec;

    // COMPAT(ignat)
    //! Forbids performing set inside Cypress.
    bool ForbidSetCommand;

    // COMPAT(shakurov)
    //! Controls if unlock command is enabled.
    bool EnableUnlockCommand;

    TDynamicCypressManagerConfig();
};

DEFINE_REFCOUNTED_TYPE(TDynamicCypressManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
