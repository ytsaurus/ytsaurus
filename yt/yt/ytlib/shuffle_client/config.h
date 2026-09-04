#pragma once

#include "public.h"

#include <yt/yt/ytlib/distributed_chunk_session_client/public.h>

#include <yt/yt/ytlib/push_based_shuffle_client/public.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NShuffleClient {

////////////////////////////////////////////////////////////////////////////////

struct TPullShuffleConfig
    : public NYTree::TYsonStruct
{
    NTableClient::TTableReaderConfigPtr Reader;
    NTableClient::TTableWriterConfigPtr Writer;

    REGISTER_YSON_STRUCT(TPullShuffleConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPullShuffleConfig)

////////////////////////////////////////////////////////////////////////////////

struct TPushShuffleConfig
    : public NYTree::TYsonStruct
{
    //! Map-side L2 writer config (client).
    NPushBasedShuffleClient::TShuffleWriterConfigPtr Writer;
    //! Reducer-side L2 reader config (client).
    NPushBasedShuffleClient::TPartitionReaderConfigPtr Reader;
    //! Sequencer journal writer config: batch/flush knobs (server).
    NApi::TJournalChunkWriterConfigPtr JournalWriter;
    //! Distributed chunk session pool config, e.g. max_active_sessions_per_slot (server).
    NDistributedChunkSessionClient::TDistributedChunkSessionPoolConfigPtr SessionPool;

    REGISTER_YSON_STRUCT(TPushShuffleConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPushShuffleConfig)

////////////////////////////////////////////////////////////////////////////////

//! Caller-supplied shuffle configuration. Set once at start_shuffle, travels on the signed
//! handle and is shared by every writer and reader of the shuffle. Exactly one mode section
//! applies; the other must be left unset.
struct TShuffleConfig
    : public NYTree::TYsonStruct
{
    TPullShuffleConfigPtr Pull;
    TPushShuffleConfigPtr Push;

    REGISTER_YSON_STRUCT(TShuffleConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TShuffleConfig)

////////////////////////////////////////////////////////////////////////////////

//! Fills in the mode section the shuffle actually uses when the caller left it unset, so that
//! both the coordinator and the client can dereference it unconditionally.
void EnsureModeSection(const TShuffleConfigPtr& config, bool usePushBasedShuffle);

////////////////////////////////////////////////////////////////////////////////

//! Config carried by #handle, with the section matching its mode filled in when absent.
// COMPAT(apollo1321): Drop in 26.3 and parse the handle config directly; a 26.1 coordinator
// mints handles without a config.
TShuffleConfigPtr GetShuffleConfig(const NApi::TShuffleHandlePtr& handle);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShuffleClient
