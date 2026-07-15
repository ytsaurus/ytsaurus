#pragma once

#include <yt/yt/flow/library/cpp/common/process_function.h>

#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>

#include <yt/yt/flow/library/cpp/common/state_client.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow::NExample {

////////////////////////////////////////////////////////////////////////////////

//! An event of the realtime stream, carrying only the key to enrich.
struct TEventRow
    : public TYsonMessage
{
    ui64 Key{};

    REGISTER_YSON_STRUCT(TEventRow);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

//! Output of the join: the event key enriched with the name looked up in the
//! reference table.
struct TEnrichedRow
    : public TYsonMessage
{
    ui64 Key{};
    std::string Name;

    REGISTER_YSON_STRUCT(TEnrichedRow);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

//! Joins each realtime event against a pre-built dynamic reference table reached
//! through a Cypress symlink. The joiner is read-only: it never writes the state,
//! it only looks it up by key. Repointing the symlink atomically swaps the whole
//! reference dataset under the running pipeline.
class TLookupJoinFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override;

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override;

private:
    TJoinedStateKeyClient<TSimpleExternalState> ReferenceJoiner_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NExample
