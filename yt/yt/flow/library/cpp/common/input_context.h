#pragma once

#include "public.h"

#include "payload_converter.h"

#include <yt/yt/client/table_client/public.h>

#include <library/cpp/yt/memory/ref_counted.h>

#include <optional>
#include <vector>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Messages, timers and visits for a single #DoProcess() call.
struct IInputContext
    : public TRefCounted
{
    virtual const std::vector<TInputMessageConstPtr>& GetMessages() const = 0;
    virtual const std::vector<TInputTimerConstPtr>& GetTimers() const = 0;
    virtual const std::vector<TInputVisitConstPtr>& GetVisits() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IInputContext);

////////////////////////////////////////////////////////////////////////////////

class TInputContext
    : public IInputContext
{
public:
    TInputContext(
        const std::vector<TInputMessageConstPtr>& messages,
        const std::vector<TInputTimerConstPtr>& timers,
        const std::vector<TInputVisitConstPtr>& visits = {});

    const std::vector<TInputMessageConstPtr>& GetMessages() const override;
    const std::vector<TInputTimerConstPtr>& GetTimers() const override;
    const std::vector<TInputVisitConstPtr>& GetVisits() const override;

private:
    std::vector<TInputMessageConstPtr> Messages_;
    std::vector<TInputTimerConstPtr> Timers_;
    std::vector<TInputVisitConstPtr> Visits_;
};

////////////////////////////////////////////////////////////////////////////////

//! Returns keys for messages and timers in |context|, optionally filtered by stream and
//! re-extracted under |schemaOverride| (which must contain only non-expression columns).
THashSet<TKey> ExtractKeys(
    const IInputContextPtr& context,
    const NTableClient::TTableSchemaPtr& schemaOverride,
    const std::optional<THashSet<TStreamId>>& streamFilter,
    const IPayloadConverterCachePtr& converterCache);

//! Shorthand for ``ExtractKeys(context, nullptr, std::nullopt, nullptr)``.
THashSet<TKey> ExtractKeys(const IInputContextPtr& context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
