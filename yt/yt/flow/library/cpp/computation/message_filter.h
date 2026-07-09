#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/message.h>

#include <library/cpp/yt/memory/ref_counted.h>

#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Reserved column names through which the predicate references message meta.
//! Payload columns are referenced by their own names.
inline constexpr std::string_view MessageIdFilterColumnName = "$message_id";
inline constexpr std::string_view StreamIdFilterColumnName = "$stream_id";
inline constexpr std::string_view SystemTimestampFilterColumnName = "$system_timestamp";
inline constexpr std::string_view EventTimestampFilterColumnName = "$event_timestamp";
inline constexpr std::string_view AlignmentTimestampFilterColumnName = "$alignment_timestamp";

////////////////////////////////////////////////////////////////////////////////

struct TMessageFilterResult
{
    std::vector<TInputMessageConstPtr> Kept;
    std::vector<TInputMessageConstPtr> Skipped;
};

//! A filter driven by the dynamic-spec ``skip_if_expression``.
//! A message is skipped when the YTQL predicate evaluates to boolean
//! true; a non-boolean result (including NULL) throws.
//!
//! Thread-safe: #Reconfigure() may run concurrently with the read methods.
struct IMessageFilter
    : public TRefCounted
{
    //! Switches to a new expression, rebuilding the evaluator only when it changes.
    virtual void Reconfigure(const std::optional<std::string>& skipIfExpression) = 0;

    //! False when no expression is configured; reads are a no-op in that case.
    virtual bool IsEnabled() const = 0;

    virtual bool ShouldSkip(const TMessage& message) const = 0;

    //! Splits messages into kept and skipped.
    virtual TMessageFilterResult Partition(std::vector<TInputMessageConstPtr> messages) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IMessageFilter)

////////////////////////////////////////////////////////////////////////////////

IMessageFilterPtr CreateMessageFilter(const std::optional<std::string>& skipIfExpression = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
