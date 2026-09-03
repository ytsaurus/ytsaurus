#pragma once

#include "public.h"

#include "message.h"
#include "timer.h"

#include <library/cpp/yt/memory/ref_counted.h>

#include <optional>
#include <string>
#include <vector>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Selects how a Swift computation identifies sibling output messages.
class TOutputMessageIdSuffix
{
public:
    enum class EMode
    {
        SequenceNumber,
        PayloadHash,
        UserDefined,
    };

    static TOutputMessageIdSuffix FromSequenceNumber();
    static TOutputMessageIdSuffix FromPayloadHash();
    static TOutputMessageIdSuffix FromUserDefined(std::string suffix);

    EMode GetMode() const;
    const std::string& GetValue() const;
    std::string Resolve(const TMessage& message, i64 sequenceNumber) const;

    bool operator==(const TOutputMessageIdSuffix&) const = default;

private:
    EMode Mode_;
    std::string Value_;

    explicit TOutputMessageIdSuffix(EMode mode, std::string value = {});
};

////////////////////////////////////////////////////////////////////////////////

struct TAddMessageOptions
{
    bool Distribute = true;
    TOutputMessageIdSuffix MessageIdSuffix = TOutputMessageIdSuffix::FromSequenceNumber();
};

////////////////////////////////////////////////////////////////////////////////

//! Sink for messages and timers produced by a computation's (or process function's)
//! processing logic. The worker backs it with TRootOutputCollector / TOutputCollector
//! (see library/cpp/computation); tests back it with a recording implementation.
struct IOutputCollector
    : public TRefCounted
{
    // TODO(YTFLOW-500): drop the default and migrate downstream 2-arg callers.
    [[nodiscard]] virtual IOutputCollectorPtr SetParents(
        const std::vector<TInputMessageConstPtr>& messages,
        const std::vector<TInputTimerConstPtr>& timers,
        const std::vector<TInputVisitConstPtr>& visits = {}) = 0;
    //! Adds an output message.
    void AddMessage(TMessage&& message, TAddMessageOptions options = {});
    void AddMessage(TMessage&& message, bool distribute);

    virtual void AddTimer(TSystemTimestamp triggerTimestamp, std::optional<TSystemTimestamp> eventTimestamp = {}) = 0;
    virtual void AddTimer(const TStreamId& streamId, TSystemTimestamp triggerTimestamp, std::optional<TSystemTimestamp> eventTimestamp = {}) = 0;
    virtual void AddTimer(TTimer&& timer) = 0;

private:
    //! For a source computation, |options.Distribute| = false keeps the message out of the
    //! downstream output while still letting it advance the watermark. Other computations drop it.
    virtual void DoAddMessage(TMessage&& message, TAddMessageOptions options) = 0;
};

DEFINE_REFCOUNTED_TYPE(IOutputCollector);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
