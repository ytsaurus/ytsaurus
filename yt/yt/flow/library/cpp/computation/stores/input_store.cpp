#include "input_store.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/inflight_tracker.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/tables/input_messages.h>

#include <library/cpp/iterator/iterate_values.h>

#include <util/generic/hash.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

using NConcurrency::WaitFor;

////////////////////////////////////////////////////////////////////////////////

class TInputStore
    : public IInputStore
{
public:
    explicit TInputStore(TInputStoreContextPtr context, TDynamicInputStoreSpecPtr dynamicSpec)
        : Context_(std::move(context))
        , DynamicSpec_(std::move(dynamicSpec))
        , Table_(Context_->InputMessagesTable)
        , Logger(Context_->Logger)
        , CheckedCounter_(Context_->Profiler.WithPrefix("/input_store").Counter("/checked"))
        , FilteredCounter_(Context_->Profiler.WithPrefix("/input_store").Counter("/filtered"))
    {
        YT_VERIFY(Table_);
    }

    void Reconfigure(TDynamicInputStoreSpecPtr dynamicSpec) override
    {
        DynamicSpec_ = std::move(dynamicSpec);
    }

    void AdvanceSystemWatermark(TSystemTimestamp systemWatermark) override
    {
        SystemTimestamp_.store(systemWatermark);
    }

    TSystemTimestamp GetSystemWatermark() const override
    {
        return SystemTimestamp_.load();
    }

    TFilterResult Filter(const std::vector<TInputMessageConstPtr>& messages, bool checkState) override
    {
        CheckedCounter_.Increment(std::ssize(messages));
        if (!checkState) {
            const auto systemWatermark = GetSystemWatermark();
            TFilterResult result;
            result.Unprocessed.reserve(messages.size());
            for (const auto& message : messages) {
                if (message->SystemTimestamp < systemWatermark) {
                    YT_LOG_DEBUG("MessageLifeCycle.PartitionStore: filtered by SystemTimestamp (MessageId: %v, StreamId: %v, SystemTimestamp: %v, InputSystemWatermark: %v)",
                        message->MessageId,
                        message->StreamId,
                        message->SystemTimestamp,
                        systemWatermark);
                    result.Processed.push_back(message);
                } else {
                    result.Unprocessed.push_back(message);
                }
            }
            FilteredCounter_.Increment(std::ssize(result.Processed));
            return result;
        }

        std::vector<NTables::TInputMessages::TMessage> messageKeys;
        messageKeys.reserve(messages.size());
        for (const auto& message : messages) {
            messageKeys.push_back({.Key = message->Key, .MessageId = message->MessageId, .SystemTimestamp = message->SystemTimestamp});
        }
        auto isProcessed = WaitFor(Table_->Contains(Context_->Partition->ComputationId, messageKeys)).ValueOrThrow();

        const auto systemWatermark = GetSystemWatermark();
        YT_VERIFY(std::ssize(isProcessed) == std::ssize(messages));

        TFilterResult result;
        for (int i = 0; i < std::ssize(messages); ++i) {
            if (isProcessed[i]) {
                YT_LOG_DEBUG("MessageLifeCycle.PartitionStore: filtered by InputMessages (MessageId: %v, StreamId: %v)",
                    messages[i]->MessageId,
                    messages[i]->StreamId);
                result.Processed.push_back(messages[i]);
            } else if (messages[i]->SystemTimestamp < systemWatermark) {
                YT_LOG_DEBUG("MessageLifeCycle.PartitionStore: filtered by SystemTimestamp (MessageId: %v, StreamId: %v, SystemTimestamp: %v, InputSystemWatermark: %v)",
                    messages[i]->MessageId,
                    messages[i]->StreamId,
                    messages[i]->SystemTimestamp,
                    systemWatermark);
                result.Processed.push_back(messages[i]);
            } else {
                result.Unprocessed.push_back(messages[i]);
            }
        }

        YT_VERIFY(std::ssize(result.Processed) + std::ssize(result.Unprocessed) == std::ssize(messages));
        FilteredCounter_.Increment(std::ssize(result.Processed));
        return result;
    }

    void Register(const std::vector<TInputMessageConstPtr>& messages) override
    {
        for (const auto& message : messages) {
            // ValidateMessage(*message);
            if (!Context_->InputStreamIds.contains(message->StreamId)) {
                THROW_ERROR_EXCEPTION("Unknown input stream %Qv",
                    message->StreamId);
            }

            YT_LOG_DEBUG("MessageLifeCycle.InputStore: message was registered (MessageId: %v, StreamId: %v)",
                message->MessageId,
                message->StreamId);

            ConfirmedMessages_.push_back({
                .Key = message->Key,
                .MessageId = message->MessageId,
                .SystemTimestamp = message->SystemTimestamp,
            });
        }
    }

    TFuture<void> Init() override
    {
        return OKFuture;
    }

    void Sync(NApi::IDynamicTableTransactionPtr tx) override
    {
        Table_->Write(tx, Context_->Partition->ComputationId, ConfirmedMessages_);
        ConfirmedMessages_ = {};
    }

private:
    const TInputStoreContextPtr Context_;
    TDynamicInputStoreSpecPtr DynamicSpec_;
    const NTables::IInputMessagesPtr Table_;
    const NLogging::TLogger Logger;
    std::atomic<TSystemTimestamp> SystemTimestamp_ = ZeroSystemTimestamp;

    std::vector<NTables::TInputMessages::TMessage> ConfirmedMessages_;

    NProfiling::TCounter CheckedCounter_;
    NProfiling::TCounter FilteredCounter_;
};

////////////////////////////////////////////////////////////////////////////////

IInputStorePtr CreateInputStore(TInputStoreContextPtr context, TDynamicInputStoreSpecPtr dynamicSpec)
{
    return New<TInputStore>(std::move(context), std::move(dynamicSpec));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
