#include "input_store.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/inflight_tracker.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/tables/input_messages.h>

#include <library/cpp/iterator/iterate_values.h>
#include <library/cpp/iterator/zip.h>

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
        const auto systemWatermark = GetSystemWatermark();
        TFilterResult result;
        std::vector<TInputMessageConstPtr> messagesToCheck;
        messagesToCheck.reserve(messages.size());
        for (const auto& message : messages) {
            if (message->SystemTimestamp < systemWatermark) {
                YT_TLOG_DEBUG("MessageLifeCycle.PartitionStore: filtered by SystemTimestamp")
                    .With("MessageId", message->MessageId)
                    .With("StreamId", message->StreamId)
                    .With("SystemTimestamp", message->SystemTimestamp)
                    .With("InputSystemWatermark", systemWatermark);
                result.Processed.push_back(message);
            } else {
                messagesToCheck.push_back(message);
            }
        }

        if (checkState && !messagesToCheck.empty()) {
            std::vector<NTables::TInputMessages::TMessage> messageKeys;
            messageKeys.reserve(messagesToCheck.size());
            for (const auto& message : messagesToCheck) {
                messageKeys.push_back({.Key = message->Key, .MessageId = message->MessageId, .SystemTimestamp = message->SystemTimestamp});
            }
            auto isProcessed = WaitFor(Table_->Contains(Context_->Partition->ComputationId, messageKeys))
                .ValueOrThrow();
            YT_VERIFY(std::ssize(isProcessed) == std::ssize(messagesToCheck));

            result.Unprocessed.reserve(messagesToCheck.size());
            for (auto&& [message, alreadyProcessed] : Zip(messagesToCheck, isProcessed)) {
                if (alreadyProcessed) {
                    YT_TLOG_DEBUG("MessageLifeCycle.PartitionStore: filtered by InputMessages")
                        .With("MessageId", message->MessageId)
                        .With("StreamId", message->StreamId);
                    result.Processed.push_back(std::move(message));
                } else {
                    result.Unprocessed.push_back(std::move(message));
                }
            }
        } else {
            result.Unprocessed = std::move(messagesToCheck);
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

            YT_TLOG_DEBUG("MessageLifeCycle.InputStore: message was registered")
                .With("MessageId", message->MessageId)
                .With("StreamId", message->StreamId);

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
