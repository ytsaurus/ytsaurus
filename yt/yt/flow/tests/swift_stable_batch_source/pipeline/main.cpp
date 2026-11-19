#include <yt/yt/flow/library/cpp/computation/transform_computation.h>

#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/resources/yt_client_factory.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <library/cpp/yt/farmhash/farm_hash.h>

#include <util/string/join.h>

#include <map>

namespace NExample {

using namespace NYT;
using namespace NYT::NFlow;
using namespace NYT::NTableClient;

////////////////////////////////////////////////////////////////////////////////

struct TAggregateByKeyParameters
    : public NYTree::TYsonStruct
{
    //! Ordered dynamic table, with a cluster, that gets a row for every processed read group.
    std::optional<NYPath::TRichYPath> GroupLogPath;

    REGISTER_YSON_STRUCT(TAggregateByKeyParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("group_log_path", &TThis::GroupLogPath)
            .Default();
    }
};

//! Aggregates the messages of one read group by key: one output per key carries the count and the
//! input `event_id`s, and lists the key's messages as its parents.
class TAggregateByKey
    : public IBatchProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override
    {
        GroupLogPath_ = initContext->GetParameters<TAggregateByKeyParameters>()->GroupLogPath;
        if (GroupLogPath_) {
            GroupLogClient_ = initContext->GetStaticResource("YTClientFactory")
                ->As<IYTClientFactory>()
                ->GetClient(*GroupLogPath_->GetCluster());
        }
    }

    void Process(
        const IInputContextPtr& input,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        const auto& groupMessages = input->GetMessages();
        LogGroup(groupMessages);
        const auto& groupId = groupMessages.front()->MessageId.Underlying();

        // An ordered map keeps the output order deterministic.
        std::map<ui64, std::vector<TInputMessageConstPtr>> messagesByKey;
        for (const auto& message : groupMessages) {
            messagesByKey[GetColumnValue<ui64>(message, "key")].push_back(message);
        }

        for (const auto& [key, messages] : messagesByKey) {
            std::vector<i64> eventIds;
            eventIds.reserve(messages.size());
            for (const auto& message : messages) {
                eventIds.push_back(GetColumnValue<i64>(message, "event_id"));
            }
            auto builder = context->MakeOutputMessageBuilder();
            builder.Payload().SetValue(MakeUnversionedUint64Value(key), "key");
            builder.Payload().SetValue(MakeUnversionedInt64Value(std::ssize(messages)), "count");
            builder.Payload().SetValue(MakeUnversionedStringValue(JoinSeq(",", eventIds)), "event_ids");
            builder.Payload().SetValue(MakeUnversionedStringValue(groupId), "group_id");
            output->SetParents(messages, {}, {})->AddMessage(builder.Finish());
        }
    }

private:
    std::optional<NYPath::TRichYPath> GroupLogPath_;
    NApi::IClientPtr GroupLogClient_;

    //! Writes the group in its own transaction, outside the epoch, so that every processing of the
    //! group, a replay included, leaves a row.
    void LogGroup(const std::vector<TInputMessageConstPtr>& messages)
    {
        if (!GroupLogClient_) {
            return;
        }

        TFingerprint idsHash = 0;
        std::vector<i64> eventIds;
        eventIds.reserve(messages.size());
        for (const auto& message : messages) {
            idsHash = FarmFingerprint(idsHash, FarmFingerprint(TStringBuf(message->MessageId.Underlying())));
            eventIds.push_back(GetColumnValue<i64>(message, "event_id"));
        }

        auto nameTable = New<TNameTable>();
        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedStringValue(messages.front()->MessageId.Underlying(), nameTable->RegisterNameOrThrow("group_id")));
        builder.AddValue(MakeUnversionedStringValue(messages.back()->MessageId.Underlying(), nameTable->RegisterNameOrThrow("last_message_id")));
        builder.AddValue(MakeUnversionedInt64Value(std::ssize(messages), nameTable->RegisterNameOrThrow("count")));
        builder.AddValue(MakeUnversionedStringValue(JoinSeq(",", eventIds), nameTable->RegisterNameOrThrow("event_ids")));
        builder.AddValue(MakeUnversionedUint64Value(idsHash, nameTable->RegisterNameOrThrow("ids_hash")));
        auto row = builder.FinishRow();

        auto transaction = NConcurrency::WaitFor(GroupLogClient_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
            .ValueOrThrow();
        transaction->WriteRows(GroupLogPath_->GetPath(), nameTable, MakeSharedRange(std::vector<TUnversionedRow>{row}, row));
        NConcurrency::WaitFor(transaction->Commit())
            .ThrowOnError();
    }
};

YT_FLOW_DEFINE_PROCESS_FUNCTION(TAggregateByKey, TAggregateByKeyParameters);

////////////////////////////////////////////////////////////////////////////////

//! Writes every aggregate with its message id into the sink queue.
class TWriter
    : public TTransformComputation
{
private:
    struct TWriterDynamicParameters
        : public virtual TTransformComputation::TDynamicParameters
    {
        //! Keeps the epoch from completing, so no aggregate is delivered and the read groups stay
        //! unpersisted.
        bool HoldProcessing = false;

        REGISTER_YSON_STRUCT(TWriterDynamicParameters);

        static void Register(TRegistrar registrar)
        {
            registrar.Parameter("hold_processing", &TThis::HoldProcessing)
                .Default(false);
        }
    };

public:
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TWriterDynamicParameters);

    using TTransformComputation::TTransformComputation;

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        while (GetDynamicParameters()->HoldProcessing) {
            NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));
        }

        auto builder = MakeOutputMessageBuilder("sink_row");
        builder.Payload().SetValue(MakeUnversionedUint64Value(GetColumnValue<ui64>(message, "key")), "key");
        builder.Payload().SetValue(MakeUnversionedInt64Value(GetColumnValue<i64>(message, "count")), "count");
        builder.Payload().SetValue(MakeUnversionedStringValue(GetColumnValue<std::string>(message, "event_ids")), "event_ids");
        builder.Payload().SetValue(MakeUnversionedStringValue(message.MessageId.Underlying()), "message_id");
        builder.Payload().SetValue(MakeUnversionedStringValue(GetColumnValue<std::string>(message, "group_id")), "group_id");
        output->AddMessage(builder.Finish());
    }
};

YT_FLOW_DEFINE_COMPUTATION(TWriter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NExample

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    return NYT::NFlow::TSimpleRunnerProgram().Run(argc, argv);
}
