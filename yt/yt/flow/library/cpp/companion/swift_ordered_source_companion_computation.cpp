#include "swift_ordered_source_companion_computation.h"

#include "companion_model.h"

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

TSwiftOrderedSourceCompanionComputation::TSwiftOrderedSourceCompanionComputation(
    TComputationContextPtr context,
    TDynamicComputationContextPtr dynamicContext)
    : TCompanionComputationBaseAdapter<TSwiftOrderedSourceComputation>(std::move(context), std::move(dynamicContext))
{ }

void TSwiftOrderedSourceCompanionComputation::DoInit(IJobInitContextPtr /*initContext*/)
{
    InitCompanionJob();

    YT_TLOG_INFO("Computation initialized");
}

void TSwiftOrderedSourceCompanionComputation::DoProcess(IInputContextPtr input, IOutputCollectorPtr output)
{
    YT_TLOG_DEBUG("Starting DoProcess")
        .With("MessagesSize", input->GetMessages().size());
    if (input->GetMessages().size() == 0) {
        YT_TLOG_DEBUG("Empty inputs. Returning.");
        return;
    }
    auto request = CreateCompanionRequest<TCompanionProcessRequest>();
    // Messages for parent ids mapping.
    THashMap<TMessageId, TInputMessageConstPtr> messageMap;
    // Stream specs of all input messages.
    THashMap<TStreamId, NTableClient::TTableSchemaPtr> sourceStreamsSchemas;
    request->Messages.reserve(input->GetMessages().size());
    for (const auto& message : input->GetMessages()) {
        request->Messages.push_back(message);
        messageMap[message->MessageId] = message;
        sourceStreamsSchemas[message->StreamId] = message->PayloadSchema;
    }

    // Custom TComputationStreamSpecStorage with source streams schemas.
    auto localStreamSpecs = CreateLocalStreamSpecs(
        sourceStreamsSchemas,
        GetSpec()->OutputStreamIds,
        GetContext()->StreamSpecStorage->GetStreamSpecs());
    request->OverrideStreamSpecs = localStreamSpecs;

    // Request to Companion.
    auto response = DoProcessWithCompanion(request);

    // Result processing.
    for (auto& group : response->Groups) {
        THROW_ERROR_EXCEPTION_UNLESS(group.Timers.empty(), "Timers are not supported in source computation");
        std::vector<TInputMessageConstPtr> groupParents;
        for (const auto& parentId : group.ParentIds) {
            groupParents.push_back(GetOrCrash(messageMap, parentId));
        }
        auto groupOutput = output->SetParents(groupParents, {}, {});
        for (int i = 0; i < std::ssize(group.Messages); ++i) {
            // Empty Distribute means "distribute all".
            bool distribute = group.Distribute.empty() ? true : group.Distribute[i];
            groupOutput->AddMessage(std::move(group.Messages[i]), distribute);
        }
    }

    YT_TLOG_DEBUG("DoProcess finished");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
