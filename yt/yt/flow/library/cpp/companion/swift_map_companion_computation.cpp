#include "swift_map_companion_computation.h"

#include "companion_model.h"

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

TSwiftMapCompanionComputation::TSwiftMapCompanionComputation(
    TComputationContextPtr context,
    TDynamicComputationContextPtr dynamicContext)
    : TCompanionComputationBaseAdapter<TSwiftMapComputation>(std::move(context), std::move(dynamicContext))
{ }

void TSwiftMapCompanionComputation::DoInit(IJobInitContextPtr /*initContext*/)
{
    FetchAndValidateCompanionInfo();

    PutJobInfoToCompanionWithReconfigure();
}

void TSwiftMapCompanionComputation::DoProcess(IInputContextPtr input, IOutputCollectorPtr output)
{
    YT_TLOG_DEBUG("Starting DoProcess")
        .With("MessagesSize", input->GetMessages().size());
    if (input->GetMessages().size() == 0) {
        YT_TLOG_DEBUG("Empty inputs. Returning.");
        return;
    }
    auto request = CreateCompanionRequest<TCompanionProcessRequest>();
    // Messages.
    THashMap<TMessageId, TInputMessageConstPtr> messageMap;
    request->Messages.reserve(input->GetMessages().size());
    for (const auto& message : input->GetMessages()) {
        request->Messages.push_back(message);
        messageMap[message->MessageId] = message;
    }

    // Request to Companion.
    auto response = DoProcessWithCompanion(request);

    // Result processing.
    for (auto& group : response->Groups) {
        THROW_ERROR_EXCEPTION_UNLESS(group.Timers.empty(), "Timers are not supported in swift map computation");
        std::vector<TInputMessageConstPtr> groupParents;
        for (const auto& parentId : group.ParentIds) {
            groupParents.push_back(GetOrCrash(messageMap, parentId));
        }
        auto groupOutput = output->SetParents(groupParents, {}, {});
        for (auto& message : group.Messages) {
            groupOutput->AddMessage(std::move(message));
        }
    }

    YT_TLOG_DEBUG("DoProcess finished");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
