#include "process_function.h"

#include "key_error.h"
#include "runtime_context.h"
#include "runtime_init_context.h"

#include <util/generic/hash.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void IProcessFunctionBase::Init(const IRuntimeInitContextPtr& /*initContext*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void IProcessFunction::ProcessMessage(const TInputMessageConstPtr& /*message*/, const IOutputCollectorPtr& /*output*/, const IRuntimeContextPtr& /*context*/)
{ }

void IProcessFunction::ProcessTimer(const TInputTimerConstPtr& /*timer*/, const IOutputCollectorPtr& /*output*/, const IRuntimeContextPtr& /*context*/)
{ }

void IProcessFunction::ProcessVisit(const TInputVisitConstPtr& /*visit*/, const IOutputCollectorPtr& /*output*/, const IRuntimeContextPtr& /*context*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void IBatchProcessFunction::Process(const IInputContextPtr& /*input*/, const IOutputCollectorPtr& /*output*/, const IRuntimeContextPtr& /*context*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void IKeyedBatchProcessFunction::ProcessKey(const IInputContextPtr& /*input*/, const IOutputCollectorPtr& /*output*/, const IRuntimeContextPtr& /*context*/)
{ }

////////////////////////////////////////////////////////////////////////////////

namespace {

//! Drives a per-element function over the whole epoch: timer, then message, then visit (the
//! worker's order, preserved within each key by the flat iteration).
class TElementBatchAdapter
    : public IBatchProcessFunction
{
public:
    explicit TElementBatchAdapter(IProcessFunctionPtr function)
        : Function_(std::move(function))
    { }

    void Process(const IInputContextPtr& input, const IOutputCollectorPtr& output, const IRuntimeContextPtr& context) override
    {
        for (const auto& timer : input->GetTimers()) {
            TagErrorWithKey("timer", timer->Key, [&] {
                Function_->ProcessTimer(timer, output->SetParents({}, {timer}, {}), context);
            });
        }
        for (const auto& message : input->GetMessages()) {
            TagErrorWithKey("message", message->Key, [&] {
                Function_->ProcessMessage(message, output->SetParents({message}, {}, {}), context);
            });
        }
        for (const auto& visit : input->GetVisits()) {
            TagErrorWithKey("visit", visit->Key, [&] {
                Function_->ProcessVisit(visit, output->SetParents({}, {}, {visit}), context);
            });
        }
    }

private:
    const IProcessFunctionPtr Function_;
};

//! Drives a per-key function: groups the epoch's input by key and invokes ProcessKey per key.
class TKeyedBatchAdapter
    : public IBatchProcessFunction
{
public:
    explicit TKeyedBatchAdapter(IKeyedBatchProcessFunctionPtr function)
        : Function_(std::move(function))
    { }

    void Process(const IInputContextPtr& input, const IOutputCollectorPtr& output, const IRuntimeContextPtr& context) override
    {
        struct TKeyGroup
        {
            std::vector<TInputMessageConstPtr> Messages;
            std::vector<TInputTimerConstPtr> Timers;
            std::vector<TInputVisitConstPtr> Visits;
        };

        THashMap<TKey, TKeyGroup> groups;
        for (const auto& message : input->GetMessages()) {
            groups[message->Key].Messages.push_back(message);
        }
        for (const auto& timer : input->GetTimers()) {
            groups[timer->Key].Timers.push_back(timer);
        }
        for (const auto& visit : input->GetVisits()) {
            groups[visit->Key].Visits.push_back(visit);
        }
        for (const auto& [key, group] : groups) {
            TagErrorWithKey("key", key, [&] {
                Function_->ProcessKey(
                    New<TInputContext>(group.Messages, group.Timers, group.Visits),
                    output->SetParents(group.Messages, group.Timers, group.Visits),
                    context);
            });
        }
    }

private:
    const IKeyedBatchProcessFunctionPtr Function_;
};

} // namespace

IBatchProcessFunctionPtr WrapAsBatch(const IProcessFunctionBasePtr& function)
{
    if (auto batch = DynamicPointerCast<IBatchProcessFunction>(function)) {
        return batch;
    }
    if (auto element = DynamicPointerCast<IProcessFunction>(function)) {
        return New<TElementBatchAdapter>(std::move(element));
    }
    if (auto keyed = DynamicPointerCast<IKeyedBatchProcessFunction>(function)) {
        return New<TKeyedBatchAdapter>(std::move(keyed));
    }
    THROW_ERROR_EXCEPTION("Process function does not implement a known granularity interface");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
