#pragma once

#include "trace_event_processor.h"

#include <yt/yt/client/table_client/table_output.h>

#include <yt/yt/core/yson/forwarding_consumer.h>

#include <yt/yt/core/json/json_writer.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TTraceConsumer
    : public NYson::TForwardingYsonConsumer
{
public:
    explicit TTraceConsumer(TJobTraceEventProcessorPtr jobTraceEventProcessor);

private:
    std::unique_ptr<NJson::IJsonWriter> CreateJsonWriter();

    TJobTraceEventProcessorPtr JobTraceEventProcessor_;

    // TODO(omgronny): Improve attribute consumer via adding a certain set of attributes.
    NYTree::IAttributeDictionaryPtr Attributes_;
    NYTree::TAttributeConsumer AttributeConsumer_;

    void OnMyListItem() override;

    std::unique_ptr<NJson::IJsonWriter> JsonWriter_;
    TStringStream JsonStream_;

    std::unique_ptr<NTableClient::TTableOutput> Output_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
