#pragma once

#include <yt/yt/flow/library/cpp/common/runtime_context.h>

#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <util/generic/hash.h>

#include <optional>
#include <string>

namespace NYT::NFlow::NTesting {

////////////////////////////////////////////////////////////////////////////////

//! Builds an IRuntimeContext with defaults for unit tests; every setter is optional (zero
//! watermarks, one output stream per registered stream, DefaultTestKeySchema()).
class TTestRuntimeContextBuilder
{
public:
    //! Registers an output stream carrying YSON message type T (schema derived from T).
    template <CYsonMessage T>
    TTestRuntimeContextBuilder& RegisterStream(const TStreamId& streamId)
    {
        auto spec = New<TStreamSpec>();
        spec->ClassName = TypeName<T>();
        spec->Schema = GetYsonMessagePayloadSchema<T>();
        EmplaceOrCrash(Streams_, streamId, std::move(spec));
        return *this;
    }

    //! Registers an output stream by its payload schema, for a function that fills the columns
    //! itself (via MakeOutputMessageBuilder) rather than emitting a YSON message.
    TTestRuntimeContextBuilder& RegisterStream(
        const TStreamId& streamId,
        NTableClient::TTableSchemaPtr schema);

    //! Sets a stream's event watermark. The input watermark mins over the spec's input_stream_ids,
    //! so declare them via SetSpec to exercise it.
    TTestRuntimeContextBuilder& SetWatermark(const TStreamId& streamId, TSystemTimestamp value);
    //! Sets the timestamp returned by IRuntimeContext::GetCurrentTimestamp().
    TTestRuntimeContextBuilder& SetCurrentTimestamp(TSystemTimestamp value);
    //! Sets the sequence number returned by IRuntimeContext::GetEpochUniqueSeqNo(). Left unset the
    //! accessor throws, matching a computation kind that publishes none.
    TTestRuntimeContextBuilder& SetEpochUniqueSeqNo(TUniqueSeqNo value);
    TTestRuntimeContextBuilder& SetKeySchema(NTableClient::TTableSchemaPtr schema);
    TTestRuntimeContextBuilder& SetSpec(TComputationSpecPtr spec);

    //! Names the hosted process function (as registered via YT_FLOW_DEFINE_PROCESS_FUNCTION);
    //! GetDynamicParameters<T>() parses the dynamic block into the type registered for it.
    TTestRuntimeContextBuilder& SetProcessingFunction(std::string name);

    template <class TFunction>
    TTestRuntimeContextBuilder& SetProcessingFunction()
    {
        return SetProcessingFunction(std::string(TypeName<TFunction>()));
    }

    //! Sets the dynamic ``function_parameters`` the context serves; |parameters| travels the
    //! production path — serialized and reparsed into the type SetProcessingFunction() names.
    TTestRuntimeContextBuilder& SetDynamicParameters(const NYTree::TYsonStructPtr& parameters);

    IRuntimeContextPtr Build() const;

private:
    THashMap<TStreamId, TStreamSpecPtr> Streams_;
    THashMap<TStreamId, TSystemTimestamp> Watermarks_;
    TSystemTimestamp CurrentTimestamp_;
    std::optional<TUniqueSeqNo> EpochUniqueSeqNo_;
    NTableClient::TTableSchemaPtr KeySchema_;
    TComputationSpecPtr Spec_;
    std::optional<std::string> ProcessingFunction_;
    NYTree::IMapNodePtr DynamicParametersNode_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTesting
