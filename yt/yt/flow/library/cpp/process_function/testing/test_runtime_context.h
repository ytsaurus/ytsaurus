#pragma once

#include <yt/yt/flow/library/cpp/common/runtime_context.h>

#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <util/generic/hash.h>

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

    //! Sets a stream's event watermark. The input watermark mins over the spec's input_stream_ids,
    //! so declare them via SetSpec to exercise it.
    TTestRuntimeContextBuilder& SetWatermark(const TStreamId& streamId, TSystemTimestamp value);
    //! Sets the timestamp returned by IRuntimeContext::GetCurrentTimestamp().
    TTestRuntimeContextBuilder& SetCurrentTimestamp(TSystemTimestamp value);
    TTestRuntimeContextBuilder& SetKeySchema(NTableClient::TTableSchemaPtr schema);
    TTestRuntimeContextBuilder& SetSpec(TComputationSpecPtr spec);

    //! Sets the dynamic ``function_parameters`` node returned by
    //! IRuntimeContext::GetDynamicParameters<T>().
    TTestRuntimeContextBuilder& SetDynamicParametersNode(NYTree::IMapNodePtr node);

    //! Typed convenience over SetDynamicParametersNode: serializes |parameters| to a node.
    template <class T>
    TTestRuntimeContextBuilder& SetDynamicParameters(const TIntrusivePtr<T>& parameters)
    {
        return SetDynamicParametersNode(NYTree::ConvertTo<NYTree::IMapNodePtr>(parameters));
    }

    IRuntimeContextPtr Build() const;

private:
    THashMap<TStreamId, TStreamSpecPtr> Streams_;
    THashMap<TStreamId, TSystemTimestamp> Watermarks_;
    TSystemTimestamp CurrentTimestamp_;
    NTableClient::TTableSchemaPtr KeySchema_;
    TComputationSpecPtr Spec_;
    NYTree::IMapNodePtr DynamicParametersNode_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTesting
