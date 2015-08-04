#pragma once

#include "public.h"
#include "async_consumer.h"

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////

//! Consumes a map fragment representing the attributes
//! and if the fragment is non-empty then encloses it with angle brackets.
class TAttributeFragmentConsumer
    : public IAsyncYsonConsumer
{
public:
    explicit TAttributeFragmentConsumer(IAsyncYsonConsumer* underlyingConsumer);
    ~TAttributeFragmentConsumer();

    virtual void OnStringScalar(const TStringBuf& value) override;
    virtual void OnInt64Scalar(i64 value) override;
    virtual void OnUint64Scalar(ui64 value) override;
    virtual void OnDoubleScalar(double value) override;
    virtual void OnBooleanScalar(bool value) override;
    virtual void OnEntity() override;
    virtual void OnBeginList() override;
    virtual void OnListItem() override;
    virtual void OnEndList() override;
    virtual void OnBeginMap() override;
    virtual void OnKeyedItem(const TStringBuf& key) override;
    virtual void OnEndMap() override;
    virtual void OnBeginAttributes() override;
    virtual void OnEndAttributes() override;
    using IAsyncYsonConsumer::OnRaw;
    virtual void OnRaw(const TStringBuf& yson, EYsonType type) override;
    virtual void OnRaw(TFuture<TYsonString> asyncStr) override;

private:
    IAsyncYsonConsumer* const UnderlyingConsumer_;
    bool HasAttributes_ = false;


    void Begin();
    void End();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT

