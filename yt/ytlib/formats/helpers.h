#pragma once

#include "public.h"

#include <core/yson/consumer.h>
#include <core/yson/parser.h>

#include <core/misc/blob_output.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TFormatsConsumerBase
    : public virtual NYson::IYsonConsumer
{
public:
    TFormatsConsumerBase();

    // This method has standard implementation for yamr, dsv and yamred dsv formats.
    virtual void OnRaw(const TStringBuf& yson, NYson::EYsonType type) override;

private:
    NYson::TStatelessYsonParser Parser;
};

////////////////////////////////////////////////////////////////////////////////

class TContextSavingMixin
{
protected:
    explicit TContextSavingMixin(
        bool enableContextSaving,
        std::unique_ptr<TOutputStream> output);

    TOutputStream* GetOutputStream();

    TBlob GetContext() const;

    void TryFlushBuffer();

    void Close();

private:
    bool EnableContextSaving_;
    TBlobOutput CurrentBuffer_;
    TBlobOutput PreviousBuffer_;
    std::unique_ptr<TOutputStream> Output_;

    void DoFlushBuffer(bool force);
};

////////////////////////////////////////////////////////////////////////////////

bool IsSpecialJsonKey(const TStringBuf& str);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
