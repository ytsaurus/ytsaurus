#pragma once

#include "public.h"
#include "config.h"
#include "helpers.h"

#include <ytlib/misc/blob_output.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

//! Note: #TYamrWriter supports only tabular data
class TYamrWriter
    : public virtual TFormatsConsumerBase
{
public:
    explicit TYamrWriter(
        TOutputStream* stream,
        // TODO(ignat): replace default value with YCHECK.
        // Default value is used in tests.
        TYamrFormatConfigPtr config = NULL);

    ~TYamrWriter();

    // IYsonConsumer overrides.
    virtual void OnStringScalar(const TStringBuf& value) override;
    virtual void OnIntegerScalar(i64 value) override;
    virtual void OnDoubleScalar(double value) override;
    virtual void OnEntity() override;
    virtual void OnBeginList() override;
    virtual void OnListItem() override;
    virtual void OnEndList() override;
    virtual void OnBeginMap() override;
    virtual void OnKeyedItem(const TStringBuf& key) override;
    virtual void OnEndMap() override;
    virtual void OnBeginAttributes() override;
    virtual void OnEndAttributes() override;

private:
    TOutputStream* Stream;
    TYamrFormatConfigPtr Config;

    TStringBuf Key;
    TStringBuf Subkey;
    TStringBuf Value;

    TBlobOutput KeyBuffer;
    TBlobOutput SubkeyBuffer;
    TBlobOutput ValueBuffer;

    bool AllowBeginMap;

    void RememberItem(const TStringBuf& item, bool takeOwnership);

    void WriteRow();
    void WriteInLenvalMode(const TStringBuf& value);

    DECLARE_ENUM(EState,
        (None)
        (ExpectingKey)
        (ExpectingSubkey)
        (ExpectingValue)
    );
    EState State;
};

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NFormats
} // namespace NYT
