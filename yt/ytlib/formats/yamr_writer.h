#pragma once

#include "public.h"
#include "config.h"
#include <ytlib/ytree/yson_consumer.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

// TODO(panin): use more efficent method OnRaw

//! Note: #TYamrWriter supports only tabular data
class TYamrWriter
    : public virtual NYTree::TYsonConsumerBase
{
public:
    explicit TYamrWriter(
        TOutputStream* stream,
        TYamrFormatConfigPtr config = NULL);

    ~TYamrWriter();

    // IYsonConsumer overrides.
    virtual void OnStringScalar(const TStringBuf& value) OVERRIDE;
    virtual void OnIntegerScalar(i64 value) OVERRIDE;
    virtual void OnDoubleScalar(double value) OVERRIDE;
    virtual void OnEntity() OVERRIDE;
    virtual void OnBeginList() OVERRIDE;
    virtual void OnListItem() OVERRIDE;
    virtual void OnEndList() OVERRIDE;
    virtual void OnBeginMap() OVERRIDE;
    virtual void OnKeyedItem(const TStringBuf& key) OVERRIDE;
    virtual void OnEndMap() OVERRIDE;
    virtual void OnBeginAttributes() OVERRIDE;
    virtual void OnEndAttributes() OVERRIDE;

private:
    TOutputStream* Stream;
    TYamrFormatConfigPtr Config;

    Stroka Key;
    Stroka Subkey;
    Stroka Value;

    bool AllowBeginMap;

    void RememberItem(const Stroka& item);
    void WriteRow();
    void WriteInLenvalMode(const Stroka& value);

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
