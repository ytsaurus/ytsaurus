#pragma once

#include "public.h"
#include "consumer.h"

#include <library/json/json_writer.h>

#include <util/generic/vector.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

enum EJsonFormat
{
    JF_TEXT,
    JF_PRETTY
};

enum EJsonAttributesMode
{
    JAM_NEVER,
    JAM_ON_DEMAND,
    JAM_ALWAYS
};

class TJsonWriter
    : public TYsonConsumerBase
{
public:
    TJsonWriter(
        TOutputStream* output,
        EYsonType type = YT_NODE,
        EJsonFormat format = JF_TEXT,
        EJsonAttributesMode attributesMode = JAM_ON_DEMAND);

    void Flush();

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

private:
    THolder<NJson::TJsonWriter> UnderlyingJsonWriter;
    NJson::TJsonWriter* JsonWriter;
    TOutputStream* Output;
    EYsonType Type;
    EJsonFormat Format;
    EJsonAttributesMode AttributesMode;

    void WriteStringScalar(const TStringBuf& value);

    void EnterNode();
    void LeaveNode();
    bool IsWriteAllowed();

    yvector<bool> HasUnfoldedStructureStack;
    int InAttributesBalance;
    bool HasAttributes;
    int Depth;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
