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

    void OnStringScalar(const TStringBuf& value) override;
    void OnInt64Scalar(i64 value) override;
    void OnUint64Scalar(ui64 value) override;
    void OnDoubleScalar(double value) override;
    void OnBooleanScalar(bool value) override;

    void OnEntity() override;

    void OnBeginList() override;
    void OnListItem() override;
    void OnEndList() override;

    void OnBeginMap() override;
    void OnKeyedItem(const TStringBuf& key) override;
    void OnEndMap() override;

    void OnBeginAttributes() override;
    void OnEndAttributes() override;

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
