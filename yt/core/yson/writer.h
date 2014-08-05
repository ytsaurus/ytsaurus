#pragma once

#include "public.h"
#include "consumer.h"

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////

//! Creates a YSON data stream from a sequence of YSON events.
class TYsonWriter
    : public TYsonConsumerBase
    , private TNonCopyable
{
public:
    //! Initializes an instance.
    /*!
     *  \param stream A stream for outputting the YSON data.
     *  \param format A format used for encoding the data.
     *  \param enableRaw Enables inserting raw portions of YSON as-is, without reparse.
     */
    TYsonWriter(
        TOutputStream* stream,
        EYsonFormat format = EYsonFormat::Binary,
        EYsonType type = EYsonType::Node,
        bool enableRaw = false,
        bool booleanAsString = false,
        int indent = 4);

    // IYsonConsumer overrides.
    virtual void OnStringScalar(const TStringBuf& value);
    virtual void OnInt64Scalar(i64 value);
    virtual void OnUint64Scalar(ui64 value);
    virtual void OnDoubleScalar(double value);
    virtual void OnBooleanScalar(bool value);
    virtual void OnEntity();

    virtual void OnBeginList();
    virtual void OnListItem();
    virtual void OnEndList();

    virtual void OnBeginMap();
    virtual void OnKeyedItem(const TStringBuf& key);
    virtual void OnEndMap();

    virtual void OnBeginAttributes();
    virtual void OnEndAttributes();

    virtual void OnRaw(const TStringBuf& yson, EYsonType type = EYsonType::Node);

protected:
    TOutputStream* Stream;
    EYsonFormat Format;
    EYsonType Type;
    bool EnableRaw;

    int Depth;
    bool BeforeFirstItem;

    bool BooleanAsString;
    const int IndentSize;

    void WriteIndent();
    void WriteStringScalar(const TStringBuf& value);

    void BeginCollection(ETokenType beginToken);
    void CollectionItem(ETokenType separatorToken);
    void EndCollection(ETokenType endToken);

    bool IsTopLevelFragmentContext() const;
    void EndNode();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT

