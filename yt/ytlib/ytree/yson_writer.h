#pragma once

#include "common.h"
#include "yson_events.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TYsonWriter
    : public IYsonConsumer
    , private TNonCopyable
{
public:
    TYsonWriter(TOutputStream* stream, bool isBinary = false);

private:
    TOutputStream* Stream;
    bool IsFirstItem;
    bool IsEmptyEntity;
    int Indent;

    bool IsBinary;

    static const int IndentSize = 4;

    void WriteIndent();

    void SetEmptyEntity();
    void ResetEmptyEntity();
    void FlushEmptyEntity();

    void BeginCollection(char openBracket);
    void CollectionItem(char separator);
    void EndCollection(char closeBracket);

    virtual void StringScalar(const Stroka& value);
    virtual void Int64Scalar(i64 value);
    virtual void DoubleScalar(double value);
    virtual void EntityScalar();

    virtual void BeginList();
    virtual void ListItem(int index);
    virtual void EndList();

    virtual void BeginMap();
    virtual void MapItem(const Stroka& name);
    virtual void EndMap();

    virtual void BeginAttributes();
    virtual void AttributesItem(const Stroka& name);
    virtual void EndAttributes();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

