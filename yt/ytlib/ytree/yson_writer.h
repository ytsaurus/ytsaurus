#pragma once

#include "common.h"
#include "yson_events.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TYsonWriter
    : public IYsonConsumer
{
public:
    typedef TIntrusivePtr<TYsonWriter> TPtr;

    TYsonWriter(TOutputStream* stream)
        : Stream(stream)
        , IsFirstItem(false)
        , IsEmptyEntity(false)
        , Indent(0)
    { }

private:
    TOutputStream* Stream;
    bool IsFirstItem;
    bool IsEmptyEntity;
    int Indent;

    static const int IndentSize = 4;

    void WriteIndent()
    {
        for (int i = 0; i < IndentSize * Indent; ++i) {
            Stream->Write(' ');
        }
    }

    void SetEmptyEntity()
    {
        IsEmptyEntity = true;
    }

    void ResetEmptyEntity()
    {
        IsEmptyEntity = false;
    }

    void FlushEmptyEntity()
    {
        if (IsEmptyEntity) {
            Stream->Write("<>");
            IsEmptyEntity = false;
        }
    }

    void BeginCollection(char openBracket)
    {
        Stream->Write(openBracket);
        IsFirstItem = true;
    }

    void CollectionItem()
    {
        if (IsFirstItem) {
            Stream->Write('\n');
            ++Indent;
        } else {
            FlushEmptyEntity();
            Stream->Write(",\n");
        }
        WriteIndent();
        IsFirstItem = false;
    }

    void EndCollection(char closeBracket)
    {
        FlushEmptyEntity();
        if (!IsFirstItem) {
            Stream->Write('\n');
            --Indent;
            WriteIndent();
        }
        Stream->Write(closeBracket);
        IsFirstItem = false;
    }


    virtual void BeginTree()
    { }

    virtual void EndTree()
    {
        FlushEmptyEntity();
    }


    virtual void StringScalar(const Stroka& value)
    {
        // TODO: escaping
        Stream->Write('"');
        Stream->Write(value);
        Stream->Write('"');
    }

    virtual void Int64Scalar(i64 value)
    {
        Stream->Write(ToString(value));
    }

    virtual void DoubleScalar(double value)
    {
        Stream->Write(ToString(value));
    }

    virtual void EntityScalar()
    {
        SetEmptyEntity();
    }


    virtual void BeginList()
    {
        BeginCollection('[');
    }

    virtual void ListItem(int index)
    {
        UNUSED(index);
        CollectionItem();
    }

    virtual void EndList()
    {
        EndCollection(']');
    }

    virtual void BeginMap()
    {
        BeginCollection('{');
    }

    virtual void MapItem(const Stroka& name)
    {
        CollectionItem();
        // TODO: escaping
        Stream->Write(name);
        Stream->Write(": ");
    }

    virtual void EndMap()
    {
        EndCollection('}');
    }


    virtual void BeginAttributes()
    {
        if (IsEmptyEntity) {
            ResetEmptyEntity();
        } else {
            Stream->Write(' ');
        }
        BeginCollection('<');
    }

    virtual void AttributesItem(const Stroka& name)
    {
        CollectionItem();
        // TODO: escaping
        Stream->Write(name);
        Stream->Write(": ");
        IsFirstItem = false;
    }

    virtual void EndAttributes()
    {
        EndCollection('>');
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

