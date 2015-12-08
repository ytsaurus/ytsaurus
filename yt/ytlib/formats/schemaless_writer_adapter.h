#pragma once 

#include "public.h"
#include "format.h"
#include "helpers.h"

#include <yt/ytlib/table_client/public.h>
#include <yt/ytlib/table_client/schemaless_writer.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/misc/blob_output.h>

#include <yt/core/yson/public.h>

#include <memory>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TSchemalessFormatWriterBase
    : public ISchemalessFormatWriter
{
public:
    virtual TFuture<void> Open() override;

    virtual bool Write(const std::vector<NTableClient::TUnversionedRow> &rows) override;

    virtual TFuture<void> GetReadyEvent() override;

    virtual TFuture<void> Close() override;

    virtual NTableClient::TNameTablePtr GetNameTable() const override;

    virtual bool IsSorted() const override;

    virtual TBlob GetContext() const;

protected:
    TControlAttributesConfigPtr ControlAttributesConfig_;

    TSchemalessFormatWriterBase(
        NTableClient::TNameTablePtr nameTable,
        NConcurrency::IAsyncOutputStreamPtr output,
        bool enableContextSaving,
<<<<<<< HEAD
        TControlAttributesConfigPtr controlAttributesConfig,
=======
        bool enableKeySwitch,
>>>>>>> origin/prestable/0.17.4
        int keyColumnCount);

    TBlobOutput* GetOutputStream();

    void TryFlushBuffer(bool force);

    virtual void DoWrite(const std::vector<NTableClient::TUnversionedRow> &rows) = 0;
    
    int KeyColumnCount_;
    
    NTableClient::TOwningKey LastKey_;
    NTableClient::TKey CurrentKey_;
    
    NTableClient::TNameTablePtr NameTable_;

    bool CheckKeySwitch(NTableClient::TUnversionedRow row, bool isLastRow);

    bool CheckKeySwitch(NTableClient::TUnversionedRow row, bool isLastRow);

    bool IsSystemColumnId(int id) const;
    bool IsTableIndexColumnId(int id) const;
    bool IsRangeIndexColumnId(int id) const;
    bool IsRowIndexColumnId(int id) const;

    // This is suitable only for switch-based control attributes,
    // e.g. in such formats as YAMR or YSON.
    void WriteControlAttributes(NTableClient::TUnversionedRow row);
    virtual void WriteTableIndex(i64 tableIndex);
    virtual void WriteRangeIndex(i64 rangeIndex);
    virtual void WriteRowIndex(i64 rowIndex);

private:
    bool EnableContextSaving_;
<<<<<<< HEAD

    NTableClient::TNameTablePtr NameTable_;
=======
    bool EnableKeySwitch_;
>>>>>>> origin/prestable/0.17.4

    TBlobOutput CurrentBuffer_;
    TBlobOutput PreviousBuffer_;
    std::unique_ptr<TOutputStream> Output_;

    NTableClient::TOwningKey LastKey_;
    NTableClient::TKey CurrentKey_;

    int KeyColumnCount_;

    int RowIndexId_ = -1;
    int RangeIndexId_ = -1;
    int TableIndexId_ = -1;

    i64 RangeIndex_ = -1;
    i64 TableIndex_ = -1;

    bool EnableRowControlAttributes_;

    TError Error_;

    void DoFlushBuffer();
};

////////////////////////////////////////////////////////////////////////////////

class TSchemalessWriterAdapter
    : public TSchemalessFormatWriterBase
{
public:
    TSchemalessWriterAdapter(
        NTableClient::TNameTablePtr nameTable,
        NConcurrency::IAsyncOutputStreamPtr output,
        bool enableContextSaving,
        TControlAttributesConfigPtr controlAttributesConfig,
        int keyColumnCount);

    void Init(const TFormat& format);
<<<<<<< HEAD

private:
    std::unique_ptr<NYson::IYsonConsumer> Consumer_;
=======

    virtual void WriteTableIndex(i32 tableIndex) override;

    virtual void WriteRangeIndex(i32 rangeIndex) override;

    virtual void WriteRowIndex(i64 rowIndex) override;

private:
    std::unique_ptr<NYson::IYsonConsumer> Consumer_;
    NTableClient::TNameTablePtr NameTable_;

    TError Error_;
>>>>>>> origin/prestable/0.17.4

    template <class T>
    void WriteControlAttribute(
        NTableClient::EControlAttribute controlAttribute,
        T value);

    void ConsumeRow(NTableClient::TUnversionedRow row);

    virtual void DoWrite(const std::vector<NTableClient::TUnversionedRow>& rows) override;

    virtual void WriteTableIndex(i64 tableIndex) override;
    virtual void WriteRangeIndex(i64 rangeIndex) override;
    virtual void WriteRowIndex(i64 rowIndex) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
