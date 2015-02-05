#include "stdafx.h"
#include "ordered_schemaful_reader.h"
#include "schemaful_reader.h"
#include "schema.h"
#include "unversioned_row.h"

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

class TOrderedSchemafulReader
    : public ISchemafulReader
{
public:
    explicit TOrderedSchemafulReader(const std::function<ISchemafulReaderPtr()>& getNextReader)
        : GetNextReader_(getNextReader)
    { }

    virtual TFuture<void> Open(const TTableSchema& schema) override
    {
        Schema_ = schema;
        return VoidFuture;
    }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        rows->clear();

        if (ReadyEvent_ && !ReadyEvent_.IsSet()) {
            return true;
        }

        if (CurrentReader_) {
            if (!CurrentReader_->Read(rows)) {
                CurrentReader_.Reset();
            } else if (rows->empty()) {
                ReadyEvent_ = CurrentReader_->GetReadyEvent();
            }
        } else {
            CurrentReader_ = GetNextReader_();
            if (!CurrentReader_) {
                return false;
            }
            ReadyEvent_ = CurrentReader_->Open(Schema_);
        }

        return true;
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return ReadyEvent_;
    }

private:
    const std::function<ISchemafulReaderPtr()> GetNextReader_;

    ISchemafulReaderPtr CurrentReader_;
    TFuture<void> ReadyEvent_;
    TTableSchema Schema_;

};

ISchemafulReaderPtr CreateOrderedSchemafulReader(const std::function<ISchemafulReaderPtr()>& getNextReader)
{
    return New<TOrderedSchemafulReader>(getNextReader);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
