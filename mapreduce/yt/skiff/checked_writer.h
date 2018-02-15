#pragma once

#include "public.h"
#include "skiff_validator.h"
#include "unchecked_writer.h"

#include <util/generic/buffer.h>

namespace NSkiff {

////////////////////////////////////////////////////////////////////////////////

class TCheckedSkiffWriter
{
public:
    TCheckedSkiffWriter(const TSkiffSchemaPtr& schema, IOutputStream* underlying);

    ~TCheckedSkiffWriter();

    void WriteInt64(i64 value);
    void WriteUint64(ui64 value);

    void WriteDouble(double value);
    void WriteBoolean(bool value);

    void WriteString32(TStringBuf value);

    void WriteYson32(TStringBuf value);

    void WriteVariant8Tag(ui8 tag);
    void WriteVariant16Tag(ui16 tag);

    void Flush();
    void Finish();

private:
    TUncheckedSkiffWriter Writer_;
    THolder<TSkiffValidator> Validator_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkiff
