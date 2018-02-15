#pragma once

#include "public.h"
#include "unchecked_parser.h"

#include <util/generic/string.h>
#include <util/generic/ptr.h>

#include <util/stream/zerocopy.h>

namespace NSkiff {

////////////////////////////////////////////////////////////////////////////////

class TCheckedSkiffParser
{
public:
    TCheckedSkiffParser(const TSkiffSchemaPtr& schema, IZeroCopyInput* stream);
    ~TCheckedSkiffParser();

    i64 ParseInt64();
    ui64 ParseUint64();
    double ParseDouble();
    bool ParseBoolean();

    TStringBuf ParseString32();
    TStringBuf ParseYson32();

    ui8 ParseVariant8Tag();
    ui16 ParseVariant16Tag();

    bool HasMoreData();

    void ValidateFinished();

private:
    TUncheckedSkiffParser Parser_;
    THolder<TSkiffValidator> Validator_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkiff
