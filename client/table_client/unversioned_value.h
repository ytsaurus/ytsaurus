#pragma once

#include "row_base.h"

#include <yt/core/misc/farm_hash.h>

#include <util/system/defaults.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

// NB: Wire protocol readers/writer rely on this fixed layout.
union TUnversionedValueData
{
    //! |Int64| value.
    i64 Int64;
    //! |Uint64| value.
    ui64 Uint64;
    //! |Double| value.
    double Double;
    //! |Boolean| value.
    bool Boolean;
    //! String value for |String| type or YSON-encoded value for |Any| type.
    const char* String;
};

static_assert(
    sizeof(TUnversionedValueData) == 8,
    "TUnversionedValueData has to be exactly 8 bytes.");

// NB: Wire protocol readers/writer rely on this fixed layout.
struct TUnversionedValue
{
    //! Column id obtained from a name table.
    ui16 Id;
    //! Column type.
    EValueType Type;
    //! Aggregate mode;
    bool Aggregate;
    //! Length of a variable-sized value (only meaningful for |String| and |Any| types).
    ui32 Length;

    TUnversionedValueData Data;
};

static_assert(
    sizeof(TUnversionedValue) == 16,
    "TUnversionedValue has to be exactly 16 bytes.");
static_assert(
    std::is_pod<TUnversionedValue>::value,
    "TUnversionedValue must be a POD type.");

////////////////////////////////////////////////////////////////////////////////

//! Computes hash for a given TUnversionedValue.
ui64 GetHash(const TUnversionedValue& value);

//! Computes FarmHash forever-fixed fingerprint for a given TUnversionedValue.
TFingerprint GetFarmFingerprint(const TUnversionedValue& value);

//! Computes FarmHash forever-fixed fingerprint for a given set of values.
TFingerprint GetFarmFingerprint(const TUnversionedValue* begin, const TUnversionedValue* end);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
