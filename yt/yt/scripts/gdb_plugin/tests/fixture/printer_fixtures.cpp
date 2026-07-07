#include "printer_fixtures.h"

#include <yt/yt/client/table_client/key_bound.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <library/cpp/yt/containers/ordered_hash_map.h>

#include <library/cpp/yt/error/error.h>
#include <library/cpp/yt/error/error_attribute.h>

#include <library/cpp/yt/memory/new.h>

#include <library/cpp/yt/misc/guid.h>

#include <library/cpp/yt/yson_string/string.h>

#include <util/system/compiler.h>

using namespace NYT;
using namespace NYT::NTableClient;
using namespace NYT::NYson;

// Pretty-printer fixtures: real typed globals (external linkage, so they stay in
// the core) that the gdb test prints to exercise lib/printers.py.
TUnversionedOwningRow GdbPrinterUnversionedRow;
TRowBufferPtr GdbPrinterRowBuffer; // keeps the versioned row's data alive
TVersionedRow GdbPrinterVersionedRow;
TOwningKeyBound GdbPrinterKeyBound;
TGuid GdbPrinterGuid;
TError GdbPrinterError;
TYsonString GdbPrinterYson;
TOrderedHashMap<TString, int> GdbPrinterOrderedMap;

void SetupGdbPrinterFixtures()
{
    {
        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedInt64Value(-42, 0));
        builder.AddValue(MakeUnversionedUint64Value(7, 1));
        builder.AddValue(MakeUnversionedStringValue("hello", 2));
        builder.AddValue(MakeUnversionedDoubleValue(2.5, 3));
        builder.AddValue(MakeUnversionedBooleanValue(true, 4));
        builder.AddValue(MakeUnversionedNullValue(5));
        GdbPrinterUnversionedRow = builder.FinishRow();
    }

    {
        GdbPrinterRowBuffer = New<TRowBuffer>();
        TVersionedRowBuilder builder(GdbPrinterRowBuffer);
        builder.AddKey(MakeUnversionedInt64Value(100, 0));
        builder.AddValue(MakeVersionedInt64Value(555, 0x20, 1));
        builder.AddValue(MakeVersionedStringValue("v", 0x20, 2));
        builder.AddWriteTimestamp(0x20);
        builder.AddDeleteTimestamp(0x10);
        GdbPrinterVersionedRow = builder.FinishRow();
    }

    {
        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedInt64Value(1000, 0));
        GdbPrinterKeyBound = TOwningKeyBound::FromRow(builder.FinishRow(), /*isInclusive*/ true, /*isUpper*/ false);
    }

    GdbPrinterGuid = TGuid(0x11111111u, 0x22222222u, 0x33333333u, 0x44444444u);

    {
        // A non-OK error captures origin attributes (host/pid/tid/thread); add a
        // couple of user attributes and an inner error to exercise every field.
        // NYT::EErrorCode is qualified to disambiguate it from NTableClient's.
        auto error = TError(NYT::EErrorCode::Generic, "Disk quota exceeded");
        error <<= TErrorAttribute("limit", 100);
        error <<= TErrorAttribute("account", "intermediate");
        error <<= TError(NYT::EErrorCode::Generic, "Underlying IO error");
        GdbPrinterError = error;
    }

    GdbPrinterYson = TYsonString(TString("{key=value;list=[1;2;3]}"));

    GdbPrinterOrderedMap["alpha"] = 1;
    GdbPrinterOrderedMap["beta"] = 22;
    GdbPrinterOrderedMap["gamma"] = 333;
    Y_DO_NOT_OPTIMIZE_AWAY(GdbPrinterOrderedMap); // keep the otherwise-unread global in the core
}
