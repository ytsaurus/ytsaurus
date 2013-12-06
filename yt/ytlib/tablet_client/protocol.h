#pragma once

#include "public.h"

#include <core/misc/enum.h>
#include <core/misc/ref.h>

#include <ytlib/new_table_client/public.h>

namespace NYT {
namespace NTabletClient {

///////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EProtocolCommand,
    // Sentinels:

    ((End)(0))

    // Read commands:
    
    ((Lookup)(1))
    // Finds a row with a given key and fetches its components.
    //
    // Input:
    //   Key
    //
    // Output:
    //   Versioned rowset containing 0 or 1 rows

    // Write commands:

    ((Insert)(2))
    // Inserts a new row or completely replaces an existing one with matching key.
    //
    // Input:
    //   Unversioned row
    // Output:
    //   None

    ((Update)(3))
    // Inserts a new row or updates some values of an existing one with matching key.
    //
    // Input:
    //   Unversioned row
    // Output:
    //   None

    ((Delete)(4))
    // Deletes a row with a given key, if it exists
    //
    // Input:
    //   Key
    // Output:
    //   None

);

class TProtocolWriter
{
public:
    TProtocolWriter();

    void WriteCommand(EProtocolCommand command);

    void WriteUnversionedRow(NVersionedTableClient::TUnversionedRow row);
    void WriteVersionedRow(NVersionedTableClient::TVersionedRow row);

    void WriteUnversionedRowset(const std::vector<NVersionedTableClient::TUnversionedRow>& rowset);
    void WriteVersionedRowset(const std::vector<NVersionedTableClient::TVersionedRow>& rowset);

    Stroka Finish();

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;

};

///////////////////////////////////////////////////////////////////////////////

class TProtocolReader
{
public:
    explicit TProtocolReader(TRef data);

    EProtocolCommand ReadCommand();
    
    NVersionedTableClient::TUnversionedRow ReadUnversionedRow();
    NVersionedTableClient::TVersionedRow ReadVersionedRow();
    
    void ReadUnversionedRowset(std::vector<NVersionedTableClient::TUnversionedRow>* rowset);
    void ReadVersionedRowset(std::vector<NVersionedTableClient::TVersionedRow>* rowset);

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;

};

///////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT

