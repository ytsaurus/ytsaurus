#pragma once

#include "public.h"

#include <core/misc/enum.h>
#include <core/misc/ref.h>

#include <ytlib/new_table_client/public.h>

#include <ytlib/api/public.h>

namespace NYT {
namespace NTabletClient {

///////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EProtocolCommand,
    // Sentinels:

    ((End)(0))

    // Read commands:
    
    ((LookupRow)(1))
    // Finds a row with a given key and fetches its components.
    //
    // Input:
    //   * Key
    //   * Column filter
    //
    // Output:
    //   * Versioned rowset containing 0 or 1 rows

    // Write commands:

    ((WriteRow)(2))
    // Inserts a new row or completely replaces an existing one with matching key.
    //
    // Input:
    //   * Unversioned row
    // Output:
    //   None

    ((DeleteRow)(3))
    // Deletes a row with a given key, if it exists.
    //
    // Input:
    //   * Key
    // Output:
    //   None

);

////////////////////////////////////////////////////////////////////////////////

class TProtocolWriter
{
public:
    TProtocolWriter();
    ~TProtocolWriter();

    void WriteCommand(EProtocolCommand command);

    void WriteColumnFilter(const NApi::TColumnFilter& filter);

    void WriteUnversionedRow(NVersionedTableClient::TUnversionedRow row);
    void WriteUnversionedRow(const std::vector<NVersionedTableClient::TUnversionedValue>& row);
    void WriteVersionedRow(NVersionedTableClient::TVersionedRow row);
    void WriteVersionedRow(const std::vector<NVersionedTableClient::TVersionedValue>& row);

    void WriteUnversionedRowset(const std::vector<NVersionedTableClient::TUnversionedRow>& rowset);
    void WriteVersionedRowset(const std::vector<NVersionedTableClient::TVersionedRow>& rowset);

    Stroka Finish();

    void foo();

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;

};

///////////////////////////////////////////////////////////////////////////////

class TProtocolReader
{
public:
    explicit TProtocolReader(const Stroka& data); 
    ~TProtocolReader();

    EProtocolCommand ReadCommand();
    
    NApi::TColumnFilter ReadColumnFilter();

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

