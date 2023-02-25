#pragma once

#include <yt/cpp/mapreduce/interface/client.h>

#include <util/generic/ptr.h>

#include <util/stream/input.h>
#include <util/stream/output.h>

namespace NYtBlobTable {

//
// https://wiki.yandex-team.ru/yt/userdoc/blob_tables/
//
// Blob table is a table that stores a number of blobs. Blobs are sliced into parts of the same size (maybe except of last part).
// Those parts are stored in the separate rows.
//
// Blob table has constraints on its schema.
//  - There must be columns that identify blob (blob id columns). These columns might be of any type.
//  - There must be a column of int64 type that identify part inside the blob.
//  - There must be a column of string type that stores actual data.
//
// Futhermore data from blob tables can be sky shared. In order to achieve this blob table must satisfy additional constrains.
//  - The last column of blob id columns must be named `filename' and have string type (all columns before `filename' form sky share id).
//  - Part index column must be named `part_index' and data column must be named `data'.
//  - Part size must be equal to 4 MB (maybe except for last part).
//  - Special attribute must be set during creation of the table (therefore we cannot alter table to be sky sharable after it is created).
//
// Before blob table can be read or sky shared it must be sorted.
//
// NOTE: skynet share is not yet in production.

struct TBlobTableSchema
{
    using TSelf = TBlobTableSchema;

    // Columns that form blob id of the table.
    // For sky sharable tables last column of blob id columns must be named filename and have string type.
    FLUENT_FIELD_DEFAULT(TVector<NYT::TColumnSchema>, BlobIdColumns,
        {NYT::TColumnSchema().Name("filename").Type(NYT::VT_STRING)});

    // Name of the column that stores index of the part inside blob.
    FLUENT_FIELD_DEFAULT(TString, PartIndexColumnName, "part_index");

    // Name of the column that stores actual blob data.
    FLUENT_FIELD_DEFAULT(TString, DataColumnName, "data");

    // Maximum size of data column. All parts of blob except the last one must be of this size.
    FLUENT_FIELD_DEFAULT(ui32, PartSize, 4 * 1024 * 1024);

    // Whether or not enable sky sharing from this table.
    // Not yet in production
    // FLUENT_FIELD_DEFAULT(bool, EnableSkynetSharing, false);
};

struct TBlobTableOpenOptions
{
    using TSelf = TBlobTableOpenOptions;

    // If create is set to false and specified table does not exist TBlobTable constructor will throw.
    // If create is set to true and specified table does not exist TBlobTable will create it and set proper attributes.
    FLUENT_FIELD_DEFAULT(bool, Create, false);

    // CreateOptions might be used to tune creation of the table.
    // NB
    FLUENT_FIELD_DEFAULT(NYT::TCreateOptions, CreateOptions, NYT::TCreateOptions());
};

class TBlobTable
{
public:
    // Open existing or create new blob table.
    TBlobTable(
        NYT::IClientBasePtr client,
        const NYT::TYPath& path,
        const TBlobTableSchema& schema,
        const TBlobTableOpenOptions& openOptions = TBlobTableOpenOptions());

    // Get table path.
    NYT::TYPath GetPath() const;

    // Checks if blob table is properly sorted.
    bool IsSorted() const;

    // Make table unsorted. Useful for appending more data.
    void ResetSorting();

    // Sort table by {blob_id_columns..., part_index_column} column so we can lookup blobs.
    NYT::IOperationPtr Sort();
    NYT::IOperationPtr SortAsync();

    // Append new blob to table.
    //
    // NOTE: appending must respect sort status of table, i.e.
    // if table is sorted you can append only blob with blobId greater than all existing blobIds.
    // If you want to append other blob you should ResetSorting before.
    NYT::IFileWriterPtr AppendBlob(const NYT::TKey& blobId);

    // Read blob from a table.
    NYT::IFileReaderPtr ReadBlob(const NYT::TKey& blobId);

private:
    NYT::IClientBasePtr Client_;
    NYT::TYPath Path_;
    TBlobTableSchema Schema_;
};

} // namespace NYtBlobTable
