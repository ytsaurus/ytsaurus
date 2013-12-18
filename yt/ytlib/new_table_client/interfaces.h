#pragma once

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  Reads non-versioned, fixed-width, strictly typed rowset with given schema.
 *  Useful for: query engine.
 */
struct ISchemedReader 
    : public virtual TRefCounted
{
    /*!
     *  \note 
     *  Read timestamp and read limits should be passed in constructor if applicable.
     */
    virtual TAsyncError Open(const TTableSchema& schema) = 0;

    /*!
     *  Every row will contain exactly one value for each column in schema, in the same order.
     */
    virtual bool Read(std::vector<TUnversionedRow>* rows) = 0;

    virtual TAsyncError GetReadyEvent() = 0;
};

/*!
 *  Writes non-versioned, fixed-width, strictly typed rowset with given schema.
 *  Useful for: query engine.
 */
struct ISchemedWriter
    : public virtual TRefCounted
{
    /*!
     *  Non-null keyColumns imply writing sorted rowset.
     *  Non-null keyColumns must be the prefix of schema columns.
     */
    virtual TAsyncError Open(const TTableSchema& schema, const TNullable<TKeyColumns>& keyColumns = Null) = 0;

    /*!
     *  Every row must contain exactly one value for each column in schema, in the same order.
     */
    virtual bool Write(const std::vector<TUnversionedRow>& rows) = 0;

    virtual TAsyncError GetReadyEvent() = 0;

};

/*!
 *  Reads versioned rowset with given schema.
 *  Versioned rowset implies that it is:
 *  1. Schemed.
 *  2. Sorted.
 *  3. No two rows share the same key.
 *
 *  Useful for: merging and compactions.
 */
struct IVersionedReader
    : public virtual TRefCounted
{
    /*!
     *  Versioned rowset is always sorted, so key columns are mandatory.
     *  KeyColumns must be the prefix of schema columns.
     */
    virtual TAsyncError Open(
        const TTableSchema& schema, 
        const TKeyColumns& keyColumns) = 0;

    /*!
     *  Depending on implementation, rows may come in two different flavours.
     *  1. Rows containing no more than one versioned value for each cell, 
     *     and exactly one timestamp, either tombstone or last committed (for merging).
     *  2. Rows containing all available versions for  and a list of timestamps (for compactions).
     *
     *  Value ids correspond to column indexes in schema.
     *  Values are sorted in ascending order by ids, and then in descending order by timestamps.
     */
    virtual bool Read(std::vector<TVersionedRow>* rows) = 0;

    virtual TAsyncError GetReadyEvent() = 0;

};

/*!
 *  Writes versioned rowset with given schema.
 *  Useful for: compactions.
 */
struct IVersionedWriter
    : public virtual TRefCounted
{
    /*!
     *  Versioned rowset is always sorted, so key columns are mandatory.
     *  KeyColumns must be the prefix of schema columns.
     */
    virtual TAsyncError Open(
        const TTableSchema& schema, 
        const TKeyColumns& keyColumns) = 0;

    /*!
     *  Value ids must correspond to column indexes in schema.
     *  Values must be sorted in ascending order by ids, and then in descending order by timestamps.
     */
    virtual bool Write(const std::vector<TVersionedRow>& rows) = 0;

    virtual TAsyncError GetReadyEvent() = 0;
};

/*!
 *  Reads unversioned rowset, supports variable columns.
 *  Useful for: mapreduce jobs, read command.
 */
struct IUnversionedReader
    : public virtual TRefCounted
{
    /*!
     *  Name table is filled by reader. 
     *  It can be used to translate value ids to column names.
     */
    virtual TAsyncError Open(
        const TNameTablePtr& nameTable, 
        const TChannel& channel) = 0;

    /*!
     *  \note
     *  Only non-null values will be read.
     */
    virtual void Read(std::vector<TUnversionedRow>* rows);

    virtual TAsyncError GetReadyEvent() = 0;
};

/*!
 *  Writes unversioned rowset with schema and variable columns.
 *  Useful for: mapreduce jobs, write command.
 */
struct IUnversionedWriter
    : public virtual TRefCounted
{
    /*!
     *  Name table must be filled outside writer.
     *  Non-null keyColumns imply sorted rowset.
     */
    virtual TAsyncError Open(
        const TNameTablePtr& nameTable,
        const TSchema& schema,
        const TChannels& channels,
        const TNullable<TKeyColumns>& keyColumns = Null) = 0;

    /*!
     *  
     */
    virtual bool Write(const std::vector<TUnversionedRow>& rows) = 0;
    //virtual bool WriteUnsafe(std::vector<TUnversionedRow>* rows) = 0;

    virtual TAsyncError GetReadyEvent() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
