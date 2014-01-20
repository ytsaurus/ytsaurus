#pragma once

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

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
