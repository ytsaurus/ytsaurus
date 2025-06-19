#pragma once

#include "unversioned_row.h"

#include <yt/yt/client/chunk_client/writer_base.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/crypto/crypto.h>

#include <library/cpp/yt/memory/range.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  Imports stuff.
 */
struct IUnversionedChunkImporter
    : public virtual TRefCounted
{
    /*!
     *  Imports given chunk.
     *
     *  Returns an asynchronous flag enabling to wait until data is written.
     */
    virtual TFuture<void> Import(std::string_view s3Key) = 0;

    //! Closes the importer. Must be the last call to the importer.
    virtual TFuture<void> Close() = 0;
};

DEFINE_REFCOUNTED_TYPE(IUnversionedChunkImporter)

////////////////////////////////////////////////////////////////////////////////

struct IUnversionedImporter
    : public IUnversionedChunkImporter
{
    virtual const TNameTablePtr& GetNameTable() const = 0;
    virtual const TTableSchemaPtr& GetSchema() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IUnversionedImporter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
