#pragma once

#include <yt/yt/ytlib/chunk_client/replication_reader.h>

#include <yt/yt/client/api/file_reader.h>

#include <yt/yt/core/misc/blob_output.h>
#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <util/generic/size_literals.h>
#include <util/string/split.h>
#include <util/system/types.h>

namespace NYT::NSquashFs {

////////////////////////////////////////////////////////////////////////////////

struct TArtifactDescription
{
    TString Path;
    i64 Size;
    i64 Offset;
};

////////////////////////////////////////////////////////////////////////////////

struct TSquashFsData
{
    TBlobOutput Head;
    std::vector<TArtifactDescription> Files;
    TBlobOutput Tail;
};

////////////////////////////////////////////////////////////////////////////////

struct TSquashFsImageTag {};

////////////////////////////////////////////////////////////////////////////////

class TSquashFsImage
    : public TRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY_NO_INIT(i64, Size);
    DEFINE_BYREF_RO_PROPERTY_NO_INIT(std::vector<TArtifactDescription>, Files);

public:
    explicit TSquashFsImage(TSquashFsData data);

    TSharedRef ReadHead(
        i64 offset,
        i64 length) const;
    i64 GetHeaderSize() const;

    TSharedRef ReadTail(
        i64 offset,
        i64 length) const;
    i64 GetTailOffset() const;
    i64 GetTailSize() const;

    // For testing purposes.
    void Dump(IOutputStream& output) const;
    void DumpHexText(IOutputStream& output) const;

private:
    TSharedRef Head_;
    TSharedRef Tail_;
    i64 TailOffset_;
};

DECLARE_REFCOUNTED_CLASS(TSquashFsImage)
DEFINE_REFCOUNTED_TYPE(TSquashFsImage)

////////////////////////////////////////////////////////////////////////////////

struct TSquashFsBuilderOptions
{
    ui32 BlockSize = 128_KB;
    ui32 Uid = 0;
    ui32 Gid = 0;
    ui32 MTime = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct ISquashFsBuilder
    : public virtual TRefCounted
{
    // Adds directories and file to file system.
    // Takes absolute address to file, its permissions and size.
    virtual void AddFile(
        const TString& path,
        i64 size,
        ui16 permissions) = 0;

    // Builds squashFs that contains all directories and files added previously.
    virtual TSquashFsImagePtr Build() = 0;
};

DECLARE_REFCOUNTED_STRUCT(ISquashFsBuilder)
DEFINE_REFCOUNTED_TYPE(ISquashFsBuilder)

////////////////////////////////////////////////////////////////////////////////

ISquashFsBuilderPtr CreateSquashFsBuilder(TSquashFsBuilderOptions options = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSquashFs
