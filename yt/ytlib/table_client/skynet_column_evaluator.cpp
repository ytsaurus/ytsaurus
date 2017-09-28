#include "skynet_column_evaluator.h"

#include "name_table.h"
#include "row_buffer.h"

#include <contrib/libs/openssl/crypto/md5/md5.h>
#include <contrib/libs/openssl/crypto/sha/sha.h>

#include <array>

namespace NYT {
namespace NTableClient {

static constexpr i64 SkynetDataSize = 4_MB;

////////////////////////////////////////////////////////////////////////////////

//! Compute SHA1 of each block and rolling md5.
class TSkynetHashState
{
public:
    TSkynetHashState()
    {
        MD5_Init(&Md5Ctx_);
    }

    void Update(TStringBuf data)
    {
        MD5_Update(&Md5Ctx_, data.Data(), data.Size());

        SHA1_Init(&Sha1Ctx_);
        SHA1_Update(&Sha1Ctx_, data.Data(), data.Size());
    }

    std::array<char, 16> GetMD5()
    {
        std::array<char, 16> md5;

        MD5_CTX ctx = Md5Ctx_;
        MD5_Final(reinterpret_cast<unsigned char*>(md5.data()), &ctx);
        return md5;
    }

    std::array<char, 20> GetSHA1()
    {
        std::array<char, 20> sha1;
        SHA1_Final(reinterpret_cast<unsigned char*>(sha1.data()), &Sha1Ctx_);
        return sha1;
    }

private:
    MD5_CTX Md5Ctx_;
    SHA_CTX Sha1Ctx_;
};

////////////////////////////////////////////////////////////////////////////////

void ValidateSkynetSchema(const TTableSchema& schema)
{
    std::vector<TError> validationErrors;
    auto checkColumn = [&] (const TString& name, ELogicalValueType type, const TString& group) {
        auto columnSchema = schema.FindColumn(name);
        if (!columnSchema) {
            validationErrors.push_back(TError("Table is missing %Qv column", name));
            return;
        }

        if (columnSchema->GetLogicalType()!= type) {
            validationErrors.push_back(TError("Column %Qv has invalid type", name)
                << TErrorAttribute("expected", ToString(type))
                << TErrorAttribute("actual", ToString(columnSchema->GetLogicalType())));
        }

        if (columnSchema->Group != group) {
            validationErrors.push_back(TError("Column %Qv has invalid group", name)
                << TErrorAttribute("expected", group)
                << TErrorAttribute("actual", columnSchema->Group.Get("#;")));
        }
    };

    checkColumn("filename", ELogicalValueType::String, "meta");
    checkColumn("part_index", ELogicalValueType::Int64, "meta");
    checkColumn("sha1", ELogicalValueType::String, "meta");
    checkColumn("md5", ELogicalValueType::String, "meta");
    checkColumn("data_size", ELogicalValueType::Int64, "meta");

    checkColumn("data", ELogicalValueType::String, "data");

    if (!validationErrors.empty()) {
        THROW_ERROR_EXCEPTION(EErrorCode::SchemaViolation, "Invalid schema for skynet shared table")
            << validationErrors;
    }
}

TSkynetColumnEvaluator::TSkynetColumnEvaluator(const TTableSchema& schema)
    : FilenameId_(schema.GetColumnIndexOrThrow("filename"))
    , PartIndexId_(schema.GetColumnIndexOrThrow("part_index"))
    , DataId_(schema.GetColumnIndexOrThrow("data"))
    , Sha1Id_(schema.GetColumnIndexOrThrow("sha1"))
    , Md5Id_(schema.GetColumnIndexOrThrow("md5"))
    , DataSizeId_(schema.GetColumnIndexOrThrow("data_size"))
{
    ValidateSkynetSchema(schema);
}

void TSkynetColumnEvaluator::ValidateAndComputeHashes(
    TMutableUnversionedRow fullRow,
    const TRowBufferPtr& buffer)
{
    TStringBuf filename;
    TStringBuf data;
    i64 partIndex;
    TUnversionedValue* sha1 = nullptr;
    TUnversionedValue* md5 = nullptr;
    TUnversionedValue* dataSize = nullptr;

    UnpackFields(fullRow, &filename, &data, &partIndex, &sha1, &md5, &dataSize);

    //! Start new file.
    if (!LastFilename_ || *LastFilename_ != filename) {
        LastFilename_ = TString(filename);
        LastDataSize_ = SkynetDataSize;

        HashState_.reset(new TSkynetHashState());
    }

    if (LastDataSize_ != SkynetDataSize) {
        THROW_ERROR_EXCEPTION("Data block with size %v found in the middle of the file",
            LastDataSize_)
            << TErrorAttribute("part_index", partIndex)
            << TErrorAttribute("filename", filename)
            << TErrorAttribute("actual_size", LastDataSize_)
            << TErrorAttribute("expected_size", SkynetDataSize);
    }

    LastDataSize_ = data.Size();
    *dataSize = MakeUnversionedInt64Value(data.Size(), DataSizeId_);

    HashState_->Update(data);

    auto sha1Hash = HashState_->GetSHA1();
    auto md5Hash = HashState_->GetMD5();

    *sha1 = MakeUnversionedStringValue(TStringBuf(sha1Hash.data(), sha1Hash.size()), Sha1Id_);
    buffer->Capture(sha1);

    *md5 = MakeUnversionedStringValue(TStringBuf(md5Hash.data(), md5Hash.size()), Md5Id_);
    buffer->Capture(md5);
}

void TSkynetColumnEvaluator::UnpackFields(
    TMutableUnversionedRow fullRow,
    TStringBuf* filename,
    TStringBuf* data,
    i64* partIndex,
    TUnversionedValue** sha1,
    TUnversionedValue** md5,
    TUnversionedValue** dataSize)
{
    YCHECK(fullRow.GetCount() >= FilenameId_ && fullRow[FilenameId_].Type == EValueType::String);
    *filename = TStringBuf(fullRow[FilenameId_].Data.String, fullRow[FilenameId_].Length);

    YCHECK(fullRow.GetCount() >= PartIndexId_ && fullRow[PartIndexId_].Type == EValueType::Int64);
    *partIndex = fullRow[PartIndexId_].Data.Int64;

    YCHECK(fullRow.GetCount() >= DataId_ && fullRow[DataId_].Type == EValueType::String);
    *data = TStringBuf(fullRow[DataId_].Data.String, fullRow[DataId_].Length);

    YCHECK(fullRow.GetCount() >= Sha1Id_);
    *sha1 = &fullRow[Sha1Id_];

    YCHECK(fullRow.GetCount() >= Md5Id_);
    *md5 = &fullRow[Md5Id_];

    YCHECK(fullRow.GetCount() >= DataSizeId_);
    *dataSize = &fullRow[DataSizeId_];
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
