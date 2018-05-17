#include "skynet_column_evaluator.h"

#include "name_table.h"
#include "row_buffer.h"

#include <yt/core/crypto/crypto.h>

#include <array>

namespace NYT {
namespace NTableClient {

using namespace NCrypto;

static constexpr i64 SkynetPartSize = 4_MB;

////////////////////////////////////////////////////////////////////////////////

//! Compute SHA1 of each block and rolling MD5.
class TSkynetHashState
{
public:
    void Update(TStringBuf data)
    {
        MD5_.Append(data);

        SHA1_ = TSHA1Hasher();
        SHA1_.Append(data);
    }

    TMD5Hash GetMD5()
    {
        auto md5Copy = MD5_;
        return md5Copy.GetDigest();
    }

    TSHA1Hash GetSHA1()
    {
        return SHA1_.GetDigest();
    }

private:
    TMD5Hasher MD5_;
    TSHA1Hasher SHA1_;
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

        if (columnSchema->LogicalType()!= type) {
            validationErrors.push_back(TError("Column %Qv has invalid type: expected %Qlv, actual %Qlv",
                name,
                type,
                columnSchema->LogicalType()));
        }

        if (columnSchema->Group() != group) {
            validationErrors.push_back(TError("Column %Qv has invalid group: expected %Qv, actual %Qv",
                name,
                group,
                columnSchema->Group().Get("<none>")));
        }
    };

    checkColumn("filename", ELogicalValueType::String, "meta");
    checkColumn("part_index", ELogicalValueType::Int64, "meta");
    checkColumn("sha1", ELogicalValueType::String, "meta");
    checkColumn("md5", ELogicalValueType::String, "meta");
    checkColumn("data_size", ELogicalValueType::Int64, "meta");

    checkColumn("data", ELogicalValueType::String, "data");

    if (!validationErrors.empty()) {
        THROW_ERROR_EXCEPTION(EErrorCode::SchemaViolation, "Invalid schema for Skynet shared table")
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
    , EffectiveKeySize_(schema.GetKeyColumnCount())
{
    ValidateSkynetSchema(schema);

    if (schema.GetColumnOrThrow("part_index").SortOrder()) {
        EffectiveKeySize_ -= 1;
    }
}

void TSkynetColumnEvaluator::ValidateAndComputeHashes(
    TMutableUnversionedRow fullRow,
    const TRowBufferPtr& buffer,
    bool isLastRow)
{
    TStringBuf fileName;
    TStringBuf data;
    i64 partIndex;
    TUnversionedValue* sha1 = nullptr;
    TUnversionedValue* md5 = nullptr;
    TUnversionedValue* dataSize = nullptr;

    UnpackFields(fullRow, &fileName, &data, &partIndex, &sha1, &md5, &dataSize);

    // Skip all validation if sha1 and md5 fields are already present.
    if (sha1->Type == EValueType::String && md5->Type == EValueType::String) {
        return;
    }

    bool keySwitched = IsKeySwitched(fullRow, isLastRow);

    //! Start new file.
    if (!LastFileName_ || *LastFileName_ != fileName || keySwitched) {
        LastFileName_ = TString(fileName);
        LastDataSize_ = SkynetPartSize;
        NextPartIndex_ = 0;

        HashState_ = std::make_unique<TSkynetHashState>();
    }

    if (partIndex != NextPartIndex_) {
        THROW_ERROR_EXCEPTION("Invalid \"part_index\" column value for %Qv file: expected %v, got %v; "
            "parts must be contiguously numbered starting from zero",
            fileName,
            NextPartIndex_,
            partIndex);
    }
    NextPartIndex_++;

    if (LastDataSize_ != SkynetPartSize) {
        THROW_ERROR_EXCEPTION("Table contains data part #%v with size %v in the middle of file %Qv; "
           "all but the last file part must be exactly %vMb in size",
            partIndex,
            LastDataSize_,
            fileName,
            SkynetPartSize / 1_MB);
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
    YCHECK(fullRow.GetCount() >= FilenameId_);
    if (fullRow[FilenameId_].Type != EValueType::String) {
        THROW_ERROR_EXCEPTION("Missing \"filename\" column");
    }
    *filename = TStringBuf(fullRow[FilenameId_].Data.String, fullRow[FilenameId_].Length);

    YCHECK(fullRow.GetCount() >= PartIndexId_);
    if (fullRow[PartIndexId_].Type != EValueType::Int64) {
        THROW_ERROR_EXCEPTION("Missing \"part_index\" column");
    }
    *partIndex = fullRow[PartIndexId_].Data.Int64;

    YCHECK(fullRow.GetCount() >= DataId_);
    if (fullRow[DataId_].Type != EValueType::String) {
        THROW_ERROR_EXCEPTION("Missing \"data\" column");
    }
    *data = TStringBuf(fullRow[DataId_].Data.String, fullRow[DataId_].Length);

    YCHECK(fullRow.GetCount() >= Sha1Id_);
    *sha1 = &fullRow[Sha1Id_];

    YCHECK(fullRow.GetCount() >= Md5Id_);
    *md5 = &fullRow[Md5Id_];

    YCHECK(fullRow.GetCount() >= DataSizeId_);
    *dataSize = &fullRow[DataSizeId_];
}

bool TSkynetColumnEvaluator::IsKeySwitched(TUnversionedRow fullRow, bool isLastRow)
{
    if (EffectiveKeySize_ == 0) {
        return false;
    }

    bool keyChanged = LastKey_ && CompareRows(fullRow, LastKey_, EffectiveKeySize_) != 0;
    if (isLastRow) {
        LastKeyHolder_ = GetKeyPrefix(fullRow, EffectiveKeySize_);
        LastKey_ = LastKeyHolder_;
    } else {
        LastKey_ = fullRow;
    }
    return keyChanged;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
