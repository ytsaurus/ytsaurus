#include "squash_fs_layout_builder.h"

namespace NYT::NSquashFS {

using namespace NConcurrency;
using namespace NNbd;

////////////////////////////////////////////////////////////////////////////////

// Min/max sizes of blocks where file data is stored.
// https://dr-emann.github.io/squashfs/squashfs.html#_the_superblock .
constexpr ui32 MinDataBlockSize = 4_KB;
constexpr ui32 MaxDataBlockSize = 1_MB;

// Resulting SquashFS file must be a multiple of 4KB.
// https://dr-emann.github.io/squashfs/squashfs.html#_overview .
constexpr ui16 DeviceBlockSize = 4_KB;

// Size of metadata block.
// https://dr-emann.github.io/squashfs/squashfs.html#_packing_metadata .
constexpr ui16 MetadataBlockSize = 8_KB;

// Number of entries in one Id block.
// https://dr-emann.github.io/squashfs/squashfs.html#_id_table .
constexpr ui16 EntriesInIdTableBlock = MetadataBlockSize / 4;

constexpr i64 DirectoryInodeSize = 40;
// The next size counted without name field.
constexpr i64 FileInodeSize = 56;
constexpr i64 DirectoryTableHeaderSize = 12;
// The next size counted without name field.
constexpr i64 DirectoryTableEntrySize = 8;

constexpr ui16 DirectoryPermissions = 0777;

// Directory inode constants.
// https://dr-emann.github.io/squashfs/squashfs.html#_directory_inodes .
constexpr ui32 DirectoryLinkCountPadding = 2;
constexpr ui32 DirectoryFileSizePadding = 3;

// Value stored in inode type field.
// https://dr-emann.github.io/squashfs/squashfs.html#_common_inode_header .
DEFINE_ENUM_WITH_UNDERLYING_TYPE(EInodeType, ui16,
    ((BasicDirectory) (1))
    ((BasicFile) (2))
    ((ExtendedDirectory) (8))
    ((ExtendedFile) (9))
);

// A file name in SquashFS can be at most 256 characters long.
// https://dr-emann.github.io/squashfs/squashfs.html#_directory_table .
constexpr ui16 MaxEntryNameLength = 256;

// A header must be followed by at most 256 entries.
// https://dr-emann.github.io/squashfs/squashfs.html#_directory_table .
constexpr ui16 MaxEntriesInDirectoryTablePage = 256;

// Id table can store at most 2^16 UID/GID.
// https://dr-emann.github.io/squashfs/squashfs.html#_the_superblock .
constexpr ui32 MaxEntriesInIdTable = 1 << 16;

// Offset to Data blocks. Equal to Superblock size.
constexpr ui64 DataBlocksOffset = 0x60;

////////////////////////////////////////////////////////////////////////////////

// This struct stores inode header.
// https://dr-emann.github.io/squashfs/squashfs.html#_common_inode_header .
struct TInode
    : public virtual TRefCounted
{
    EInodeType Type;
    ui16 Permissions;
    ui16 Uid;
    ui16 Gid;
    ui32 MTime;
    ui32 InodeNumber;

    // InodeBlockStart stores the offset from the beginning of
    // the inode table to the block that contains this inode.
    ui64 InodeBlockStart;
    // InodeBlockOffset stores the offset from
    // the start of the block to the inode.
    ui16 InodeBlockOffset;
};

DECLARE_REFCOUNTED_STRUCT(TInode)
DEFINE_REFCOUNTED_TYPE(TInode)

////////////////////////////////////////////////////////////////////////////////

// This struct stores extended directory inode.
// https://dr-emann.github.io/squashfs/squashfs.html#_directory_inodes .
struct TDirectoryInode
    : public TInode
{
    ui32 LinkCount = DirectoryLinkCountPadding;
    ui32 FileSize = DirectoryFileSizePadding;
    ui32 BlockIndex;
    ui32 ParentInode;
    // We don't use directory indexes.
    const ui16 IndexCount = 0;
    ui16 BlockOffset;
    // We don't use xattr table.
    const ui32 XattrIndex = 0xFFFFFFFF;
};

DECLARE_REFCOUNTED_STRUCT(TDirectoryInode)
DEFINE_REFCOUNTED_TYPE(TDirectoryInode)

////////////////////////////////////////////////////////////////////////////////

// This struct stores extended file inode.
// https://dr-emann.github.io/squashfs/squashfs.html#_file_inodes .
struct TFileInode
    : public TInode
{
    ui64 BlocksStart;
    ui64 FileSize;
    // We don't use check sparse.
    const ui64 Sparse = 0;
    // We don't support links.
    const ui32 LinkCount = 0;
    // We don't use fragment table.
    const ui32 FragIndex = 0xFFFFFFFF;
    const ui32 BlockOffset = 0;
    // We don't use xattr table.
    const ui32 XattrIndex = 0xFFFFFFFF;
    std::vector<ui32> BlockSizes;
};

DECLARE_REFCOUNTED_STRUCT(TFileInode)
DEFINE_REFCOUNTED_TYPE(TFileInode)

////////////////////////////////////////////////////////////////////////////////

// Classes for storing FS tree.

struct IEntry
    : public virtual TRefCounted
{
    virtual const TString& Name() const = 0;
    virtual ui16 GetPermissions() const = 0;
    virtual ui32 GetUid() const = 0;
    virtual ui32 GetGid() const = 0;
    virtual ui32 GetMTime() const = 0;
    virtual TInodePtr GetInode() const = 0;
    virtual EInodeType GetType() const = 0;
};

DECLARE_REFCOUNTED_STRUCT(IEntry)
DEFINE_REFCOUNTED_TYPE(IEntry)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TDirectory)
DECLARE_REFCOUNTED_CLASS(TFile)

class TDirectory
    : public IEntry
{
public:
    DEFINE_BYREF_RO_PROPERTY_NO_INIT_OVERRIDE(TString, Name);
    DEFINE_BYVAL_RO_PROPERTY_NO_INIT_OVERRIDE(ui16, Permissions);
    DEFINE_BYVAL_RO_PROPERTY_NO_INIT_OVERRIDE(ui32, Uid);
    DEFINE_BYVAL_RO_PROPERTY_NO_INIT_OVERRIDE(ui32, Gid);
    DEFINE_BYVAL_RO_PROPERTY_NO_INIT_OVERRIDE(ui32, MTime);
    DEFINE_BYREF_RO_PROPERTY_NO_INIT(std::vector<IEntryPtr>, Entries);

public:
    TDirectory(
        TString name,
        ui16 permissions,
        ui32 uid,
        ui32 gid,
        ui32 mTime);

    TInodePtr GetInode() const override;
    const TDirectoryInodePtr& GetDirectoryInode() const;
    void SetInode(TDirectoryInodePtr inode);

    EInodeType GetType() const override;

    TDirectoryPtr CreateDirectory(const TString& name);

    void CreateFile(
        const TString& name,
        const TString& path,
        ui16 permissions,
        IRandomAccessFileReaderPtr reader);

    void SortEntries();

private:
    TDirectoryInodePtr Inode_;

    IEntryPtr GetEntry(const TString& name) const;
};

DEFINE_REFCOUNTED_TYPE(TDirectory)

////////////////////////////////////////////////////////////////////////////////

class TFile
    : public IEntry
{
public:
    DEFINE_BYREF_RO_PROPERTY_NO_INIT_OVERRIDE(TString, Name);
    DEFINE_BYVAL_RO_PROPERTY_NO_INIT(TString, Path);
    DEFINE_BYVAL_RO_PROPERTY_NO_INIT(i64, Size);
    DEFINE_BYVAL_RO_PROPERTY_NO_INIT_OVERRIDE(ui16, Permissions);
    DEFINE_BYVAL_RO_PROPERTY_NO_INIT_OVERRIDE(ui32, Uid);
    DEFINE_BYVAL_RO_PROPERTY_NO_INIT_OVERRIDE(ui32, Gid);
    DEFINE_BYVAL_RO_PROPERTY_NO_INIT_OVERRIDE(ui32, MTime);
    DEFINE_BYVAL_RO_PROPERTY_NO_INIT(IRandomAccessFileReaderPtr, Reader);

public:
    TFile(
        TString name,
        TString path,
        ui16 permissions,
        ui32 uid,
        ui32 gid,
        ui32 mTime,
        IRandomAccessFileReaderPtr reader);

    TInodePtr GetInode() const override;
    const TFileInodePtr& GetFileInode() const;
    void SetInode(TFileInodePtr inode);

    EInodeType GetType() const override;

private:
    TFileInodePtr Inode_;
};

DEFINE_REFCOUNTED_TYPE(TFile)

////////////////////////////////////////////////////////////////////////////////

class TSquashFSLayoutBuilder
    : public ISquashFSLayoutBuilder
{
public:
    explicit TSquashFSLayoutBuilder(TSquashFSLayoutBuilderOptions options);

    void AddFile(
        TString path,
        ui16 permissions,
        IRandomAccessFileReaderPtr reader) override;
    TSquashFSLayoutPtr Build() override;

private:

    // Default structures of SquashFS.

    // This struct stores superblock.
    // https://dr-emann.github.io/squashfs/squashfs.html#_the_superblock .
    struct TSuperblock
    {
        // Special constant. Must be set to 0x73717368.
        const ui32 Magic = 0x73717368;
        ui32 InodeCount;
        ui32 ModTime;
        ui32 BlockSize;
        // We don't use fragments now.
        const ui32 FragCount = 0;
        // We don't use compressor now.
        const ui16 Compressor = 1;
        ui16 BlockLog;
        // This field stores bit wise OR of flags:
        // 0x0001 Inodes are stored uncompressed;
        // 0x0002 Data blocks are stored uncompressed;
        // 0x0010 Fragments are not used;
        // 0x0200 There are no Xattrs in the archive;
        // 0x0800 The Id table is uncompressed.
        const ui16 Flags = 0x0A13;
        ui16 IdCount;
        // Special constant. Must be set to 4.
        const ui16 VersionMajor = 4;
        // Special constant. Must be set to 0.
        const ui16 VersionMinor = 0;
        ui64 RootInode;
        ui64 BytesUsed;
        ui64 IdTable;
        // We don't use Xattrs now.
        const ui64 XAttrTable = 0xFFFFFFFFFFFFFFFF;
        ui64 InodeTable;
        ui64 DirTable;
        ui64 FragTable;
        // We don't use Export table now.
        const ui64 ExportTable = 0xFFFFFFFFFFFFFFFF;
    };

    // This struct stores header of block in directory header.
    // https://dr-emann.github.io/squashfs/squashfs.html#_directory_table .
    struct TDirectoryTableHeader
    {
        ui32 Count;
        ui32 Start;
        ui32 InodeNumber;
    };

    // This struct stores one entry of directory header.
    // https://dr-emann.github.io/squashfs/squashfs.html#_directory_table .
    struct TDirectoryTableEntry
    {
        ui16 Offset;
        i16 InodeOffset;
        EInodeType Type;
        ui16 NameSize;
        std::vector<ui8> Name;
    };

    struct TDirectoryTablePage
    {
        TDirectoryTableHeader Header;
        std::vector<TDirectoryTableEntry> Entries;
    };

    class TMetadataBlockOffsets
    {
    public:
        i64 GetSize() const;

    protected:
        i64 CurrentBlock_ = 0;
        i64 CurrentOffset_ = 0;

        void Shift(i64 delta);
        void ResetOffsets();
    };

    // This class stores inode table.
    // https://dr-emann.github.io/squashfs/squashfs.html#_inode_table .
    class TInodeTable
        : public TMetadataBlockOffsets
    {
    public:
        void Add(TInodePtr inodePtr);

        void ShiftDataBlocksOffsetInFileInodes(i64 offset);

        void Dump(TBlobOutput& buffer) const;

        void Reset();

    private:
        std::vector<TInodePtr> InodeTableEntries_;
    };

    // This class stores directory table.
    // https://dr-emann.github.io/squashfs/squashfs.html#_directory_table .
    class TDirectoryTable
        : public TMetadataBlockOffsets
    {
    public:
        void Add(const TDirectoryPtr& directory);

        void Dump(TBlobOutput& buffer) const;

        void Reset();

    private:
        std::vector<TDirectoryTablePage> Pages_;
    };

    // This class stores id table.
    // https://dr-emann.github.io/squashfs/squashfs.html#_id_table .
    class TIdTable
    {
    public:
        ui16 Get(ui32 id);

        ui16 GetEntryCount() const;

        ui32 GetOffsetToLookupTable() const;
        ui32 GetSize() const;

        void Dump(
            TBlobOutput& buffer,
            i64 tailOffset) const;

        void Reset();

    private:
        std::unordered_map<ui32, ui16> IdToIndex_;
        std::vector<ui32> Buffer_;

        ui8 GetBlockCount() const;
    };

    // This class stores data blocks.
    // https://dr-emann.github.io/squashfs/squashfs.html#_data_and_fragment_blocks .
    class TDataBlocks
    {
    public:
        explicit TDataBlocks(ui32 blockSize);

        void AddFile(const TFilePtr& file);

        i64 GetSize() const;

        void Dump(std::vector<IRandomAccessFileReaderPtr>& readers);

        void Reset();

    private:
        ui32 BlockSize_;
        i64 CurrentOffset_;
        std::vector<IRandomAccessFileReaderPtr> Readers_;
    };

    TInodeTable InodeTable_;
    TDirectoryTable DirectoryTable_;
    TIdTable IdTable_;
    TDataBlocks DataBlocks_;

    ui32 BlockSize_;
    ui32 MTime_;

    ui32 InodeCount_ = 0;

    TDirectoryPtr Root_;

    // Scan file system. Fill all standard blocks.
    void Traverse();
    void TraverseRecursive(const TDirectoryPtr& directory);

    TSuperblock BuildSuperblock();

    void DumpSuperblock(
        TSuperblock superblock,
        TBlobOutput& buffer) const;

    void BuildDirectoryInode(
        const TDirectoryPtr& directory,
        ui32 inodeNumber,
        ui32 parentNumber);

    void BuildFileInode(
        const TFilePtr& file,
        ui32 inodeNumber);

    static i64 GetInodeSize(const TInodePtr& inode);

    static void DumpInode(
        const TInodePtr& inode,
        TBlobOutput& buffer);

    static i64 GetDirectoryTableEntrySize(const TDirectoryTableEntry& entry);

    // Helper functions.
    static ui16 SetUncompressedMetadataFlag(ui16 value);
    static ui32 SetUncompressedDataBlockFlag(ui32 value);

    // Store image in buffer by metadata rules
    // https://dr-emann.github.io/squashfs/squashfs.html#_packing_metadata
    // and return lookup table at the begining of each block.
    // https://dr-emann.github.io/squashfs/squashfs.html#_storing_lookup_tables .
    static std::vector<ui64> AppendMetadata(
        TBlobOutput& buffer,
        const TBlob& metadata);
};

////////////////////////////////////////////////////////////////////////////////

TSquashFSLayout::TSquashFSLayout(TSquashFSData data)
    : Head_(TSharedRef::FromBlob(std::move(data.Head.Blob())))
    , Tail_(TSharedRef::FromBlob(std::move(data.Tail.Blob())))
{
    i64 offset = std::ssize(Head_);

    for (auto& reader : data.Readers) {
        i64 size = reader->GetSize();
        Parts_.push_back({
            .Offset = offset,
            .Size = size,
            .Reader = std::move(reader)
        });
        offset += size;
    }

    TailOffset_ = offset;
    offset += std::ssize(Tail_);

    Size_ = AlignUp<i64>(
        offset,
        DeviceBlockSize);
}

TSharedRef TSquashFSLayout::ReadHead(
    i64 offset,
    i64 length) const
{
    if (offset < 0 ||
        length < 0 ||
        offset + length > std::ssize(Head_))
    {
        THROW_ERROR_EXCEPTION("Invalid read of squashfs header")
            << TErrorAttribute("offset", offset)
            << TErrorAttribute("length", length)
            << TErrorAttribute("head_size", std::ssize(Head_));
    }

    return Head_.Slice(
        offset,
        offset + length);
}

i64 TSquashFSLayout::GetHeadSize() const
{
    return std::ssize(Head_);
}

TSharedRef TSquashFSLayout::ReadTail(
    i64 offset,
    i64 length) const
{
    if (offset < 0 ||
        length < 0 ||
        offset + length > std::ssize(Tail_))
    {
        THROW_ERROR_EXCEPTION("Invalid read of squashfs tail")
            << TErrorAttribute("offset", offset)
            << TErrorAttribute("length", length)
            << TErrorAttribute("tail_size", std::ssize(Tail_));
    }

    return Tail_.Slice(
        offset,
        offset + length);
}

i64 TSquashFSLayout::GetTailOffset() const
{
    return TailOffset_;
}

i64 TSquashFSLayout::GetTailSize() const
{
    return std::ssize(Tail_);
}

const std::vector<TSquashFSLayout::TPart>& TSquashFSLayout::GetParts() const
{
    return Parts_;
}

void TSquashFSLayout::Dump(IOutputStream& output) const
{
    output.Write(
        Head_.Begin(),
        std::ssize(Head_));

    for (auto part : Parts_) {
        auto ref = WaitFor(part.Reader->Read(0, part.Size))
            .ValueOrThrow("Failed to read part");
        output.Write(
            ref.Begin(),
            std::ssize(ref));
    }

    output.Write(
        Tail_.Begin(),
        std::ssize(Tail_));

    for (auto i = TailOffset_ + std::ssize(Tail_); i < Size_; ++i) {
        output.Write(0);
    }
}

void TSquashFSLayout::DumpHexText(IOutputStream& output) const
{
    TBlobOutput blobOutput;
    Dump(blobOutput);
    TBlob& blob = blobOutput.Blob();

    auto toHex = [] (char data) {
        if (data < 10) {
            return data + '0';
        } else {
            return data - 10 + 'a';
        }
    };
    for (int i = 0; i < std::ssize(blob); ++i) {
        if (i % 16 == 0) {
            for (int j = 7; j >= 0; --j) {
                output.Write(toHex((i >> 4 * j) & 0xf));
            }

            output.Write(": ");
        }

        output.Write(toHex((blob[i] >> 4) & 0xf));
        output.Write(toHex(blob[i] & 0xf));

        if (i % 16 == 15) {
            output.Write('\n');
        } else if (i % 2 == 1) {
            output.Write(' ');
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TDirectory::TDirectory(
    TString name,
    ui16 permissions,
    ui32 uid,
    ui32 gid,
    ui32 mTime)
    : Name_(std::move(name))
    , Permissions_(permissions)
    , Uid_(uid)
    , Gid_(gid)
    , MTime_(mTime)
{ }

TInodePtr TDirectory::GetInode() const
{
    return Inode_;
}

const TDirectoryInodePtr& TDirectory::GetDirectoryInode() const
{
    return Inode_;
}

void TDirectory::SetInode(TDirectoryInodePtr inode)
{
    Inode_ = std::move(inode);
}

EInodeType TDirectory::GetType() const
{
    return EInodeType::BasicDirectory;
}

TDirectoryPtr TDirectory::CreateDirectory(const TString& name)
{
    auto entry = GetEntry(name);
    if (entry) {
        if (entry->GetType() != EInodeType::BasicDirectory) {
            THROW_ERROR_EXCEPTION("Cannot open directory: file with the same name already exists")
                << TErrorAttribute("name", name);
        }

        return DynamicPointerCast<TDirectory>(entry);
    }

    auto newDirectory = New<TDirectory>(
        name,
        DirectoryPermissions,
        Uid_,
        Gid_,
        MTime_);
    Entries_.push_back(newDirectory);
    return newDirectory;
}

void TDirectory::CreateFile(
    const TString& name,
    const TString& path,
    ui16 permissions,
    IRandomAccessFileReaderPtr reader)
{
    auto entry = GetEntry(name);
    if (entry) {
        if (entry->GetType() == EInodeType::BasicFile) {
            THROW_ERROR_EXCEPTION("The file was already created")
                << TErrorAttribute("name", name);
        }
        THROW_ERROR_EXCEPTION("Cannot create file: directory with the same name already exists")
            << TErrorAttribute("name", name);
    }

    auto newFile = New<TFile>(
        name,
        path,
        permissions,
        Uid_,
        Gid_,
        MTime_,
        std::move(reader));
    Entries_.push_back(std::move(newFile));
}

void TDirectory::SortEntries()
{
    auto compareNames = [] (const IEntryPtr& a, const IEntryPtr& b) {
        return a->Name() < b->Name();
    };

    std::sort(
        Entries_.begin(),
        Entries_.end(),
        compareNames);
}

IEntryPtr TDirectory::GetEntry(const TString& name) const
{
    for (const auto& entry : Entries_) {
        if (entry->Name() == name) {
            return entry;
        }
    }

    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

TFile::TFile(
    TString name,
    TString path,
    ui16 permissions,
    ui32 uid,
    ui32 gid,
    ui32 mTime,
    IRandomAccessFileReaderPtr reader)
    : Name_(std::move(name))
    , Path_(std::move(path))
    , Size_(reader->GetSize())
    , Permissions_(permissions)
    , Uid_(uid)
    , Gid_(gid)
    , MTime_(mTime)
    , Reader_(std::move(reader))
{ }

TInodePtr TFile::GetInode() const
{
    return Inode_;
}

const TFileInodePtr& TFile::GetFileInode() const
{
    return Inode_;
}

void TFile::SetInode(TFileInodePtr inode)
{
    Inode_ = std::move(inode);
}

EInodeType TFile::GetType() const
{
    return EInodeType::BasicFile;
}

////////////////////////////////////////////////////////////////////////////////

TSquashFSLayoutBuilder::TSquashFSLayoutBuilder(TSquashFSLayoutBuilderOptions options)
    : DataBlocks_(options.BlockSize)
    , BlockSize_(options.BlockSize)
    , MTime_(options.MTime)
    , Root_(New<TDirectory>(
        /*name*/ "",
        DirectoryPermissions,
        options.Uid,
        options.Gid,
        options.MTime))
{
    if (BlockSize_ < MinDataBlockSize ||
        MaxDataBlockSize < BlockSize_ ||
        // Checks it's not a power of 2
        std::popcount(BlockSize_) != 1)
    {
        THROW_ERROR_EXCEPTION("Incorrect squashfs block size: it must be a power of two between %v and %v bytes",
            MinDataBlockSize,
            MaxDataBlockSize)
            << TErrorAttribute("block_size", BlockSize_);
    }
}

void TSquashFSLayoutBuilder::AddFile(
    TString path,
    ui16 permissions,
    IRandomAccessFileReaderPtr reader)
{
    auto splittedPath = StringSplitter(path).Split('/').ToList<TString>();

    if (splittedPath.empty()) {
        THROW_ERROR_EXCEPTION("The path is empty");
    }

    if (!splittedPath[0].empty()) {
        THROW_ERROR_EXCEPTION("The path is not absolute");
    }

    if (splittedPath.back().empty()) {
        THROW_ERROR_EXCEPTION("The path ends by directory, not by file");
    }

    auto validateName = [] (const TString& name) {
        if (name.empty() ||
            name.size() > MaxEntryNameLength)
        {
            THROW_ERROR_EXCEPTION("The directory/file name has incorrect number of symbols: it must be between 1 and %v characters",
                MaxEntryNameLength)
                << TErrorAttribute("name", name)
                << TErrorAttribute("name_size", std::ssize(name));
        }
    };

    auto currentDirectory = Root_;
    for (int i = 1; i + 1 < std::ssize(splittedPath); ++i) {
        validateName(splittedPath[i]);
        currentDirectory = currentDirectory->CreateDirectory(splittedPath[i]);
    }

    validateName(splittedPath.back());
    currentDirectory->CreateFile(
        splittedPath.back(),
        std::move(path),
        permissions,
        std::move(reader));
}

TSquashFSLayoutPtr TSquashFSLayoutBuilder::Build()
{
    // Code works in little-endian assumption.
    static_assert(std::endian::native == std::endian::little);

    // Reset all.
    InodeCount_ = 0;
    InodeTable_.Reset();
    DirectoryTable_.Reset();
    IdTable_.Reset();
    DataBlocks_.Reset();

    // Scan FS tree.
    Traverse();

    auto superblock = BuildSuperblock();
    InodeTable_.ShiftDataBlocksOffsetInFileInodes(DataBlocksOffset);

    // Dump result.
    TSquashFSData data;
    DumpSuperblock(superblock, data.Head);
    DataBlocks_.Dump(data.Readers);
    InodeTable_.Dump(data.Tail);
    DirectoryTable_.Dump(data.Tail);
    IdTable_.Dump(data.Tail, superblock.InodeTable);

    return New<TSquashFSLayout>(std::move(data));
}

void TSquashFSLayoutBuilder::Traverse()
{
    // Build inode for the root.
    BuildDirectoryInode(
        Root_,
        ++InodeCount_,
        /*parentNumber*/ 0);

    // Start scanning the tree.
    TraverseRecursive(Root_);

    // Insert the root inode into the table.
    auto rootInode = Root_->GetDirectoryInode();
    rootInode->ParentInode = InodeCount_ + 1;
    InodeTable_.Add(std::move(rootInode));
}

void TSquashFSLayoutBuilder::TraverseRecursive(const TDirectoryPtr& directory)
{
    // Preparation.
    const auto& directoryInode = directory->GetDirectoryInode();
    ui32 inodeNumber = directoryInode->InodeNumber;
    directory->SortEntries();
    const auto& entries = directory->Entries();
    ui32 newInodeNumber = InodeCount_;
    InodeCount_ += entries.size();

    // Set inodeNumber for children and do recursive call for subdirectories.
    for (const auto& entry : entries) {
        if (entry->GetType() == EInodeType::BasicDirectory) {
            auto subdirectory = DynamicPointerCast<TDirectory>(entry);
            YT_VERIFY(subdirectory);
            ++directoryInode->LinkCount;
            BuildDirectoryInode(
                subdirectory,
                ++newInodeNumber,
                inodeNumber);
            TraverseRecursive(subdirectory);
        } else {
            auto file = DynamicPointerCast<TFile>(entry);
            YT_VERIFY(file);
            BuildFileInode(
                file,
                ++newInodeNumber);
            DataBlocks_.AddFile(file);
        }
    }

    // Insert children inodes into the table.
    for (const auto& entry : entries) {
        InodeTable_.Add(entry->GetInode());
    }

    // Add current directory to the directory table.
    DirectoryTable_.Add(directory);
}

TSquashFSLayoutBuilder::TSuperblock TSquashFSLayoutBuilder::BuildSuperblock()
{
    TSuperblock superblock;

    // Fill superblock.
    superblock.ModTime = MTime_;
    superblock.BlockSize = BlockSize_;
    superblock.BlockLog = std::countr_zero(BlockSize_);
    superblock.InodeCount = InodeCount_;
    superblock.IdCount = IdTable_.GetEntryCount();;

    // Calculate offsets to standard blocks.
    superblock.RootInode = (Root_->GetInode()->InodeBlockStart << 16) | Root_->GetInode()->InodeBlockOffset;
    superblock.InodeTable = DataBlocksOffset + DataBlocks_.GetSize();
    superblock.DirTable = superblock.InodeTable + InodeTable_.GetSize();
    superblock.FragTable = superblock.DirTable + DirectoryTable_.GetSize();
    superblock.IdTable = superblock.FragTable + IdTable_.GetOffsetToLookupTable();
    superblock.BytesUsed = superblock.FragTable + IdTable_.GetSize();

    return superblock;
}

void TSquashFSLayoutBuilder::BuildDirectoryInode(
    const TDirectoryPtr& directory,
    ui32 inodeNumber,
    ui32 parentNumber)
{
    auto directoryInode = New<TDirectoryInode>();
    directoryInode->Type = EInodeType::ExtendedDirectory;
    directoryInode->Permissions = directory->GetPermissions();
    directoryInode->Uid = IdTable_.Get(directory->GetUid());
    directoryInode->Gid = IdTable_.Get(directory->GetGid());
    directoryInode->MTime = directory->GetMTime();
    directoryInode->InodeNumber = inodeNumber;
    directoryInode->LinkCount = DirectoryLinkCountPadding;
    directoryInode->ParentInode = parentNumber;
    directory->SetInode(std::move(directoryInode));
}

void TSquashFSLayoutBuilder::BuildFileInode(
    const TFilePtr& file,
    ui32 inodeNumber)
{
    auto fileInode = New<TFileInode>();
    fileInode->Type = EInodeType::ExtendedFile;
    fileInode->Permissions = file->GetPermissions();
    fileInode->Uid = IdTable_.Get(file->GetUid());
    fileInode->Gid = IdTable_.Get(file->GetGid());
    fileInode->MTime = file->GetMTime();
    fileInode->InodeNumber = inodeNumber;
    fileInode->FileSize = file->GetSize();
    file->SetInode(std::move(fileInode));
}

i64 TSquashFSLayoutBuilder::GetInodeSize(const TInodePtr& inode)
{
    if (inode->Type == EInodeType::ExtendedDirectory) {
        return DirectoryInodeSize;
    }

    auto file = DynamicPointerCast<TFileInode>(inode);
    YT_VERIFY(file);
    return FileInodeSize + file->BlockSizes.size() * sizeof(ui32);
}

i64 TSquashFSLayoutBuilder::GetDirectoryTableEntrySize(const TDirectoryTableEntry& entry)
{
    return DirectoryTableEntrySize + entry.Name.size() * sizeof(ui8);
}

ui16 TSquashFSLayoutBuilder::SetUncompressedMetadataFlag(ui16 value)
{
    // Setted MSB means that metadata block stored uncompressed.
    // https://dr-emann.github.io/squashfs/squashfs.html#_packing_metadata .
    return value | (1 << 15);
}

ui32 TSquashFSLayoutBuilder::SetUncompressedDataBlockFlag(ui32 value)
{
    // Setted 24th bit means that data block stored uncompressed.
    // https://dr-emann.github.io/squashfs/squashfs.html#_packing_file_data .
    return value | (1 << 24);
}

std::vector<ui64> TSquashFSLayoutBuilder::AppendMetadata(
    TBlobOutput& buffer,
    const TBlob& metadata)
{
    std::vector<ui64> lookupTable;
    if (metadata.IsEmpty()) {
        return lookupTable;
    }

    i64 metadataSize = std::ssize(metadata);
    i64 currentPosition = std::ssize(buffer);

    lookupTable.resize((metadataSize + MetadataBlockSize - 1) / MetadataBlockSize);

    for (i64 i = 0, j = 0; i < metadataSize; ++j) {
        lookupTable[j] = currentPosition;

        ui16 blockSize = std::min<i64>(
            metadataSize - i,
            MetadataBlockSize);
        ui16 blockHeader = SetUncompressedMetadataFlag(blockSize);

        WritePod(buffer, blockHeader);
        WriteRef(
            buffer,
            TRef(metadata.Begin() + i, blockSize));

        i += blockSize;
        currentPosition += sizeof(ui16) + blockSize;
    }

    return lookupTable;
}

void TSquashFSLayoutBuilder::DumpSuperblock(
    TSuperblock superblock,
    TBlobOutput& buffer) const
{
    // Hex code of example Superblock
    // with delimiters between values.
    // 00000000:|6873 7173|0500 0000|8bd7 a166|0000 0200| hsqs.......f....
    // 00000010:|0000 0000|0100|1100|1b03|0200|0400|0000| ................
    // 00000020:|8c00 0000 0000 0000|6401 0000 0000 0000| ........d.......
    // 00000030:|5c01 0000 0000 0000|ffff ffff ffff ffff| \...............
    // 00000040:|6600 0000 0000 0000|1401 0000 0000 0000| f...............
    // 00000050:|5201 0000 0000 0000|ffff ffff ffff ffff| R...............

    WritePod(buffer, superblock.Magic);
    WritePod(buffer, superblock.InodeCount);
    WritePod(buffer, superblock.ModTime);
    WritePod(buffer, superblock.BlockSize);

    WritePod(buffer, superblock.FragCount);
    WritePod(buffer, superblock.Compressor);
    WritePod(buffer, superblock.BlockLog);
    WritePod(buffer, superblock.Flags);
    WritePod(buffer, superblock.IdCount);
    WritePod(buffer, superblock.VersionMajor);
    WritePod(buffer, superblock.VersionMinor);

    WritePod(buffer, superblock.RootInode);
    WritePod(buffer, superblock.BytesUsed);

    WritePod(buffer, superblock.IdTable);
    WritePod(buffer, superblock.XAttrTable);

    WritePod(buffer, superblock.InodeTable);
    WritePod(buffer, superblock.DirTable);

    WritePod(buffer, superblock.FragTable);
    WritePod(buffer, superblock.ExportTable);
}

void TSquashFSLayoutBuilder::DumpInode(
    const TInodePtr& inode,
    TBlobOutput& buffer)
{
    if (inode->Type == EInodeType::ExtendedDirectory) {
        auto directoryInode = DynamicPointerCast<TDirectoryInode>(inode);
        YT_VERIFY(directoryInode);

        WritePod(buffer, directoryInode->Type);
        WritePod(buffer, directoryInode->Permissions);
        WritePod(buffer, directoryInode->Uid);
        WritePod(buffer, directoryInode->Gid);
        WritePod(buffer, directoryInode->MTime);
        WritePod(buffer, directoryInode->InodeNumber);

        WritePod(buffer, directoryInode->LinkCount);
        WritePod(buffer, directoryInode->FileSize);
        WritePod(buffer, directoryInode->BlockIndex);
        WritePod(buffer, directoryInode->ParentInode);

        WritePod(buffer, directoryInode->IndexCount);
        WritePod(buffer, directoryInode->BlockOffset);
        WritePod(buffer, directoryInode->XattrIndex);
    } else {
        auto fileInode = DynamicPointerCast<TFileInode>(inode);
        YT_VERIFY(fileInode);

        WritePod(buffer, fileInode->Type);
        WritePod(buffer, fileInode->Permissions);
        WritePod(buffer, fileInode->Uid);
        WritePod(buffer, fileInode->Gid);
        WritePod(buffer, fileInode->MTime);
        WritePod(buffer, fileInode->InodeNumber);

        WritePod(buffer, fileInode->BlocksStart);
        WritePod(buffer, fileInode->FileSize);

        WritePod(buffer, fileInode->Sparse);
        WritePod(buffer, fileInode->LinkCount);
        WritePod(buffer, fileInode->FragIndex);

        WritePod(buffer, fileInode->BlockOffset);
        WritePod(buffer, fileInode->XattrIndex);

        WriteRef(
            buffer,
            TRef(fileInode->BlockSizes.begin(), sizeof(ui32) * fileInode->BlockSizes.size()));
    }
}

////////////////////////////////////////////////////////////////////////////////

i64 TSquashFSLayoutBuilder::TMetadataBlockOffsets::GetSize() const
{
    i64 size = CurrentBlock_ + CurrentOffset_;

    // Addition for metadata header of last block.
    if (CurrentOffset_ != 0) {
        size += sizeof(ui16);
    }

    return size;
}

void TSquashFSLayoutBuilder::TMetadataBlockOffsets::Shift(i64 delta)
{
    YT_VERIFY(delta >= 0);
    i64 newOffset = CurrentOffset_ + delta;
    i64 skipBlocks = newOffset / MetadataBlockSize;
    CurrentBlock_ += skipBlocks * (MetadataBlockSize + sizeof(ui16));
    CurrentOffset_ = newOffset - skipBlocks * MetadataBlockSize;
}

void TSquashFSLayoutBuilder::TMetadataBlockOffsets::ResetOffsets()
{
    CurrentBlock_ = 0;
    CurrentOffset_ = 0;
}

////////////////////////////////////////////////////////////////////////////////

void TSquashFSLayoutBuilder::TInodeTable::Add(TInodePtr inode)
{
    inode->InodeBlockStart = CurrentBlock_;
    inode->InodeBlockOffset = CurrentOffset_;
    Shift(GetInodeSize(inode));
    InodeTableEntries_.push_back(std::move(inode));
}

void TSquashFSLayoutBuilder::TInodeTable::ShiftDataBlocksOffsetInFileInodes(i64 offset)
{
    for (const auto& inode : InodeTableEntries_) {
        if (inode->Type == EInodeType::ExtendedFile) {
            auto fileInode = DynamicPointerCast<TFileInode>(inode);
            YT_VERIFY(fileInode);
            fileInode->BlocksStart += offset;
        }
    }
}

void TSquashFSLayoutBuilder::TInodeTable::Dump(TBlobOutput& buffer) const
{
    TBlobOutput inodeBuffer;

    for (const auto& inode : InodeTableEntries_) {
        DumpInode(
            inode,
            inodeBuffer);
    }

    AppendMetadata(
        buffer,
        inodeBuffer.Blob());
}

void TSquashFSLayoutBuilder::TInodeTable::Reset()
{
    InodeTableEntries_.clear();
    ResetOffsets();
}

////////////////////////////////////////////////////////////////////////////////

void TSquashFSLayoutBuilder::TDirectoryTable::Add(const TDirectoryPtr& directory)
{
    const auto& entries = directory->Entries();

    // Empty directory does not have any directory table info.
    if (entries.empty()) {
        return;
    }

    // Preparation.
    const auto& directoryInode = directory->GetDirectoryInode();
    directoryInode->BlockIndex = CurrentBlock_;
    directoryInode->BlockOffset = CurrentOffset_;

    ui32 fileSize = 0;
    int partStart = 0;
    int partFinish = 0;
    while (partStart < std::ssize(entries)) {
        ui64 inodeBlockStart = entries[partStart]->GetInode()->InodeBlockStart;

        // Try to add entries[partFinish] to current page.
        while (partFinish < std::ssize(entries) &&
            partFinish + 1 - partStart <= MaxEntriesInDirectoryTablePage &&
            inodeBlockStart == entries[partFinish]->GetInode()->InodeBlockStart)
        {
            ++partFinish;
        }

        TDirectoryTablePage page;

        // Create header for the page.
        ui32 inodeNumber = entries[partStart]->GetInode()->InodeNumber;
        page.Header = TDirectoryTableHeader(
            partFinish - partStart - 1,
            inodeBlockStart,
            inodeNumber);
        Shift(DirectoryTableHeaderSize);
        fileSize += DirectoryTableHeaderSize;

        // Add all entries of the page.
        for (int i = partStart; i < partFinish; ++i) {
            // Preparation.
            const auto& entry = entries[i];
            auto inode = entry->GetInode();
            const auto& nameString = entry->Name();
            std::vector<ui8> nameVector(nameString.begin(), nameString.end());

            // Create entry.
            auto newEntry = TDirectoryTableEntry(
                inode->InodeBlockOffset,
                inode->InodeNumber - inodeNumber,
                entry->GetType(),
                nameVector.size() - 1,
                std::move(nameVector));

            i64 entrySize = GetDirectoryTableEntrySize(newEntry);
            Shift(entrySize);
            fileSize += entrySize;

            page.Entries.push_back(std::move(newEntry));
        }

        Pages_.push_back(std::move(page));
        partStart = partFinish;
    }

    fileSize += DirectoryFileSizePadding;

    directoryInode->FileSize = fileSize;
}

void TSquashFSLayoutBuilder::TDirectoryTable::Dump(TBlobOutput& buffer) const
{
    TBlobOutput directoryBuffer;
    for (const auto& page : Pages_) {
        WritePod(directoryBuffer, page.Header.Count);
        WritePod(directoryBuffer, page.Header.Start);
        WritePod(directoryBuffer, page.Header.InodeNumber);

        for (const auto& entry : page.Entries) {
            WritePod(directoryBuffer, entry.Offset);
            WritePod(directoryBuffer, entry.InodeOffset);
            WritePod(directoryBuffer, entry.Type);
            WritePod(directoryBuffer, entry.NameSize);
            WriteRef(
                directoryBuffer,
                TRef(entry.Name.begin(), entry.Name.size()));
        }
    }

    AppendMetadata(
        buffer,
        directoryBuffer.Blob());
}

void TSquashFSLayoutBuilder::TDirectoryTable::Reset()
{
    Pages_.clear();
    ResetOffsets();
}

////////////////////////////////////////////////////////////////////////////////

ui16 TSquashFSLayoutBuilder::TIdTable::Get(ui32 id)
{
    // Try to find given id.
    auto result = IdToIndex_.find(id);
    if (result != IdToIndex_.end()) {
        return result->second;
    }

    if (Buffer_.size() >= MaxEntriesInIdTable) {
        THROW_ERROR_EXCEPTION("The number of unique UID/GID values is too large");
    }

    // Give new value for this id.
    IdToIndex_[id] = Buffer_.size();
    Buffer_.push_back(id);
    return IdToIndex_[id];
}

ui8 TSquashFSLayoutBuilder::TIdTable::GetBlockCount() const
{
    return (Buffer_.size() + EntriesInIdTableBlock - 1) / EntriesInIdTableBlock;
}

ui32 TSquashFSLayoutBuilder::TIdTable::GetOffsetToLookupTable() const
{
    return sizeof(ui16) * GetBlockCount() + sizeof(ui32) * Buffer_.size();
}

ui16 TSquashFSLayoutBuilder::TIdTable::GetEntryCount() const
{
    return Buffer_.size();
}

ui32 TSquashFSLayoutBuilder::TIdTable::GetSize() const
{
    return GetOffsetToLookupTable() + sizeof(ui64) * GetBlockCount();
}

void TSquashFSLayoutBuilder::TIdTable::Dump(
    TBlobOutput& buffer,
    i64 tailOffset) const
{
    TBlobOutput idBuffer;
    WriteRef(
        idBuffer,
        TRef(Buffer_.begin(), Buffer_.size() * sizeof(ui32)));

    std::vector<ui64> lookupTable = AppendMetadata(
        buffer,
        idBuffer.Blob());
    for (auto& offset : lookupTable) {
        offset += tailOffset;
    }

    WriteRef(
        buffer,
        TRef(lookupTable.begin(), lookupTable.size() * sizeof(ui64)));
}

void TSquashFSLayoutBuilder::TIdTable::Reset()
{
    IdToIndex_.clear();
    Buffer_.clear();
}

////////////////////////////////////////////////////////////////////////////////

TSquashFSLayoutBuilder::TDataBlocks::TDataBlocks(ui32 blockSize)
    : BlockSize_(blockSize)
    , CurrentOffset_(0)
{ }

void TSquashFSLayoutBuilder::TDataBlocks::AddFile(const TFilePtr& file)
{
    const auto& inode = file->GetFileInode();
    ui64 size = inode->FileSize;
    inode->BlocksStart = CurrentOffset_;
    inode->BlockSizes.clear();

    // Split file on blocks.
    for (ui64 i = 0; i < size;) {
        ui32 currentBlockSize = std::min<ui64>(size - i, BlockSize_);
        inode->BlockSizes.push_back(SetUncompressedDataBlockFlag(currentBlockSize));
        i += currentBlockSize;
    }

    Readers_.push_back(file->GetReader());
    CurrentOffset_ += size;
}

i64 TSquashFSLayoutBuilder::TDataBlocks::GetSize() const
{
    return CurrentOffset_;
}

void TSquashFSLayoutBuilder::TDataBlocks::Dump(std::vector<IRandomAccessFileReaderPtr>& readers)
{
    readers = std::move(Readers_);
}

void TSquashFSLayoutBuilder::TDataBlocks::Reset()
{
    CurrentOffset_ = 0;
    Readers_.clear();
}

////////////////////////////////////////////////////////////////////////////////

ISquashFSLayoutBuilderPtr CreateSquashFSLayoutBuilder(TSquashFSLayoutBuilderOptions options)
{
   return New<TSquashFSLayoutBuilder>(options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSquashFS
