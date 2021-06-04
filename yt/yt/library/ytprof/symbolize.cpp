#include "symbolize.h"
#include "util/generic/yexception.h"

#include <util/folder/path.h>
#include <util/generic/hash.h>
#include <util/string/printf.h>
#include <util/system/filemap.h>
#include <util/system/unaligned_mem.h>
#include <util/memory/tempbuf.h>

#include <dlfcn.h>
#include <link.h>
#include <elf.h>
#include <sys/auxv.h>

#include <exception>
#include <cxxabi.h>

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

static TString DemangleCxxName(const char* mangledName)
{
    TTempBuf buffer;

    size_t returnedLength = buffer.Size();
    int returnedStatus = 0;

    abi::__cxa_demangle(mangledName, buffer.Data(), &returnedLength, &returnedStatus);
    return TString(returnedStatus == 0 ? buffer.Data() : mangledName);
}

////////////////////////////////////////////////////////////////////////////////

class TDLAddrSymbolizer
{
public:
    explicit TDLAddrSymbolizer(NProto::Profile* profile)
        : Profile_(profile)
    { }

    void Symbolize()
    {
        for (int i = 0; i < Profile_->function_size(); i++) {
            auto function = Profile_->mutable_function(i);

            void* ip = reinterpret_cast<void*>(function->id());

            Dl_info dlinfo{};
            if (dladdr(ip, &dlinfo) == 0) {
                continue;
            }

            TString name;
            TString demangledName;
            if (dlinfo.dli_sname == nullptr) {
                auto offset = reinterpret_cast<intptr_t>(ip) - reinterpret_cast<intptr_t>(dlinfo.dli_fbase);
                auto filename = TFsPath{dlinfo.dli_fname}.Basename();
                name = Sprintf("%p", reinterpret_cast<void*>(offset)) + "@" + filename;
                demangledName = name;
            } else {
                name = dlinfo.dli_sname;
                demangledName = DemangleCxxName(dlinfo.dli_sname);
            }

            function->set_name(SymbolizeString(demangledName));
            function->set_system_name(SymbolizeString(name));
        }
    }

private:
    NProto::Profile* const Profile_;

    THashMap<TString, ui64> Strings_;

    ui64 SymbolizeString(const TString& str)
    {
        auto it = Strings_.find(str);
        if (it != Strings_.end()) {
            return it->second;
        }

        auto id = Profile_->string_table_size();
        Strings_[str] = id;
        Profile_->add_string_table(str);
        return id;
    }
};

////////////////////////////////////////////////////////////////////////////////

using TElfAddr = ElfW(Addr);
using TElfEhdr = ElfW(Ehdr);
using TElfOff = ElfW(Off);
using TElfPhdr = ElfW(Phdr);
using TElfShdr = ElfW(Shdr);
using TElfNhdr = ElfW(Nhdr);
using TElfSym = ElfW(Sym);

class TElf final
{
public:
    class TSection
    {
    public:
        const TElfShdr& Header;

        const char* Name() const
        {
            if (!Elf_.SectionNames_) {
                throw yexception() << "Section names are not initialized";
            }

            if (Header.sh_name > Elf_.SectionNamesSize_) {
                throw yexception() << "Section name point outside of strings table";
            }

            return Elf_.SectionNames_ + Header.sh_name;
        }

        const char* begin() const
        {
            return Elf_.Mapped_ + Header.sh_offset;
        }

        const char* end() const
        {
            return begin() + size();
        }

        size_t size() const
        {
            return Header.sh_size;
        }

        TSection(const TElfShdr& header, const TElf& elf)
            : Header(header)
            , Elf_(elf)
        { }

    private:
        const TElf& Elf_;
    };

    explicit TElf(const TString& path)
        : FileMap_(path)
    {
        FileMap_.Map(0, FileMap_.GetFile().GetLength());

        ElfSize_ = FileMap_.MappedSize();
        Mapped_ = reinterpret_cast<const char*>(FileMap_.Ptr());

        if (ElfSize_ < sizeof(TElfEhdr)) {
            throw yexception() << "The size of ELF file is too small: " << ElfSize_;
        }

        Header_ = reinterpret_cast<const TElfEhdr*>(Mapped_);

        if (memcmp(Header_->e_ident, "\x7F""ELF", 4) != 0) {
            throw yexception() << "The file is not ELF according to magic";
        }

        TElfOff sectionHeaderOffset = Header_->e_shoff;
        uint16_t sectionHeaderNumEntries = Header_->e_shnum;

        if (!sectionHeaderOffset ||
            !sectionHeaderNumEntries ||
            sectionHeaderOffset + sectionHeaderNumEntries * sizeof(TElfShdr) > ElfSize_
        ) {
            throw yexception() << "The ELF is truncated (section header points after end of file)";
        }

        SectionHeaders_ = reinterpret_cast<const TElfShdr*>(Mapped_ + sectionHeaderOffset);

        auto sectionStrtab = FindSection([&] (const TSection& section, size_t idx) {
            return section.Header.sh_type == SHT_STRTAB && Header_->e_shstrndx == idx;
        });

        if (!sectionStrtab) {
            throw yexception() << "The ELF doesn't have string table with section names";
        }

        TElfOff sectionNamesOffset = sectionStrtab->Header.sh_offset;
        if (sectionNamesOffset >= ElfSize_) {
            throw yexception() << "The ELF is truncated (section names string table points after end of file)";
        }
        if (sectionNamesOffset + SectionNamesSize_ > ElfSize_) {
            throw yexception() << "The ELF is truncated (section names string table is truncated)";
        }

        SectionNames_ = reinterpret_cast<const char *>(Mapped_ + sectionNamesOffset);
        SectionNamesSize_ = sectionStrtab->Header.sh_offset;

        TElfOff programHeaderOffset = Header_->e_phoff;
        uint16_t programHeaderNumEntries = Header_->e_phnum;

        if (!programHeaderOffset ||
            !programHeaderNumEntries ||
            programHeaderOffset + programHeaderNumEntries * sizeof(TElfPhdr) > ElfSize_
        ) {
            throw yexception() << "The ELF is truncated (program header points after end of file)";
        }

        ProgramHeaders_ = reinterpret_cast<const TElfPhdr*>(Mapped_ + programHeaderOffset);
    }

    bool IterateSections(std::function<bool(const TSection& section, size_t idx)> pred) const
    {
        for (size_t idx = 0; idx < Header_->e_shnum; ++idx) {
            TSection section(SectionHeaders_[idx], *this);

            if (section.Header.sh_offset + section.Header.sh_size > ElfSize_) {
                continue;
            }

            if (pred(section, idx)) {
                return true;
            }
        }
        return false;

    }

    std::optional<TSection> FindSection(std::function<bool(const TSection& section, size_t idx)> pred) const
    {
        std::optional<TSection> result;

        IterateSections([&] (const TSection & section, size_t idx) {
            if (pred(section, idx)) {
                result.emplace(section);
                return true;
            }
            return false;
        });

        return result;

    }

    std::optional<TSection> FindSectionByName(const char* name) const
    {
        return FindSection([&](const TSection & section, size_t) { return 0 == strcmp(name, section.Name()); });
    }

    const char* begin() const { return Mapped_; }
    const char* end() const { return Mapped_ + ElfSize_; }
    size_t size() const { return ElfSize_; }

    TString GetBuildId() const {
        for (size_t idx = 0; idx < Header_->e_phnum; ++idx) {
            const TElfPhdr& phdr = ProgramHeaders_[idx];

            if (phdr.p_type == PT_NOTE) {
                return GetBuildId(Mapped_ + phdr.p_offset, phdr.p_filesz);
            }
        }

        return {};
    }

    static TString GetBuildId(const char* nhdrPos, size_t nhdrSize)
    {
        const char* nhdrEnd = nhdrPos + nhdrSize;

        while (nhdrPos < nhdrEnd) {
            TElfNhdr nhdr = ReadUnaligned<TElfNhdr>(nhdrPos);

            nhdrPos += sizeof(TElfNhdr) + nhdr.n_namesz;
            if (nhdr.n_type == NT_GNU_BUILD_ID) {
                const char* build_id = nhdrPos;
                return {build_id, nhdr.n_descsz};
            }
            nhdrPos += nhdr.n_descsz;
        }

        return {};
    }

private:
    TFileMap FileMap_;

    size_t ElfSize_ = 0;
    const char* Mapped_ = nullptr;
    const TElfEhdr* Header_;
    const TElfShdr* SectionHeaders_;
    const TElfPhdr* ProgramHeaders_;

    const char* SectionNames_ = nullptr;
    size_t SectionNamesSize_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::optional<std::pair<void*, void*>> GetExecutableRange(dl_phdr_info* info)
{
    const TElfPhdr* load = nullptr;
    for (int i = 0; i < info->dlpi_phnum; i++) {
        load = &(info->dlpi_phdr[i]);
        if (load->p_type == PT_LOAD && (load->p_flags & PF_X) != 0) {
            break;
        }
    }

    if (!load) {
        return {};
    }

    return std::pair{
        reinterpret_cast<void*>(info->dlpi_addr + load->p_vaddr),
        reinterpret_cast<void*>(info->dlpi_addr + load->p_vaddr + load->p_memsz)
    };
}

#if defined(_msan_enabled_)
extern "C" void __msan_unpoison_string(const volatile void* a);
#endif

class TSymbolIndex
{
public:
    TSymbolIndex()
    {
        dl_iterate_phdr(CollectSymbols, this);

        std::sort(Objects_.begin(), Objects_.end(), [] (const TObject& a, const TObject& b) { return a.AddressBegin < b.AddressBegin; });
        std::sort(Symbols_.begin(), Symbols_.end(), [](const TSymbol& a, const TSymbol & b) { return a.AddressBegin < b.AddressBegin; });

        /// We found symbols both from loaded program headers and from ELF symbol tables.
        Symbols_.erase(std::unique(Symbols_.begin(), Symbols_.end(), [] (const TSymbol &a, const TSymbol& b) {
            return a.AddressBegin == b.AddressBegin && a.AddressEnd == b.AddressEnd;
        }), Symbols_.end());
    }

    static int CollectSymbols(dl_phdr_info* info, size_t, void* ptr)
    {
        TSymbolIndex* symbolIndex = reinterpret_cast<TSymbolIndex*>(ptr);

        symbolIndex->CollectSymbolsFromProgramHeaders(info);
        symbolIndex->CollectSymbolsFromElf(info);

        /* Continue iterations */
        return 0;
    }

    struct TSymbol
    {
        const void* AddressBegin;
        const void* AddressEnd;
        const char* Name;
    };

    struct TObject
    {
        const void* AddressBegin;
        const void* AddressEnd;

        TString Name;
        TString BuildId;

        std::shared_ptr<TElf> Elf;
    };

    const TSymbol* FindSymbol(const void* address) const
    {
        return Find(address, Symbols_);
    }

    const TObject* FindObject(const void* address) const
    {
        return Find(address, Objects_);
    }

    const std::vector<TSymbol>& Symbols() const
    {
        return Symbols_;
    }

    const std::vector<TObject>& Objects() const
    {
        return Objects_;
    }

private:
    std::vector<TSymbol> Symbols_;
    std::vector<TObject> Objects_;

    template <typename T>
    static const T* Find(const void* address, const std::vector<T> & vec)
    {
        auto it = std::lower_bound(vec.begin(), vec.end(), address, [] (const T & symbol, const void * addr) {
            return symbol.AddressBegin <= addr;
        });

        if (it == vec.begin()) {
            return nullptr;
        } else {
            --it; /// Last range that has left boundary less or equals than address.
        }

        if (address >= it->AddressBegin && address < it->AddressEnd) {
            return &*it;
        } else {
            return nullptr;
        }
    }

    static TString GetBuildId(dl_phdr_info* info)
    {
        for (size_t header_index = 0; header_index < info->dlpi_phnum; ++header_index) {
            const TElfPhdr& phdr = info->dlpi_phdr[header_index];
            if (phdr.p_type != PT_NOTE)
                continue;

            return TElf::GetBuildId(reinterpret_cast<const char *>(info->dlpi_addr + phdr.p_vaddr), phdr.p_memsz);
        }
        return {};
    }

    void CollectSymbolsFromProgramHeaders(dl_phdr_info* info)
    {
        /* Iterate over all headers of the current shared lib
        * (first call is for the executable itself)
        */
        for (size_t headerIndex = 0; headerIndex < info->dlpi_phnum; ++headerIndex) {
            /* Further processing is only needed if the dynamic section is reached
            */
            if (info->dlpi_phdr[headerIndex].p_type != PT_DYNAMIC)
                continue;

            /* Get a pointer to the first entry of the dynamic section.
            * It's address is the shared lib's address + the virtual address
            */
            const ElfW(Dyn)* dynBegin = reinterpret_cast<const ElfW(Dyn) *>(info->dlpi_addr + info->dlpi_phdr[headerIndex].p_vaddr);

            /// For unknown reason, addresses are sometimes relative sometimes absolute.
            auto correctAddress = [](ElfW(Addr) base, ElfW(Addr) ptr) {
                return ptr > base ? ptr : base + ptr;
            };

            /* Iterate over all entries of the dynamic section until the
            * end of the symbol table is reached. This is indicated by
            * an entry with d_tag == DT_NULL.
            */

            size_t symCnt = 0;
            for (const auto * it = dynBegin; it->d_tag != DT_NULL; ++it) {
                if (it->d_tag == DT_GNU_HASH) {
                    /// This code based on Musl-libc.

                    const uint32_t* buckets = nullptr;
                    const uint32_t* hashval = nullptr;

                    const ElfW(Word)* hash = reinterpret_cast<const ElfW(Word) *>(correctAddress(info->dlpi_addr, it->d_un.d_ptr));

                    buckets = hash + 4 + (hash[2] * sizeof(size_t) / 4);

                    for (ElfW(Word) i = 0; i < hash[0]; ++i) {
                        if (buckets[i] > symCnt) {
                            symCnt = buckets[i];
                        }
                    }

                    if (symCnt) {
                        symCnt -= hash[1];
                        hashval = buckets + hash[0] + symCnt;
                        do {
                            ++symCnt;
                        } while (!(*hashval++ & 1));
                    }

                    break;
                }
            }

            if (!symCnt) {
                continue;
            }

            const char* strtab = nullptr;
            for (const auto * it = dynBegin; it->d_tag != DT_NULL; ++it) {
                if (it->d_tag == DT_STRTAB) {
                    strtab = reinterpret_cast<const char *>(correctAddress(info->dlpi_addr, it->d_un.d_ptr));
                    break;
                }
            }

            if (!strtab) {
                continue;
            }

            for (const auto * it = dynBegin; it->d_tag != DT_NULL; ++it) {
                if (it->d_tag == DT_SYMTAB) {
                    /* Get the pointer to the first entry of the symbol table */
                    const ElfW(Sym) * elfSym = reinterpret_cast<const ElfW(Sym) *>(correctAddress(info->dlpi_addr, it->d_un.d_ptr));

                    /* Iterate over the symbol table */
                    for (ElfW(Word) symIndex = 0; symIndex < ElfW(Word)(symCnt); ++symIndex) {
                        /// We are not interested in empty symbols.
                        if (!elfSym[symIndex].st_size) {
                            continue;
                        }

                        /* Get the name of the sym_index-th symbol.
                        * This is located at the address of st_name relative to the beginning of the string table.
                        */
                        const char * symName = &strtab[elfSym[symIndex].st_name];

                        if (!symName) {
                            continue;
                        }

                        TSymbol symbol;
                        symbol.AddressBegin = reinterpret_cast<const void *>(info->dlpi_addr + elfSym[symIndex].st_value);
                        symbol.AddressEnd = reinterpret_cast<const void *>(info->dlpi_addr + elfSym[symIndex].st_value + elfSym[symIndex].st_size);
                        symbol.Name = symName;
                        Symbols_.push_back(symbol);
                    }

                    break;
                }
            }
        }
    }

    void CollectSymbolsFromElf(dl_phdr_info* info)
    {
        /// MSan does not know that the program segments in memory are initialized.
#if defined(_msan_enabled_)
        __msan_unpoison_string(info->dlpi_name);
#endif

        TString objectName = info->dlpi_name;
        auto buildId = GetBuildId(info);

        /// If the name is empty and there is a non-empty build-id - it's main executable.
        /// Find a elf file for the main executable and set the build-id.
        if (objectName.empty()) {
            objectName = TFsPath{"/proc/self/exe"}.ReadLink();
        } else {
            TFsPath debugInfoPath = TFsPath("/usr/lib/debug") / TFsPath{objectName}.Basename();
            if (debugInfoPath.Exists()) {
                objectName = debugInfoPath;
            }
        }

        TObject object;
        object.BuildId = buildId;

        auto range = GetExecutableRange(info);
        if (!range) {
            return;
        }

        object.Name = objectName;

        object.AddressBegin = range->first;
        object.AddressEnd = range->second;

        Objects_.push_back(std::move(object));

        if (!TFsPath{objectName}.Exists()) {
            return;
        }

        Objects_.back().Elf = std::make_unique<TElf>(objectName);
        TString fileBuildId = Objects_.back().Elf->GetBuildId();
        if (buildId != fileBuildId) {
            Objects_.back().Elf.reset();
            return;
        }

        SearchAndCollectSymbolsFromELFSymbolTable(info, *Objects_.back().Elf, SHT_SYMTAB, ".strtab");
    }

    bool SearchAndCollectSymbolsFromELFSymbolTable(
        dl_phdr_info* info,
        const TElf& elf,
        unsigned sectionHeaderType,
        const char* stringTableName)
    {
        std::optional<TElf::TSection> symbolTable;
        std::optional<TElf::TSection> stringTable;

        if (!elf.IterateSections([&] (const TElf::TSection & section, size_t) {
            if (section.Header.sh_type == sectionHeaderType) {
                symbolTable.emplace(section);
            } else if (section.Header.sh_type == SHT_STRTAB && 0 == strcmp(section.Name(), stringTableName)) {
                stringTable.emplace(section);
            }

            return (symbolTable && stringTable);
        })) {
            return false;
        }

        CollectSymbolsFromELFSymbolTable(info, elf, *symbolTable, *stringTable);
        return true;
    }

    void CollectSymbolsFromELFSymbolTable(
        dl_phdr_info* info,
        const TElf& elf,
        const TElf::TSection& symbol_table,
        const TElf::TSection& string_table)
    {
        const TElfSym* symbolTableEntry = reinterpret_cast<const TElfSym*>(symbol_table.begin());
        const TElfSym* symbolTableEnd = reinterpret_cast<const TElfSym*>(symbol_table.end());

        const char* strings = string_table.begin();
        for (; symbolTableEntry < symbolTableEnd; ++symbolTableEntry) {
            if (!symbolTableEntry->st_name
                || !symbolTableEntry->st_value
                || !symbolTableEntry->st_size
                || strings + symbolTableEntry->st_name >= elf.end()) {
                continue;
                }

            /// Find the name in strings table.
            const char * symbolName = strings + symbolTableEntry->st_name;

            if (!symbolName) {
                continue;
            }

            TSymbolIndex::TSymbol symbol;
            symbol.AddressBegin = reinterpret_cast<const void *>(info->dlpi_addr + symbolTableEntry->st_value);
            symbol.AddressEnd = reinterpret_cast<const void *>(info->dlpi_addr + symbolTableEntry->st_value + symbolTableEntry->st_size);
            symbol.Name = symbolName;
            Symbols_.push_back(symbol);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSymbolIndexSymbolizer
{
public:
    explicit TSymbolIndexSymbolizer(NProto::Profile* profile)
        : Profile_(profile)
    { }

    void Symbolize()
    {
        for (const auto& object : SymbolIndex_.Objects()) {
            SymbolizeObject(&object);
        }

        for (int i = 0; i < Profile_->location_size(); i++) {
            auto location = Profile_->mutable_location(i);

            void* ip = reinterpret_cast<void*>(location->address());

            auto object = SymbolIndex_.FindObject(ip);
            if (object) {
                location->set_mapping_id(SymbolizeObject(object));
            }
        }

        for (int i = 0; i < Profile_->function_size(); i++) {
            auto function = Profile_->mutable_function(i);

            void* ip = reinterpret_cast<void*>(function->id());

            if (auto symbol = SymbolIndex_.FindSymbol(ip)) {
                function->set_name(SymbolizeString(DemangleCxxName(symbol->Name)));
                function->set_system_name(SymbolizeString(symbol->Name));
            } else if(auto object = SymbolIndex_.FindObject(ip)) {
                auto offset = reinterpret_cast<intptr_t>(ip) - reinterpret_cast<intptr_t>(object->AddressBegin);
                auto filename = TFsPath{object->Name}.Basename();
                auto name =  Sprintf("%p", reinterpret_cast<void*>(offset)) + "@" + filename;

                function->set_name(SymbolizeString(name));
                function->set_system_name(SymbolizeString(name));
            }
        }
    }

private:
    NProto::Profile* const Profile_;

    TSymbolIndex SymbolIndex_;

    THashMap<const TSymbolIndex::TObject*, ui64> Objects_;
    THashMap<TString, ui64> Strings_;

    ui64 SymbolizeString(const TString& str)
    {
        auto it = Strings_.find(str);
        if (it != Strings_.end()) {
            return it->second;
        }

        auto id = Profile_->string_table_size();
        Strings_[str] = id;
        Profile_->add_string_table(str);
        return id;
    }

    ui64 SymbolizeObject(const TSymbolIndex::TObject* object)
    {
        auto it = Objects_.find(object);
        if (it != Objects_.end()) {
            return it->second;
        }

        auto mapping = Profile_->add_mapping();
        mapping->set_id(Profile_->mapping_size());
        Objects_[object] = mapping->id();

        mapping->set_memory_start(reinterpret_cast<ui64>(object->AddressBegin));
        mapping->set_memory_limit(reinterpret_cast<ui64>(object->AddressEnd));
        mapping->set_filename(SymbolizeString(object->Name));
        return mapping->id();
    }
};

////////////////////////////////////////////////////////////////////////////////

static int OnPhdr(struct dl_phdr_info *info, size_t /* size */, void *data)
{
    auto vdso = (uintptr_t) getauxval(AT_SYSINFO_EHDR);

    auto vdsoRange = reinterpret_cast<std::pair<void*, void*>*>(data);
    if (info->dlpi_addr == vdso) {
        auto range = GetExecutableRange(info);
        if (range) {
            *vdsoRange = *range;
        }
    }

    return 0;
}

std::pair<void*, void*> GetVdsoRange()
{
    std::pair<void*, void*> vdsoRange;
    dl_iterate_phdr(OnPhdr, &vdsoRange);
    return vdsoRange;
}

////////////////////////////////////////////////////////////////////////////////

void Symbolize(NProto::Profile* profile)
{
    TSymbolIndexSymbolizer symbolizer(profile);
    symbolizer.Symbolize();
    return;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
