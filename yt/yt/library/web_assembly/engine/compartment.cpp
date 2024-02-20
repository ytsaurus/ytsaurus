#include "wavm_private_imports.h"
#include "intrinsics.h"
#include "system_libraries.h"

#include <yt/yt/library/web_assembly/api/compartment.h>

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/resource/resource.h>

#include <contrib/restricted/wavm/Lib/Runtime/RuntimePrivate.h>

#include <util/generic/hash_set.h>
#include <util/system/type_name.h>

namespace NYT::NWebAssembly {

using namespace WAVM;

////////////////////////////////////////////////////////////////////////////////

/*
 *    Memory Layout:
 *    +----------------------------------+          +---------------------+-----+
 *    | system libs | <- stack | heap -> |          | Global Offset Table | ... |
 *    +----------------------------------+          +---------------------+-----+
 *    0             512k       576k      128G       0             min: 2048
 *
 */

static constexpr I64 PageSize = 64_KB;
static constexpr I64 Padding = 64;

static constexpr I64 SystemLibsSize = 512_KB;
static constexpr I64 StackMaxSize = 64_KB;
static constexpr I64 MaxMemorySize  = 128_GB;

static constexpr I64 MemoryBase = 0;
static constexpr I64 SystemLibsLow = MemoryBase;
static constexpr I64 SystemLibsHigh = SystemLibsLow + SystemLibsSize;
static constexpr I64 StackLow = SystemLibsHigh + Padding;
static constexpr I64 StackHigh = StackLow + StackMaxSize;
static constexpr I64 HeapBase = StackHigh + Padding;
static constexpr I64 MinMemorySize = HeapBase + 512_KB;

static constexpr I64 StackPointer = StackHigh;

static constexpr I64 TableBase = 0;
static constexpr I32 TableBase32 = 0;

static constexpr U64 MinGlobalOffsetTableSize = 2048;

struct TMemoryLayoutData
{
    Runtime::GCPointer<Runtime::Memory> LinearMemory;
    Runtime::GCPointer<Runtime::Table> GlobalOffsetTable;
    Runtime::GCPointer<Runtime::Global> StackPointer;
    Runtime::GCPointer<Runtime::Global> HeapBase;
    Runtime::GCPointer<Runtime::Global> MemoryBase;
    Runtime::GCPointer<Runtime::Global> TableBase;
    Runtime::GCPointer<Runtime::Global> TableBase32;
    Runtime::GCPointer<Runtime::Global> StackLow;
    Runtime::GCPointer<Runtime::Global> StackHigh;

    TMemoryLayoutData BuildMemoryLayoutData(Runtime::Compartment* compartment);
    static void Clear(TMemoryLayoutData* data);
};

TMemoryLayoutData BuildMemoryLayoutData(Runtime::Compartment* compartment)
{
    static const auto mutableI64Global = IR::GlobalType{IR::ValueType::i64, true};
    static const auto immutableI64Global = IR::GlobalType{IR::ValueType::i64, false};
    static const auto immutableI32Global = IR::GlobalType{IR::ValueType::i32, false};

    auto data = TMemoryLayoutData{
        .LinearMemory = Runtime::createMemory(
            compartment,
            IR::MemoryType{
                /*isShared*/ false,
                /*indexType*/ IR::IndexType::i64,
                /*size*/ IR::SizeConstraints{MinMemorySize / PageSize, MaxMemorySize / PageSize}
            },
            "__linear_memory"),
        .GlobalOffsetTable = Runtime::createTable(
            compartment,
            IR::TableType{
                /*elementType*/ IR::ReferenceType::funcref,
                /*isShared*/ false,
                /*indexType*/ IR::IndexType::i32,
                /*size*/ IR::SizeConstraints{MinGlobalOffsetTableSize, std::numeric_limits<ui64>::max()},
            },
            nullptr,
            "__global_offset_table"),
        .StackPointer = Runtime::createGlobal(compartment, mutableI64Global, "__stack_pointer"),
        .HeapBase = Runtime::createGlobal(compartment, mutableI64Global, "__heap_base"),
        .MemoryBase = Runtime::createGlobal(compartment, immutableI64Global, "__memory_base"),
        .TableBase = Runtime::createGlobal(compartment, immutableI64Global, "__table_base"),
        .TableBase32 = Runtime::createGlobal(compartment, immutableI32Global, "__table_base32"),
        .StackLow = Runtime::createGlobal(compartment, mutableI64Global, "__stack_low"),
        .StackHigh = Runtime::createGlobal(compartment, mutableI64Global, "__stack_high"),
    };

    initializeGlobal(data.StackPointer, IR::Value{StackPointer});
    initializeGlobal(data.HeapBase, IR::Value{HeapBase});
    initializeGlobal(data.MemoryBase, IR::Value{MemoryBase});
    initializeGlobal(data.TableBase, IR::Value{TableBase});
    initializeGlobal(data.TableBase32, IR::Value{TableBase32});
    initializeGlobal(data.StackLow, IR::Value{StackLow});
    initializeGlobal(data.StackHigh, IR::Value{StackHigh});

    return data;
}

void TMemoryLayoutData::Clear(TMemoryLayoutData* data)
{
    data->LinearMemory = nullptr;
    data->GlobalOffsetTable = nullptr;
    data->StackPointer = nullptr;
    data->HeapBase = nullptr;
    data->MemoryBase = nullptr;
    data->TableBase = nullptr;
    data->TableBase32 = nullptr;
    data->StackLow = nullptr;
    data->StackHigh = nullptr;
}

////////////////////////////////////////////////////////////////////////////////

Runtime::ModuleRef LoadModuleFromBytecode(TRef bytecode)
{
    auto featureSpec = IR::FeatureSpec();
    featureSpec.memory64 = true;

    auto loadError = WASM::LoadError();
    auto wasmModule = Runtime::ModuleRef();

    bool succeeded = Runtime::loadBinaryModule(
        std::bit_cast<const U8*>(bytecode.Begin()),
        bytecode.size(),
        wasmModule,
        featureSpec,
        &loadError);

    if (!succeeded) {
        THROW_ERROR_EXCEPTION("Could not load binary module: %v", loadError.message);
    }

    return wasmModule;
}

IR::Module ParseWast(const TString& wast)
{
    auto irModule = IR::Module();
    irModule.featureSpec.memory64 = true;

    auto wastErrors = std::vector<WAST::Error>();

    bool succeeded = WAST::parseModule(
        wast.Data(),
        wast.Size() + 1, // String must be zero-terminated.
        irModule,
        wastErrors);

    if (!succeeded) {
        THROW_ERROR_EXCEPTION("Incorrect Wast file format");
    }

    return irModule;
}

////////////////////////////////////////////////////////////////////////////////

struct TNamedGlobalOffsetTableElements
{
    THashMap<std::string, int> Functions;
    THashMap<std::string, int> DataEntries;
};

////////////////////////////////////////////////////////////////////////////////

class TWebAssemblyCompartment
    : public IWebAssemblyCompartment
{
public:
    TWebAssemblyCompartment() = default;

    ~TWebAssemblyCompartment()
    {
        IntrinsicsInstance_ = nullptr;
        RuntimeLibraryInstance_ = nullptr;

        for (auto& instance : Instances_) {
            instance = nullptr;
        }

        Context_ = nullptr;
        TMemoryLayoutData::Clear(&MemoryLayoutData_);

        auto collected = Runtime::tryCollectCompartment(std::move(Compartment_));
        YT_ASSERT(collected);
    }

    virtual void AddModule(TRef bytecode, TStringBuf name = "") override
    {
        auto wavmModule = LoadModuleFromBytecode(bytecode);
        const auto& irModule = Runtime::getModuleIR(wavmModule);
        auto linkResult = LinkModule(irModule);
        AddExportsToGlobalOffsetTable(irModule);
        InstantiateModule(wavmModule, linkResult, name);
    }

    virtual void AddModule(const TString& wast, TStringBuf name = "") override
    {
        auto irModule = ParseWast(wast);
        auto wavmModule = Runtime::compileModule(irModule);
        auto linkResult = LinkModule(irModule);
        AddExportsToGlobalOffsetTable(irModule);
        InstantiateModule(wavmModule, linkResult, name);
    }

    // Strip erases the linking metadata. This can speed up the clone operation.
    // After stripping, the compartment can execute loaded functions, but further linking is no longer possible.
    virtual void Strip() override
    {
        YT_ASSERT(!Stripped_);
        Stripped_ = true;

        static const std::vector<std::string> shouldSaveExports{
            "malloc",
            "free",
            "EvaluateExpression",
            "EvaluateQuery",
            "init",
            "update",
            "merge",
            "finalize",
        };

        for (auto& instance : Instances_) {
            auto strippedExportMap = HashMap<std::string, Runtime::Object*>();
            for (const auto& item : shouldSaveExports) {
                auto it = instance->exportMap.get(item);
                if (it) {
                    strippedExportMap.add(item, *it);
                }
            }
            instance->exportMap = std::move(strippedExportMap);
        }

        GlobalOffsetTableElements_.Functions.clear();
        GlobalOffsetTableElements_.DataEntries.clear();
    }

    virtual void* GetFunction(const TString& name) override
    {
        auto& instance = Instances_.back();
        auto* function = Runtime::asFunction(Runtime::getInstanceExport(instance, name.c_str()));
        return static_cast<void*>(function);
    }

    virtual void* GetFunction(size_t index) override
    {
        auto* tableElement = Runtime::getTableElement(GetGlobalOffsetTable(), std::bit_cast<Uptr>(index));
        return static_cast<void*>(Runtime::asFunction(tableElement));
    }

    virtual void* GetContext() override
    {
        return static_cast<void*>(Context_);
    }

    virtual uintptr_t AllocateBytes(size_t length) override
    {
        static const auto signature = IR::FunctionType(/*inResults*/ {IR::ValueType::i64}, /*inParams*/ {IR::ValueType::i64});
        auto* mallocFunction = Runtime::getTypedInstanceExport(RuntimeLibraryInstance_, "malloc", signature);
        auto arguments = std::array<IR::UntaggedValue, 1>{std::bit_cast<Uptr>(length)};
        auto result = IR::UntaggedValue{};
        Runtime::invokeFunction(Context_, mallocFunction, signature, arguments.data(), &result);
        return result.u64;
    }

    virtual void FreeBytes(uintptr_t offset) override
    {
        static const auto signature = IR::FunctionType(/*inResults*/ {}, /*inParams*/ {IR::ValueType::i64});
        auto* freeFunction = getTypedInstanceExport(RuntimeLibraryInstance_, "free", signature);
        auto arguments = std::array<IR::UntaggedValue, 1>{std::bit_cast<Uptr>(offset)};
        Runtime::invokeFunction(Context_, freeFunction, signature, arguments.data(), {});
    }

    virtual void* GetHostPointer(uintptr_t offset, size_t length) override
    {
        char* bytes = Runtime::memoryArrayPtr<char>(MemoryLayoutData_.LinearMemory, std::bit_cast<ui64>(offset), length);
        return static_cast<void*>(bytes);
    }

    virtual uintptr_t GetCompartmentOffset(void* hostAddress) override
    {
        ui64 hostAddressAsUint = std::bit_cast<ui64>(hostAddress);
        ui64 baseAddress = std::bit_cast<ui64>(Runtime::getMemoryBaseAddress(MemoryLayoutData_.LinearMemory));
        uintptr_t offset = hostAddressAsUint - baseAddress;
        return offset;
    }

    virtual std::unique_ptr<IWebAssemblyCompartment> Clone() const override
    {
        auto result = std::unique_ptr<TWebAssemblyCompartment>(new TWebAssemblyCompartment());
        Clone(*this, result.get());
        return result;
    }

    Runtime::Memory* GetLinearMemory()
    {
        return MemoryLayoutData_.LinearMemory;
    }

    Runtime::Table* GetGlobalOffsetTable()
    {
        return MemoryLayoutData_.GlobalOffsetTable;
    }

private:
    friend class TLinker;
    friend std::unique_ptr<TWebAssemblyCompartment> BuildBaseImageOnce();

    static constexpr int TypicalModuleCount = 5;

    Runtime::GCPointer<Runtime::Compartment> Compartment_;
    Runtime::GCPointer<Runtime::Context> Context_;

    Runtime::GCPointer<Runtime::Instance> IntrinsicsInstance_;
    Runtime::GCPointer<Runtime::Instance> RuntimeLibraryInstance_;
    TCompactVector<Runtime::GCPointer<Runtime::Instance>, 5> Instances_;

    TCompactVector<Runtime::ModuleRef, TypicalModuleCount> Modules_;

    TMemoryLayoutData MemoryLayoutData_;
    TNamedGlobalOffsetTableElements GlobalOffsetTableElements_;

    bool Stripped_ = false;

    Runtime::LinkResult LinkModule(const IR::Module& irModule);
    void AddExportsToGlobalOffsetTable(const IR::Module& irModule);
    void InstantiateModule(
        const Runtime::ModuleRef& wavmModule,
        const Runtime::LinkResult& linkResult,
        TStringBuf debugName);

    static void Clone(const TWebAssemblyCompartment& from, TWebAssemblyCompartment* to);
};

////////////////////////////////////////////////////////////////////////////////

class TLinker
    : public Runtime::Resolver
{
public:
    explicit TLinker(TWebAssemblyCompartment* compartment)
        : Compartment_(compartment)
    { }

    bool resolve(
        const std::string& moduleName,
        const std::string& objectName,
        IR::ExternType type,
        Runtime::Object*& outObject) override
    {
        if (auto result = ResolveMemoryLayoutGlobals(objectName); result.has_value()) {
            outObject = *result;
            return true;
        }

        if (auto result = ResolveIntrinsics(objectName); result.has_value()) {
            outObject = *result;
            return true;
        }

        if (auto result = ResolveAlreadyLoaded(moduleName, objectName, type); result.has_value()) {
            outObject = *result;
            return true;
        }

        if (auto result = ResolveLocalUDFs(moduleName, objectName, type); result.has_value()) {
            outObject = *result;
            return true;
        }

        return false;
    }

private:
    TWebAssemblyCompartment* const Compartment_;

    std::optional<Runtime::Object*> ResolveMemoryLayoutGlobals(const std::string& objectName)
    {
        if (objectName == "__linear_memory" || objectName == "memory") {
            return Runtime::asObject(Compartment_->MemoryLayoutData_.LinearMemory);
        } else if (objectName == "__indirect_function_table") {
            return Runtime::asObject(Compartment_->MemoryLayoutData_.GlobalOffsetTable);
        } else if (objectName == "__stack_pointer") {
            return Runtime::asObject(Compartment_->MemoryLayoutData_.StackPointer);
        } else if (objectName == "__heap_base") {
            return Runtime::asObject(Compartment_->MemoryLayoutData_.HeapBase);
        } else if (objectName == "__memory_base") {
            return Runtime::asObject(Compartment_->MemoryLayoutData_.MemoryBase);
        } else if (objectName == "__table_base") {
            return Runtime::asObject(Compartment_->MemoryLayoutData_.TableBase);
        } else if (objectName == "__table_base32") {
            return Runtime::asObject(Compartment_->MemoryLayoutData_.TableBase32);
        } else if (objectName == "__stack_low") {
            return Runtime::asObject(Compartment_->MemoryLayoutData_.StackLow);
        } else if (objectName == "__stack_high") {
            return Runtime::asObject(Compartment_->MemoryLayoutData_.StackHigh);
        }
        return std::nullopt;
    }

    std::optional<Runtime::Object*> ResolveIntrinsics(const std::string& objectName)
    {
        auto function = Runtime::getInstanceExport(Compartment_->IntrinsicsInstance_, objectName);
        if (function != nullptr) {
            return function;
        }
        return std::nullopt;
    }

    std::optional<Runtime::Object*> ResolveAlreadyLoaded(
        const std::string& moduleName,
        const std::string& objectName,
        IR::ExternType type)
    {
        if (auto result = ResolveFunctionFromGlobalOffsetTable(moduleName, objectName, type); result.has_value()) {
            return result;
        }

        if (auto result = ResolveMemoryFromGlobalOffsetTable(moduleName, objectName, type); result.has_value()) {
            return result;
        }

        for (const auto& instance : Compartment_->Instances_) {
            auto object = Runtime::getInstanceExport(instance, objectName);
            if (object != nullptr) {
                return object;
            }
        }

        return std::nullopt;
    }

    std::optional<Runtime::Object*> ResolveFunctionFromGlobalOffsetTable(
        const std::string& moduleName,
        const std::string& objectName,
        IR::ExternType type)
    {
        if (moduleName != "GOT.func" && moduleName != "GOT.mem") {
            return std::nullopt;
        }

        auto demangled = CppDemangle(TString(objectName));

        auto it = Compartment_->GlobalOffsetTableElements_.Functions.find(demangled);
        if (it == Compartment_->GlobalOffsetTableElements_.Functions.end()) {
            return std::nullopt;
        }

        I64 globalOffsetTableIndex = it->second;
        auto globalType = asGlobalType(type);
        globalType.isMutable = true;

        auto resultOffset = Runtime::createGlobal(
            Compartment_->Compartment_,
            globalType,
            std::string(demangled));

        YT_ASSERT(resultOffset != nullptr);
        Runtime::initializeGlobal(resultOffset, globalOffsetTableIndex);
        return Runtime::asObject(resultOffset);
    }

    std::optional<Runtime::Object*> ResolveMemoryFromGlobalOffsetTable(
        const std::string& moduleName,
        const std::string& objectName,
        IR::ExternType type)
    {
        if (moduleName != "GOT.mem") {
            return std::nullopt;
        }

        auto demangled = CppDemangle(TString(objectName));

        auto it = Compartment_->GlobalOffsetTableElements_.DataEntries.find(demangled);
        if (it == Compartment_->GlobalOffsetTableElements_.DataEntries.end()) {
            return std::nullopt;
        }

        I64 globalOffsetTableIndex = it->second;
        auto globalType = asGlobalType(type);
        globalType.isMutable = true;

        auto resultOffset = Runtime::createGlobal(
            Compartment_->Compartment_,
            globalType,
            std::string(demangled));

        YT_ASSERT(resultOffset != nullptr);
        Runtime::initializeGlobal(resultOffset, globalOffsetTableIndex);
        return Runtime::asObject(resultOffset);
    }

    std::optional<Runtime::Object*> ResolveLocalUDFs(
        const std::string& /*moduleName*/,
        const std::string& /*objectName*/,
        IR::ExternType /*type*/)
    {
        return std::nullopt; // TODO(dtorilov): resolve known UDFs
    }
};

////////////////////////////////////////////////////////////////////////////////

Runtime::LinkResult TWebAssemblyCompartment::LinkModule(const IR::Module& irModule)
{
    auto linker = TLinker(this);
    auto linkResult = Runtime::linkModule(irModule, linker);

    if (!linkResult.success) {
        TStringBuilder description;

        description.AppendString("WebAssembly linkage error. Missing: ");

        for (int i = 0; i < std::ssize(linkResult.missingImports) - 1; ++i) {
            description.AppendString(linkResult.missingImports[i].exportName);
            description.AppendString(", ");
        }

        description.AppendString(linkResult.missingImports.back().exportName);

        THROW_ERROR_EXCEPTION(description.Flush());
    }

    return linkResult;
}

void TWebAssemblyCompartment::InstantiateModule(
    const Runtime::ModuleRef& wavmModule,
    const Runtime::LinkResult& linkResult,
    TStringBuf debugName)
{
    YT_VERIFY(linkResult.success);

    auto instance = instantiateModule(
        Compartment_,
        wavmModule,
        Runtime::ImportBindings{linkResult.resolvedImports},
        debugName.data());

    YT_ASSERT(instance);

    Modules_.push_back(wavmModule);
    Instances_.push_back(instance);
}

void TWebAssemblyCompartment::AddExportsToGlobalOffsetTable(const IR::Module& irModule)
{
    IR::DisassemblyNames disassemblyNames;
    getDisassemblyNames(irModule, disassemblyNames);

    auto exportedFunctions = THashSet<std::string>();
    for (const auto& item : irModule.exports) {
        if (item.kind == IR::ExternKind::function) {
            exportedFunctions.insert(CppDemangle(TString(item.name)));
        }
    }

    int offset = 0;
    for (const auto& elementSegment : irModule.elemSegments) {
        for (int index = 0; index < std::ssize(elementSegment.contents->elemIndices); index++) {
            int functionIndex = elementSegment.contents->elemIndices[index];
            std::string& functionName = disassemblyNames.functions[functionIndex].name;
            if (exportedFunctions.contains(functionName)) {
                int globalOffsetTableIndex = offset + index;
                GlobalOffsetTableElements_.Functions[functionName] = globalOffsetTableIndex;
            }
        }
    }

    for (const auto& exportedDataEntry : irModule.exports) {
        if (exportedDataEntry.kind != IR::ExternKind::global) {
            continue;
        }

        auto& global = irModule.globals.getDef(exportedDataEntry.index);
        i64 offset = 0;
        i64 value = offset + global.initializer.i64;
        auto demangled = CppDemangle(TString(exportedDataEntry.name));
        GlobalOffsetTableElements_.DataEntries[demangled] = value;
    }
}

void TWebAssemblyCompartment::Clone(const TWebAssemblyCompartment& source, TWebAssemblyCompartment* destination)
{
    destination->Compartment_ = Runtime::cloneCompartment(source.Compartment_);
    destination->Context_ = Runtime::cloneContext(source.Context_, destination->Compartment_);
    YT_ASSERT(destination->Compartment_->instances.size() >= 2);
    destination->IntrinsicsInstance_ = *destination->Compartment_->instances.get(0);
    destination->Instances_.push_back(destination->IntrinsicsInstance_);
    destination->RuntimeLibraryInstance_ = *destination->Compartment_->instances.get(1);
    destination->Instances_.push_back(destination->RuntimeLibraryInstance_);

    for (int index = 2; index < std::ssize(destination->Compartment_->instances); ++index) {
        destination->Instances_.push_back(*destination->Compartment_->instances.get(index));
    }

    destination->MemoryLayoutData_.LinearMemory = *destination->Compartment_->memories.get(0);
    destination->MemoryLayoutData_.GlobalOffsetTable = *destination->Compartment_->tables.get(0);
    destination->GlobalOffsetTableElements_ = source.GlobalOffsetTableElements_;

    for (auto* global : destination->Compartment_->globals) {
        if (global->debugName == "__stack_pointer") {
            destination->MemoryLayoutData_.StackPointer = global;
        } else if (global->debugName == "__heap_base") {
            destination->MemoryLayoutData_.HeapBase = global;
        } else if (global->debugName == "__memory_base") {
            destination->MemoryLayoutData_.MemoryBase = global;
        } else if (global->debugName == "__table_base") {
            destination->MemoryLayoutData_.TableBase = global;
        } else if (global->debugName == "__table_base32") {
            destination->MemoryLayoutData_.TableBase32 = global;
        } else if (global->debugName == "__stack_low") {
            destination->MemoryLayoutData_.StackLow = global;
        } else if (global->debugName == "__stack_high") {
            destination->MemoryLayoutData_.StackHigh = global;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

Runtime::ModuleRef LoadSystemLibraries()
{
    auto featureSpec = IR::FeatureSpec();
    featureSpec.memory64 = true;

    if (EnableSystemLibraries()) {
        auto bytecode = NResource::Find("libc.so.wasm");
        auto irModule = IR::Module(std::move(featureSpec));

        auto loadError = WASM::LoadError();
        bool succeeded = WASM::loadBinaryModule(
            std::bit_cast<U8*>(bytecode.begin()),
            bytecode.size(),
            irModule,
            &loadError);

        if (!succeeded) {
            THROW_ERROR_EXCEPTION("Could not load WebAssembly system libraries: %v", loadError.message);
        }

        auto resource = NResource::Find("compiled-libc");
        auto objectCode = std::vector<U8>(resource.size());
        ::memcpy(objectCode.data(), resource.data(), resource.size());

        return std::make_shared<Runtime::Module>(std::move(irModule), std::move(objectCode));
    }

    // Fallback to stub.
    static const TString code = R"(
        (module
            (type (;0;) (func))
            (type (;1;) (func (param i64) (result i64)))
            (type (;2;) (func (param i64)))

            (import "env" "__linear_memory" (memory (;0;) i64 0))
            (import "env" "__heap_base" (global (;0;) (mut i64)))

            (func $malloc (type 1) (param i64) (result i64)
                (local $address i64)
                (local.set $address (global.get 0))
                (global.set 0 (i64.add (local.get $address) (local.get 0)))
                (local.get $address)
            )

            (func $free (type 2) (param i64))

            (export "malloc" (func $malloc))
            (export "free" (func $free))
        ))";

    auto irModule = ParseWast(code);
    auto wavmModule = Runtime::compileModule(irModule);
    return wavmModule;
}

Runtime::ModuleRef LoadLocalUDFs()
{
    auto bytecode = NResource::Find("all-udfs.so.wasm");
    auto asRef = TRef(bytecode.begin(), bytecode.size());
    return LoadModuleFromBytecode(asRef);
}

std::unique_ptr<TWebAssemblyCompartment> BuildBaseImageOnce()
{
    auto compartment = std::make_unique<TWebAssemblyCompartment>();

    compartment->Compartment_ = Runtime::createCompartment();
    compartment->Context_ = Runtime::createContext(compartment->Compartment_);
    compartment->MemoryLayoutData_ = BuildMemoryLayoutData(compartment->Compartment_);

    compartment->IntrinsicsInstance_ = Intrinsics::instantiateModule(
        compartment->Compartment_,
        {WAVM_INTRINSIC_MODULE_REF(env)},
        "env");

    {
        auto wasmModule = LoadSystemLibraries();
        const auto& irModule = Runtime::getModuleIR(wasmModule);
        auto linkResult = compartment->LinkModule(irModule);
        compartment->AddExportsToGlobalOffsetTable(irModule);
        compartment->InstantiateModule(wasmModule, linkResult, "env");
        compartment->RuntimeLibraryInstance_ = compartment->Instances_.back();
    }

    if (EnableSystemLibraries()) {
        auto wasmModule = LoadLocalUDFs();
        const auto& irModule = Runtime::getModuleIR(wasmModule);
        auto linkResult = compartment->LinkModule(irModule);
        compartment->AddExportsToGlobalOffsetTable(irModule);
        compartment->InstantiateModule(wasmModule, linkResult, "env");
        compartment->RuntimeLibraryInstance_ = compartment->Instances_.back();
    }

    return std::move(compartment);
}

std::unique_ptr<IWebAssemblyCompartment> CreateBaseImage()
{
    static std::unique_ptr<TWebAssemblyCompartment> leakyBaseImageSingleton = BuildBaseImageOnce();
    return leakyBaseImageSingleton->Clone();
}

////////////////////////////////////////////////////////////////////////////////

static thread_local IWebAssemblyCompartment* CurrentCompartment;

IWebAssemblyCompartment* GetCurrentCompartment()
{
    return CurrentCompartment;
}

void SetCurrentCompartment(IWebAssemblyCompartment* compartment)
{
    CurrentCompartment = compartment;
    if (compartment) {
        Runtime::Table::setCurrentTable(
            static_cast<TWebAssemblyCompartment*>(CurrentCompartment)->GetGlobalOffsetTable());
        Runtime::Memory::setCurrentMemory(
            static_cast<TWebAssemblyCompartment*>(CurrentCompartment)->GetLinearMemory());
    } else {
        Runtime::Table::setCurrentTable(nullptr);
        Runtime::Memory::setCurrentMemory(nullptr);
    }
}

bool HasCurrentCompartment()
{
    return CurrentCompartment != nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NWebAssembly
