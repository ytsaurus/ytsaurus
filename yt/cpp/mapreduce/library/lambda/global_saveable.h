#pragma once

#include <yt/cpp/mapreduce/interface/operation.h>

#include <util/generic/singleton.h>
#include <util/generic/typetraits.h>
#include <util/ysaveload.h>

// ==============================================
namespace NYT {
// ==============================================

/** Example
struct TMyGlobalConfig : ISerializableForJob {
    ui64 Limit;
    Y_SAVELOAD_JOB(Limit);
};

TMyGlobalConfig MyGlobalConfig; // must use RegisterGlobalSaveable

struct TMyGlobalConfig2 {
    ui64 Limit;
    Y_SAVELOAD_DEFINE(Limit);
};

TSaveable<TMyGlobalConfig2> MyGlobalConfig2; // RegisterGlobalSaveable must not be used

struct TMyGlobalConfig3 {
    ui64 Limit;
};

TSaveablePOD<TMyGlobalConfig3> MyGlobalConfig3; // RegisterGlobalSaveable must not be used

int main(...) {
    MyGlobalConfig.Limit = ParseArgs(argc, argv).Limit;
...
    RegisterGlobalSaveable(MyGlobalConfig); // TLambdaOpBase прочитает этот список и будет вызывать Save и Load
    NYT::Initialize(argc, argv);
...
    CopyIf<TNode>(client, input,  output,
        [](auto& row) { return row["Val"].AsUint64() < MyGlobalConfig.Limit; });
}
*/


class TSaveableBase : public ISerializableForJob {
protected:
    TSaveableBase() = default;
};

template<class T>
class TSaveable : public TSaveableBase, public T {
public:
    TSaveable();
    void Save(IOutputStream& stream) const override {
        T::Save(&stream);
    }
    void Load(IInputStream& stream) override {
        T::Load(&stream);
    }
};

template<class T>
class TSaveablePOD : public TSaveableBase, public T {
public:
    TSaveablePOD();
    void Save(IOutputStream& stream) const override {
        /* This check is only for the most trivial mistakes.
           It does not check that there are no pointer-type members.
           A bit more relaxed than real POD, e.g. to allow
           user-initialization of data members of T.*/
        static_assert(std::is_trivially_copyable<T>::value && std::is_standard_layout<T>::value);
        SavePodType(&stream, *static_cast<const T*>(this));
    }
    void Load(IInputStream& stream) override {
        LoadPodType(&stream, *static_cast<T*>(this));
    }
};

class TSaveablesRegistry
{
public:
    static TSaveablesRegistry* Get()
    {
        return Singleton<TSaveablesRegistry>();
    }

    template<class T>
    void RegisterOrdered(T* obj) {
        static_assert(!std::is_base_of<TSaveableBase, T>::value, "Objects derived from TSaveable are saved automatically");
        OrderedSaveables.emplace_back(std::type_index(typeid(T)), obj);
    }

    template<template<class T> class TW, class T>
    void Register(TW<T>* obj) {
        const auto idx = std::type_index(typeid(T));
        if (UnorderedSaveables.contains(idx)) {
            ythrow yexception() << "Sorry, only one object of each type can be saved";
        }
        UnorderedSaveables[idx] = obj;
    }

    void SaveAll(IOutputStream& stream) const {
        for (auto& desc : OrderedSaveables) {
            ::Save(&stream, TString(desc.first.name()));
            desc.second->Save(stream);
        }
        for (auto& desc : UnorderedSaveables) {
            ::Save(&stream, TString(desc.first.name()));
            desc.second->Save(stream);
        }
    }

    void LoadAll(IInputStream& stream) {
        for (auto& desc : OrderedSaveables) {
            LoadObjCheckType(stream, desc);
        }
        for (auto& desc : UnorderedSaveables) {
            LoadObjCheckType(stream, desc);
        }
    }

private:
    template<class TDesc>
    void LoadObjCheckType(IInputStream& stream, TDesc& desc) {
        TString name;
        ::Load(&stream, name);
        if (name != desc.first.name()) {
            ythrow yexception() << "Unexpected type name " << name << ", expected " << desc.first.name();
        }
        desc.second->Load(stream);
    }

    TVector<std::pair<std::type_index, ISerializableForJob*>> OrderedSaveables;

    // These are kind of unordered, but are currently expected to be
    // of the same order when the executable is the same.
    TMap<std::type_index, ISerializableForJob*> UnorderedSaveables;
};

template<class T>
inline TSaveable<T>::TSaveable() {
    TSaveablesRegistry::Get()->Register(this);
}

template<class T>
inline TSaveablePOD<T>::TSaveablePOD() {
    TSaveablesRegistry::Get()->Register(this);
}

template<class T>
inline void RegisterGlobalSaveable(T& objRef) {
    static_assert(std::is_base_of<ISerializableForJob, T>::value, "Object must derive ISerializableForJob");
    TSaveablesRegistry::Get()->RegisterOrdered(&objRef);
}

// ==============================================
} // namespace NYT
// ==============================================
