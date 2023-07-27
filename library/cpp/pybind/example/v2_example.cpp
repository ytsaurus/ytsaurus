#include <library/cpp/pybind/v2.h>

//
// Sample c++ module with classes, functions, etc
//

bool TrueFunc() {
    return true;
}

struct TModule {
    static TString GetName() {
        return TModule::MyName;
    }

    static TString SetName(const TString& name) {
        TModule::MyName = name;
        return name;
    }

    static TString MyName;
};

TString TModule::MyName;

struct TSimpleExportStruct {
    int Field1 = -1;
    TString Field2;
};

class TComplexClass {
public:
    TComplexClass(const TString&) {
    }

    TSimpleExportStruct Get(int f) {
        return {f, ""};
    }

    TSimpleExportStruct Get2(TString f) {
        return {0, f};
    }

    TString GetName() {
        return MyName;
    }

    TString SetName(const TString& name) {
        MyName = name;
        return name;
    }

    void FromStruct(const TSimpleExportStruct* x) {
        MyName = x->Field2;
    }

    void DoNothing() {
    }

    void ThrowException0() {
        throw std::runtime_error("try to catch me");
    }

    int ThrowException1(int) {
        throw std::runtime_error("catch 1 arg");
    }

    const TVector<int>& GetList() const {
        return List;
    }

    TVector<int> List= {0,1,2,3};
    TString MyName;
};

struct TDerivedExportStruct : public TSimpleExportStruct{
    TString NewField = "aba";
};

//
// Start export code
//

auto export_simple() {
    // Simple can't be crearted from python
    return NPyBind::TPyClass<TSimpleExportStruct, NPyBind::TPyClassConfigTraits<true>>("simple")
        .Def("f", &TSimpleExportStruct::Field1)
        .Def("f2", &TSimpleExportStruct::Field2)
        .Complete();
}


// Define if you want to return TSimpleExportStruct from functions besides consturctor
DEFINE_CONVERTERS(export_simple);

// Define if you want to use pointer to TSimpleExportStruct as argument
DEFINE_TRANSFORMERS(export_simple);


auto export_derived() {
    std::function<TString(TDerivedExportStruct&)> v = [](TDerivedExportStruct&) -> TString{ return "aaa";};
    return NPyBind::TPyClass<TDerivedExportStruct, NPyBind::TPyClassConfigTraits<true, TSimpleExportStruct>>("derived")
        .Def("newField", &TDerivedExportStruct::NewField)
        .AsPropertyByFunc("lambda_field", [](const TDerivedExportStruct&) { return 1;})
        .DefByFunc("lambda_method", std::function<TString(TDerivedExportStruct&)>([](TDerivedExportStruct&) -> TString {return "abacaba";}))
        .DefByFunc("lambda_method2", std::function<bool(PyObject*, TDerivedExportStruct*, PyObject*, PyObject*, PyObject*&)>([](PyObject*, TDerivedExportStruct*, PyObject*, PyObject*, PyObject*& res) -> bool {
            res = Py_True;
            return true;
        }))
        .Complete();
}

template<class T>
class TSimpleIterator
{
public:
    TSimpleIterator() = default;
    TSimpleIterator(T begin, T end)
        : Iter(MakeHolder<T>(begin))
        , End(MakeHolder<T>(end))
    {}
    TSimpleIterator(TSimpleIterator<T>&& other) = default;
    TSimpleIterator& operator=(TSimpleIterator<T>&& other) = default;
    TSimpleIterator(const TSimpleIterator<T>& other) {
        if (other.Iter) {
            Iter = MakeHolder<T>(*other.Iter);
        }
        if (other.End) {
            End = MakeHolder<T>(*other.Iter);
        }
    }

    TSimpleIterator<T>& operator=(const TSimpleIterator<T>& other) {
        TSimpleIterator<T>(other).Swap(*this);
        return *this;
    }

    typename std::iterator_traits<T>::value_type GetNext() const {
        if (*Iter == *End) {
            PyErr_SetNone(PyExc_StopIteration);
            ythrow yexception() << "Stop iteration" << Endl;
        } else {
            auto res = **Iter;
            ++(*Iter);
            return res;
        }
    }

    void Swap(TSimpleIterator<T>& other) {
        Iter.Swap(other.Iter);
        End.Swap(other.End);
    }

private:
    mutable THolder<T> Iter;
    THolder<T> End;
};

template<class T>
using TIteratorPyClass = NPyBind::TPyClass<TSimpleIterator<T>, NPyBind::TPyClassConfigTraits<true>>;

auto export_iterator() {
    using TIterator = TVector<int>::const_iterator;
    return TIteratorPyClass<TIterator>("iterator_t")
        .Def("__next__", &TSimpleIterator<TIterator>::GetNext)
        .Def("next", &TSimpleIterator<TIterator>::GetNext)
        .DefByFunc("__iter__", std::function<bool(PyObject*, TSimpleIterator<TIterator>*, PyObject*, PyObject*, PyObject*&)>(
                [](PyObject* owner, TSimpleIterator<TIterator>*, PyObject*, PyObject*, PyObject*& res) {
                    res = owner;
                    Py_XINCREF(owner);
                    return true;
                }))
        .Complete();
}

DEFINE_CONVERTERS(export_iterator);

PyObject* GetIterator(const TVector<int>& x) {
    using TIterator =  TVector<int>::const_iterator;
    TSimpleIterator<TIterator> it(x.begin(), x.end());
    NPyBind::TPyObjectPtr ptr(BuildPyObject(std::move(it)));
    return PyObject_GetIter(ptr.Get());
}

// Define if you want to return TSimpleExportStruct from functions besides consturctor
DEFINE_CONVERTERS(export_derived);
// Define if you want to use pointer to TSimpleExportStruct as argument
DEFINE_TRANSFORMERS(export_derived);
void export_complex() {
    // Complex can be created from python with const TString& as parameter
    NPyBind::TPyClass<TComplexClass, NPyBind::TPyClassConfigTraits<true>, const TString&>("complex")
        .Def("get", &TComplexClass::Get)
        .Def("get", &TComplexClass::Get2)
        .Def("set_name", &TComplexClass::SetName)
        .Def("get_name", &TComplexClass::GetName)
        .Def("do_nothing", &TComplexClass::DoNothing)
        .Def("throw0", &TComplexClass::ThrowException0)
        .Def("throw1", &TComplexClass::ThrowException1)
        .Def("from_struct", &TComplexClass::FromStruct)
        .DefByFunc("get_iterator", std::function<PyObject*(TComplexClass&)>([](TComplexClass& self) {
            return GetIterator(self.GetList());
        }))
        .Complete();
}


class TSimpleClass  {
public:
    int GetF() const {
        return -1;
    }
};

class TDerivedClass : public TSimpleClass {
public:
    int GetF() const {
        return 0;
    }
};

auto export_simple_class() {
    return NPyBind::TPyClass<TSimpleClass, NPyBind::TPyClassConfigTraits<true>>("simple_class")
        .Def("get_f", &TSimpleClass::GetF)
        .Complete();
}

auto export_derived_class() {
    return NPyBind::TPyClass<TDerivedClass, NPyBind::TPyClassConfigTraits<true, TSimpleClass>>("derived_class")
        .Def("get_f", &TDerivedClass::GetF)
        .Complete();
}

class TRawInitClass {
public:
    TRawInitClass(PyObject* args, PyObject* kwargs) {
        static const char* kwlist[] = {"a", "b", NULL};
        char* bData = nullptr;
        Py_ssize_t bLen = 0;
        auto res = PyArg_ParseTupleAndKeywords(args, kwargs, "|Ks#", (char**)kwlist, &A, &bData, &bLen);
        if (!res) {
            ythrow yexception() << "Can not parse arguments";
        }
        if (bData && bLen) {
            B = TString{bData, static_cast<size_t>(bLen)};
        }
    }
    ui64 GetA() const { return A; }
    const TString& GetB() const { return B; }
private:
    ui64 A = 0;
    TString B = "";
};

auto export_raw_init_class() {
    return NPyBind::TPyClass<TRawInitClass, NPyBind::TPyClassRawInitConfigTraits<>>("raw_init_class")
        .AsProperty("a", &TRawInitClass::GetA)
        .AsProperty("b", &TRawInitClass::GetB)
        .Complete();
}

void DoInitExample() {
    DefFunc("true_func", &TrueFunc);
    DefFunc("get_name", &TModule::GetName);
    DefFunc("set_name", &TModule::SetName);

    NPyBind::TPyModuleDefinition::InitModule("pybindexample");
    export_simple();
    export_complex();
    export_derived();
    export_simple_class();
    export_derived_class();
    export_iterator();
    export_raw_init_class();
}
