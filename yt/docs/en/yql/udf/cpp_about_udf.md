### Overview of UDFs in C++

In C++ code, a UDF is simply a `TBoxedValue` subclass with the mandatory `Run()` method.

An easy way to implement a particular UDF inside a UDF module is to use a class with three overridden functions (`Name()`, `DeclareSignature(...)`, and `Run(...)`) to be called within the module as needed.

Example of a UDF module class:
```cpp
class TSomeYQLModule: public IUdfModule {
public:    
    TStringRef Name() const {
        return TStringRef::Of("SomeModule");
    }

    void GetAllFunctions(IFunctionsSink& sink) const final {
        sink.Add(TSomeUdfFunction::Name());
        sink.Add(TSomeTypeAwareUdfFunction::Name())->SetTypeAwareness();
    }

    void BuildFunctionTypeInfo(const TStringRef& name,
                               TType* userType,
                               const TStringRef& typeConfig,
                               ui32 flags,
                               IFunctionTypeInfoBuilder& builder) const override {
        try {
            Y_UNUSED(typeConfig);

            bool typesOnly = (flags & TFlags::TypesOnly);

            if (TSomeUdfFunction::Name() == name) {
                TSomeUdfFunction::DeclareSignature(typesOnly, builder);
            } else if (TSomeTypeAwareUdfFunction::Name() == name) {
                TSomeTypeAwareUdfFunction::DeclareSignature(
                        typesOnly, builder, userType);
            }                    
        } catch (const std::exception& e) {
            builder.SetError(CurrentExceptionMessage());
        }
    }
};
```

Method description:
* `static const TStringRef& Name()`

  A method that returns the function name to be used in YQL. In this implementation, the method is used to add a function to a UDF module.

* `static bool DeclareSignature(bool typesOnly, IFunctionTypeInfoBuilder& builder [, TType* userType])`

  A method that declares the function signature. It is used in the ```BuildFunctionTypeInfo``` method of a UDF module to get information about the indexes of structure fields used inside the class and to create an instance of a particular UDF.

* `TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const override`

  A method that implements all the internal logic of a UDF. When the UDF is called from YQL, it is this particular method that is called.

Example of a UDF class:
```cpp
class TMyUdfFunction : public ::NKikimr::NUdf::TBoxedValue {
public:
    // Each member struct is a C++ description of a YQL struct and has it's own
    // constructor that takes in a builder instance and initializes a proper
    // struct from it. If you want to build your own YQL struct and work with
    // it in C++, you will need to write the same kind of constructor. These
    // structs contain indices from the YQL runtime (in C++ you can't use field
    // names, only indices) and TType* ResultStructType (YQL description of the
    // struct), which is a description of the struct understandable for YQL and
    // will be used later when specifying the return type of the UDF.
    struct TMemberIndices {
        TMemberIndices(::NKikimr::NUdf::IFunctionTypeInfoBuilder& builder) {
            ...
        }
    };

    // Name() function, needed to register UDF in YQL, the function in YQL will
    // have the name we specify here. In our UDF Module class we have a
    // GetAllFunctions method, where we add all function names into our module.
    static const ::NKikimr::NUdf::TStringRef& Name() {
        static auto name = ::NKikimr::NUdf::TStringRef::Of("MyUdfFunction");
        return name;
    }

    // This function is also used in the UDF module class in the
    // BuildFunctionTypeInfo method. Here we use the passed in builder instance
    //      1) to take information about indices we need for our class to work
    //      2) to create an instance of this UDF function
    // This is the only function where we can grab info about indices for our
    // class constructor, thus this is the only place where constructor is
    // called
    static bool DeclareSignature(
            bool typesOnly, ::NKikimr::NUdf::IFunctionTypeInfoBuilder& builder,
            ::NKikimr::NUdf::TType* userType) {
        auto members = buildReturnSignature(builder, userType);
        // If typesOnly flag is specified, we don't need to register
        // implementation, just build the signature
        if (!typesOnly) {
            builder.Implementation(new TMyUdfFunction(members));
        }
        return true;
    }

    // Run() function is derived from TBoxedValue (and we need to override it)
    // and contains our UDF logic. (When we call it from YQL, this function is
    // what is gonna happen).
    // It always has signature
    // Run(IValueBuilder*, TUnboxedValuePod*) -> TUnboxedValue
    ::NKikimr::NUdf::TUnboxedValue Run(
            const ::NKikimr::NUdf::IValueBuilder* valueBuilder,
            const ::NKikimr::NUdf::TUnboxedValuePod* args) const override {
        ...
    }

private:
    TMemberIndices IndicesDescription;

    /// Constructor accepts YQL struct member descriptions and source code
    /// position, for logging.
    /// Instance of this class is created within DeclareSignature method,
    /// as that is the only method that knows YQL Struct member indices
    /// For the reasons above, it is made private
    TMyUdfFunction(const TMemberIndices& indicesDescription,
                   const ::NKikimr::NUdf::TSourcePosition& pos) {
        ...
    }

    /// This function will initialize proper signature on given builder.
    /// It will also return a struct with indices of YQL Struct members (in C++
    /// you have to access YQL Struct members by index, not by name)
    static TMemberIndices buildReturnSignature(
            ::NKikimr::NUdf::IFunctionTypeInfoBuilder& builder,
            ::NKikimr::NUdf::TType* userType) {
        ...
    }
};
```
