### Коротко о UDF-функциях в C++

Внутри cpp-кода UDF-функция - это просто класс-наследник `TBoxedValue` с обязательным методом `Run()`.

Для реализации конкретной UDF-функции внутри UDF-модуля удобно использовать класс с 3 переопределёнными функциями: `Name()`, `DeclareSignature(...)`, `Run(...)` - которые будут вызываться внутри модуля в нужные моменты.

Пример класса UDF-модуля:
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

Описание методов:
* `static const TStringRef& Name()`

  Метод, возвращающий имя функции, которое будет использоваться в YQL. В данной реализации этот метод используется для добавления функции в UDF-модуль.

* `static bool DeclareSignature(bool typesOnly, IFunctionTypeInfoBuilder& builder [, TType* userType])`

  Метод, объявляющий сигнатуру функции. Используется в методе ```BuildFunctionTypeInfo``` UDF-модуля для получения информации об индексах полей структур, которые используются внутри класса, и создания экземляра конкретной UDF-функции.

* `TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const override`

  Метод, реализующий всю внутреннюю логику UDF-функции. Когда вызывается UDF-функция из YQL, именно этот метод и будет вызван.

Пример класса UDF-функции:
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
