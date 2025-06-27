### Type-aware UDFs

Type awareness is implemented using the `TType* userType` argument of the `DeclareSignature(...)` and `buildReturnSignature(...)` methods. You can use this argument to see with which arguments a UDF function was called from YQL and build the desired signature. A user-defined type can be parsed using the `ITypeInfoHelper::TPtr` object, which is returned by the `builder.TypeInfoHelper()` method and which can be used to create TypeInspectors, such as `TTupleTypeInspector`, `TCallableTypeInspector`, `TOptionalTypeInspector`, and others.

{% note warning %}

For the YQL engine to pass a non-empty userType to `DeclareSignature()` as input, the class of your UDF must contain a subtype definition:

```
typedef bool TTypeAwareMarker;
```

{% endnote %}

Using such functions, you can:
* Support optional or default arguments:

  `SomeTypeAwareUdfFunc(Double, Double) -> Bool`

  `SomeTypeAwareUdfFunc(Double, Double, Double) -> Bool`

  ```cpp
  TMemberIndices TSomeTypeAwareUdfFunc::buildReturnSignature(IFunctionTypeInfoBuilder& builder, TType* userType) {
      ...
      auto userTypeInspector = TTupleTypeInspector(*typeHelper, userType);
      auto argsTypeTuple = userTypeInspector.GetElementType(0);
      auto argsTypeInspector = TTupleTypeInspector(*typeHelper, argsTypeTuple);

      // Build right signature by number of arguments
      if (argsTypeInspector.GetElementsCount() == 2) {
          /* Build one signature */
          ...
      } else if (argsTypeInspector.GetElementsCount() == 3) {
          /* Build other signature */
          ...
      } else {
          ...
      }
  ...
  }

  TUnboxedValue TSomeTypeAwareUdfFunc::Run(IValueBuilder* valueBuilder, TUnboxedValuePod* args) {
      ...
      double value = 0.0;  // default
      if (/* hasThirdElement */) {
          value = args[2].Get<double>(); // non-default
      }
      ...
  }
  ```

* Support overloading:

  `SomeTypeAwareUdfFunc(List<Int>) -> String`

  `SomeTypeAwareUdfFunc(Stream<Int>) -> String`

  ```cpp
  TMemberIndices buildReturnSignature(IFunctionTypeInfoBuilder& builder, TType* userType) {
      ...
      auto userTypeInspector = TTupleTypeInspector(*typeHelper, userType);
      auto argsTypeTuple = userTypeInspector.GetElementType(0);
      auto argsTypeInspector = TTupleTypeInspector(*typeHelper, argsTypeTuple);

      auto typeKind = typeHelper->GetTypeKind(argsTypeInspector.GetElementType(0));
      if (typeKind == ETypeKind::List) {
          ...
      } else if (typeKind == ETypeKind::Stream) {
          ...
      } else {
          ...
      }
      ...
  }
  ```

* Support changing the response type depending on the type returned by the custom callback:

  `SomeTypeAwareUdfFunc(Callable<...>->Int)->List<Int>`

  `SomeTypeAwareUdfFunc(Callable<...>->Double)->List<Double>`

  `SomeTypeAwareUdfFunc(Callable<...>->CallbackReturnType)->List<CallbackReturnType>`

  ```cpp
  TMemberIndices buildReturnSignature(IFunctionTypeInfoBuilder& builder, TType* userType) {
      ...
      auto userTypeInspector = TTupleTypeInspector(*typeHelper, userType);
      auto argsTypeTuple = userTypeInspector.GetElementType(0);
      auto argsTypeInspector = TTupleTypeInspector(*typeHelper, argsTypeTuple);
      auto callbackInspector = TCallableTypeInspector(*typeHelper, argsTypeInspector.GetElementType(0));

      auto resultType = builder.List()->Item(callbackInspector.GetReturnType()).Build();
      builder.Returns(resultType).Args()->Add(argsTypeInspector.GetElementType(0)).Done();
      ...
  }
  ```
