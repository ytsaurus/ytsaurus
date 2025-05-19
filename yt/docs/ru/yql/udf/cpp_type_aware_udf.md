### Type-aware UDF-функции

Type awareness реализована с помощью аргумента ```TType* userType``` методов ```DeclareSignature(...)``` и ```buildReturnSignature(...)```. Этот аргумент позволяет посмотреть, с какими аргументами была вызвана UDF-функция из YQL, и построить нужную сигнатуру. Пользовательский тип можно распарсить с помощью ```ITypeInfoHelper::TPtr```, который принимает в конструкторе ```builder.TypeInfoHelper()``` , из которого в свою очередь конструируются TypeInspector'ы: ```TTupleTypeInspector```, ```TCallableTypeInspector```, ```TOptionalTypeInspector``` и др.

{% note warning %}

Для того чтобы движок YQL передавал на вход ```DeclareSignature()``` не пустой userType, класс вашей UDF должен содержать определение подтипа:

```
typedef bool TTypeAwareMarker;
```

{% endnote %}

С помощью таких функций можно:
* Поддержать опциональные или дефолтные аргументы:

  ```SomeTypeAwareUdfFunc(Double, Double) -> Bool```

  ```SomeTypeAwareUdfFunc(Double, Double, Double) -> Bool```

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

* Поддержать перегрузку:

  ```SomeTypeAwareUdfFunc(List<Int>) -> String```

  ```SomeTypeAwareUdfFunc(Stream<Int>) -> String```

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

* Поддержать изменение типа ответа по возвращаемому типу пользовательского колбэка:

  ```SomeTypeAwareUdfFunc(Callable<...>->Int)->List<Int>```

  ```SomeTypeAwareUdfFunc(Callable<...>->Double)->List<Double>```

  ```SomeTypeAwareUdfFunc(Callable<...>->CallbackReturnType)->List<CallbackReturnType>```

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
