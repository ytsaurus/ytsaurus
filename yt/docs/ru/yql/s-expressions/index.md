# S-expressions синтаксис YQL (sYQL)

## Введение

YQL — высокоуровневый язык для работы с данными. Имеет два вида синтаксиса — SQL-подобный и s-expressions (a la Lisp). Данная статья посвящена более низкоуровневому синтаксису s-expressions. Об SQL-синтаксисе и использовании YQL в целом [см. основное руководство пользователя](https://yql.yandex-team.ru/docs/yt/syntax).

## Структура AST

{% cut "Структура" %}

```
**AST** := **AST_ATOM**
  | **AST_LIST**
  | **AST_SHORT_QUOTE**

**AST_ATOM** := {любые символы — CONTROL_SYMBOLS To— WHITE_SPACE}
  | ", {строка символов с поддержкой escape-последовательностей — "}, "
  | x", {шестнадцатеричная строка}, "
  | @@, {длинная строка}, @@

AST_SHORT_QUOTE something автоматически преобразуется в:
AST_LIST (AST_ATOM("quote") something)

**AST_SHORT_QUOTE** := ' :: **AST** — символ апострофа

**AST_LIST** := ( :: **AST*** :: )

**COMMENT** := # :: {любые символы} :: \n

**WHITE_SPACE** := " " | \t | \n | \r

**CONTROL_SYMBOLS** := ( | ) | # | ' | "
```


{% endcut %}

{% cut "Escape-последовательности" %}


| \" | двойные кавычки |
| --- | --- |
| \\ | обратный слеш |
| \n | новая строка |
| \r | возврат каретки |
| \t | горизонтальная табуляция |
| \v | вертикальная табуляция |
| \b | удаление предыдущего символа |
| \f | подача страницы |
| \a | звуковой сигнал |
| \ooo | Восьмеричное представление байта. Всегда должны быть указаны три цифры от 0 до 7. |
| \xhh | Шестнадцатеричное представление байта. Всегда должно быть указано две цифры от 0 до f (регистр не важен). |
| \uxxxx | Юникод (конвертируется в UTF-8 последовательность). Всегда должно быть указано 4 цифры. |
| \Uxxxxxxxx | Юникод (конвертируется в UTF-8 последовательность). Всегда должно быть указано 8 цифр. |


{% endcut %}

{% cut "Длинные строки" %}


В длинных строках escape-последовательности не интерпретируются. Для того чтобы в длинной строке использовать последовательность '@@' с четным количеством символов '@', ее нужно повторить. Для нечетного количества символов '@' повторение не требуется.
Примеры:

| # | Запись в длинной строке | Интерпретируется как |
| --- | --- | --- |
| 1 | @ | @ |
| 2 | @@ | конец строки |
| 3 | @@@ | @@@ |
| 4 | @@@@ | @@ |
| 5 | @@@@@ | @@@@@ |
| 6 | @@@@@@ | @@@ |
| ... | ... | ... |


{% endcut %}

{% cut "Примеры" %}


``` lisp
a
'a
()
(a b)
(a 'b)
'()
'(a '(b c))
"string \\ \"with\" \n escape \t sequences"
"\u043F\u0440\u0438\u0432\u0435\u0442" # то же самое, что и "привет"
x"0123456789abcdef"                    # то же самое, что и "\x01\x23\x45\x67\x89\xAB\xCD\xEF"
@@Lorem ipsum
dolor sit amet, consectetur adipiscing elit,
sed do eiusmod tempor incididunt ut labore et
dolore magna aliqua. Ut enim ad minim veniam,
quis nostrud exercitation ullamco laboris nisi
ut aliquip ex ea commodo consequat.
@@
```


{% endcut %}

### Структура программы
{% cut "Структура" %}


**PROGRAM** := **BLOCK_BODY**
**BLOCK_BODY** := **AST_LIST** (**LET** * :: **RETURN**)
**LET** := **AST_LIST** (**AST_ATOM**("let") :: **AST_ATOM**(name) :: **VALUE**)
**RETURN** := **AST_LIST** (**AST_ATOM**("return") :: **VALUE**)
**VALUE** := **ATOM_VALUE** | **NAME_VALUE** | **LIST_VALUE** | **FUNC** | **LAMBDA** | **BLOCK**
**ATOM_VALUE** := **AST_LIST** (**AST_ATOM**("quote") :: **AST_ATOM**(value)) — значение атома value используется как содержимое значения
**NAME_VALUE** := **AST_ATOM**(value) — значение атома value используется как имя из **LET**
**LIST_VALUE** := **AST_LIST** (**AST_ATOM**("quote") :: **AST_LIST** (**VALUE** *))
**FUNC** := **AST_LIST** (**AST_ATOM**(funcName) :: **VALUE** *)
**BLOCK** := **AST_LIST** (**AST_ATOM**("block") :: **AST_LIST**(**AST_ATOM**("quote") :: **BLOCK_BODY**) )
**LAMBDA** := **AST_LIST** (**AST_ATOM**("lambda") :: **LAMBDA_ARGUMENTS** :: **LAMBDA_BODY**)
**LAMBDA_ARGUMENTS** := **AST_LIST**(**AST_ATOM**("quote") :: **AST_LIST**(**ARGUMENT** *))
**ARGUMENT** := **AST_ATOM**(name) — имя аргумента, доступно из тела **LAMBDA_BODY**, скрывает вышестоящие имена.
**LAMBDA_BODY** := **VALUE**

Есть предопределенное имя world при начале разбора программы.


{% endcut %}

{% cut "Примеры" %}


``` lisp
(
(return world)
)

(
(let x 'y)
(return x)
)

(
(let x (f 'a 'b))
(let y (g x '()))
(return y)
)

(
(let sum (lambda '(x y) (+ x y))
(return sum)
)

(
(let x (Int32 '1))
(let z (block '(
  (let y (+ x (Int32 '2)))
  (return y)
)))
(return z)
)

```


{% endcut %}
