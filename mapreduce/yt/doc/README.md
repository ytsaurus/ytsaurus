# Генерирование документации
Скрипт `doxyt` генерирует документацию для YT Wrapper.  Для его работы, на машине должен быть установлен `doxygen`.

Чтобы получить архив с документацией используется

    ./doxyt

Чтобы поднять локальный web сервер с документацией используется

    ./doxyt serve

# Написание документации
Мы пишем документацию на английском языке используя doxygen комментарии.

Мы используем только `///` для doxygen комментариев.

Мы используем только `@` для команд, например `@brief` (не `\brief`).

Функции, которые более-менее очевидны и могут быть задокументированы одной строкой, мы документируем так:

    /// Add two integers.
    int Sum(int, int);

Функции, которые требуют более детальной документации, документируются так:

    ///
    /// @brief Add two integers.
    ///
    /// Invoke mathematical operation of addition to compute an integer.
    /// This function is thread safe.
    ///
    /// @return result of computation
    int Sum(int, int);

Для дополнительной разметки (ссылки, выделения и т.п.) используем [Markdown](https://daringfireball.net/projects/markdown/syntax).

## Основные doxygen команды
[Здесь](http://www.doxygen.nl/manual/commands.html) живёт документация по командам Doxygen.

Самые нужные нам команды такие:
  - @brief :: краткое описание документируемой сущности;
  - @ref :: ссылка на какую-то другую задокументированную сущность, нужно использовать полное имя с namespace
      и для функций добавлять скобки; примеры `@ref NYT::IClient` `@ref NYT::CreateClient()`;
  - @file :: секция с описанием файла, обратите внимание, что doxygen не будет генерировать документацию для глобальных функций,
      если файл не задокументирован.

# Настройка редактора
## vim
Чтобы удобнее работать с doxygen комменатриями в `vim` можно добавить следующие строки в `.vimrc`

    autocmd FileType c,cpp set comments-=://
    autocmd FileType c,cpp set comments+=:///
    autocmd FileType c,cpp set comments+=://

