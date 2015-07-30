LIBRARY()

OWNER(monster)

SRCS(
    zigzag.h
    lexer.cpp
    token.cpp
    tokenizer.cpp
    consumer.cpp
    parser.cpp
    writer.cpp
    json_writer.cpp
)

PEERDIR(
    util
    library/json
)

END()
