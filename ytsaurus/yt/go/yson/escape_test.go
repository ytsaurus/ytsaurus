package yson

import (
	"encoding/binary"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testData = []struct {
	escaped, raw string
}{
	{"http://ya.ru/", "http://ya.ru/"},
	{"http://ya.ru/\x17\\n", "http://ya.ru/\x17\n"},

	{"http://ya.ru/\\0", "http://ya.ru/" + string([]byte{0})},
	{"http://ya.ru/\\0\\0", "http://ya.ru/" + string([]byte{0, 0})},
	{"http://ya.ru/\\0\\0000", "http://ya.ru/" + string([]byte{0, 0}) + "0"},
	{"http://ya.ru/\\0\\0001", "http://ya.ru/" + string([]byte{0}) + "\x00" + "1"},

	{"\\2\\4\\00678", string([]byte{2, 4, 6}) + "78"},
	{"\\2\\4\\689", string([]byte{2, 4, 6}) + "89"},

	{"\\\"Hello\\\", Alice said.", "\"Hello\", Alice said."},
	{"Slash\\\\dash!", "Slash\\dash!"},
	{"There\\nare\\r\\nnewlines.", "There\nare\r\nnewlines."},
	{"There\\tare\\ttabs.", "There\tare\ttabs."},

	{"русский текст", "русский текст"},
}

func TestEscape(t *testing.T) {
	for _, testCase := range testData {
		assert.Equal(t, []byte(testCase.escaped), escapeC([]byte(testCase.raw)))

		assert.Equal(t, []byte(testCase.raw), unescapeC([]byte(testCase.escaped)))
	}

	assert.Equal(t, []byte("http://ya.ru/\x17\\n\\xAB"), escapeC([]byte("http://ya.ru/\x17\n\xab")))
	assert.Equal(t, []byte("http://ya.ru/\x17\n\xab"), unescapeC([]byte("http://ya.ru/\\x17\\n\\xAB")))

	assert.Equal(t, []byte("h"), escapeC([]byte("h")))
	assert.Equal(t, []byte("h"), unescapeC([]byte("h")))

	assert.Equal(t, []byte("\\xFF"), escapeC([]byte("\xFF")))
	assert.Equal(t, []byte("\xFF"), unescapeC([]byte("\\xFF")))

	assert.Equal(t, []byte("\\377f"), escapeC([]byte("\xff"+"f")))
	assert.Equal(t, []byte("\xff"+"f"), unescapeC([]byte("\\377f")))

	assert.Equal(t, []byte("\\xFFg"), escapeC([]byte("\xff"+"g")))
	assert.Equal(t, []byte("\xff"+"g"), unescapeC([]byte("\\xFFg")))

	assert.Equal(t, []byte("\xEA\x9A\x96"), unescapeC([]byte("\\uA696")))

	assert.Equal(t, []byte("Странный компроматтест"), unescapeC([]byte("\\u0421\\u0442\\u0440\\u0430\\u043d\\u043d\\u044b\\u0439 \\u043a\\u043e\\u043c\\u043f\\u0440\\u043e\\u043c\\u0430\\u0442тест")))
}

func TestEscapeGoCompat(t *testing.T) {
	for i := 0; i < 1<<16; i++ {
		var x [2]byte
		binary.LittleEndian.PutUint16(x[:], uint16(i))

		q := strconv.Quote(string(x[:]))
		q = q[1 : len(q)-1]

		u := unescapeC([]byte(q))

		require.Equalf(t, u, x[:], "%s in:%q out:%q", q, x[:], u)
	}

	for i := 0; i < 1<<20; i++ {
		r := rune(i)

		x := []byte(string([]rune{r}))

		q := strconv.Quote(string(x[:]))
		q = q[1 : len(q)-1]

		u := unescapeC([]byte(q))

		require.Equalf(t, u, x, "%s in:%q out:%q", q, x, u)
	}
}
