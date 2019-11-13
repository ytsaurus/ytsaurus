// Package guid implements YT flavor of uuid-s.
//
// Unfortunately YT uses non standard text representation. Because of this we can't use gofrs/uuid directly.
package guid

import (
	"encoding/binary"
	"fmt"

	"github.com/gofrs/uuid"
	"golang.org/x/xerrors"

	"a.yandex-team.ru/yt/go/yson"
)

type GUID uuid.UUID

var _ yson.StreamMarshaler = GUID{}

var _ yson.Unmarshaler = &GUID{}

func (g GUID) Parts() (a, b, c, d uint32) {
	a = uint32(g[0]) | (uint32(g[1]) << 8) | (uint32(g[2]) << 16) | (uint32(g[3]) << 24)
	b = uint32(g[4]) | (uint32(g[5]) << 8) | (uint32(g[6]) << 16) | (uint32(g[7]) << 24)
	c = uint32(g[8]) | (uint32(g[9]) << 8) | (uint32(g[10]) << 16) | (uint32(g[11]) << 24)
	d = uint32(g[12]) | (uint32(g[13]) << 8) | (uint32(g[14]) << 16) | (uint32(g[15]) << 24)
	return
}

func (g GUID) Halves() (a, b uint64) {
	a = binary.LittleEndian.Uint64(g[0:8])
	b = binary.LittleEndian.Uint64(g[8:16])
	return
}

func FromParts(a, b, c, d uint32) (g GUID) {
	g[0] = byte(a)
	g[1] = byte(a >> 8)
	g[2] = byte(a >> 16)
	g[3] = byte(a >> 24)

	g[4] = byte(b)
	g[5] = byte(b >> 8)
	g[6] = byte(b >> 16)
	g[7] = byte(b >> 24)

	g[8] = byte(c)
	g[9] = byte(c >> 8)
	g[10] = byte(c >> 16)
	g[11] = byte(c >> 24)

	g[12] = byte(d)
	g[13] = byte(d >> 8)
	g[14] = byte(d >> 16)
	g[15] = byte(d >> 24)

	return
}

func FromHalves(a, b uint64) (g GUID) {
	binary.LittleEndian.PutUint64(g[0:8], a)
	binary.LittleEndian.PutUint64(g[8:16], b)
	return
}

const format = "%x-%x-%x-%x"

func (g GUID) String() string {
	a, b, c, d := g.Parts()
	return fmt.Sprintf(format, a, b, c, d)
}

func ParseString(s string) (g GUID, err error) {
	var a, b, c, d uint32

	var n int
	n, err = fmt.Sscanf(s, format, &a, &b, &c, &d)
	if err != nil {
		err = xerrors.Errorf("invalid GUID format: %v", err)
		return
	}
	if n != 4 {
		err = xerrors.Errorf("invalid GUID format")
		return
	}

	g = FromParts(a, b, c, d)
	return
}

func (g GUID) MarshalText() ([]byte, error) {
	return []byte(g.String()), nil
}

func (g GUID) MarshalYSON(w *yson.Writer) error {
	w.String(g.String())
	return nil
}

func (g *GUID) UnmarshalText(data []byte) (err error) {
	if *g, err = ParseString(string(data)); err != nil {
		return err
	}

	return nil
}

func (g *GUID) UnmarshalYSON(data []byte) (err error) {
	var value string
	if err := yson.Unmarshal(data, &value); err != nil {
		return err
	}

	if *g, err = ParseString(value); err != nil {
		return err
	}

	return nil
}

func New() GUID {
	guid, err := uuid.NewV4()
	if err != nil {
		panic(fmt.Sprintf("failed to generate uuid: %+v", err))
	}

	return GUID(guid)
}
