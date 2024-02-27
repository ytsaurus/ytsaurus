// Package guid implements YT flavor of uuid-s.
//
// Unfortunately YT uses non standard text representation. Because of this we can't use gofrs/uuid directly.
package guid

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/gofrs/uuid"
	"golang.org/x/xerrors"

	"go.ytsaurus.tech/yt/go/yson"
)

// GUID is 16-byte value.
type GUID uuid.UUID

var _ yson.StreamMarshaler = GUID{}

var _ yson.Unmarshaler = &GUID{}

func (g GUID) Parts() (a, b, c, d uint32) {
	a = binary.LittleEndian.Uint32(g[0:4])
	b = binary.LittleEndian.Uint32(g[4:8])
	c = binary.LittleEndian.Uint32(g[8:12])
	d = binary.LittleEndian.Uint32(g[12:16])
	return
}

func (g GUID) Halves() (a, b uint64) {
	a = binary.LittleEndian.Uint64(g[0:8])
	b = binary.LittleEndian.Uint64(g[8:16])
	return
}

func FromParts(a, b, c, d uint32) (g GUID) {
	binary.LittleEndian.PutUint32(g[0:4], a)
	binary.LittleEndian.PutUint32(g[4:8], b)
	binary.LittleEndian.PutUint32(g[8:12], c)
	binary.LittleEndian.PutUint32(g[12:16], d)
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
	return fmt.Sprintf(format, d, c, b, a)
}

func (g GUID) HexString() string {
	for i := 0; i < 8; i++ {
		tmp := g[i]
		g[i] = g[15-i]
		g[15-i] = tmp
	}
	return hex.EncodeToString(g[:])
}

func ParseString(s string) (g GUID, err error) {
	var a, b, c, d uint32

	var n int
	n, err = fmt.Sscanf(s, format, &d, &c, &b, &a)
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
