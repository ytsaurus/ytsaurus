// This package defines interface which provides fast and cryptographically secure authorization tickets: https://docs.yandex-team.ru/tvm/.
//
// Encoded ticket is a valid ASCII string: [0-9a-zA-Z_-:]+.
//
// This package defines interface. All libraries should depend on this package.
// Pure Go implementations of interface is located in library/go/yandex/tvm/tvmtool.
// CGO implementation is located in library/go/yandex/tvm/tvmauth
package tvm

import (
	"fmt"
	"strings"

	"go.ytsaurus.tech/library/go/core/xerrors"
)

// ClientID represents ID of the application. Another name - TvmID.
type ClientID uint32

// UID represents ID of the user in Passport.
type UID uint64

// PorgID represents ID of the porganization
type PorgID uint64

// MUid represents ID of the user's master account in Passport
type MUid uint64

// Locatuin represents identifier of user's location (ru, eu, etc)
type Location uint16

// DeviceType represents the user's device type
type DeviceType uint64

// BlackboxEnv describes environment of Passport: https://docs.yandex-team.ru/blackbox/concepts/getting-access#okruzheniya
type BlackboxEnv int

type UserExtFields struct {
	UID           UID
	CurrentPorgID PorgID
	CurrentMUid   MUid
}

type KeyPair struct {
	Public  string
	Private string
}

// This constants must be in sync with EBlackboxEnv from library/cpp/tvmauth/checked_user_ticket.h
const (
	BlackboxProd BlackboxEnv = iota
	BlackboxTest
	BlackboxProdYateam
	BlackboxTestYateam
	BlackboxStress
)

func (e BlackboxEnv) String() string {
	switch e {
	case BlackboxProd:
		return "Prod"
	case BlackboxTest:
		return "Test"
	case BlackboxProdYateam:
		return "ProdYateam"
	case BlackboxTestYateam:
		return "TestYateam"
	case BlackboxStress:
		return "Stress"
	default:
		return fmt.Sprintf("Unknown%d", e)
	}
}

func BlackboxEnvFromString(envStr string) (BlackboxEnv, error) {
	switch strings.ToLower(envStr) {
	case "prod":
		return BlackboxProd, nil
	case "test":
		return BlackboxTest, nil
	case "prodyateam", "prod_yateam":
		return BlackboxProdYateam, nil
	case "testyateam", "test_yateam":
		return BlackboxTestYateam, nil
	case "stress":
		return BlackboxStress, nil
	default:
		return BlackboxEnv(-1), xerrors.Errorf("blackbox env is unknown: '%s'", envStr)
	}
}

type TicketStatus int

// This constants must be in sync with EStatus from library/cpp/tvmauth/ticket_status.h
const (
	TicketOk TicketStatus = iota
	TicketExpired
	TicketInvalidBlackboxEnv
	TicketInvalidDst
	TicketInvalidTicketType
	TicketMalformed
	TicketMissingKey
	TicketSignBroken
	TicketUnsupportedVersion
	TicketNoRoles
	TicketMissingUidSignKey
	TicketUidOutsideInterval
	TicketUidSignatureBroken
	TicketUidSignatureExpired
	UntrustedKey

	// Go-only statuses below
	TicketStatusOther
	TicketInvalidScopes
	TicketInvalidSrcID
)

func (s TicketStatus) String() string {
	switch s {
	case TicketOk:
		return "Ok"
	case TicketExpired:
		return "Expired"
	case TicketInvalidBlackboxEnv:
		return "InvalidBlackboxEnv"
	case TicketInvalidDst:
		return "InvalidDst"
	case TicketInvalidTicketType:
		return "InvalidTicketType"
	case TicketMalformed:
		return "Malformed"
	case TicketMissingKey:
		return "MissingKey"
	case TicketMissingUidSignKey:
		return "MissingUidSignKey"
	case TicketSignBroken:
		return "SignBroken"
	case TicketUidOutsideInterval:
		return "UidOutsideInterval"
	case TicketUidSignatureBroken:
		return "UidSignatureBroken"
	case TicketUidSignatureExpired:
		return "UidSignatureExpired"
	case TicketUnsupportedVersion:
		return "UnsupportedVersion"
	case UntrustedKey:
		return "UntrustedKey"
	case TicketNoRoles:
		return "NoRoles"
	case TicketStatusOther:
		return "Other"
	case TicketInvalidScopes:
		return "InvalidScopes"
	case TicketInvalidSrcID:
		return "InvalidSrcID"
	default:
		return fmt.Sprintf("Unknown%d", s)
	}
}

type KeyType int

const (
	Trusted KeyType = iota
	Untrusted
)
