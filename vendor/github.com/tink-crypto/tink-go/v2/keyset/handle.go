// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package keyset

import (
	"context"
	"fmt"
	"slices"

	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"github.com/tink-crypto/tink-go/v2/internal/internalapi"
	"github.com/tink-crypto/tink-go/v2/internal/internalregistry"
	"github.com/tink-crypto/tink-go/v2/internal/monitoringutil"
	"github.com/tink-crypto/tink-go/v2/internal/primitiveset"
	"github.com/tink-crypto/tink-go/v2/internal/protoserialization"
	"github.com/tink-crypto/tink-go/v2/internal/registryconfig/legacyprimitive"
	"github.com/tink-crypto/tink-go/v2/key"
	"github.com/tink-crypto/tink-go/v2/monitoring"
	"github.com/tink-crypto/tink-go/v2/tink"

	tinkpb "github.com/tink-crypto/tink-go/v2/proto/tink_go_proto"
)

var errInvalidKeyset = fmt.Errorf("keyset.Handle: invalid keyset")

// Handle provides access to a keyset to limit the exposure of the internal
// keyset representation, which may hold sensitive key material.
type Handle struct {
	entries          []*Entry
	annotations      map[string]string
	primaryKeyEntry  *Entry
	monitoringClient monitoring.Client
}

// KeyStatus is the key status.
type KeyStatus int

const (
	// Unknown is the default invalid value.
	Unknown KeyStatus = iota
	// Enabled means the key is enabled.
	Enabled
	// Disabled means the key is disabled.
	Disabled
	// Destroyed means the key is marked for destruction.
	Destroyed
)

// String implements fmt.Stringer.
func (ks KeyStatus) String() string {
	switch ks {
	case Enabled:
		return "Enabled"
	case Disabled:
		return "Disabled"
	case Destroyed:
		return "Destroyed"
	default:
		return "Unknown"
	}
}

type keyExportLogger interface {
	LogKeyExport(keyID uint32)
}

// Entry represents an entry in a keyset.
type Entry struct {
	// Object that represents a full Tink key, i.e., key material, parameters and algorithm.
	key       key.Key
	isPrimary bool
	keyID     uint32
	status    KeyStatus
	logger    keyExportLogger
}

func newUnmonitoredEntry(key key.Key, isPrimary bool, keyID uint32, status KeyStatus) *Entry {
	return &Entry{
		key:       key,
		isPrimary: isPrimary,
		keyID:     keyID,
		status:    status,
		logger:    &monitoringutil.DoNothingLogger{},
	}
}

// Key returns the key.
func (e *Entry) Key() key.Key {
	e.logger.LogKeyExport(e.keyID)
	return e.key
}

// IsPrimary returns true if the key is the primary key.
func (e *Entry) IsPrimary() bool {
	return e.isPrimary
}

// KeyID returns the key ID.
func (e *Entry) KeyID() uint32 {
	return e.keyID
}

// KeyStatus returns the key status.
func (e *Entry) KeyStatus() KeyStatus {
	return e.status
}

func (e *Entry) toUnmonitored() *Entry {
	return newUnmonitoredEntry(e.key, e.isPrimary, e.keyID, e.status)
}

func keyStatusFromProto(status tinkpb.KeyStatusType) (KeyStatus, error) {
	switch status {
	case tinkpb.KeyStatusType_ENABLED:
		return Enabled, nil
	case tinkpb.KeyStatusType_DISABLED:
		return Disabled, nil
	case tinkpb.KeyStatusType_DESTROYED:
		return Destroyed, nil
	default:
		return Unknown, fmt.Errorf("unknown key status: %v", status)
	}
}

func keyStatusToProto(status KeyStatus) (tinkpb.KeyStatusType, error) {
	switch status {
	case Enabled:
		return tinkpb.KeyStatusType_ENABLED, nil
	case Disabled:
		return tinkpb.KeyStatusType_DISABLED, nil
	case Destroyed:
		return tinkpb.KeyStatusType_DESTROYED, nil
	default:
		return tinkpb.KeyStatusType_UNKNOWN_STATUS, fmt.Errorf("unknown key status: %v", status)
	}
}

// entryToProtoKey converts an Entry to a tinkpb.Keyset_Key. Assumes entry is not nil.
func entryToProtoKey(entry *Entry) (*tinkpb.Keyset_Key, error) {
	protoKeyStatus, err := keyStatusToProto(entry.KeyStatus())
	if err != nil {
		return nil, err
	}
	protoKeySerialization, err := protoserialization.SerializeKey(entry.Key())
	if err != nil {
		return nil, err
	}
	return &tinkpb.Keyset_Key{
		KeyId:            entry.KeyID(),
		Status:           protoKeyStatus,
		OutputPrefixType: protoKeySerialization.OutputPrefixType(),
		KeyData:          protoKeySerialization.KeyData(),
	}, nil
}

func entriesToProtoKeyset(entries []*Entry, shouldLogKeyExport bool) (*tinkpb.Keyset, error) {
	if len(entries) == 0 {
		return nil, fmt.Errorf("entries is empty")
	}
	protoKeyset := &tinkpb.Keyset{
		Key: make([]*tinkpb.Keyset_Key, len(entries)),
	}

	for i, entry := range entries {
		if !shouldLogKeyExport {
			entry = entry.toUnmonitored()
		}
		protoKey, err := entryToProtoKey(entry)
		if err != nil {
			return nil, err
		}
		protoKeyset.Key[i] = protoKey
		if entry.IsPrimary() {
			protoKeyset.PrimaryKeyId = entry.KeyID()
		}
	}
	return protoKeyset, nil
}

func setEntryMonitoringIfNeeded(h *Handle) error {
	if len(h.annotations) == 0 {
		return nil
	}
	monitorinKeysetInfo, err := monitoringutil.MonitoringKeysetInfoFromKeysetInfo(h.KeysetInfo(), h.annotations)
	if err != nil {
		return err
	}
	for _, entry := range h.entries {
		l, err := h.monitoringClient.NewLogger(&monitoring.Context{
			Primitive:   "keyset",
			APIFunction: "get_key",
			KeysetInfo:  monitorinKeysetInfo,
		})
		if err != nil {
			return err
		}
		entry.logger = l
	}
	return nil
}

func newFromEntries(entries []*Entry, opts ...Option) (*Handle, error) {
	var primaryKeyEntry *Entry = nil
	for _, entry := range entries {
		if entry.IsPrimary() {
			primaryKeyEntry = entry
		}
		if entry.KeyStatus() == Unknown {
			return nil, fmt.Errorf("keyset.Handle: unknown key status for key with id %d", entry.KeyID())
		}
	}
	if primaryKeyEntry == nil {
		return nil, fmt.Errorf("keyset.Handle: no primary key")
	}
	h := &Handle{
		entries:          entries,
		primaryKeyEntry:  primaryKeyEntry,
		monitoringClient: internalregistry.GetMonitoringClient(),
	}

	if err := applyOptions(h, opts...); err != nil {
		return nil, fmt.Errorf("keyset.Handle: %v", err)
	}
	if err := setEntryMonitoringIfNeeded(h); err != nil {
		return nil, fmt.Errorf("keyset.Handle: %v", err)
	}
	return h, nil
}

func hasSecrets(ks *tinkpb.Keyset) bool {
	return slices.ContainsFunc(ks.GetKey(), func(protoKey *tinkpb.Keyset_Key) bool {
		switch protoKey.GetKeyData().GetKeyMaterialType() {
		case tinkpb.KeyData_UNKNOWN_KEYMATERIAL, tinkpb.KeyData_ASYMMETRIC_PRIVATE, tinkpb.KeyData_SYMMETRIC:
			return true
		}
		return false
	})
}

func keysetToEntries(ks *tinkpb.Keyset) ([]*Entry, error) {
	if err := Validate(ks); err != nil {
		return nil, fmt.Errorf("invalid keyset: %v", err)
	}
	entries := make([]*Entry, len(ks.GetKey()))
	for i, protoKey := range ks.GetKey() {
		protoKeyData := protoKey.GetKeyData()
		keyID := protoKey.GetKeyId()
		if protoKey.GetOutputPrefixType() == tinkpb.OutputPrefixType_RAW {
			keyID = 0
		}
		protoKeySerialization, err := protoserialization.NewKeySerialization(protoKeyData, protoKey.GetOutputPrefixType(), keyID)
		if err != nil {
			return nil, err
		}
		key, err := protoserialization.ParseKey(protoKeySerialization)
		if err != nil {
			return nil, err
		}
		keyStatus, err := keyStatusFromProto(protoKey.GetStatus())
		if err != nil {
			return nil, err
		}
		entries[i] = newUnmonitoredEntry(key, protoKey.GetKeyId() == ks.GetPrimaryKeyId(), protoKey.GetKeyId(), keyStatus)
	}
	return entries, nil
}

// NewHandle creates a keyset handle that contains a single fresh key generated according
// to the given KeyTemplate.
func NewHandle(kt *tinkpb.KeyTemplate) (*Handle, error) {
	manager := NewManager()
	keyID, err := manager.Add(kt)
	if err != nil {
		return nil, fmt.Errorf("keyset.Handle: cannot generate new keyset: %s", err)
	}
	err = manager.SetPrimary(keyID)
	if err != nil {
		return nil, fmt.Errorf("keyset.Handle: cannot set primary: %s", err)
	}
	handle, err := manager.Handle()
	if err != nil {
		return nil, fmt.Errorf("keyset.Handle: cannot get keyset handle: %s", err)
	}
	return handle, nil
}

// NewHandleWithNoSecrets creates a new instance of KeysetHandle from the
// the given keyset which does not contain any secret key material.
func NewHandleWithNoSecrets(ks *tinkpb.Keyset) (*Handle, error) {
	if hasSecrets(ks) {
		return nil, fmt.Errorf("keyset.Handle: importing unencrypted secret key material is forbidden")
	}
	return newKeysetHandleFromProto(ks)
}

// Read tries to create a Handle from an encrypted keyset obtained via reader.
func Read(reader Reader, masterKey tink.AEAD) (*Handle, error) {
	return ReadWithAssociatedData(reader, masterKey, []byte{})
}

// ReadWithAssociatedData tries to create a Handle from an encrypted keyset obtained via reader using the provided associated data.
func ReadWithAssociatedData(reader Reader, masterKey tink.AEAD, associatedData []byte) (*Handle, error) {
	encryptedKeyset, err := reader.ReadEncrypted()
	if err != nil {
		return nil, err
	}
	protoKeyset, err := decrypt(encryptedKeyset, masterKey, associatedData)
	if err != nil {
		return nil, err
	}
	return newKeysetHandleFromProto(protoKeyset)
}

// ReadWithContext creates a keyset.Handle from an encrypted keyset obtained via
// reader using the provided AEADWithContext.
func ReadWithContext(ctx context.Context, reader Reader, keyEncryptionAEAD tink.AEADWithContext, associatedData []byte) (*Handle, error) {
	encryptedKeyset, err := reader.ReadEncrypted()
	if err != nil {
		return nil, err
	}
	protoKeyset, err := decryptWithContext(ctx, encryptedKeyset, keyEncryptionAEAD, associatedData)
	if err != nil {
		return nil, err
	}
	return newKeysetHandleFromProto(protoKeyset)
}

// ReadWithNoSecrets tries to create a keyset.Handle from a keyset obtained via reader.
func ReadWithNoSecrets(reader Reader) (*Handle, error) {
	protoKeyset, err := reader.Read()
	if err != nil {
		return nil, err
	}
	return NewHandleWithNoSecrets(protoKeyset)
}

// Primary returns the primary key of the keyset.
func (h *Handle) Primary() (*Entry, error) {
	if h == nil {
		return nil, fmt.Errorf("keyset.Handle: nil handle")
	}
	if h.primaryKeyEntry == nil {
		return nil, fmt.Errorf("keyset.Handle: no primary key")
	}
	return h.primaryKeyEntry, nil
}

// Entry returns the key at index i from the keyset.
// i must be within the range [0, Handle.Len()).
func (h *Handle) Entry(i int) (*Entry, error) {
	if h == nil {
		return nil, fmt.Errorf("keyset.Handle: nil handle")
	}
	if i < 0 || i >= h.Len() {
		return nil, fmt.Errorf("keyset.Handle: index %d out of range", i)
	}
	return h.entries[i], nil
}

// privateKey represents a key with a public key.
type privateKey interface {
	PublicKey() (key.Key, error)
}

// Public returns a Handle of the public keys if the managed keyset contains private keys.
func (h *Handle) Public() (*Handle, error) {
	if h == nil {
		return nil, fmt.Errorf("keyset.Handle: nil handle")
	}
	if h.Len() == 0 {
		return nil, fmt.Errorf("keyset.Handle: entries list is empty or nil")
	}
	entries := make([]*Entry, h.Len())
	for i, entry := range h.entries {
		// NOTE: Purposely not using entry.Key() here to avoid logging the key export.
		privateKey, ok := entry.key.(privateKey)
		if !ok {
			return nil, fmt.Errorf("keyset.Handle: keyset contains a non-private key")
		}
		publicKey, err := privateKey.PublicKey()
		if err != nil {
			return nil, fmt.Errorf("keyset.Handle: %v", err)
		}
		entries[i] = newUnmonitoredEntry(publicKey, entry.isPrimary, entry.keyID, entry.status)
	}
	return newFromEntries(entries)
}

// String returns a string representation of the managed keyset.
// The result does not contain any sensitive key material.
func (h *Handle) String() string {
	c, err := prototext.MarshalOptions{}.Marshal(h.KeysetInfo())
	if err != nil {
		return ""
	}
	return string(c)
}

// Len returns the number of keys in the keyset.
func (h *Handle) Len() int {
	if h == nil {
		return 0
	}
	return len(h.entries)
}

// KeysetInfo returns [*tinkpb.KeysetInfo] representation of the managed
// keyset.
//
// The result does not contain any sensitive key material.
func (h *Handle) KeysetInfo() *tinkpb.KeysetInfo {
	protoKeyset, err := entriesToProtoKeyset(h.entries, false)
	if err != nil {
		// This should never happen.
		panic(err)
	}
	return getKeysetInfo(protoKeyset)
}

// Write encrypts and writes the enclosing keyset.
func (h *Handle) Write(writer Writer, masterKey tink.AEAD) error {
	if h == nil {
		return fmt.Errorf("keyset.Handle: nil handle")
	}
	return h.WriteWithAssociatedData(writer, masterKey, []byte{})
}

// WriteWithAssociatedData encrypts and writes the enclosing keyset using the provided associated data.
func (h *Handle) WriteWithAssociatedData(writer Writer, masterKey tink.AEAD, associatedData []byte) error {
	if h == nil {
		return fmt.Errorf("keyset.Handle: nil handle")
	}
	protoKeyset, err := entriesToProtoKeyset(h.entries, false)
	if err != nil {
		return err
	}
	encrypted, err := encrypt(protoKeyset, masterKey, associatedData)
	if err != nil {
		return err
	}
	return writer.WriteEncrypted(encrypted)
}

// WriteWithContext encrypts and writes the keyset using the provided AEADWithContext.
func (h *Handle) WriteWithContext(ctx context.Context, writer Writer, keyEncryptionAEAD tink.AEADWithContext, associatedData []byte) error {
	if h == nil {
		return fmt.Errorf("keyset.Handle: nil handle")
	}
	protoKeyset, err := entriesToProtoKeyset(h.entries, false)
	if err != nil {
		return fmt.Errorf("keyset.Handle: %v", err)
	}
	encrypted, err := encryptWithContext(ctx, protoKeyset, keyEncryptionAEAD, associatedData)
	if err != nil {
		return fmt.Errorf("keyset.Handle: %v", err)
	}
	return writer.WriteEncrypted(encrypted)
}

// WriteWithNoSecrets exports the keyset in h to the given Writer w returning an error if the keyset
// contains secret key material.
func (h *Handle) WriteWithNoSecrets(w Writer) error {
	if h == nil {
		return fmt.Errorf("keyset.Handle: nil handle")
	}
	protoKeyset, err := entriesToProtoKeyset(h.entries, false)
	if err != nil {
		return err
	}
	if hasSecrets(protoKeyset) {
		return fmt.Errorf("keyset.Handle: exporting unencrypted secret key material is forbidden")
	}
	return w.Write(protoKeyset)
}

// Config provides methods to create primitives from [key.Key]s.
type Config interface {
	// PrimitiveFromKey creates a primitive from a [key.Key].
	PrimitiveFromKey(key key.Key, _ internalapi.Token) (any, error)
}

type primitiveOptions struct {
	config Config
}

// Primitives creates a [primitiveset.PrimitiveSet] with primitives of type T
// from keys in h.
//
// Only ENABLED keys are considered. This function uses either the given
// [Config] or a global registry to create the primitives.
//
// Example usage:
//
//	ps, err := keyset.Primitives[tink.AEAD](h, internalapi.Token{}, keyset.WithConfig(config.V0()))
//
// The returned [primitiveset.PrimitiveSet] is intended to be used by primitive
// factories.
//
// NOTE: This is an internal API.
func Primitives[T any](h *Handle, config Config, _ internalapi.Token) (*primitiveset.PrimitiveSet[T], error) {
	if h == nil {
		return nil, fmt.Errorf("keyset.Handle: nil handle")
	}
	if h.Len() == 0 {
		return nil, fmt.Errorf("keyset.Handle: empty keyset")
	}
	primitiveSet := primitiveset.New[T]()
	primitiveSet.Annotations = h.annotations
	for _, entry := range h.entries {
		// Don't add disabled keys to the primitive set.
		if entry.KeyStatus() != Enabled {
			continue
		}
		if err := addToPrimitiveSet(primitiveSet, entry, config); err != nil {
			return nil, fmt.Errorf("keyset.Handle: cannot add primitive: %v", err)
		}
	}
	return primitiveSet, nil
}

func addToPrimitiveSet[T any](primitiveSet *primitiveset.PrimitiveSet[T], entry *Entry, config Config) error {
	// Don't monitor this as key export.
	entry = entry.toUnmonitored()
	protoKey, err := entryToProtoKey(entry)
	if err != nil {
		return err
	}
	var primitive any
	isFullPrimitive := true
	primitive, err = config.PrimitiveFromKey(entry.Key(), internalapi.Token{})
	if err != nil {
		return err
	}
	// Check if the primitive is a legacy primitive.
	if legacyPrimitive, ok := primitive.(legacyprimitive.LegacyPrimitive); ok {
		isFullPrimitive = false
		primitive = legacyPrimitive.Primitive()
	}
	actualPrimitive, ok := primitive.(T)
	if !ok {
		return fmt.Errorf("primitive is of type %T, want %T", primitive, (*T)(nil))
	}
	psEntry := &primitiveset.Entry[T]{
		KeyID:     protoKey.GetKeyId(),
		Key:       entry.Key(),
		IsPrimary: entry.isPrimary,
	}
	if isFullPrimitive {
		psEntry.FullPrimitive = actualPrimitive
	} else {
		psEntry.Primitive = actualPrimitive
	}
	return primitiveSet.Add(psEntry)
}

func decrypt(encryptedKeyset *tinkpb.EncryptedKeyset, keyEncryptionAEAD tink.AEAD, associatedData []byte) (*tinkpb.Keyset, error) {
	if encryptedKeyset == nil || keyEncryptionAEAD == nil {
		return nil, fmt.Errorf("keyset.Handle: invalid encrypted keyset")
	}
	decrypted, err := keyEncryptionAEAD.Decrypt(encryptedKeyset.GetEncryptedKeyset(), associatedData)
	if err != nil {
		return nil, fmt.Errorf("keyset.Handle: decryption failed: %v", err)
	}
	keyset := new(tinkpb.Keyset)
	if err := proto.Unmarshal(decrypted, keyset); err != nil {
		return nil, errInvalidKeyset
	}
	return keyset, nil
}

// decryptWithContext does the same as decrypt, but uses an AEADWithContext instead of an AEAD.
func decryptWithContext(ctx context.Context, encryptedKeyset *tinkpb.EncryptedKeyset, keyEncryptionAEAD tink.AEADWithContext, associatedData []byte) (*tinkpb.Keyset, error) {
	if encryptedKeyset == nil || keyEncryptionAEAD == nil {
		return nil, fmt.Errorf("keyset.Handle: invalid encrypted keyset")
	}
	decrypted, err := keyEncryptionAEAD.DecryptWithContext(ctx, encryptedKeyset.GetEncryptedKeyset(), associatedData)
	if err != nil {
		return nil, fmt.Errorf("keyset.Handle: decryption failed: %v", err)
	}
	keyset := new(tinkpb.Keyset)
	if err := proto.Unmarshal(decrypted, keyset); err != nil {
		return nil, errInvalidKeyset
	}
	return keyset, nil
}

func encrypt(keyset *tinkpb.Keyset, keyEncryptionAEAD tink.AEAD, associatedData []byte) (*tinkpb.EncryptedKeyset, error) {
	serializedKeyset, err := proto.Marshal(keyset)
	if err != nil {
		return nil, errInvalidKeyset
	}
	encrypted, err := keyEncryptionAEAD.Encrypt(serializedKeyset, associatedData)
	if err != nil {
		return nil, fmt.Errorf("keyset.Handle: encryption failed: %v", err)
	}
	// get keyset info
	encryptedKeyset := &tinkpb.EncryptedKeyset{
		EncryptedKeyset: encrypted,
		KeysetInfo:      getKeysetInfo(keyset),
	}
	return encryptedKeyset, nil
}

// encryptWithContext does the same as encrypt, but uses an AEADWithContext instead of an AEAD.
func encryptWithContext(ctx context.Context, keyset *tinkpb.Keyset, keyEncryptionAEAD tink.AEADWithContext, associatedData []byte) (*tinkpb.EncryptedKeyset, error) {
	serializedKeyset, err := proto.Marshal(keyset)
	if err != nil {
		return nil, errInvalidKeyset
	}
	encrypted, err := keyEncryptionAEAD.EncryptWithContext(ctx, serializedKeyset, associatedData)
	if err != nil {
		return nil, fmt.Errorf("keyset.Handle: encryption failed: %v", err)
	}
	// get keyset info
	encryptedKeyset := &tinkpb.EncryptedKeyset{
		EncryptedKeyset: encrypted,
		KeysetInfo:      getKeysetInfo(keyset),
	}
	return encryptedKeyset, nil
}

// getKeysetInfo returns a KeysetInfo from a Keyset protobuf.
func getKeysetInfo(keyset *tinkpb.Keyset) *tinkpb.KeysetInfo {
	if keyset == nil {
		panic("keyset.Handle: keyset must be non nil")
	}
	keyInfos := make([]*tinkpb.KeysetInfo_KeyInfo, len(keyset.GetKey()))
	for i, key := range keyset.GetKey() {
		keyInfos[i] = getKeyInfo(key)
	}
	return &tinkpb.KeysetInfo{
		PrimaryKeyId: keyset.PrimaryKeyId,
		KeyInfo:      keyInfos,
	}
}

// getKeyInfo returns a KeyInfo from a Key protobuf.
func getKeyInfo(key *tinkpb.Keyset_Key) *tinkpb.KeysetInfo_KeyInfo {
	return &tinkpb.KeysetInfo_KeyInfo{
		TypeUrl:          key.KeyData.TypeUrl,
		Status:           key.Status,
		KeyId:            key.KeyId,
		OutputPrefixType: key.OutputPrefixType,
	}
}
