package publicid

import (
	"fmt"
	"strings"

	nanoid "github.com/matoous/go-nanoid/v2"
)

const (
	Alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz" // base58
	Length   = 12
)

// New generates a unique public ID.
func New() (string, error) {
	return nanoid.Generate(Alphabet, Length)
}

// NewWithPrefix generates a unique public ID with a given prefix.
func NewWithPrefix(prefix string) (string, error) {
	id, err := New()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s_%s", prefix, id), nil
}

// Must is the same as New, but panics on error.
func Must() string {
	return nanoid.MustGenerate(Alphabet, Length)
}

// MustWithPrefix is the same as NewWithPrefix, but panics on error.
func MustWithPrefix(prefix string) string {
	id := Must()
	return fmt.Sprintf("%s_%s", prefix, id)
}

// Validate checks if a given field name’s public ID value is valid according to
// the constraints defined by package publicid.
func Validate(fieldName, id string) error {
	if id == "" {
		return fmt.Errorf("%s cannot be blank", fieldName)
	}

	if len(id) != Length {
		return fmt.Errorf("%s should be %d characters long", fieldName, Length)
	}

	if strings.Trim(id, Alphabet) != "" {
		return fmt.Errorf("%s has invalid characters", fieldName)
	}

	return nil
}

// ValidateWithPrefix checks if a given field name’s public ID value with a prefix is valid according to
// the constraints defined by package publicid.
func ValidateWithPrefix(fieldName, id, prefix string) error {
	if id == "" {
		return fmt.Errorf("%s cannot be blank", fieldName)
	}

	expectedPrefix := fmt.Sprintf("%s_", prefix)
	if !strings.HasPrefix(id, expectedPrefix) {
		return fmt.Errorf("%s must start with the prefix %s", fieldName, prefix)
	}

	// Remove the prefix and the underscore from the ID for further validation.
	trimmedID := strings.TrimPrefix(id, expectedPrefix)

	return Validate(fieldName, trimmedID)
}
