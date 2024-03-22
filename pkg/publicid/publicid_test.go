package publicid

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	id, err := New()
	assert.NoError(t, err)
	assert.Len(t, id, Length, fmt.Sprintf("ID should be %d characters long", Length))
	assert.Equal(t, "", strings.Trim(id, Alphabet), "ID should only contain characters from the Alphabet")
}

func TestNewWithPrefix(t *testing.T) {
	prefix := "test"
	id, err := NewWithPrefix(prefix)
	assert.NoError(t, err)
	assert.True(t, strings.HasPrefix(id, prefix+"_"), "ID should have the correct prefix")
	assert.Len(t, id, len(prefix)+1+Length, fmt.Sprintf("ID should be %d characters long", len(prefix)+1+Length))
	assert.Equal(t, "", strings.Trim(id[len(prefix)+1:], Alphabet), "ID should only contain characters from the Alphabet after the prefix")
}

func TestMust(t *testing.T) {
	assert.NotPanics(t, func() {
		id := Must()
		assert.Len(t, id, Length, fmt.Sprintf("ID should be %d characters long", Length))
		assert.Equal(t, "", strings.Trim(id, Alphabet), "ID should only contain characters from the Alphabet")
	})
}

func TestMustWithPrefix(t *testing.T) {
	prefix := "must"
	assert.NotPanics(t, func() {
		id := MustWithPrefix(prefix)
		assert.True(t, strings.HasPrefix(id, prefix+"_"), "ID should have the correct prefix")
		assert.Len(t, id, len(prefix)+1+Length, fmt.Sprintf("ID should be %d characters long", len(prefix)+1+Length))
		assert.Equal(t, "", strings.Trim(id[len(prefix)+1:], Alphabet), "ID should only contain characters from the Alphabet after the prefix")
	})
}

func TestValidate(t *testing.T) {
	validId, _ := New()

	testCases := []struct {
		name        string
		fieldName   string
		id          string
		wantErr     bool
		errContains string
	}{
		{"Valid ID", "field", validId, false, ""},
		{"Empty ID", "field", "", true, "cannot be blank"},
		{"Incorrect Length", "field", "123", true, "should be 12 characters long"},
		{"Invalid Characters", "field", "invalidchar!", true, "has invalid characters"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate(tc.fieldName, tc.id)
			if tc.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.errContains)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateWithPrefix(t *testing.T) {
	prefix := "prefix"
	validIdWithPrefix, _ := NewWithPrefix(prefix)

	testCases := []struct {
		name        string
		fieldName   string
		id          string
		prefix      string
		wantErr     bool
		errContains string
	}{
		{"Valid ID with Prefix", "field", validIdWithPrefix, prefix, false, ""},
		{"Empty ID", "field", "", prefix, true, "cannot be blank"},
		{"No Prefix", "field", validIdWithPrefix[len(prefix)+1:], prefix, true, "must start with the prefix"},
		{"Wrong Prefix", "field", "wrong_" + validIdWithPrefix, prefix, true, "must start with the prefix"},
		{"Invalid Characters with Prefix", "field", prefix + "_invalidchar!", prefix, true, "has invalid characters"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateWithPrefix(tc.fieldName, tc.id, tc.prefix)
			if tc.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.errContains)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
