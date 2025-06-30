package rewind

import (
	"strings"

	"github.com/google/uuid"
)

func newSavepointName() string {
	spn := uuid.NewString()
	spn = strings.ReplaceAll(spn, "-", "")
	return "_" + spn
}
