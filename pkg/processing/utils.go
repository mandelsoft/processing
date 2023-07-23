package processing

import (
	"strings"
)

func ElementName(typ string, names ...string) string {
	name := strings.Join(names, ":")
	if len(name) > 0 {
		return typ + ":" + name
	} else {
		return typ
	}
}
