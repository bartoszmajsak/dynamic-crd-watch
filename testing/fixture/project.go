package fixture

import (
	"os"
	"path/filepath"
)

// ProjectRoot walks up from the caller's working directory until it finds
// the directory containing go.mod, which is the project root.
// This avoids brittle relative paths like filepath.Join("..", "..") in tests.
func ProjectRoot() string {
	dir, err := os.Getwd()
	if err != nil {
		panic("failed to get working directory: " + err.Error())
	}

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			panic("could not find project root (no go.mod found)")
		}

		dir = parent
	}
}
