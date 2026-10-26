//go:build !windows

package memnode

import "os"

func publishFencingGeneration(temp, final string) (bool, error) {
	if err := os.Link(temp, final); err != nil {
		return false, err
	}
	if err := os.Remove(temp); err != nil {
		return true, err
	}
	return true, nil
}

func syncFencingNamespace(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func removeFencingGeneration(path string) error {
	return os.Remove(path)
}
