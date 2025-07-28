package analyze

import "fmt"

func equalUnordered(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	counts := make(map[string]int)

	for _, val := range a {
		counts[val]++
	}

	for _, val := range b {
		if counts[val] == 0 {
			return false
		}
		counts[val]--
	}

	return true
}

// removeString removes string from slice of strings
func removeString(s []string, target string) []string {
	for i, v := range s {
		if v == target {
			return append(s[:i], s[i+1:]...)
		}
	}
	return s // target not found, return original
}

// equalSlices checks to see if slices are equal and ordered the same
func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func formatBytes(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := unit, 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
