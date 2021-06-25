package clear

func InStrings(x string, y ...string) bool {
	if len(y) == 0 {
		return false
	}

	for _, v := range y {
		if x == v {
			return true
		}
	}

	return false
}
