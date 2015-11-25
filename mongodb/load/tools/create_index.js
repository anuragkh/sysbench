for (i = 0; i < 104; i++) {
	var idx = {}
	idx["field" + i] = 1
	db.data.ensureIndex(idx);
}