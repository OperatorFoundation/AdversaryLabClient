package storage

const int64Size = 8
const cellsize = int64(int64Size * 2)
const headerSize = cellsize * 2

const indexHeaderOffset = 0
const totalHeaderOffset = 1

const storeCellSize = 3
const storeCellByteSize = int64(int64Size * storeCellSize)
const indexStoreCellOffset = 0 * int64Size
const offsetStoreCellOffset = 1 * int64Size
const lengthStoreCellOffset = 2 * int64Size
