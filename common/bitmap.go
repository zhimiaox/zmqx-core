package common

const BitmapMaxSize = uint16(65535)

type Bitmap struct {
	values []byte
	size   uint16
}

func NewBitmap(size uint16) *Bitmap {
	if size == 0 {
		size = BitmapMaxSize
	}
	buk := size / 8
	if size%8 != 0 {
		buk++
	}
	return &Bitmap{size: size, values: make([]byte, buk)}
}

// Size returns the Bitmap size
func (b *Bitmap) Size() uint16 {
	return b.size
}

// Set the value of the offset position to value(0/1)
func (b *Bitmap) Set(offset uint16, value bool) bool {
	if b.size < offset {
		return false
	}
	if !value {
		b.values[offset/8] &^= 1 << (offset % 8)
	} else {
		b.values[offset/8] |= 1 << (offset % 8)
	}
	return true
}

// Get the value at the offset position
func (b *Bitmap) Get(offset uint16) bool {
	if b.size < offset {
		return false
	}
	return b.values[offset/8]&(1<<(offset%8)) > 0
}
