package lz4

import (
	"bytes"
	"io"
	"testing"
)

// Every frame, from a new or a pooled writer, declares 64KB blocks and
// decodes back to its input.
func TestWriterUses64KBBlocks(t *testing.T) {
	codec := new(Codec)
	input := bytes.Repeat([]byte("market data "), 20000)
	for i := 0; i < 3; i++ {
		var buf bytes.Buffer
		w := codec.NewWriter(&buf)
		if _, err := w.Write(input); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		frame := buf.Bytes()
		if len(frame) < 6 {
			t.Fatalf("frame %d is %d bytes", i, len(frame))
		}
		// Frame descriptor BD byte, bits 4-6: 4 is 64KB, 7 is 4MB.
		if size := frame[5] >> 4 & 0x7; size != 4 {
			t.Fatalf("frame %d block size index %d, want 4 (64KB)", i, size)
		}
		r := codec.NewReader(bytes.NewReader(frame))
		got, err := io.ReadAll(r)
		r.Close()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, input) {
			t.Fatalf("frame %d decoded to %d bytes, want %d", i, len(got), len(input))
		}
	}
}
