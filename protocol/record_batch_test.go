package protocol

import (
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/segmentio/kafka-go/compress"
)

type memoryRecord struct {
	offset  int64
	time    time.Time
	key     []byte
	value   []byte
	headers []Header
}

func (m *memoryRecord) Record() Record {
	return Record{
		Offset:  m.offset,
		Time:    m.time,
		Key:     NewBytes(m.key),
		Value:   NewBytes(m.value),
		Headers: m.headers,
	}
}

func makeRecords(memoryRecords []memoryRecord) []Record {
	records := make([]Record, len(memoryRecords))
	for i, m := range memoryRecords {
		records[i] = m.Record()
	}
	return records
}

func TestRecordReader(t *testing.T) {
	now := time.Now()

	records := []memoryRecord{
		{
			offset: 1,
			time:   now,
			key:    []byte("key-1"),
		},
		{
			offset: 2,
			time:   now.Add(time.Millisecond),
			value:  []byte("value-1"),
		},
		{
			offset: 3,
			time:   now.Add(time.Second),
			key:    []byte("key-3"),
			value:  []byte("value-3"),
			headers: []Header{
				{Key: "answer", Value: []byte("42")},
			},
		},
	}

	r1 := NewRecordReader(makeRecords(records)...)
	r2 := NewRecordReader(makeRecords(records)...)
	assertRecords(t, r1, r2)
}

func TestMultiRecordReader(t *testing.T) {
	now := time.Now()

	records := []memoryRecord{
		{
			offset: 1,
			time:   now,
			key:    []byte("key-1"),
		},
		{
			offset: 2,
			time:   now.Add(time.Millisecond),
			value:  []byte("value-1"),
		},
		{
			offset: 3,
			time:   now.Add(time.Second),
			key:    []byte("key-3"),
			value:  []byte("value-3"),
			headers: []Header{
				{Key: "answer", Value: []byte("42")},
			},
		},
	}

	r1 := NewRecordReader(makeRecords(records)...)
	r2 := MultiRecordReader(
		NewRecordReader(makeRecords(records[:1])...),
		NewRecordReader(makeRecords(records[1:])...),
	)
	assertRecords(t, r1, r2)
}

func TestControlRecord(t *testing.T) {
	now := time.Now()

	records := []ControlRecord{
		{
			Offset:  1,
			Time:    now,
			Version: 2,
			Type:    3,
		},
		{
			Offset:  2,
			Time:    now.Add(time.Second),
			Version: 4,
			Type:    5,
			Data:    []byte("Hello World!"),
			Headers: []Header{
				{Key: "answer", Value: []byte("42")},
			},
		},
	}

	batch := NewControlBatch(records...)
	found := make([]ControlRecord, 0, len(records))

	for {
		r, err := batch.ReadControlRecord()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				t.Fatal(err)
			}
			break
		}
		found = append(found, *r)
	}

	if !reflect.DeepEqual(records, found) {
		t.Error("control records mismatch")
	}
}

func assertRecords(t *testing.T, r1, r2 RecordReader) {
	t.Helper()

	for {
		rec1, err1 := r1.ReadRecord()
		rec2, err2 := r2.ReadRecord()

		if err1 != nil || err2 != nil {
			if !errors.Is(err1, err2) {
				t.Error("errors mismatch:")
				t.Log("expected:", err2)
				t.Log("found:   ", err1)
			}
			return
		}

		if !equalRecords(rec1, rec2) {
			t.Error("records mismatch:")
			t.Logf("expected: %+v", rec2)
			t.Logf("found:    %+v", rec1)
		}
	}
}

func equalRecords(r1, r2 *Record) bool {
	if r1.Offset != r2.Offset {
		return false
	}

	if !r1.Time.Equal(r2.Time) {
		return false
	}

	k1 := readAll(r1.Key)
	k2 := readAll(r2.Key)

	if !reflect.DeepEqual(k1, k2) {
		return false
	}

	v1 := readAll(r1.Value)
	v2 := readAll(r2.Value)

	if !reflect.DeepEqual(v1, v2) {
		return false
	}

	return reflect.DeepEqual(r1.Headers, r2.Headers)
}

func readAll(bytes Bytes) []byte {
	if bytes != nil {
		defer bytes.Close()
	}
	b, err := ReadAll(bytes)
	if err != nil {
		panic(err)
	}
	return b
}

func TestRecordSetWritesBatchProducer(t *testing.T) {
	cases := []struct {
		name     string
		producer *BatchProducer
		want     BatchProducer
	}{
		{name: "no producer", want: BatchProducer{ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1}},
		{name: "idempotent producer", producer: &BatchProducer{ProducerID: 4021, ProducerEpoch: 3, BaseSequence: 2147483646}, want: BatchProducer{ProducerID: 4021, ProducerEpoch: 3, BaseSequence: 2147483646}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now().Truncate(time.Millisecond)
			records := []memoryRecord{
				{offset: 0, time: now, key: []byte("k"), value: []byte("v-1")},
				{offset: 1, time: now, key: []byte("k"), value: []byte("v-2")},
			}
			buf := newPageBuffer()
			defer buf.unref()
			rs := RecordSet{
				Version:    2,
				Attributes: Attributes(compress.Lz4),
				Records:    NewRecordReader(makeRecords(records)...),
				Producer:   tc.producer,
			}
			if _, err := rs.WriteTo(buf); err != nil {
				t.Fatal(err)
			}
			var got RecordSet
			if _, err := got.ReadFrom(buf); err != nil {
				t.Fatal(err)
			}
			stream, ok := got.Records.(*RecordStream)
			if !ok || len(stream.Records) != 1 {
				t.Fatalf("decoded records %T, want one batch", got.Records)
			}
			batch, ok := stream.Records[0].(*RecordBatch)
			if !ok {
				t.Fatalf("decoded batch %T, want *RecordBatch", stream.Records[0])
			}
			if have := (BatchProducer{batch.ProducerID, batch.ProducerEpoch, batch.BaseSequence}); have != tc.want {
				t.Fatalf("batch producer %+v, want %+v", have, tc.want)
			}
			if batch.Attributes.Compression() != compress.Lz4 {
				t.Fatalf("batch compression %v, want lz4", batch.Attributes.Compression())
			}
			assertRecords(t, NewRecordReader(makeRecords(records)...), batch)
		})
	}
}

func TestRecordSetRejectsBatchProducerBeforeV2(t *testing.T) {
	buf := newPageBuffer()
	defer buf.unref()
	rs := RecordSet{
		Version:  1,
		Records:  NewRecordReader(Record{Value: NewBytes([]byte("v"))}),
		Producer: &BatchProducer{ProducerID: 1},
	}
	if _, err := rs.WriteTo(buf); err == nil {
		t.Fatal("v1 record set with a producer was written")
	}
}
