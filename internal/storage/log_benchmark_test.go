package storage

import (
	"path/filepath"
	"testing"
)

func BenchmarkLogAppend_FullDurable(b *testing.B) {
	logPath := filepath.Join(b.TempDir(), "test.log")
	l, err := NewLogFullDurable(logPath, 0)
	if err != nil {
		b.Fatal(err)
	}

	for b.Loop() {
		payload, err := GenerateRandomBytes(100)
		if err != nil {
			b.Fatal(err)
		}

		err = l.Append(payload)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLogAppend_MediumDurable(b *testing.B) {
	logPath := filepath.Join(b.TempDir(), "test.log")
	l, err := NewLogMediumDurable(logPath, 0)
	if err != nil {
		b.Fatal(err)
	}

	for b.Loop() {
		payload, err := GenerateRandomBytes(100)
		if err != nil {
			b.Fatal(err)
		}

		err = l.Append(payload)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLogAppend_Async(b *testing.B) {
	logPath := filepath.Join(b.TempDir(), "test.log")
	l, err := NewLogAsync(logPath, 0)
	if err != nil {
		b.Fatal(err)
	}

	for b.Loop() {
		payload, err := GenerateRandomBytes(100)
		if err != nil {
			b.Fatal(err)
		}

		err = l.Append(payload)
		if err != nil {
			b.Fatal(err)
		}
	}
}
