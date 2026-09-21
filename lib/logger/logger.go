package logger

import (
	"io"
	"log/slog"
	"os"
)

type LevelWriter struct {
	fileWriter io.Writer
}

var levelNames = map[slog.Level]string{
	slog.LevelDebug: "[DEBUG]",
	slog.LevelInfo: "[INFO]",
	slog.LevelWarn: "[WARNING]",
	slog.LevelError: "[ERROR]",
}

func NewLevelWriter(file io.Writer) *LevelWriter {
	return &LevelWriter{fileWriter: file}
}

func (lw *LevelWriter) Write(p []byte) (n int, err error) {
	n, err = lw.fileWriter.Write(p)
	if containsHighPriorityLevel(p) {
		_, _ = os.Stdout.Write(p)
	}
	return n, err
}

func containsHighPriorityLevel(p []byte) bool {
	str := string(p)
	return (len(str) > 20 && (indexOf(str, `"level":ERROR"`) != -1 || indexOf(str, `"level":"WARN"`) != -1))
}

func indexOf(s, sub string) int {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func InitLogger(logFilePath string) (*os.File, error) {
	file, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	opts := &slog.HandlerOptions{
		Level: slog.LevelDebug,
		AddSource: true,
	}

	writer := NewLevelWriter(file)
	handler := slog.NewJSONHandler(writer, opts)

	slog.SetDefault(slog.New(handler))

	return file, nil
}