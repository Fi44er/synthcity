package logger

import (
	"context"
	"os"

	"go.opentelemetry.io/contrib/bridges/otelzap"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
)

type ctxKey string

const loggerKey ctxKey = "logger"

type Logger struct {
	*zap.Logger
}

func New(serviceName string, level string) *Logger {
	var zapLevel zapcore.Level
	if err := zapLevel.UnmarshalText([]byte(level)); err != nil {
		zapLevel = zap.InfoLevel
	}

	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	consoleCore := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		zapcore.AddSync(os.Stdout),
		zapLevel,
	)

	otelCore := otelzap.NewCore(serviceName)

	core := zapcore.NewTee(consoleCore, otelCore)

	l := zap.New(core,
		zap.AddCaller(),
		zap.Fields(zap.String("service", serviceName)),
	)

	return &Logger{l}
}

func (l *Logger) ContextWithLogger(ctx context.Context, fields ...zap.Field) context.Context {
	return context.WithValue(ctx, loggerKey, l.With(fields...))
}

func FromContext(ctx context.Context) *Logger {
	l, ok := ctx.Value(loggerKey).(*zap.Logger)
	if !ok {
		l = zap.L()
	}

	spanContext := trace.SpanFromContext(ctx).SpanContext()
	if spanContext.HasTraceID() {
		l = l.With(
			zap.String("trace_id", spanContext.TraceID().String()),
			zap.String("span_id", spanContext.SpanID().String()),
		)
	}
	return &Logger{l}
}

func (l *Logger) WithTrace(traceID string) *Logger {
	return &Logger{l.With(zap.String("trace_id", traceID))}
}

func (l *Logger) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		loggerWithMethod := l.With(zap.String("grpc.method", info.FullMethod))

		// Кладем обогащенный логгер в контекст
		ctx = context.WithValue(ctx, loggerKey, loggerWithMethod)

		return handler(ctx, req)
	}
}
