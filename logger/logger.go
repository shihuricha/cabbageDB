package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
)

var log *zap.SugaredLogger

func InitLogger(cfgID string, cfgLogLevel string) {
	// 创建日志文件
	file, err := os.OpenFile("server_"+cfgID+".log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		panic("failed to open log file: " + err.Error())
	}

	// 正确初始化 EncoderConfig
	config := zap.NewProductionEncoderConfig()
	config.EncodeTime = zapcore.ISO8601TimeEncoder

	// 创建编码器
	consoleEncoder := zapcore.NewConsoleEncoder(config)
	fileEncoder := zapcore.NewJSONEncoder(config)

	// 获取日志级别
	logLevel := getLoggerLevel(cfgLogLevel)
	atomicLevel := zap.NewAtomicLevelAt(logLevel)

	// 正确构造 Tee 核心
	core := zapcore.NewTee(
		zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), atomicLevel),
		zapcore.NewCore(fileEncoder, zapcore.AddSync(file), atomicLevel),
	)

	logger := zap.New(core, zap.AddCaller(), zap.AddCallerSkip(2))
	log = logger.Sugar()
}

func getLoggerLevel(lvl string) zapcore.Level {

	switch lvl {
	case "debug":
		return zap.DebugLevel
	case "info":
		return zap.InfoLevel
	case "warn":
		return zap.WarnLevel
	case "error":
		return zap.ErrorLevel
	case "panic":
		return zap.PanicLevel
	case "dpanic":
		return zap.DPanicLevel
	case "fatal":
		return zap.FatalLevel
	}

	return zapcore.InfoLevel
}

func Debug(args ...interface{}) {
	log.Debug(args...)
}

func Debugf(format string, args ...interface{}) {
	log.Debugf(format, args...)
}

func Info(args ...interface{}) {
	log.Info(args...)
}

func Infof(format string, args ...interface{}) {
	log.Infof(format, args...)
}

func Warn(args ...interface{}) {
	log.Warn(args...)
}

func Warnf(format string, args ...interface{}) {
	log.Warnf(format, args...)
}

func Error(args ...interface{}) {
	log.Error(args...)
}

func Errorf(format string, args ...interface{}) {
	log.Errorf(format, args...)
}

func DPanic(args ...interface{}) {
	log.DPanic(args...)
}

func DPanicf(format string, args ...interface{}) {
	log.DPanicf(format, args...)
}

func Panic(args ...interface{}) {
	log.Panic(args...)
}

func Panicf(format string, args ...interface{}) {
	log.Panicf(format, args...)
}

func Fatal(args ...interface{}) {
	log.Fatal(args...)
}

func Fatalf(format string, args ...interface{}) {
	log.Fatalf(format, args...)
}
