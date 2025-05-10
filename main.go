package main

import (
	"bytes"
	"cabbageDB/bitcask"
	"cabbageDB/gobReg"
	"cabbageDB/log"
	"cabbageDB/logger"
	server2 "cabbageDB/server"
	"cabbageDB/sql/engine"
	"flag"
	"fmt"
	"github.com/google/btree"
	"github.com/spf13/viper"
	"path/filepath"
	"strconv"
)

type St struct {
	Pos1 int64
	Pos2 int64
}

type ByteItem struct {
	Key   []byte
	Value *St
}

func (bi *ByteItem) Less(than btree.Item) bool {
	other := than.(*ByteItem)
	return bytes.Compare(bi.Key, other.Key) < 0
}

func main() {

	configFile := flag.String("config", "config/db.yaml", "Configuration file path")
	flag.Parse()
	if *configFile != "" {
		fmt.Printf("config is: %s\n", *configFile)
	} else {
		fmt.Println("No configuration file provided.")
	}

	root := filepath.Dir(*configFile)

	cfg := LoadConfig(*configFile)
	if cfg.ID == 0 {
		panic("id not allow equal 0")
	}
	gobReg.GobRegMain()

	logger.InitLogger(strconv.Itoa(int(cfg.ID)), cfg.LogLevel)

	var raftLog *log.RaftLog
	if cfg.StorageRaft == "bitcask" {
		raftLog = log.NewRaftLog(bitcask.NewCompact(filepath.Join(root, cfg.DataDir, "log"), cfg.CompactThresh))
	} else {
		panic(fmt.Sprintf("Unknown Raft storage engine %s", cfg.StorageRaft))
	}

	var raftState log.RaftTxnState
	if cfg.StorageRaft == "bitcask" {
		stateEngine := bitcask.NewCompact(filepath.Join(root, cfg.DataDir, "state"), cfg.CompactThresh)
		raftState = engine.NewState(stateEngine)
	} else {
		panic(fmt.Sprintf("Unknown Raft storage engine %s", cfg.StorageRaft))
	}

	ser := server2.NewServer(cfg.ID, cfg.Peers, raftLog, raftState)
	ser.Listen(cfg.ListenSQL, cfg.ListenRaft)
	ser.Serve()
}

type Config struct {
	ID            log.NodeID            `json:"id" mapstructure:"id"`
	Peers         map[log.NodeID]string `json:"peers" mapstructure:"peers"`
	ListenSQL     string                `json:"listen_sql" mapstructure:"listen_sql"`
	ListenRaft    string                `json:"listen_raft" mapstructure:"listen_raft"`
	LogLevel      string                `json:"log_level" mapstructure:"log_level"`
	DataDir       string                `json:"data_dir" mapstructure:"data_dir"`
	CompactThresh float64               `json:"compact_threshold" mapstructure:"compact_threshold"`
	StorageRaft   string                `json:"storage_raft" mapstructure:"storage_raft"`
	StorageSQL    string                `json:"storage_sql" mapstructure:"storage_sql"`
}

func DefaultConfig() *Config {
	return &Config{
		ID:            1,
		Peers:         map[log.NodeID]string{},
		ListenSQL:     "0.0.0.0:9605",
		ListenRaft:    "0.0.0.0:9705",
		LogLevel:      "INFO",
		DataDir:       "data",
		CompactThresh: 0.2,
		StorageRaft:   "bitcask",
		StorageSQL:    "bitcask",
	}
}

func LoadConfig(configFile string) *Config {
	viperCfg := viper.New()
	viperCfg.AddConfigPath(".")
	viperCfg.SetConfigFile(configFile)
	if err := viperCfg.ReadInConfig(); err != nil {
		fmt.Println("Read Config error:", err.Error())
	}

	config := DefaultConfig()
	if err := viperCfg.Unmarshal(config); err != nil {
		fmt.Println(err)
		return DefaultConfig()
	}
	return config
}
