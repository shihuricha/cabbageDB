package main

import (
	"cabbageDB/gobReg"
	"cabbageDB/sql/catalog"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/chzyer/readline"
	"log"
	"net"
	"os"
	"strings"
	"text/tabwriter"
)

// 命令行参数解析
type Options struct {
	Command string
	Host    string
	Port    uint
}

func ParseArgs() *Options {
	opts := &Options{}
	flag.StringVar(&opts.Host, "H", "127.0.0.1", "Host to connect to")
	flag.StringVar(&opts.Host, "host", "127.0.0.1", "Host to connect to")
	flag.UintVar(&opts.Port, "p", 9605, "Port number to connect to")
	flag.UintVar(&opts.Port, "port", 9605, "Port number to connect to")
	flag.Parse()

	if args := flag.Args(); len(args) > 0 {
		opts.Command = strings.Join(args, " ")
	}
	return opts
}

type SQLClient struct {
	Client      *Client
	Editor      *readline.Instance
	HistoryPath string
	ShowHeaders bool
}

func NewSQLClient(host string, port uint) (*SQLClient, error) {
	conn, err := net.Dial("tcp", (fmt.Sprintf("%s:%d", host, port)))
	client := Client{
		Conn: conn,
	}
	if err != nil {
		return nil, err
	}

	home, _ := os.UserHomeDir()
	return &SQLClient{
		Client:      &client,
		HistoryPath: fmt.Sprintf("%s/.toysql_history", home),
		ShowHeaders: true,
	}, nil
}

// 执行命令入口
func (c *SQLClient) Execute(input string) error {
	input = strings.TrimSpace(input)
	if input == "" {
		return nil
	}

	if strings.HasPrefix(input, "!") {
		return c.ExecuteCommand(input)
	}
	return c.ExecuteQuery(input)
}

// 处理元命令
func (c *SQLClient) ExecuteCommand(input string) error {
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return nil
	}

	cmd := parts[0]
	args := parts[1:]

	err1 := errors.New("Invalid return value")

	switch cmd {
	case "!headers":
		if len(args) != 1 {
			return fmt.Errorf("usage: !headers <on|off>")
		}
		if args[0] == "false" {
			c.ShowHeaders = false
		} else {
			c.ShowHeaders = true
		}
		//c.ShowHeaders = (args[0] == "on")
		fmt.Printf("Headers %s\n", args[0])

	case "!help":
		fmt.Print(`
Enter a SQL statement terminated by a semicolon (;) to execute it.
Available commands:

    !help              Show this help
    !status            Show server status
    !table [name]      Show table schema
    !tables            List tables
`)

	case "!status":
		status := c.Client.Status()
		if status == nil {
			return err1
		}
		statusJson, _ := json.Marshal(status)
		fmt.Println(string(statusJson))

	case "!table":
		if len(args) != 1 {
			return fmt.Errorf("usage: !table <name>")
		}
		schema, err := c.Client.GetTable(args[0])
		if err != nil {
			return err
		}
		if schema == nil {
			return nil
		}
		if schema.Name != "" {
			fmt.Println(schema.String())
		}
	case "!tables":
		tables := c.Client.ListTables()
		if tables == nil {
			return nil
		}
		for _, table := range tables {
			fmt.Println(table)
		}

	default:
		return fmt.Errorf("unknown command: %s", cmd)
	}
	return nil
}

// 执行SQL查询
func (c *SQLClient) ExecuteQuery(query string) error {
	result, err := c.Client.Execute(query)
	if err != nil {
		return err
	}

	switch res := result.(type) {
	case *catalog.BeginResultSet:
		if res.ReadOnly == false {
			fmt.Printf("Began read-write transaction at version %d\n", res.Version)
		} else {
			fmt.Printf("Began read-only transaction at version %d\n", res.Version)
		}
	case *catalog.CommitResultSet:
		fmt.Printf("Committed transaction %d\n", res.Version)
	case *catalog.RollbackResultSet:
		fmt.Printf("Rolled back transaction %d\n", res.Version)
	case *catalog.CreateResultSet:
		fmt.Printf("Created %d rows\n", res.Count)
	case *catalog.DeleteResultSet:
		fmt.Printf("Delete %d rows\n", res.Count)
	case *catalog.UpdateResultSet:
		fmt.Printf("Update %d rows\n", res.Count)
	case *catalog.CreateTableResultSet:
		fmt.Printf("Created table %s\n", res.Name)
	case *catalog.DropTableResultSet:
		fmt.Printf("Dropped table %s\n", res.Name)
	case *catalog.ExplainResultSet:
		fmt.Printf("------Explain info------\n%s", res.NodeInfo)
	case *catalog.QueryResultSet:
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		defer w.Flush()

		if c.ShowHeaders {
			if len(res.Columns) != 0 {
				fmt.Fprintln(w, strings.Join(res.Columns, "\t"))
			}
		}
		for _, row := range res.Rows {
			rowStr := []string{}
			for _, value := range row {
				rowStr = append(rowStr, value.String())
			}
			fmt.Fprintln(w, strings.Join(rowStr, "\t"))
		}

	default:
		jsonByte, _ := json.Marshal(res)
		fmt.Printf("%+v\n", string(jsonByte))
	}
	return nil
}

// REPL交互循环
func (c *SQLClient) Run() error {
	rl, err := readline.NewEx(&readline.Config{
		HistoryFile:     c.HistoryPath,
		AutoComplete:    c.CreateCompleter(),
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	})
	if err != nil {
		return err
	}
	defer rl.Close()
	c.Editor = rl

	// 加载历史记录
	if _, err := os.Stat(c.HistoryPath); err == nil {
		rl.SetHistoryPath(c.HistoryPath)
	}

	var (
		multiLineBuffer strings.Builder
		prompt          = "sql> "
	)

	for {
		// 动态设置提示符
		rl.SetPrompt(prompt)

		input, err := rl.Readline()
		if err != nil { // 处理Ctrl+D/Ctrl+C
			break
		}

		line := strings.TrimSpace(input)
		if line == "" {
			continue
		}

		// 累积多行输入
		if multiLineBuffer.Len() > 0 {
			multiLineBuffer.WriteByte(' ')
		}
		multiLineBuffer.WriteString(line)

		// 检查语句结束
		if strings.HasSuffix(line, ";") {
			// 执行完整SQL语句
			query := strings.TrimSuffix(multiLineBuffer.String(), ";")
			if err := c.Execute(query); err != nil {
				fmt.Printf("Error: %v\n", err)
			}

			// 重置状态
			multiLineBuffer.Reset()
			prompt = "sql> "
			rl.SaveHistory(c.HistoryPath)
		} else {
			// 进入多行模式
			prompt = "  -> " // 或者用 "...> " 等其他提示符
		}
	}
	return nil
}

// 创建自动补全器
func (c *SQLClient) CreateCompleter() *readline.PrefixCompleter {
	return readline.NewPrefixCompleter(
		readline.PcItem("SELECT",
			readline.PcItem("FROM"),
			readline.PcItem("WHERE"),
		),
		readline.PcItem("INSERT"),
		readline.PcItem("UPDATE"),
		readline.PcItem("BEGIN"),
		readline.PcItem("COMMIT"),
		readline.PcItem("ROLLBACK"),
		readline.PcItem("!help"),
		readline.PcItem("!exit"),
	)
}

func main() {
	gobReg.GobRegMain()
	opts := ParseArgs()

	sqlClient, err := NewSQLClient(opts.Host, (opts.Port))
	if err != nil {
		log.Fatal(err)
	}

	if opts.Command != "" {
		if err := sqlClient.Execute(opts.Command); err != nil {
			log.Fatal(err)
		}
		return
	}

	if err := sqlClient.Run(); err != nil {
		log.Fatal(err)
	}
}
