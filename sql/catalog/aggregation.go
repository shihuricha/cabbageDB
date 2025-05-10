package catalog

import (
	"bytes"
	"cabbageDB/logger"
	"cabbageDB/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type AggregationExec struct {
	Source       Executor
	Aggregates   []Aggregate
	Accumulators map[string][]Accumulator
}

type Accumulator interface {
	Accumulate(value *sql.ValueData)
	Aggregate() *sql.ValueData
}

func (a *AggregationExec) Execute(txn Transaction) (ResultSet, error) {
	aggCount := len(a.Aggregates)

	resultSet, err := a.Source.Execute(txn)
	if err != nil {
		return nil, err
	}

	switch v := resultSet.(type) {
	case *QueryResultSet:
		if len(v.Rows) == 0 && aggCount > 0 {
			// 创建默认空分组键的累加器
			keyStr := "" // 空分组键的特殊标识
			accs := make([]Accumulator, aggCount)
			for i, agg := range a.Aggregates {
				accs[i] = NewAccumulator(agg) // 初始化所有聚合器
			}
			a.Accumulators[keyStr] = accs
		} else {
			// 正常处理每一行数据
			for i := range v.Rows {
				// 防御性检查：确保行数据足够分割
				if len(v.Rows[i]) < aggCount {
					return nil, errors.New("column len err")
				}
				// 分割当前行的聚合参数和分组键
				aggValues := v.Rows[i][:aggCount] // 前N列是聚合函数的输入值
				groupKey := v.Rows[i][aggCount:]  // 后续列是分组键
				// 修复点2: 使用确定性编码代替JSON序列化
				keyStr := encodeGroupKey(groupKey) // 自定义编码保证唯一性
				// 获取或创建当前分组的累加器
				accs, exists := a.Accumulators[keyStr]
				if !exists {
					accs = make([]Accumulator, aggCount)
					for i, agg := range a.Aggregates {
						accs[i] = NewAccumulator(agg) // 根据聚合类型初始化
					}
					a.Accumulators[keyStr] = accs
				}
				for i2, acc := range accs {
					acc.Accumulate(aggValues[i2])
				}
			}
		}
		// 构建结果列
		columns := make([]string, len(v.Columns))
		for i := range columns {
			if i < aggCount {
				agg := a.Aggregates[i]
				columns[i] = fmt.Sprintf("%s(%s)", agg.String(), v.Columns[i])
			} else {
				columns[i] = v.Columns[i] // 保留分组列原名
			}
		}

		// 生成最终结果行
		var rows [][]*sql.ValueData
		for keyStr, accs := range a.Accumulators {
			var groupKey []*sql.ValueData
			if keyStr != "" {
				groupKey = decodeGroupKey(keyStr)
			}

			// 构建结果行：聚合结果 + 分组键
			row := make([]*sql.ValueData, 0, len(accs)+len(groupKey))
			for _, acc := range accs {
				val := acc.Aggregate() // 获取最终聚合值
				row = append(row, val)
			}
			row = append(row, groupKey...)
			rows = append(rows, row)
		}

		return &QueryResultSet{
			Columns: columns,
			Rows:    rows,
		}, nil

	}
	return nil, errors.New("AggregationExec Invalid return ResultSet")
}

func encodeKey(k *sql.ValueData) string {
	var buf bytes.Buffer
	switch k.Type {
	case sql.NullType:
		buf.WriteString("nNULL|") // n前缀表示null

	case sql.FloatType:
		// 保持与String()相同的精度（4位小数）
		f := k.Value.(float64)
		buf.WriteString(fmt.Sprintf("f%.4f|", f)) // f前缀表示float

	case sql.BoolType:
		// 严格对应String()的TRUE/FALSE全大写
		if k.Value.(bool) {
			buf.WriteString("bTRUE|") // b前缀表示bool
		} else {
			buf.WriteString("bFALSE|")
		}

	case sql.IntType:
		// 统一转换为十进制字符串
		var num int64
		switch n := k.Value.(type) {
		case int:
			num = int64(n)
		case int64:
			num = n
		case uint64:
			num = int64(n) // 可能溢出，但保持与String()一致
		}
		buf.WriteString(fmt.Sprintf("i%d|", num)) // i前缀表示integer

	case sql.StringType:
		// 包含长度防御（防止s3:ab和s2:abc碰撞）
		s := k.Value.(string)
		buf.WriteString(fmt.Sprintf("s%d:%s|", len(s), s)) // s前缀表示string

	default:
		logger.Info("unsupported type: %v", k.Type)
		return ""
	}
	return buf.String()
}

func encodeGroupKey(keys []*sql.ValueData) string {
	var buf bytes.Buffer
	for _, k := range keys {
		// 根据String()方法逻辑处理各类型
		keyStr := encodeKey(k)
		buf.WriteString(keyStr)
	}
	return buf.String()
}
func decodeGroupKey(s string) []*sql.ValueData {
	// 步骤1：按分隔符"|"切分编码字符串
	parts := strings.Split(s, "|")
	var values []*sql.ValueData

	for _, part := range parts {
		// 跳过空段（最后一个竖线后的空字符串）
		if part == "" {
			continue
		}

		// 步骤2：验证基础格式
		if len(part) < 1 {
			logger.Info("invalid encoded part: empty string")
			return nil
		}

		// 步骤3：提取类型前缀和数据部分
		prefix := part[0]
		data := part[1:]

		switch prefix {
		// 案例1：Null类型处理
		case 'n':
			if data != "NULL" {
				logger.Info("invalid null encoding: %s", part)
				return nil
			}
			values = append(values, &sql.ValueData{
				Type:  sql.NullType,
				Value: nil,
			})

		// 案例2：Float类型处理
		case 'f':
			val, err := strconv.ParseFloat(data, 64)
			if err != nil {
				logger.Info("float parse error: %s, %v", data, err)
				return nil
			}
			values = append(values, &sql.ValueData{
				Type:  sql.FloatType,
				Value: val,
			})

		// 案例3：Bool类型处理
		case 'b':
			switch data {
			case "TRUE":
				values = append(values, &sql.ValueData{
					Type:  sql.BoolType,
					Value: true,
				})
			case "FALSE":
				values = append(values, &sql.ValueData{
					Type:  sql.BoolType,
					Value: false,
				})
			default:
				logger.Info("invalid bool value: %s", data)
				return nil
			}

		// 案例4：Int类型处理
		case 'i':
			val, err := strconv.ParseInt(data, 10, 64)
			if err != nil {
				logger.Info("int parse error: %s, %v", data, err)
				return nil
			}
			values = append(values, &sql.ValueData{
				Type:  sql.IntType,
				Value: val,
			})

		// 案例5：String类型处理（核心难点）
		case 's':
			// 步骤5.1：切分长度和实际内容
			split := strings.SplitN(data, ":", 2)
			if len(split) != 2 {
				logger.Info("invalid string format: %s", data)
				return nil
			}

			// 步骤5.2：解析声明长度
			length, err := strconv.Atoi(split[0])
			if err != nil {

				logger.Info("invalid length: %s", split[0])
				return nil
			}

			// 步骤5.3：验证实际长度
			str := split[1]
			if len(str) != length {

				logger.Info("length mismatch: declared %d, actual %d", length, len(str))
				return nil
			}

			values = append(values, &sql.ValueData{
				Type:  sql.StringType,
				Value: str,
			})

		// 案例6：未知类型处理
		default:
			logger.Info("unknown prefix: %c", prefix)
			return nil
		}
	}

	return values
}

func NewAccumulator(agg Aggregate) Accumulator {
	switch agg {
	case Average:
		return &AverageAcc{
			Count: &CountAcc{},
			Sum:   &SumAcc{},
		}
	case Max:
		return &MaxAcc{}
	case Min:
		return &MinAcc{}
	case Count:
		return &CountAcc{}
	case Sum:
		return &SumAcc{}
	}
	return nil
}

type AverageAcc struct {
	Count *CountAcc
	Sum   *SumAcc
}

func (a *AverageAcc) Accumulate(cmp *sql.ValueData) {
	a.Count.Accumulate(cmp)
	a.Sum.Accumulate(cmp)
}

func (a *AverageAcc) Aggregate() *sql.ValueData {
	sum := a.Sum.Aggregate()
	count := a.Count.Aggregate()
	if sum.Type == sql.IntType && count.Type == sql.IntType {
		value := &sql.ValueData{
			Type: sql.IntType,
		}

		switch sum.Value.(type) {
		case int64:
			value.Value = sum.Value.(int64) / count.Value.(int64)
		case int:
			value.Value = sum.Value.(int) / count.Value.(int)

		}
		return value
	}
	if sum.Type == sql.FloatType && count.Type == sql.IntType {

		var intV float64
		switch count.Value.(type) {
		case int64:
			intV = float64(count.Value.(int64))
		case int:
			intV = float64(count.Value.(int))
		}

		return &sql.ValueData{
			Type:  sql.FloatType,
			Value: sum.Value.(float64) / intV,
		}
	}

	return &sql.ValueData{
		Type: sql.NullType,
	}

}

type MaxAcc struct {
	MaxV *sql.ValueData
}

func (m *MaxAcc) Accumulate(cmp *sql.ValueData) {
	if cmp == nil {
		return
	}
	if m.MaxV == nil {
		m.MaxV = &sql.ValueData{
			Type:  cmp.Type,
			Value: cmp.Value,
		}
		return
	}
	if m.MaxV.Type != cmp.Type {
		m.MaxV = nil
		return
	}
	if m.MaxV.Type == sql.IntType {
		switch m.MaxV.Value.(type) {
		case int64:
			maxv := m.MaxV.Value.(int64)
			cmpv := cmp.Value.(int64)
			if maxv < cmpv {
				m.MaxV = cmp
			}
			return
		case int:
			maxv := m.MaxV.Value.(int)
			cmpv := cmp.Value.(int)
			if maxv < cmpv {
				m.MaxV = cmp
			}
			return

		}

	}

	if m.MaxV.Type == sql.FloatType {
		maxv := m.MaxV.Value.(float64)
		cmpv := cmp.Value.(float64)
		if maxv < cmpv {
			m.MaxV = cmp
		}
		return
	}

}

func (m *MaxAcc) Aggregate() *sql.ValueData {
	return m.MaxV
}

type MinAcc struct {
	MinV *sql.ValueData
}

func (m *MinAcc) Accumulate(cmp *sql.ValueData) {
	if cmp == nil {
		return
	}
	if m.MinV == nil {
		m.MinV = &sql.ValueData{
			Type:  cmp.Type,
			Value: cmp.Value,
		}
		return
	}
	if m.MinV.Type != cmp.Type {
		m.MinV = nil
		return
	}
	if m.MinV.Type == sql.IntType {
		switch m.MinV.Value.(type) {
		case int64:
			minv := m.MinV.Value.(int64)
			cmpv := cmp.Value.(int64)
			if minv > cmpv {
				m.MinV = cmp
			}
			return
		case int:
			minv := m.MinV.Value.(int64)
			cmpv := cmp.Value.(int64)
			if minv > cmpv {
				m.MinV = cmp
			}
			return
		}

	}

	if m.MinV.Type == sql.FloatType {
		minv := m.MinV.Value.(float64)
		cmpv := cmp.Value.(float64)
		if minv > cmpv {
			m.MinV = cmp
		}
		return
	}

}

func (m *MinAcc) Aggregate() *sql.ValueData {
	return m.MinV
}

type SumAcc struct {
	SumV *sql.ValueData
}

func (s *SumAcc) Accumulate(cmp *sql.ValueData) {
	if cmp == nil {
		return
	}

	if s.SumV == nil && cmp != nil {
		s.SumV = &sql.ValueData{
			Type:  cmp.Type,
			Value: cmp.Value,
		}
		return
	}

	if s.SumV.Type == sql.IntType {
		switch s.SumV.Value.(type) {
		case int64:
			sum := s.SumV.Value.(int64)
			cmpv := cmp.Value.(int64)
			s.SumV.Value = sum + cmpv
		case int:
			sum := s.SumV.Value.(int)
			cmpv := cmp.Value.(int)
			s.SumV.Value = sum + cmpv
		}

		return
	}

	if s.SumV.Type == sql.FloatType {
		sum := s.SumV.Value.(float64)
		cmpv := cmp.Value.(float64)
		s.SumV.Value = sum + cmpv
		return
	}

}

func (s *SumAcc) Aggregate() *sql.ValueData {
	return s.SumV
}

type CountAcc struct {
	CountV int
}

func (c *CountAcc) Accumulate(cmp *sql.ValueData) {
	if cmp == nil {
		return
	}
	if cmp.Type == sql.NullType {
		return
	}
	c.CountV += 1
}
func (c *CountAcc) Aggregate() *sql.ValueData {
	return &sql.ValueData{
		Type:  sql.IntType,
		Value: c.CountV,
	}
}
