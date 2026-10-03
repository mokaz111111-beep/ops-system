package topics

import (
	"context"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// EnsureOptions 控制与集群的交互。
type EnsureOptions struct {
	// CheckOnly 只计算并核对，不创建、不加分区、不改配置。
	CheckOnly bool
	ClientID  string
}

// Result 是一次 Ensure 的结果。
type Result struct {
	Profile Profile
	Policy  ReplicaPolicy
	Plan    Plan
	Applied []Action
}

// Ensure 把 IF-2 两个 topic 做到与规格一致。
func Ensure(ctx context.Context, brokers []string, profile Profile, opts EnsureOptions) (Result, error) {
	desired, err := Desired(profile)
	if err != nil {
		return Result{}, err
	}
	pol, err := Policy(profile)
	if err != nil {
		return Result{}, err
	}

	have, adm, closeFn, err := inspect(ctx, brokers, opts.ClientID, namesOf(desired))
	if err != nil {
		return Result{}, err
	}
	defer closeFn()

	plan := Diff(desired, have)
	out := Result{Profile: profile, Policy: pol, Plan: plan}
	if len(plan.Problems) > 0 {
		return out, fmt.Errorf("topics: 存在不能自动修复的偏差:\n  %s", strings.Join(plan.Problems, "\n  "))
	}
	if opts.CheckOnly {
		if !plan.Empty() {
			return out, fmt.Errorf("topics: check-only 发现偏差（%d 个动作）", len(plan.Actions))
		}
		return out, nil
	}
	if err := apply(ctx, adm, plan); err != nil {
		return out, err
	}
	out.Applied = plan.Actions
	return out, nil
}

func namesOf(specs []Spec) []string {
	out := make([]string, len(specs))
	for i, s := range specs {
		out[i] = s.Name
	}
	return out
}

func inspect(ctx context.Context, brokers []string, clientID string, names []string) ([]Existing, *kadm.Client, func(), error) {
	if len(brokers) == 0 {
		return nil, nil, nil, fmt.Errorf("topics: 未配置 broker")
	}
	// 不调用 AllowAutoTopicCreation：默认关闭。打开会让漏跑本命令时冒出 1 分区的
	// logs.raw，而分区只增不减，那种脏 topic 只能靠运维收拾。
	opts := []kgo.Opt{kgo.SeedBrokers(brokers...)}
	if clientID != "" {
		opts = append(opts, kgo.ClientID(clientID))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("topics: 创建客户端失败: %w", err)
	}
	adm := kadm.NewClient(cl)

	listed, err := adm.ListTopics(ctx, names...)
	if err != nil {
		cl.Close()
		return nil, nil, nil, fmt.Errorf("topics: 列出 topic 失败: %w", err)
	}

	var existingNames []string
	for _, name := range names {
		td, ok := listed[name]
		if !ok || td.Err != nil {
			continue
		}
		existingNames = append(existingNames, name)
	}

	cfgs := map[string]map[string]string{}
	if len(existingNames) > 0 {
		res, err := adm.DescribeTopicConfigs(ctx, existingNames...)
		if err != nil {
			cl.Close()
			return nil, nil, nil, fmt.Errorf("topics: 读取 topic 配置失败: %w", err)
		}
		for _, r := range res {
			if r.Err != nil {
				continue
			}
			m := make(map[string]string, len(r.Configs))
			for _, c := range r.Configs {
				if c.Value != nil {
					m[c.Key] = *c.Value
				}
			}
			cfgs[r.Name] = m
		}
	}

	var have []Existing
	for _, name := range names {
		td, ok := listed[name]
		if !ok || td.Err != nil {
			continue
		}
		rf := 0
		if len(td.Partitions) > 0 {
			rf = len(td.Partitions.Sorted()[0].Replicas)
		}
		have = append(have, Existing{
			Name:              name,
			Partitions:        len(td.Partitions),
			ReplicationFactor: rf,
			Configs:           cfgs[name],
		})
	}
	return have, adm, cl.Close, nil
}

func apply(ctx context.Context, adm *kadm.Client, plan Plan) error {
	for _, a := range plan.Actions {
		switch a.Kind {
		case ActionCreate:
			cfg := ptrMap(a.Spec.Configs)
			res, err := adm.CreateTopics(ctx, int32(a.Spec.Partitions), int16(a.Spec.ReplicationFactor), cfg, a.Spec.Name)
			if err != nil {
				return fmt.Errorf("topics: 创建 %s 失败: %w", a.Spec.Name, err)
			}
			if t, ok := res[a.Spec.Name]; ok && t.Err != nil {
				return fmt.Errorf("topics: 创建 %s 失败: %w", a.Spec.Name, t.Err)
			}
		case ActionIncreasePartitions:
			res, err := adm.UpdatePartitions(ctx, a.Spec.Partitions, a.Spec.Name)
			if err != nil {
				return fmt.Errorf("topics: 增加 %s 分区失败: %w", a.Spec.Name, err)
			}
			if t, ok := res[a.Spec.Name]; ok && t.Err != nil {
				return fmt.Errorf("topics: 增加 %s 分区失败: %w", a.Spec.Name, t.Err)
			}
		case ActionAlterConfig:
			ops := make([]kadm.AlterConfig, 0, len(a.Spec.Configs))
			for k, v := range a.Spec.Configs {
				val := v
				ops = append(ops, kadm.AlterConfig{Op: kadm.SetConfig, Name: k, Value: &val})
			}
			res, err := adm.AlterTopicConfigs(ctx, ops, a.Spec.Name)
			if err != nil {
				return fmt.Errorf("topics: 修改 %s 配置失败: %w", a.Spec.Name, err)
			}
			for _, r := range res {
				if r.Err != nil {
					return fmt.Errorf("topics: 修改 %s 配置失败: %w", a.Spec.Name, r.Err)
				}
			}
		default:
			return fmt.Errorf("topics: 未知动作 %s", a.Kind)
		}
	}
	return nil
}

func ptrMap(in map[string]string) map[string]*string {
	out := make(map[string]*string, len(in))
	for k, v := range in {
		val := v
		out[k] = &val
	}
	return out
}
