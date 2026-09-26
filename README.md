# 建设数据中心电力需求响应协同基础平台

本项目是一套可离线运行的 Python 服务端平台，用于管理数据中心、互联通道、加速卡资源批次、租户预约、容量分配、交付情景、硬件稳定性准入，以及园区电力需求响应协同（指令版本、削减曲线、资源冻结与回执归并）。业务状态、幂等结果和审计事件保存在 SQLite 中，适合调度、质量、风险和审计人员在单个 Linux 应用容器内协作。

## 目录

- `src/compute_fabric/`：站点、通道、资源库存、预约、容量分配和情景分析；
- `src/demand_response/`：电网需求响应指令版本、目标削减曲线、站点基线与资源可调属性，生成延迟作业/功率封顶/储能释放候选组合，确认后一次性冻结资源，并按事件时间归并乱序回执；
- `src/accelerator_lab/`：加速卡测点导入、排除复核、分析任务租约和准入决定；
- `src/silicon_qualification/`：AI 加速芯片批次、测量、分析与质量审批；
- `fixtures/`：离线验收使用的结构化协议与测点；
- `tests/`：核心规则、错误边界、事务、API 和命令行验收测试。

## 需求响应协同

园区收到电网需求响应指令后，在承诺窗口内按目标削减曲线降负荷，同时保护不可中断
训练与租户最低算力。模块职责：

1. **登记**：指令按 `source_revision` 版本化，曲线按连续阶段声明，并登记各站点基线
   与租户最低算力保底；
2. **候选组合**：按 merit order（延迟作业 → 功率封顶 → 储能释放）逐阶段生成确定性
   候选，硬约束包括租户保护、设备爬坡（阶段间增量）与恢复窗口（储能可充回、作业可
   恢复）；不可行时按约束类型归因短口；
3. **确认冻结**：审批人确认后，一次性把组合写入不可变冻结动作并锁定全部阶段；
4. **指令修订**：已回执锁定的阶段曲线不可变更，修订只重排未执行阶段，旧版本未锁阶
   段冻结动作自动作废，已确认终态受保护；
5. **回执归并**：执行回执可能乱序或重复，按 `source_event_id` 幂等去重、按
   `event_time` 归并；`achieved/failed/restored` 为终态，迟到或回退事件一律留痕但
   不覆盖；
6. **解释查询**：`GET /directives/{id}/report` 返回目标/计划/实际削减量、未达标原因、
   各租户峰值影响与恢复计划（作业恢复、封顶解除、储能回充完成时间）。

## 环境

- Linux
- Python 3.11 或更高版本
- 运行时仅使用 Python 标准库和 SQLite

## 测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## 构建检查

```bash
python3 -m compileall -q src tests
```

## 离线验收

```bash
PYTHONPATH=src python3 -m compute_fabric.acceptance --workspace .
PYTHONPATH=src python3 -m demand_response.acceptance --workspace .
PYTHONPATH=src python3 -m accelerator_lab.acceptance --workspace .
PYTHONPATH=src python3 -m silicon_qualification.acceptance
```

这些命令使用临时 SQLite 数据库完成站点、资源、预约、分配、测点分析和芯片准入流程，不访问外部网络。

## HTTP 服务

```bash
PYTHONPATH=src python3 -m compute_fabric.api --database compute.sqlite3 --host 127.0.0.1 --port 8080
PYTHONPATH=src python3 -m accelerator_lab.api --database lab.sqlite3 --host 127.0.0.1 --port 8081
PYTHONPATH=src python3 -m silicon_qualification.api --database silicon.sqlite3 --host 127.0.0.1 --port 8082
PYTHONPATH=src python3 -m demand_response.api --database demand_response.sqlite3 --host 127.0.0.1 --port 8083
```

服务均提供 `GET /health`，其余接口使用 JSON。进程重启后可以继续查询 SQLite 中的业务状态和审计历史。
