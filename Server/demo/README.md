# Demo 数据目录（与 `main/views.py` 对应）

后端从本目录 **只读 JSON**。路径为 **`Server/demo/`**。

## 表数据（IMDB 全量）

`scripts/generate_demo_data.py` 从 **`ASM/datasets/imdb/<表名>.csv`** 读入 **全部行**（默认不限制行数），与 `Web/src/api/mock.ts` 中 **`AVAILABLE_TABLES`** 列出的 **21 张表** 一一对应，生成：

```
demo/tables/aka_name.json
demo/tables/aka_title.json
...（共 21 个）
```

```bash
cd Server/demo
# 读入每张 CSV 的全部行（大表耗内存与磁盘，请预留空间）
python3 scripts/generate_demo_data.py

# 仅调试：每张表最多 1000 行
python3 scripts/generate_demo_data.py --max-rows 1000
```

**注意**：`cast_info`、`movie_info` 等表极大时，生成的 JSON 可达数 GB，浏览器一次加载可能卡顿或超时；必要时调大前端 `axios` 超时、或后端改为分页（需另行开发）。

## JOB 结果

`demo/response/<method>/<job_id>.json` 仍由同一脚本生成（与 Cardinality method / JOB 编号对应）。

## JSON 形状

见此前约定：`tables/*.json` 顶层为 `{"table": { col_name, col_type, rows }}`，空 CSV 单元格为 `null`。
