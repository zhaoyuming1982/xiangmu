# 数据库操作说明

本文档记录本项目本地 MySQL 数据库的自动建表 SQL、表用途和字段含义。主程序为 `C:\xiangmu\更新程序\更新程序\main.py`，运行后会自动创建数据库和基础表，再下载 baostock 数据并写入本地 MySQL。

## 数据库配置

数据库连接配置由程序中现有 `MYSQL_*` 配置项读取，本次不修改数据库配置文件。

当前程序使用的数据库名：

```sql
stock_db
```

自动建库 SQL：

```sql
CREATE DATABASE IF NOT EXISTS `stock_db`
DEFAULT CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;
```

## 表结构总览

程序会自动维护三类表：

| 表名 | 用途 |
| --- | --- |
| `stock_daily` | 股票日线主表，保存所有下载到的日线行情和计算后的衍生指标。 |
| `stock_limit` | 涨跌停明细表，从 `stock_daily` 中筛选 `limit_status IN (1, 2)` 的记录同步得到。 |
| 行业表 | 按 `industry` 字段动态创建，一行业一张表，结构与 `stock_daily` 相同，用于按行业拆分查询。 |

行业表表名由行业名称清洗后生成，只保留中文、英文、数字和下划线。例如行业为 `银行` 时，表名为 `银行`。

## 通用建表 SQL

`stock_daily`、`stock_limit` 和所有行业表都使用同一套字段结构。

`stock_daily` 建表 SQL：

```sql
CREATE TABLE IF NOT EXISTS `stock_daily` (
    id BIGINT NOT NULL AUTO_INCREMENT,
    date DATE NOT NULL COMMENT '交易日期',
    code VARCHAR(16) NOT NULL COMMENT '股票代码，格式如 sh.600000',
    code_name VARCHAR(64) NOT NULL DEFAULT '' COMMENT '股票名称',
    industry VARCHAR(128) NOT NULL DEFAULT '未知行业' COMMENT '所属行业',
    preclose DECIMAL(12,4) NULL COMMENT '前收盘价',
    open DECIMAL(12,4) NULL COMMENT '开盘价',
    high DECIMAL(12,4) NULL COMMENT '最高价',
    low DECIMAL(12,4) NULL COMMENT '最低价',
    close DECIMAL(12,4) NULL COMMENT '收盘价',
    intra_pct DECIMAL(12,4) NULL COMMENT '日内涨跌幅，(close-open)/open*100',
    amplitude DECIMAL(12,4) NULL COMMENT '振幅，(high-low)/preclose*100',
    up_limit DECIMAL(12,4) NULL COMMENT '涨停价',
    down_limit DECIMAL(12,4) NULL COMMENT '跌停价',
    limit_status TINYINT NOT NULL DEFAULT 0 COMMENT '涨跌停状态：0普通，1涨停，2跌停',
    limit_days INT NOT NULL DEFAULT 0 COMMENT '连续涨停天数',
    volume BIGINT NULL COMMENT '成交量',
    amount DECIMAL(20,4) NULL COMMENT '成交额',
    turn DECIMAL(12,4) NULL COMMENT '换手率',
    pctChg DECIMAL(12,4) NULL COMMENT '涨跌幅',
    volume_ratio DECIMAL(12,4) NULL COMMENT '5日均量比',
    vol_ratio_today DECIMAL(12,4) NULL COMMENT '今日成交量/昨日成交量',
    circ_market DECIMAL(24,2) NULL COMMENT '流通市值估算',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_code_date (code, date),
    KEY idx_date (date),
    KEY idx_code (code),
    KEY idx_industry (industry),
    KEY idx_limit_status (limit_status)
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci
COMMENT='股票日线行情及衍生指标表';
```

`stock_limit` 建表 SQL：

```sql
CREATE TABLE IF NOT EXISTS `stock_limit` LIKE `stock_daily`;
```

实际程序中为了保证所有表结构一致，会直接使用与 `stock_daily` 相同的完整字段 SQL 创建 `stock_limit`。

行业表建表 SQL 模板：

```sql
CREATE TABLE IF NOT EXISTS `行业表名` (
    -- 字段结构与 stock_daily 完全一致
);
```

## 字段详细说明

| 字段 | 类型 | 来源/计算方式 | 说明 |
| --- | --- | --- | --- |
| `id` | `BIGINT` | 数据库自增 | 表内唯一主键。 |
| `date` | `DATE` | baostock | 交易日期。 |
| `code` | `VARCHAR(16)` | baostock | 股票代码，格式如 `sh.600000`、`sz.000001`。 |
| `code_name` | `VARCHAR(64)` | baostock 全量股票列表 | 股票名称。 |
| `industry` | `VARCHAR(128)` | `C:\xiangmu\stock_industry.csv` 匹配 | 股票所属行业，无法匹配时为 `未知行业`。 |
| `preclose` | `DECIMAL(12,4)` | baostock | 前一交易日收盘价。 |
| `open` | `DECIMAL(12,4)` | baostock | 当日开盘价。 |
| `high` | `DECIMAL(12,4)` | baostock | 当日最高价。 |
| `low` | `DECIMAL(12,4)` | baostock | 当日最低价。 |
| `close` | `DECIMAL(12,4)` | baostock | 当日收盘价。 |
| `intra_pct` | `DECIMAL(12,4)` | 程序计算 | 日内涨跌幅，公式为 `(close - open) / open * 100`。 |
| `amplitude` | `DECIMAL(12,4)` | 程序计算 | 振幅，公式为 `(high - low) / preclose * 100`。 |
| `up_limit` | `DECIMAL(12,4)` | 程序计算 | 涨停价。创业板/科创板按 20% 计算，其他按 10% 计算。 |
| `down_limit` | `DECIMAL(12,4)` | 程序计算 | 跌停价。创业板/科创板按 20% 计算，其他按 10% 计算。 |
| `limit_status` | `TINYINT` | 程序计算 | 涨跌停状态：`0` 普通，`1` 涨停，`2` 跌停。 |
| `limit_days` | `INT` | 程序计算 | 连续涨停天数；非涨停日为 `0`。 |
| `volume` | `BIGINT` | baostock | 成交量。 |
| `amount` | `DECIMAL(20,4)` | baostock | 成交额。 |
| `turn` | `DECIMAL(12,4)` | baostock | 换手率。 |
| `pctChg` | `DECIMAL(12,4)` | baostock | 当日涨跌幅。 |
| `volume_ratio` | `DECIMAL(12,4)` | 程序计算 | 当前成交量 / 最近 5 个历史交易日平均成交量。 |
| `vol_ratio_today` | `DECIMAL(12,4)` | 程序计算 | 当前成交量 / 上一交易日成交量。 |
| `circ_market` | `DECIMAL(24,2)` | 程序估算 | 流通市值估算，当前公式为 `volume / turn * close * 100`，当 `turn <= 0` 时为 `0`。 |
| `created_at` | `TIMESTAMP` | 数据库自动生成 | 数据插入时间。 |
| `updated_at` | `TIMESTAMP` | 数据库自动更新 | 数据最后更新时间。 |

## 数据写入流程

1. 主程序登录 baostock，获取最近可用交易日。
2. 自动创建 `stock_db` 数据库。
3. 自动创建 `stock_daily` 和 `stock_limit` 基础表。
4. 根据 baostock 全量股票列表生成 `C:\xiangmu\stock_task.csv`，并用 `stock_industry.csv` 匹配行业。
5. 根据 `stock_daily` 中每只股票的最大日期更新任务起始日期，实现断点续传。
6. 启动 10 个 worker 并行下载日线行情，写入 `stock_daily`。
7. 执行兜底下载，确保遗漏数据补齐。
8. 计算日内涨跌幅、振幅、涨跌停价、涨跌停状态、连板天数、量比、流通市值等衍生字段。
9. 将涨停/跌停记录同步到 `stock_limit`。
10. 按行业自动创建行业表，并把 `stock_daily` 数据分批同步到对应行业表。

## 去重规则

所有行情明细表都使用唯一键：

```sql
UNIQUE KEY uk_code_date (code, date)
```

含义：同一只股票同一交易日只保存一条记录。下载和同步时遇到重复数据不会重复插入。

