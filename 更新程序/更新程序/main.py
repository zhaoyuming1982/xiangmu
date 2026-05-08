import baostock as bs
import pandas as pd
from datetime import datetime, timedelta
import pymysql
import subprocess
import sys
import os
import time
import threading
import re
import html
from concurrent.futures import ThreadPoolExecutor, as_completed
# ====================== 配置 ======================
fields = "date,code,preclose,open,high,low,close,volume,amount,turn,pctChg"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = r"C:\xiangmu\stock_task.csv"
INDUSTRY_PATH = r"C:\xiangmu\stock_industry.csv"
INDUSTRY_LOG_PATH = r"C:\xiangmu\industry.txt"
FORCE_INCLUDE_CODES = {"600340"}

MYSQL_USER = "root"
MYSQL_PASSWORD = "Root123456"
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_DB = "stock_db"

BASE_UPDATE_ID_BATCH_SIZE = 100000
METRIC_CODE_BATCH_SIZE = 500
METRIC_SYNC_WORKERS = 10
INDUSTRY_SYNC_DATE_BATCH_DAYS = 90
INDUSTRY_SYNC_ROW_BATCH_SIZE = 10000
INDUSTRY_SYNC_WORKERS = 10

STOCK_RECORD_COLUMNS = """
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
"""

# 确保目录存在
os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)

# ====================== 数据库连接 ======================
def get_server_conn():
    return pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        port=MYSQL_PORT,
        charset="utf8mb4"
    )


def get_conn():
    return pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        port=MYSQL_PORT,
        charset="utf8mb4"
    )


def quote_identifier(identifier):
    return f"`{str(identifier).replace('`', '``')}`"


def iter_batches(items, batch_size):
    for start in range(0, len(items), batch_size):
        yield items[start:start + batch_size]


def normalize_date_value(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return datetime.strptime(value[:10], "%Y-%m-%d").date()
    return value


def current_thread_label(fallback=False):
    if fallback:
        return "单线程兜底"

    name = threading.current_thread().name
    if "_" in name:
        suffix = name.rsplit("_", 1)[-1]
        if suffix.isdigit():
            return f"线程{int(suffix) + 1}"
    return name


def ensure_index(cursor, table_name, index_name, index_sql):
    cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = %s
          AND index_name = %s
    """, (table_name, index_name))
    if cursor.fetchone()[0] == 0:
        cursor.execute(index_sql)


def ensure_column(cursor, table_name, column_name, column_sql):
    cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = %s
          AND column_name = %s
    """, (table_name, column_name))
    if cursor.fetchone()[0] == 0:
        cursor.execute(f"ALTER TABLE {quote_identifier(table_name)} ADD COLUMN {column_sql}")


def ensure_stock_daily_performance_indexes(cursor):
    ensure_index(
        cursor,
        "stock_daily",
        "idx_metric_volume_pending",
        "ALTER TABLE stock_daily ADD INDEX idx_metric_volume_pending (volume_ratio, code)"
    )
    ensure_index(
        cursor,
        "stock_daily",
        "idx_metric_limit_pending",
        "ALTER TABLE stock_daily ADD INDEX idx_metric_limit_pending (limit_status, limit_days, code)"
    )


def ensure_industry_sync_performance_indexes(cursor):
    ensure_column(
        cursor,
        "stock_daily",
        "updated_at",
        "updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
    )
    ensure_index(
        cursor,
        "stock_daily",
        "idx_industry_updated_id",
        "ALTER TABLE stock_daily ADD INDEX idx_industry_updated_id (industry, updated_at, id)"
    )


def ensure_industry_sync_state_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS industry_sync_state (
            industry VARCHAR(128) NOT NULL,
            synced_at TIMESTAMP NULL,
            row_count BIGINT NOT NULL DEFAULT 0,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (industry)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
          COMMENT='行业表增量同步水位表'
    """)


def create_stock_record_table(cursor, table_name):
    table = quote_identifier(table_name)
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            {STOCK_RECORD_COLUMNS}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
          COMMENT='股票日线行情及衍生指标表';
    """)


def init_database_schema():
    """
    启动下载前自动确保数据库和基础表存在。
    数据库连接配置仍使用上方原有 MYSQL_* 常量，不修改任何配置文件。
    """
    print("\n" + "=" * 50)
    print(" 初始化 MySQL 数据库和基础表")
    print("=" * 50)

    server_conn = get_server_conn()
    server_cursor = server_conn.cursor()
    server_cursor.execute(
        f"CREATE DATABASE IF NOT EXISTS {quote_identifier(MYSQL_DB)} "
        "DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
    )
    server_conn.commit()
    server_cursor.close()
    server_conn.close()

    conn = get_conn()
    cursor = conn.cursor()
    create_stock_record_table(cursor, "stock_daily")
    create_stock_record_table(cursor, "stock_limit")
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ 数据库与 stock_daily / stock_limit 表已就绪")


bs.login()

# ====================== 获取最新交易日 ======================
def get_last_trade_day():
    today = datetime.now()
    for i in range(10):
        day = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        df = bs.query_all_stock(day=day).get_data()
        if not df.empty:
            return day
    return today.strftime("%Y-%m-%d")

LATEST_DAY = get_last_trade_day()


def normalize_code_num(code):
    return str(code).strip().split(".")[-1].zfill(6)


def force_include_code_nums():
    return {normalize_code_num(code) for code in FORCE_INCLUDE_CODES}


def append_industry_log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(INDUSTRY_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")


def extract_final_industry(industry_text):
    text = html.unescape(str(industry_text))
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return ""

    for sep in ["—", "--", "-", "－", ">", "＞", "/"]:
        if sep in text:
            text = text.split(sep)[-1].strip()
    return text


def fetch_ths_industry(code_num):
    """
    从同花顺 F10 页面获取股票所属申万行业，返回末级行业名称。
    示例：电力设备 — 电池 -> 电池。
    """
    import requests

    code_num = normalize_code_num(code_num)
    urls = [
        f"https://basic.10jqka.com.cn/{code_num}/company.html",
        f"https://basic.10jqka.com.cn/{code_num}/",
    ]
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0 Safari/537.36"
        )
    }

    patterns = [
        r"所属申万行业[：:]\s*</strong>\s*<span[^>]*>(.*?)</span>",
        r"所属申万行业[：:]\s*</span>\s*<span[^>]*>(.*?)</span>",
        r"所属申万行业[：:]\s*([^<\n\r]+)",
    ]

    last_error = ""
    for url in urls:
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            text = response.content.decode("gbk", errors="ignore")
            for pattern in patterns:
                match = re.search(pattern, text, re.S)
                if match:
                    industry = extract_final_industry(match.group(1))
                    if industry:
                        return industry, url
        except Exception as e:
            last_error = str(e)

    raise RuntimeError(last_error or "同花顺页面未解析到所属申万行业")


def append_stock_industry_records(records):
    """
    将补齐的同花顺行业追加到 stock_industry.csv。
    records: [{"code_num": "301599", "code_name": "理奇智能", "industry": "电池"}]
    """
    if not records:
        return 0

    if os.path.exists(INDUSTRY_PATH):
        df_ind = pd.read_csv(INDUSTRY_PATH, dtype=str)
        column_map = {str(col).strip(): col for col in df_ind.columns}
        name_col = column_map.get("名称", "名称")
        industry_col = column_map.get("所属行业", "所属行业")
        code_col = column_map.get("代码", "代码")
        for col in [name_col, industry_col, code_col]:
            if col not in df_ind.columns:
                df_ind[col] = ""
    else:
        name_col, industry_col, code_col = "名称", "所属行业", "代码"
        df_ind = pd.DataFrame(columns=[name_col, industry_col, code_col])

    existing_codes = set(df_ind[code_col].astype(str).str.strip().str.zfill(6))
    new_rows = []
    for record in records:
        code_num = normalize_code_num(record["code_num"])
        if code_num in existing_codes:
            continue
        row = {col: "" for col in df_ind.columns}
        row[name_col] = record["code_name"]
        row[industry_col] = record["industry"]
        row[code_col] = code_num
        new_rows.append(row)
        existing_codes.add(code_num)

    if not new_rows:
        return 0

    df_ind = pd.concat([df_ind, pd.DataFrame(new_rows)], ignore_index=True)
    df_ind[code_col] = df_ind[code_col].astype(str).str.strip().str.zfill(6)
    df_ind.to_csv(INDUSTRY_PATH, index=False, encoding="utf-8-sig")
    return len(new_rows)


def auto_update_stock_industry_for_unknown(df_task):
    unknown_df = df_task[df_task["industry"].fillna("").astype(str).str.strip().eq("未知行业")].copy()
    if unknown_df.empty:
        print("✅ stock_task.csv 未发现未知行业")
        return df_task

    print(f"🔎 发现未知行业股票 {len(unknown_df)} 只，开始从同花顺补齐行业")
    append_industry_log(f"开始检查未知行业股票：{len(unknown_df)} 只")

    records = []
    for _, row in unknown_df.iterrows():
        code = str(row["code"]).strip()
        code_num = normalize_code_num(code)
        code_name = str(row["code_name"]).strip()
        try:
            industry, source_url = fetch_ths_industry(code_num)
            records.append({
                "code_num": code_num,
                "code_name": code_name,
                "industry": industry,
            })
            df_task.loc[df_task["code"].eq(code), "industry"] = industry
            append_industry_log(
                f"成功 | 代码：{code_num} | 名称：{code_name} | 行业：{industry} | 来源：{source_url}"
            )
            print(f"✅ 同花顺行业补齐：{code_num} {code_name} -> {industry}")
        except Exception as e:
            append_industry_log(f"失败 | 代码：{code_num} | 名称：{code_name} | 原因：{str(e)}")
            print(f"⚠️ 同花顺行业获取失败：{code_num} {code_name} | {str(e)}")

    added_count = append_stock_industry_records(records)
    if added_count:
        print(f"✅ 已追加 {added_count} 条到 stock_industry.csv，并记录 industry.txt")
    else:
        print("ℹ️ 没有新增行业记录需要追加到 stock_industry.csv")

    return df_task


# ====================== 初始化 stock_task.csv ======================
def init_task_csv():
    # 1. 获取全量股票
    df_stock = bs.query_all_stock(day=LATEST_DAY).get_data()

    df_stock.rename(columns={"ts_code": "code"}, inplace=True)
    
    # 2. 筛选主板/创业板/科创板等，剔除ST、退市；白名单股票保留。
    df_plate = df_stock[
        df_stock["code"].str.startswith(("sh.60",  "sh.68" )) |
        df_stock["code"].str.startswith(("sz.00",  "sz.30"))
    ].copy()
    df_plate["code_num"] = df_plate["code"].map(normalize_code_num)
    force_mask = df_plate["code_num"].isin(force_include_code_nums())
    df_no_st = df_plate[(~df_plate["code_name"].str.contains("ST", na=False)) | force_mask]
    df_final = df_no_st[(~df_no_st["code_name"].str.contains("退", na=False)) | force_mask].copy()
    
    # 3. 提取纯数字代码
    df_final["code_num"] = df_final["code"].map(normalize_code_num)
    df_final["industry"] = "未知行业"
    
    # 4. 匹配行业文件
    if os.path.exists(INDUSTRY_PATH):
        try:
            df_ind = pd.read_csv(INDUSTRY_PATH, dtype=str)
            df_ind.columns = df_ind.columns.str.strip()
            
            if "代码" in df_ind.columns and "所属行业" in df_ind.columns:
                df_ind["代码"] = df_ind["代码"].astype(str).str.strip().str.zfill(6)
                
                df_final = df_final.merge(
                    df_ind[["代码", "所属行业"]],
                    left_on="code_num",
                    right_on="代码",
                    how="left"
                )
                df_final["industry"] = df_final["所属行业"].fillna("未知行业")
        except Exception as e:
            print(f"⚠️ 行业文件读取失败：{str(e)}")

    # 4.1 保留已有 stock_task.csv 中手动维护的行业，避免重新初始化时被覆盖。
    if os.path.exists(CSV_PATH):
        try:
            df_old = pd.read_csv(CSV_PATH, dtype=str)
            if "code" in df_old.columns and "industry" in df_old.columns:
                df_old["code"] = df_old["code"].astype(str).str.strip()
                df_old["industry"] = df_old["industry"].astype(str).str.strip()
                manual_industry = df_old[
                    (df_old["industry"] != "") &
                    (df_old["industry"] != "nan") &
                    (df_old["industry"] != "未知行业")
                ].drop_duplicates("code").set_index("code")["industry"]

                if not manual_industry.empty:
                    old_industry = df_final["industry"].copy()
                    df_final["industry"] = df_final["code"].map(manual_industry).fillna(df_final["industry"])
                    preserved = (df_final["industry"] != old_industry).sum()
                    if preserved:
                        print(f"✅ 已保留 stock_task.csv 手动维护行业：{preserved} 只股票")
        except Exception as e:
            print(f"⚠️ 读取旧 stock_task.csv 手动行业失败：{str(e)}")
    
    # 5. 生成自增 id（从 1 开始）
    df_final.insert(0, "id", range(1, len(df_final) + 1))

    # 6. 生成任务参数
    df_final["start_date"] = "2024-01-01"
    df_final["end_date"] = LATEST_DAY
    df_final["frequency"] = "d"
    df_final["adjustflag"] = 3
    df_final["fields"] = fields
    
    # 7. 最终列（包含 id）
    result = df_final[[
        "id", "code", "code_name", "industry", "start_date", "end_date", "frequency", "adjustflag", "fields"
    ]]
    result = auto_update_stock_industry_for_unknown(result)
    
    result.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
    print(f"✅ stock_task.csv 初始化完成，共 {len(result)} 只股票")

# ====================== 更新 CSV 中的 start_date（断点续传）======================
def update_csv_start_date():
    if not os.path.exists(CSV_PATH):
        print("❌ 未找到 stock_task.csv，跳过更新")
        return
    
    df = pd.read_csv(CSV_PATH, dtype={"code": str, "id": int})
    conn = get_conn()
    cursor = conn.cursor()
    
    for idx, row in df.iterrows():
        code = row["code"]
        cursor.execute("SELECT MAX(date) FROM stock_daily WHERE code = %s", (code,))
        last_date = cursor.fetchone()[0]
        if last_date is not None:
            new_start = (datetime.strptime(str(last_date), "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            df.at[idx, "start_date"] = new_start
    
    df.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
    conn.close()
    print("✅ stock_task.csv 起始日期更新完成")

# ====================== 启动 10 个worker ======================


def start_workers():
    print("\n🚀 启动 10 个并行下载脚本...")
    
    # 保存 worker 编号、进程、启动时间
    workers = []
    for i in range(1, 11):
        worker_path = os.path.join(SCRIPT_DIR, f"worker{i}.py")
        p = subprocess.Popen([sys.executable, worker_path], cwd=SCRIPT_DIR)
        workers.append({
            "id": i,
            "process": p,
            "start_time": time.time()  # 记录启动时间
        })

    # 最大运行时间：10分钟 = 600 秒
    MAX_RUN_SECONDS = 600

    while workers:
        for index, worker in enumerate(workers):
            wid = worker["id"]
            p = worker["process"]
            start_time = worker["start_time"]
            now = time.time()

            # --------------------------
            # 情况1：进程已经正常结束
            # --------------------------
            if p.poll() is not None:
                workers.pop(index)
                print(f"✅ worker{wid} 正常执行完毕并退出")
                break

            # --------------------------
            # 情况2：超时 → 强制杀死
            # --------------------------
            if now - start_time > MAX_RUN_SECONDS:
                try:
                    p.terminate()  # 温和关闭
                    time.sleep(0.5)
                    p.kill()       # 强制杀死
                except:
                    pass
                workers.pop(index)
                print(f"⏹️ worker{wid} 运行超过10分钟，已强制关闭")
                break

        # 正在运行的列表
        running = [f"worker{x['id']}" for x in workers]
        if running:
            print(f"📌 仍在运行：{', '.join(running)} | 每2秒检查一次")
        time.sleep(2)

    print("\n🎉 所有10个下载进程 全部执行/强制关闭 完毕！")
    
    # ====================== 兜底全量下载 ======================
def final_full_download():
    print("\n" + "=" * 50)
    print(" 开始【兜底全量下载】，确保所有数据完整无遗漏")
    print("=" * 50)

    if not os.path.exists(CSV_PATH):
        print("❌ 无任务文件，跳过兜底")
        return

    df = pd.read_csv(CSV_PATH, dtype={"code": str, "id": int})
    conn = get_conn()
    cursor = conn.cursor()

    for _, row in df.iterrows():
        code = row["code"]
        code_name = row["code_name"]
        industry = row["industry"]
        start_date = str(row["start_date"])
        end_date = str(row["end_date"])
        frequency = str(row["frequency"])
        adjustflag = str(row["adjustflag"])
        fields = row["fields"]

        if start_date > end_date:
            continue

        print(f"📥 兜底下载 {code} {code_name} | {start_date} ~ {end_date}")

        rs = bs.query_history_k_data_plus(
            code=code,
            fields=fields,
            start_date=start_date,
            end_date=end_date,
            frequency=frequency,
            adjustflag=adjustflag
        )
        df_data = rs.get_data()

        if not df_data.empty:
            for _, r in df_data.iterrows():
                sql = """
                INSERT IGNORE INTO stock_daily
                (date, code, code_name, industry, preclose, open, high, low, close, volume, amount, turn, pctChg)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """
                cursor.execute(sql, (
                    r["date"], r["code"], code_name, industry,
                    r["preclose"], r["open"], r["high"], r["low"], r["close"],
                    r["volume"], r["amount"], r["turn"], r["pctChg"]
                ))
            conn.commit()

            
    cursor.execute("SELECT COUNT(*) FROM stock_daily")
    total_count = cursor.fetchone()[0]

    # 查询不重复股票数量
    cursor.execute("SELECT COUNT(DISTINCT code) FROM stock_daily")
    stock_count = cursor.fetchone()[0]

    # 打印结果
    print("\n==================================")
    print(f"📊 股票日线数据库统计")
    print(f"✅ 总数据条数：{total_count:,} 条")
    print(f"✅ 股票数量：{stock_count} 只")
    print("==================================\n")


    
    cursor.close()
    conn.close()
    print("✅ 兜底下载完成！所有股票数据已确保完整")

# ====================== 终极正确版：连板天数真正连续计算 ======================
def get_pending_metric_codes(cursor):
    cursor.execute("""
        SELECT code
        FROM stock_daily
        WHERE volume_ratio IS NULL
        GROUP BY code
        UNION
        SELECT code
        FROM stock_daily
        WHERE limit_status = 1 AND limit_days = 0
        GROUP BY code
        ORDER BY code
    """)
    return [row[0] for row in cursor.fetchall()]


def calculate_metric_code_batch(batch_index, code_batch, fallback=False):
    conn = get_conn()
    cursor = conn.cursor()
    label = current_thread_label(fallback)

    try:
        placeholders = ",".join(["%s"] * len(code_batch))
        cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_stock_metrics")
        cursor.execute("""
            CREATE TEMPORARY TABLE tmp_stock_metrics (
                id BIGINT NOT NULL,
                volume_ratio DECIMAL(12,4) NULL,
                vol_ratio_today DECIMAL(12,4) NULL,
                limit_days INT NOT NULL DEFAULT 0,
                PRIMARY KEY (id)
            ) ENGINE=InnoDB
        """)

        cursor.execute(f"""
            INSERT INTO tmp_stock_metrics (id, volume_ratio, vol_ratio_today, limit_days)
            WITH base_rows AS (
                SELECT
                    id,
                    code,
                    date,
                    volume,
                    limit_status,
                    AVG(volume) OVER (
                        PARTITION BY code
                        ORDER BY date ASC
                        ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
                    ) AS avg_prev5_volume,
                    LAG(volume) OVER (
                        PARTITION BY code
                        ORDER BY date ASC
                    ) AS prev_volume
                FROM stock_daily
                WHERE code IN ({placeholders})
            ),
            grouped_rows AS (
                SELECT
                    id,
                    code,
                    date,
                    volume,
                    limit_status,
                    avg_prev5_volume,
                    prev_volume,
                    SUM(CASE WHEN limit_status = 1 THEN 0 ELSE 1 END) OVER (
                        PARTITION BY code
                        ORDER BY date ASC
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS streak_group
                FROM base_rows
            ),
            calculated_rows AS (
                SELECT
                    id,
                    CASE
                        WHEN avg_prev5_volume > 0 THEN ROUND(volume / avg_prev5_volume, 2)
                        ELSE 0.0
                    END AS volume_ratio,
                    CASE
                        WHEN prev_volume > 0 THEN ROUND(volume / prev_volume, 2)
                        ELSE NULL
                    END AS vol_ratio_today,
                    CASE
                        WHEN limit_status = 1 THEN
                            SUM(CASE WHEN limit_status = 1 THEN 1 ELSE 0 END) OVER (
                                PARTITION BY code, streak_group
                                ORDER BY date ASC
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                            )
                        ELSE 0
                    END AS limit_days
                FROM grouped_rows
            )
            SELECT id, volume_ratio, vol_ratio_today, limit_days
            FROM calculated_rows
        """, code_batch)
        metrics_count = cursor.rowcount

        cursor.execute("""
            UPDATE stock_daily AS d
            JOIN tmp_stock_metrics AS m ON d.id = m.id
            SET
                d.volume_ratio = m.volume_ratio,
                d.vol_ratio_today = m.vol_ratio_today,
                d.limit_days = m.limit_days
            WHERE NOT (d.volume_ratio <=> m.volume_ratio)
               OR NOT (d.vol_ratio_today <=> m.vol_ratio_today)
               OR NOT (d.limit_days <=> m.limit_days)
        """)
        update_count = cursor.rowcount
        conn.commit()

        return metrics_count, update_count, (
            f"{label} | 指标批次：{batch_index:3d} | 股票：{len(code_batch)} 只 | "
            f"计算：{metrics_count} 条 | 回写：{update_count} 条"
        )

    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"{label} | 指标批次：{batch_index:3d} | 失败：{str(e)}") from e
    finally:
        cursor.close()
        conn.close()


def calculate_all_stock_fields():
    import warnings
    warnings.filterwarnings('ignore')

    print("\n" + "=" * 60)
    print(" 第一步：批量更新基础字段（日内涨跌幅、振幅、涨跌停价、状态、流通市值）")
    print("=" * 60)

    conn = get_conn()
    cursor = conn.cursor()

    try:
        sql_base = """
        UPDATE stock_daily
        SET
            intra_pct     = CASE WHEN open > 0 THEN ROUND((close - open) / open * 100, 2) ELSE NULL END,
            amplitude     = CASE WHEN preclose > 0 THEN ROUND((high - low) / preclose * 100, 2) ELSE NULL END,
            up_limit      = ROUND(preclose * CASE WHEN code_name LIKE '%ST%' THEN 1.05 WHEN code RLIKE '^(sz\\.30|sh\\.68)' THEN 1.2 ELSE 1.1 END, 2),
            down_limit    = ROUND(preclose * CASE WHEN code_name LIKE '%ST%' THEN 0.95 WHEN code RLIKE '^(sz\\.30|sh\\.68)' THEN 0.8 ELSE 0.9 END, 2),
            limit_status  = CASE
                WHEN close = ROUND(preclose * CASE WHEN code_name LIKE '%ST%' THEN 1.05 WHEN code RLIKE '^(sz\\.30|sh\\.68)' THEN 1.2 ELSE 1.1 END, 2) THEN 1
                WHEN close = ROUND(preclose * CASE WHEN code_name LIKE '%ST%' THEN 0.95 WHEN code RLIKE '^(sz\\.30|sh\\.68)' THEN 0.8 ELSE 0.9 END, 2) THEN 2
                ELSE 0
            END,
            circ_market   = CASE WHEN turn > 0 THEN ROUND(volume / turn * close * 100, 2) ELSE 0 END
        WHERE id >= %s
          AND id <= %s
          AND (
              intra_pct IS NULL
              OR amplitude IS NULL
              OR up_limit IS NULL
              OR down_limit IS NULL
              OR circ_market IS NULL
          );
        """

        cursor.execute("SELECT MIN(id), MAX(id) FROM stock_daily")
        min_id, max_id = cursor.fetchone()
        base_update_total = 0

        if min_id is not None and max_id is not None:
            batch_start = min_id
            while batch_start <= max_id:
                batch_end = batch_start + BASE_UPDATE_ID_BATCH_SIZE - 1
                cursor.execute(sql_base, (batch_start, batch_end))
                base_update_total += cursor.rowcount
                conn.commit()
                print(
                    f"基础字段批次：{batch_start}-{min(batch_end, max_id)} | "
                    f"更新：{cursor.rowcount} 条"
                )
                batch_start = batch_end + 1

        print(f"✅ 基础字段分批更新完成：{base_update_total} 条")

        print("\n" + "=" * 60)
        print(" 第二步：MySQL窗口函数计算量比、今日量比、连板天数")
        print("=" * 60)

        ensure_stock_daily_performance_indexes(cursor)
        conn.commit()

        codes = get_pending_metric_codes(cursor)
        total_codes = len(codes)
        total_metrics = 0
        total_updates = 0

        if total_codes == 0:
            print("✅ 没有发现需要重算指标的股票，跳过第三步")
            return

        print("\n" + "=" * 60)
        print(f" 第三步：启动 {METRIC_SYNC_WORKERS} 线程按股票代码分批生成临时结果并回写")
        print("=" * 60)

        code_batches = list(iter_batches(codes, METRIC_CODE_BATCH_SIZE))
        with ThreadPoolExecutor(max_workers=METRIC_SYNC_WORKERS, thread_name_prefix="指标线程") as executor:
            futures = [
                executor.submit(calculate_metric_code_batch, batch_index, code_batch)
                for batch_index, code_batch in enumerate(code_batches, start=1)
            ]
            for future in as_completed(futures):
                metrics_count, update_count, message = future.result()
                total_metrics += metrics_count
                total_updates += update_count
                print(message)

        fallback_codes = get_pending_metric_codes(cursor)
        fallback_metrics = 0
        fallback_updates = 0
        if fallback_codes:
            print("\n🧯 开始单线程兜底检查指标计算")
            for batch_index, code_batch in enumerate(iter_batches(fallback_codes, METRIC_CODE_BATCH_SIZE), start=1):
                metrics_count, update_count, message = calculate_metric_code_batch(
                    batch_index,
                    code_batch,
                    fallback=True
                )
                fallback_metrics += metrics_count
                fallback_updates += update_count
                print(message)
            total_metrics += fallback_metrics
            total_updates += fallback_updates
            print(f"🧯 指标单线程兜底完成：计算 {fallback_metrics} 条，回写 {fallback_updates} 条")
        else:
            print("🧯 指标单线程兜底检查完成：无遗漏")

        print(f"✅ 指标分批计算完成：计算 {total_metrics} 条，回写 {total_updates} 条")

    except Exception as e:
        conn.rollback()
        print(f"❌ 指标计算失败：{str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

    print("\n🎉 指标更新完成！MySQL窗口函数计算，无需Python全量读入内存！")

# ====================== 往涨跌停表stock_limit里写数据 ======================

def sync_stock_limit_data():
    # 从 stock_daily 同步 涨停/跌停 数据到 stock_limit 表
    print("\n" + "="*50)
    print(" 开始同步【涨跌停数据】到 stock_limit 表")
    print("="*50)

    conn = get_conn()
    cursor = conn.cursor()

    sql = """
    INSERT IGNORE INTO stock_limit
    (
        date, code, code_name, industry,
        preclose, open, high, low, close,
        intra_pct, amplitude, up_limit, down_limit, limit_status, limit_days,
        volume, amount, turn, pctChg, volume_ratio, vol_ratio_today, circ_market
    )
    SELECT
        date, code, code_name, industry,
        preclose, open, high, low, close,
        intra_pct, amplitude, up_limit, down_limit, limit_status, limit_days,
        volume, amount, turn, pctChg, volume_ratio, vol_ratio_today, circ_market
    FROM stock_daily
    WHERE limit_status IN (1, 2)
    """

    try:
        cursor.execute(sql)
        conn.commit()
        print(f"✅ 同步成功！本次新增涨跌停数据：{cursor.rowcount} 条")
    except Exception as e:
        print(f"❌ 同步失败：{str(e)}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

    print("✅ 涨跌停数据同步完成\n")

    # ====================== 往行业表里写数据 ======================


def sync_one_industry_table(index, industry, fallback=False):
    conn = get_conn()
    cursor = conn.cursor()
    conn.autocommit = False
    table_name = ''.join(c for c in industry if c.isalnum() or c == '_')
    if not table_name:
        table_name = "未知行业"
    label = current_thread_label(fallback)

    try:
        create_stock_record_table(cursor, table_name)

        cursor.execute(
            "SELECT COUNT(*), MAX(updated_at) FROM stock_daily WHERE industry = %s",
            (industry,)
        )
        source_count, max_updated_at = cursor.fetchone()
        if not source_count or max_updated_at is None:
            return 0, f"{label} | 序号：{index:2d} | 写入行业：{industry:12s} | 无数据，跳过"

        cursor.execute(f"SELECT COUNT(*) FROM {quote_identifier(table_name)}")
        target_count = cursor.fetchone()[0]

        cursor.execute(
            "SELECT synced_at FROM industry_sync_state WHERE industry = %s",
            (industry,)
        )
        state_row = cursor.fetchone()
        last_synced_at = state_row[0] if state_row else None

        if fallback and target_count >= source_count and (last_synced_at is None or last_synced_at >= max_updated_at):
            cursor.execute("""
                INSERT INTO industry_sync_state (industry, synced_at, row_count)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    synced_at = VALUES(synced_at),
                    row_count = VALUES(row_count)
            """, (industry, max_updated_at, source_count))
            conn.commit()
            return 0, (
                f"{label} | 序号：{index:2d} | 写入行业：{industry:12s} | 表名：{table_name:12s} | "
                "已完整，跳过"
            )

        if fallback and target_count < source_count:
            last_synced_at = datetime(1970, 1, 1)

        if last_synced_at is None:
            if target_count >= source_count:
                cursor.execute("""
                    INSERT INTO industry_sync_state (industry, synced_at, row_count)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        synced_at = VALUES(synced_at),
                        row_count = VALUES(row_count)
                """, (industry, max_updated_at, source_count))
                conn.commit()
                return 0, (
                    f"{label} | 序号：{index:2d} | 写入行业：{industry:12s} | 表名：{table_name:12s} | "
                    "已是最新，建立同步水位后跳过"
                )
            last_synced_at = datetime(1970, 1, 1)

        insert_sql = f"""
            INSERT INTO {quote_identifier(table_name)} (
                date, code, code_name, industry, preclose, open, high, low, close,
                intra_pct, amplitude, up_limit, down_limit, limit_status, limit_days,
                volume, amount, turn, pctChg, volume_ratio, vol_ratio_today, circ_market
            )
            SELECT
                date, code, code_name, industry, preclose, open, high, low, close,
                intra_pct, amplitude, up_limit, down_limit, limit_status, limit_days,
                volume, amount, turn, pctChg, volume_ratio, vol_ratio_today, circ_market
            FROM stock_daily
            WHERE id IN ({{ids}})
            ON DUPLICATE KEY UPDATE
                code_name = VALUES(code_name),
                industry = VALUES(industry),
                preclose = VALUES(preclose),
                open = VALUES(open),
                high = VALUES(high),
                low = VALUES(low),
                close = VALUES(close),
                intra_pct = VALUES(intra_pct),
                amplitude = VALUES(amplitude),
                up_limit = VALUES(up_limit),
                down_limit = VALUES(down_limit),
                limit_status = VALUES(limit_status),
                limit_days = VALUES(limit_days),
                volume = VALUES(volume),
                amount = VALUES(amount),
                turn = VALUES(turn),
                pctChg = VALUES(pctChg),
                volume_ratio = VALUES(volume_ratio),
                vol_ratio_today = VALUES(vol_ratio_today),
                circ_market = VALUES(circ_market)
        """

        count = 0
        batch_index = 0
        batch_cursor_at = last_synced_at
        batch_cursor_id = 0
        while True:
            cursor.execute("""
                SELECT id, updated_at
                FROM stock_daily
                WHERE industry = %s
                  AND updated_at > %s
                  AND updated_at <= %s
                  AND (
                      updated_at > %s
                      OR (updated_at = %s AND id > %s)
                  )
                ORDER BY updated_at, id
                LIMIT %s
            """, (
                industry,
                last_synced_at,
                max_updated_at,
                batch_cursor_at,
                batch_cursor_at,
                batch_cursor_id,
                INDUSTRY_SYNC_ROW_BATCH_SIZE
            ))
            batch_rows = cursor.fetchall()
            if not batch_rows:
                break

            ids = [row[0] for row in batch_rows]
            placeholders = ",".join(["%s"] * len(ids))
            cursor.execute(insert_sql.replace("{ids}", placeholders), ids)
            count += cursor.rowcount
            batch_index += 1
            batch_cursor_at = batch_rows[-1][1]
            batch_cursor_id = batch_rows[-1][0]
            conn.commit()

        cursor.execute("""
            INSERT INTO industry_sync_state (industry, synced_at, row_count)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                synced_at = VALUES(synced_at),
                row_count = VALUES(row_count)
        """, (industry, max_updated_at, source_count))
        conn.commit()

        return count, f"{label} | 序号：{index:2d} | 写入行业：{industry:12s} | 表名：{table_name:12s} | 写入：{count} 条"

    except Exception as e:
        conn.rollback()
        return 0, f"{label} | 序号：{index:2d} | ⚠️ 跳过行业：{industry} | {str(e)}"
    finally:
        cursor.close()
        conn.close()


def refresh_stock_daily_industry_from_task_csv():
    """
    根据 stock_task.csv 的手动行业维护结果修正 stock_daily。
    修正后从 未知行业 表移除已归类的数据，再交给行业同步写入正确行业表。
    """
    if not os.path.exists(CSV_PATH):
        print("⚠️ 未找到 stock_task.csv，跳过行业修正")
        return

    df = pd.read_csv(CSV_PATH, dtype=str)
    required_columns = {"code", "industry"}
    if not required_columns.issubset(df.columns):
        print("⚠️ stock_task.csv 缺少 code/industry 列，跳过行业修正")
        return

    df["code"] = df["code"].astype(str).str.strip()
    df["industry"] = df["industry"].astype(str).str.strip()
    if "code_name" not in df.columns:
        df["code_name"] = ""
    else:
        df["code_name"] = df["code_name"].astype(str).str.strip()

    df = df[
        (df["code"] != "") &
        (df["industry"] != "") &
        (df["industry"] != "nan") &
        (df["industry"] != "未知行业")
    ].drop_duplicates("code")

    if df.empty:
        print("✅ stock_task.csv 暂无可用于修正的已知行业")
        return

    conn = get_conn()
    cursor = conn.cursor()

    try:
        ensure_industry_sync_state_table(cursor)

        cursor.execute("""
            CREATE TEMPORARY TABLE tmp_task_industry (
                code VARCHAR(16) COLLATE utf8mb4_unicode_ci NOT NULL,
                code_name VARCHAR(64) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
                industry VARCHAR(128) COLLATE utf8mb4_unicode_ci NOT NULL,
                PRIMARY KEY (code)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        rows = [
            (row["code"], row.get("code_name", "") or "", row["industry"])
            for _, row in df.iterrows()
        ]
        cursor.executemany(
            "INSERT INTO tmp_task_industry (code, code_name, industry) VALUES (%s, %s, %s)",
            rows
        )
        cursor.execute("""
            CREATE TEMPORARY TABLE tmp_fix_codes (
                code VARCHAR(16) COLLATE utf8mb4_unicode_ci NOT NULL,
                industry VARCHAR(128) COLLATE utf8mb4_unicode_ci NOT NULL,
                PRIMARY KEY (code)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        cursor.execute("""
            INSERT IGNORE INTO tmp_fix_codes (code, industry)
            SELECT DISTINCT d.code, t.industry
            FROM stock_daily AS d
            JOIN tmp_task_industry AS t FORCE INDEX (PRIMARY)
              ON d.code COLLATE utf8mb4_unicode_ci = t.code
            WHERE d.industry COLLATE utf8mb4_unicode_ci = _utf8mb4'未知行业' COLLATE utf8mb4_unicode_ci
        """)
        cursor.execute("""
            INSERT IGNORE INTO tmp_fix_codes (code, industry)
            SELECT DISTINCT d.code, t.industry
            FROM stock_daily AS d
            JOIN tmp_task_industry AS t FORCE INDEX (PRIMARY)
              ON d.code COLLATE utf8mb4_unicode_ci = t.code
            WHERE d.industry COLLATE utf8mb4_unicode_ci <> _utf8mb4'未知行业' COLLATE utf8mb4_unicode_ci
              AND d.industry COLLATE utf8mb4_unicode_ci <> t.industry
            LIMIT 5000
        """)
        cursor.execute("SELECT COUNT(*) FROM tmp_fix_codes")
        fix_code_count = cursor.fetchone()[0]

        if fix_code_count == 0:
            conn.commit()
            print("✅ 行业修正检查完成：没有需要修正的股票")
            return

        cursor.execute("""
            CREATE TEMPORARY TABLE tmp_changed_industry AS
            SELECT DISTINCT t.industry
            FROM tmp_fix_codes AS t
        """)

        cursor.execute("""
            UPDATE stock_daily AS d
            JOIN tmp_fix_codes AS f ON d.code COLLATE utf8mb4_unicode_ci = f.code
            JOIN tmp_task_industry AS t ON f.code = t.code
            SET
                d.industry = t.industry,
                d.code_name = CASE WHEN t.code_name <> '' THEN t.code_name ELSE d.code_name END
            WHERE d.code COLLATE utf8mb4_unicode_ci = f.code
        """)
        updated_rows = cursor.rowcount

        cursor.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
              AND table_name = 'stock_limit'
        """)
        stock_limit_exists = cursor.fetchone()[0] > 0
        limit_updated_rows = 0
        if stock_limit_exists:
            cursor.execute("""
                UPDATE stock_limit AS l
                JOIN tmp_fix_codes AS f ON l.code COLLATE utf8mb4_unicode_ci = f.code
                JOIN tmp_task_industry AS t ON f.code = t.code
                SET
                    l.industry = t.industry,
                    l.code_name = CASE WHEN t.code_name <> '' THEN t.code_name ELSE l.code_name END
                WHERE l.code COLLATE utf8mb4_unicode_ci = f.code
            """)
            limit_updated_rows = cursor.rowcount

        unknown_table = quote_identifier("未知行业")
        cursor.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
              AND table_name = '未知行业'
        """)
        unknown_exists = cursor.fetchone()[0] > 0
        removed_rows = 0
        if unknown_exists:
            cursor.execute(f"""
                DELETE u
                FROM {unknown_table} AS u
                JOIN stock_daily AS d ON u.code COLLATE utf8mb4_unicode_ci = d.code COLLATE utf8mb4_unicode_ci AND u.date = d.date
                WHERE d.industry COLLATE utf8mb4_unicode_ci <> _utf8mb4'未知行业' COLLATE utf8mb4_unicode_ci
            """)
            removed_rows = cursor.rowcount

        cursor.execute("DELETE FROM industry_sync_state WHERE industry = '未知行业'")
        cursor.execute("""
            DELETE s
            FROM industry_sync_state AS s
            JOIN tmp_changed_industry AS t ON s.industry COLLATE utf8mb4_unicode_ci = t.industry COLLATE utf8mb4_unicode_ci
        """)

        conn.commit()
        print(
            f"✅ 行业修正完成：stock_daily 更新 {updated_rows} 条，"
            f"stock_limit 更新 {limit_updated_rows} 条，未知行业表移除 {removed_rows} 条"
        )

    except Exception as e:
        conn.rollback()
        print(f"❌ 行业修正失败：{str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()


def sync_data_to_industry_tables():
    """
    数据库直写：
    1. 只读取行业清单
    2. 每个行业使用 INSERT ... SELECT 从 stock_daily 写入对应行业表
    3. 避免 Python 一次性读取 stock_daily 全量数据
    """
    conn = get_conn()
    cursor = conn.cursor()
    conn.autocommit = False

    try:
        print("🔧 正在准备行业同步索引和水位表...")
        ensure_industry_sync_performance_indexes(cursor)
        ensure_industry_sync_state_table(cursor)
        conn.commit()

        print("🧭 正在根据 stock_task.csv 修正主表行业...")
        refresh_stock_daily_industry_from_task_csv()

        print("🔍 正在读取行业清单...")
        cursor.execute("""
            SELECT DISTINCT industry
            FROM stock_daily
            WHERE industry IS NOT NULL AND TRIM(industry) <> ''
            ORDER BY industry
        """)
        industries = [row[0] for row in cursor.fetchall()]
        print(f"✅ 行业清单读取完成，共 {len(industries)} 个行业")

        total_insert = 0
        print(f"🚀 启动 {INDUSTRY_SYNC_WORKERS} 线程并发同步行业表")
        with ThreadPoolExecutor(max_workers=INDUSTRY_SYNC_WORKERS, thread_name_prefix="行业同步线程") as executor:
            futures = [
                executor.submit(sync_one_industry_table, index, industry)
                for index, industry in enumerate(industries, start=1)
            ]
            for future in as_completed(futures):
                count, message = future.result()
                total_insert += count
                print(message)

        fallback_insert = 0
        print("\n🧯 开始单线程兜底检查行业表")
        for index, industry in enumerate(industries, start=1):
            count, message = sync_one_industry_table(index, industry, fallback=True)
            fallback_insert += count
            print(message)
        total_insert += fallback_insert

        print(f"🧯 单线程兜底完成，补写：{fallback_insert} 条")
        print(f"\n🎉 全部同步完成！总新增/更新：{total_insert} 条")

    except Exception as e:
        conn.rollback()
        print(f"❌ 同步失败：{str(e)}")
    finally:
        cursor.close()
        conn.close()
     
# ====================== 终极正确版：连板天数真正连续计算主执行 ======================
if __name__ == "__main__":


    init_database_schema()
    
    print("=" * 50)
    print(" 主程序：初始化股票任务CSV")
    print("=" * 50)
    init_task_csv()

    print("\n" + "=" * 50)
    print(" 主程序：更新续传日期")
    print("=" * 50)
    update_csv_start_date()

    bs.logout()

    print("\n" + "=" * 50)
    print(" 主程序：启动 10个并行下载器")
    print("=" * 50)
    start_workers()

    print("\n" + "=" * 50)
    print(" 主程序：更新续传日期")
    print("=" * 50)
    update_csv_start_date()

    print("\n" + "=" * 50)
    print(" 主程序：启动 10个并行下载器")
    print("=" * 50)
    start_workers()

    print("\n🔌 兜底准备：重新登录 baostock")

    bs.login()

    print("\n🔄 兜底准备：重新更新任务开始时间")
    update_csv_start_date()

    final_full_download()

    # ========== 自动计算填充7个字段 ==========
    calculate_all_stock_fields()

    # ========== 根据 stock_task.csv 修正未知行业 ==========
    refresh_stock_daily_industry_from_task_csv()

    # ====================== 往涨跌停表stock_limit里写数据 ======================

    sync_stock_limit_data()

     # 调用行业数据同步
    sync_data_to_industry_tables()


    print("\n🎉 🎉 🎉 全部任务完成！数据100%完整！")
    try:
        bs.logout()
    except:
        pass

    import sys
    sys.exit(0)
