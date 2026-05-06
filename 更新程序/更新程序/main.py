import baostock as bs
import pandas as pd
from datetime import datetime, timedelta
import pymysql
import subprocess
import sys
import os
import time
# ====================== 配置 ======================
fields = "date,code,preclose,open,high,low,close,volume,amount,turn,pctChg"
CSV_PATH = r"C:\xiangmu\stock_task.csv"
INDUSTRY_PATH = r"C:\xiangmu\stock_industry.csv"

MYSQL_USER = "root"
MYSQL_PASSWORD = "Root123456"
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_DB = "stock_db"

# 确保目录存在
os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)

# ====================== 数据库连接 ======================
def get_conn():
    return pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        port=MYSQL_PORT,
        charset="utf8mb4"
    )

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

# ====================== 初始化 stock_task.csv ======================
def init_task_csv():
    # 1. 获取全量股票
    df_stock = bs.query_all_stock(day=LATEST_DAY).get_data()

    df_stock.rename(columns={"ts_code": "code"}, inplace=True)
    
    # 2. 筛选主板/创业板/科创板等，剔除ST、退市
    df_plate = df_stock[
        df_stock["code"].str.startswith(("sh.60",  "sh.68" )) |
        df_stock["code"].str.startswith(("sz.00",  "sz.30"))
    ]
    df_no_st = df_plate[~df_plate["code_name"].str.contains("ST", na=False)]
    df_final = df_no_st[~df_no_st["code_name"].str.contains("退", na=False)].copy()
    
    # 3. 提取纯数字代码
    df_final["code_num"] = df_final["code"].str.split(".").str[1]
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
    print("\n🚀 启动 2 个并行下载脚本...")
    
    # 保存 worker 编号、进程、启动时间
    workers = []
    for i in range(1, 11):
        p = subprocess.Popen([sys.executable, f"worker{i}.py"])
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
                print(f"⏹️ worker{wid} 运行超过30分钟，已强制关闭")
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
def calculate_all_stock_fields():
    import warnings
    warnings.filterwarnings('ignore')

    print("\n" + "=" * 60)
    print(" 第一步：批量更新基础字段（日内涨跌幅、振幅、涨跌停价、状态、流通市值）")
    print("=" * 60)

    conn = get_conn()
    cursor = conn.cursor()

    sql_base = """
    UPDATE stock_daily
    SET
        intra_pct     = ROUND((close - open) / open * 100, 2),
        amplitude     = ROUND((high - low) / preclose * 100, 2),
        up_limit      = ROUND(preclose * IF(code RLIKE '^(sz\\.30|sh\\.68)', 1.2, 1.1), 2),
        down_limit    = ROUND(preclose * IF(code RLIKE '^(sz\\.30|sh\\.68)', 0.8, 0.9), 2),
        limit_status  = CASE
            WHEN close = ROUND(preclose * IF(code RLIKE '^(sz\\.30|sh\\.68)', 1.2, 1.1), 2) THEN 1
            WHEN close = ROUND(preclose * IF(code RLIKE '^(sz\\.30|sh\\.68)', 0.8, 0.9), 2) THEN 2
            ELSE 0
        END,
        circ_market   = CASE WHEN turn > 0 THEN ROUND(volume / turn * close*100, 2) ELSE 0 END
    WHERE intra_pct IS NULL OR circ_market IS NULL;
    """

    cursor.execute(sql_base)
    conn.commit()
    print(f"✅ 基础字段更新完成：{cursor.rowcount} 条")

    # --------------------- 全量加载到内存 ---------------------
    print("\n" + "=" * 60)
    print(" 第二步：全量加载数据到内存（按股票+日期排序）")
    print("=" * 60)

    cursor.execute("""
        SELECT id, code, date, volume, limit_status, vol_ratio_today, volume_ratio
        FROM stock_daily
        ORDER BY code, date ASC
    """)
    all_rows = cursor.fetchall()
    total = len(all_rows)
    print(f"✅ 已加载到内存：{total} 条数据")

    from collections import defaultdict
    stock_group = defaultdict(list)
    for row in all_rows:
        stock_group[row[1]].append(row)

    # --------------------- 全内存计算（仅更新未计算的数据） ---------------------
    print("\n" + "=" * 60)
    print(" 第三步：全内存计算（仅更新未计算的数据）")
    print("=" * 60)

    update_batch = []
    count = 0

    for code, rows in stock_group.items():
        # ====================== 每只股票独立维护连板状态 ======================
        prev_limit = 0      # 前一天是否涨停
        prev_days = 0       # 前一天连板天数

        for i in range(len(rows)):
            id_num, code, date, volume, limit_status, vol_ratio_today, volume_ratio = rows[i]
            vol = float(volume)
            now_limit = int(limit_status)

            # ====================== 两个都不为空才跳过（你要的逻辑） ======================
            if vol_ratio_today is not None and volume_ratio is not None:
                # 已经计算完成，更新股票状态并跳过
                prev_limit = now_limit
                if now_limit == 1:
                    prev_days += 1
                else:
                    prev_days = 0
                count += 1
                continue

            # 1. 量比（5日平均）
            vol_list = []
            start_idx = max(0, i - 5)
            for j in range(start_idx, i):
                vol_list.append(float(rows[j][3]))
            vol_ratio = 0.0
            if vol_list and sum(vol_list) > 0:
                vol_ratio = round(vol / (sum(vol_list) / len(vol_list)), 2)

            # 2. 今日量 / 昨日量
            vol_today = None
            if i > 0:
                pre_vol = float(rows[i-1][3])
                if pre_vol > 0:
                    vol_today = round(vol / pre_vol, 2)

            # ====================== 正确连板统计算法 ======================
            if now_limit == 1:
                if prev_limit == 1:
                    days = prev_days + 1
                else:
                    days = 1
            else:
                days = 0

            # 更新当前股票的连续状态
            prev_limit = now_limit
            prev_days = days

            # 加入批量更新
            update_batch.append((vol_ratio, days, vol_today, id_num))

            count += 1
            if count % 500000 == 0:
                print(f"👉 计算进度：{count}/{total}")

    # --------------------- 批量写入数据库 ---------------------
    print("\n" + "=" * 60)
    print(f" 第四步：批量写入（仅新数据：{len(update_batch)} 条）")
    print("=" * 60)

    if not update_batch:
        print("✅ 无新数据需要更新！")
        cursor.close()
        conn.close()
        return

    BATCH_SIZE = 20000
    total_write = len(update_batch)
    written = 0

    for idx in range(0, total_write, BATCH_SIZE):
        batch = update_batch[idx:idx + BATCH_SIZE]
        cursor.executemany("""
            UPDATE stock_daily
            SET volume_ratio = %s, limit_days = %s, vol_ratio_today = %s
            WHERE id = %s
        """, batch)
        conn.commit()
        written += len(batch)
        if written % 500000 == 0:
            print(f"✅ 已写入：{written}/{total_write}")

    cursor.close()
    conn.close()
    print("\n🎉 增量更新完成！全内存 + 仅更新新数据！")

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

def sync_data_to_industry_tables():
    """
    全新思路：
    1. 一次性读取 stock_daily 全量数据到内存
    2. 内存中按 industry 分组排序
    3. 批量写入对应行业表，每行业提交一次
    """
    from collections import defaultdict

    conn = get_conn()
    cursor = conn.cursor()
    conn.autocommit = False

    try:
        # ===================== 1. 一次性全量读取主表数据 =====================
        print("🔍 正在读取 stock_daily 全量数据到内存...")
        cursor.execute("""
            SELECT 
                date, code, code_name, industry, preclose, open, high, low, close,
                intra_pct, amplitude, up_limit, down_limit, limit_status, limit_days,
                volume, amount, turn, pctChg, volume_ratio, vol_ratio_today, circ_market
            FROM stock_daily
        """)
        all_data = cursor.fetchall()
        print(f"✅ 内存加载完成，共 {len(all_data)} 条数据")

        # ===================== 2. 内存按行业分组 =====================
        industry_groups = defaultdict(list)
        for row in all_data:
            industry = row[3]  # 第4列是 industry
            if industry and industry.strip():
                industry_groups[industry].append(row)

        # ===================== 3. 按行业批量写入 =====================
        total_insert = 0
        index = 0

        for industry, rows in industry_groups.items():
            index += 1
            table_name = ''.join(c for c in industry if c.isalnum() or c == '_')

            try:
                # 批量插入（自动去重：不存在才插入）
                insert_sql = f"""
                    INSERT INTO `{table_name}` (
                        date, code, code_name, industry, preclose, open, high, low, close,
                        intra_pct, amplitude, up_limit, down_limit, limit_status, limit_days,
                        volume, amount, turn, pctChg, volume_ratio, vol_ratio_today, circ_market
                    )
                    VALUES {','.join(['(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'] * len(rows))}
                    ON DUPLICATE KEY UPDATE
                        code_name = VALUES(code_name),
                        preclose = VALUES(preclose),
                        open = VALUES(open),
                        high = VALUES(high),
                        low = VALUES(low),
                        close = VALUES(close),
                        circ_market = VALUES(circ_market)
                """

                # 扁平化参数
                params = []
                for r in rows:
                    params.extend(r)

                cursor.execute(insert_sql, params)
                count = cursor.rowcount
                total_insert += count

                print(f"序号：{index:2d} | 行业：{industry:12s} | 表名：{table_name:12s} | 写入：{count} 条")

                conn.commit()

            except Exception as e:
                print(f"序号：{index:2d} | ⚠️ 跳过 {industry}：{str(e)}")
                conn.rollback()
                continue

        print(f"\n🎉 全部同步完成！总新增/更新：{total_insert} 条")

    except Exception as e:
        conn.rollback()
        print(f"❌ 同步失败：{str(e)}")
    finally:
        cursor.close()
        conn.close()
     
# ====================== 终极正确版：连板天数真正连续计算主执行 ======================
if __name__ == "__main__":


    
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
