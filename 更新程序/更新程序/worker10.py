# ==========================================
# worker10 → 4501以上 ~ 表格最大ID【不设上限、防踢重登版】
# ==========================================
WORKER_ID = 10

import baostock as bs
import pandas as pd
import pymysql
import time
import sys
import os

# ====================== 配置 ======================
CSV_PATH = r"C:\xiangmu\stock_task.csv"
PROGRESS_LOG = r"C:\xiangmu\download_progress.log"

MYSQL_USER = "root"
MYSQL_PASSWORD = "Root123456"
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_DB = "stock_db"

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

# ====================== 安全登录函数（永不卡死） ======================
def safe_bs_login():
    try:
        bs.logout()
        time.sleep(1)
    except:
        pass
    
    for retry in range(5):
        try:
            bs.login()
            print("✅ baostock 登录成功")
            return True
        except Exception as e:
            print(f"⚠️ 登录失败，重试 {retry+1}/5")
            time.sleep(2)
    return False

safe_bs_login()

# ====================== 下载主函数 ======================
def download_data_by_task():
    if not os.path.exists(CSV_PATH):
        print("❌ stock_task.csv 不存在")
        return
    
    df = pd.read_csv(CSV_PATH, dtype={"id": int, "code": str})
    # 只取 4501及以上所有数据，自动适配最大值
    df_worker = df[df["id"] >= 4501]

    print(f"📌 Worker10 分配到：{len(df_worker)} 只股票（4501~最大ID）")

    conn = get_conn()
    cursor = conn.cursor()

    for _, row in df_worker.iterrows():
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

        print(f"📥 下载 {code} {code_name} | {start_date} ~ {end_date}")

        df_k = pd.DataFrame()
        try:
            rs = bs.query_history_k_data_plus(
                code=code,
                fields=fields,
                start_date=start_date,
                end_date=end_date,
                frequency=frequency,
                adjustflag=adjustflag
            )
            df_k = rs.get_data()

        except Exception as e:
            print(f"❌ {code} 连接断开/被踢线，自动重登中...")
            safe_bs_login()
            time.sleep(2)
            continue

        if df_k.empty:
            print(f"⚠️ {code} 数据为空 → baostock 已踢人，重新登录...")
            safe_bs_login()
            time.sleep(2)
            continue

        if not df_k.empty:
            for _, r in df_k.iterrows():
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
            print(f"✅ {code} 写入完成，共 {len(df_k)} 条")

        msg = f"[{time.strftime('%H:%M:%S')}] 🟢 Worker10 完成 → {code} {code_name}"
        try:
            with open(PROGRESS_LOG, "a", encoding="utf8mb4") as f:
                f.write(msg + "\n")
        except:
            pass

        time.sleep(0.3)

    cursor.close()
    conn.close()
    print("✅ Worker10 全部任务完成")

if __name__ == "__main__":
    try:
        download_data_by_task()
    finally:
        try:
            bs.logout()
        except:
            pass
        sys.exit(0)
