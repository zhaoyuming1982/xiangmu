import pandas as pd

# 1. 读取 Excel，强制把【代码】列当成文本读取，保留前面的 0
df = pd.read_excel(
    "333.xlsx",
    engine="openpyxl",
    dtype={"代码": str}  # 关键：强制股票代码为文本，永不丢0
)

# 2. 自动清理脏列（Unnamed、空列）
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
df = df.dropna(how='all', axis=1)
df = df.loc[:, (df != '').any(axis=0)]

# 3. 导出干净 CSV（00开头永久保留）
df.to_csv(
    "stock_industry.csv",
    index=False,
    encoding="utf-8-sig"
)

print("✅ 转换完成！")
print("✅ 股票代码 00 开头已强制保留")
print("✅ 已清理 Unnamed / 空列")
print("📌 最终列：", list(df.columns))
