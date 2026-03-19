# 最小测试
import WindPy
w = WindPy.w
w.start()

# 测试wsd
data = w.wsd("600519.SH", "open,high,low,close", "2024-01-01", "2024-01-10", "")
print(f"错误码: {data.ErrorCode}")
print(f"字段: {data.Fields}")
print(f"数据: {data.Data[:5]}")

# 测试wss
data2 = w.wss("600519.SH", "pe_ttm,pb_lf", "tradeDate=20240131")
print(f"错误码: {data2.ErrorCode}")
print(f"字段: {data2.Fields}")
print(f"数据: {data2.Data}")