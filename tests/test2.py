### 实时从 ricequant 获取实时数据
import time
import sys
sys.path.append("../")  # 将上级目录添加到系统路径，以便导入模块
from data.ricequant_provider import RiceQuantDataProvider

# 初始化RiceQuant数据提供器
provider = RiceQuantDataProvider()

# 定义查询参数
table_name = "OptionContracts"  # 期权合约表
instruments = ["510050.XSHG"]  # 示例标的代码
start_date = "20230101"
end_date = "20230301"

# 获取期权数据
data = provider.get_data(
    table_name=table_name,
    instruments=instruments,
    start_date=start_date,
    end_date=end_date
)

# 打印数据
print(data)

# 关闭连接
provider.close_connection()