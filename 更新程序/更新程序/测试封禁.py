import baostock as bs
lg = bs.login()
print("错误码：", lg.error_code)
print("错误信息：", lg.error_msg)
bs.logout()
