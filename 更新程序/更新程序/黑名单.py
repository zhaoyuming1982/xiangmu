
import baostock as bs
lg = bs.login()

# 显示登陆返回信息

print('login respond error_code:'+lg.error_code)

print('login respond  error_msg:'+lg.error_msg)

if(lg.error_code == "10001011"):

    print("IP已经加入黑名单, 需要去QQ群里求助")
