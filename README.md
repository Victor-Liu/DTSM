# DTSM
监控指定数据库表结构变化并报警，支持oracle、mysql、postgresql。
包含2个文件，主程序DTSM.py和配置文件config.ini。


# 安装依赖包：

sudo yum install postgresql-devel  python3-devel

确保python版本为3.6以上

pip install cx_Oracle pymysql psycopg2 requests APScheduler



# 监控oracle数据库需要安装oracle客户端工具:

需要安装对应oracle版本的客户端

如oracle-instantclient11.2-basic-11.2.0.4.0-1.x86_64.rpm

# 修改config.ini配置文件，加入企微报警机器人地址和待监控数据库信息
<img width="1127" height="641" alt="image" src="https://github.com/user-attachments/assets/6f7976fc-6f87-4e33-9fc1-a164d4037e97" />



# 程序运行

放入后台运行

nohup python3 DTSM.py &



# 企微报警示例

<img width="392" height="324" alt="企业微信截图_17590463318813" src="https://github.com/user-attachments/assets/0e9df234-6eab-4e71-aaae-d374ad9c62b8" />


