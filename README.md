# Database Handler Class 
ODBC/Oracle API Handler for Python/R   

数据抓取往往是建模中让人很头疼的部分，所以希望可以提供一个R/python的SQL Handler，该类可以实现以下功能：

### db_handler_base Class
1. 数据库API连接
2. 数据库连接超时自动重新连接
3. 数据库连接状态检查
4. SQL语句执行和logging 

### db_handler Class
1. select语句：支持bind variable, 以及数据类型转换（cast）
2. execute语句: 支持bind variable 
3. SQL Loader: 支持自动识别数据类型和转换，自动创建control file, log file 和 data file
