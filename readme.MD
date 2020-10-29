# oracle archivelog mining 

## 1. 安装配置

### 1.数据库设置
在oracle数据库服务器上
```shell script
sqlplus / as sysdba
shutdown immediate;
startup mount;
alter database archivelog;
alter database open;
alter database add supplemental log data (all) columns;
```

 ### 2.添加数据库用户
  登录数据库运行以下脚本
  
  普通数据库模式下
```sql
create role mining_role;
grant  create session,execute_catalog_role,select any transaction ,select any dictionary to mining_role;
create user kminer identified by kminerpass;
grant  mining_role to kminer;
alter user kminer quota unlimited on users;
```
cdb模式下
```sql

```




启动时可以手工指定起始同步的scn
```shell script
java -jar logmining.jar --mining.init-start-scn=2631941
```