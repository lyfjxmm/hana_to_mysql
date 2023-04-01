# hana_to_mysql

## 主要功能
>实现将saphana数据自动建表到mysql(数据类型目前只支持varchar和decimal)  
>将select结果存入到mysql中

## 使用方法
>修改hanaIO.py和sqlIO.py的config为目标数据库凭证  
>随意调用函数

## 可能遇到的问题
>pyhdb报错"ces-8"的错误，修改报错文件所有的ces-8为utf-8  

