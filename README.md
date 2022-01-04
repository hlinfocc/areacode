# areacode简介
areacode是用于抓取国家统计局地址数据的爬虫程序，日常项目中很多地方都需要用到地址数据，目前全国地址数据由国家统计局发布，包含：省,市/州，区县，乡镇，村等5个级别数据，共计约70万条(因地区合并等原因，每年数据均不等，实际小于70万条)。

  程序由Python编写，**推荐使用Python3**

## 配置与运行

1. 安装依赖

```
pip3 install requests
pip3 install bs4
```

2. 配置数据库表名

默认表名为areacode2020，如果需要改成其他表名，请将spiders.py文件中的areacode2020改成你的表名。

3. 保存的文件

最后生成的SQL insert语句保存的文件名及路径位于当前目录下的data目录中，默认文件包为：areacode2020-all.sql，如需修改，位于31行：

`saveFileName = "data/areacode2020-all.sql"`

4. 运行

```
python3 spiders.py
```

## 数据库表字段信息

1. MySQL数据库建表语句为：

```
CREATE TABLE IF NOT EXISTS areacode2021 (
	code  varchar(12) PRIMARY KEY NOT NULL COMMENT '地址code',
	area_name  varchar(255) DEFAULT '' COMMENT '名字',
	type  int COMMENT '级别,1:省,2:市/州，3区县，4乡镇，5村',
	parent_code varchar(12) COMMENT '父级code ',
	KEY `areacode_index` (`parent_code`)
) DEFAULT CHARSET=utf8 COMMENT='地址表2021';
```

2. PostgreSQL数据库建表语句为：

```
CREATE TABLE if not exists public.areacode2021 (
    code varchar(12) NULL,
	area_name text NULL,
	"type" integer NULL,
    parent_code varchar(12) NULL,
	CONSTRAINT areacode2021_pk PRIMARY KEY (code)
);
CREATE INDEX areacode2021_parent_code_idx ON public.areacode2021 (parent_code);
CREATE INDEX areacode2020_type_idx ON public.areacode2021 ("type");
COMMENT ON TABLE public.areacode2021 IS '地址表2021';
COMMENT ON COLUMN public.areacode2021.code IS '地址code';
COMMENT ON COLUMN public.areacode2021.area_name IS '名字';
COMMENT ON COLUMN public.areacode2021."type" IS '级别,1:省,2:市/州，3区县，4乡镇，5村';
COMMENT ON COLUMN public.areacode2021.parent_code IS '父级code';
```

