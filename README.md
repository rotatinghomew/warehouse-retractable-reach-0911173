# 活动触达事件入湖与渠道人数汇总

这是一个处理本地数据交换的仓湖项目。示例使用虚构机构和匿名合成记录，不连接业务平台。

## 数据契约

曝光粒度为event_id，不是上传文件行号。当前来源标识映射确认后固定，不进行身份推断。未经确认的标识会阻止发布而非静默排除。日度人数按日期和渠道内分析主体精确去重，曝光次数保留全部有效事件；日度人数不声明可跨日或跨渠道相加。集市名称一经发布不变。

## 代码职责

- `reachsets/events.py`：以真实曝光键去重的接入。
- `reachsets/identity.py`：稳定来源标识映射。
- `reachsets/marts.py`：精确日度人数与曝光次数。
- `reachsets/storage.py`：本地事务、内容摘要和查询工具。

## 安装与运行

使用 Python 3.11。先执行 `python3 -m pip install -r requirements.txt`；依赖仅在准备环境时安装，运行与测试均可断网。

```sh
python3 -m compileall -q reachsets tests
python3 -m unittest discover -s tests -v
python3 -m reachsets
```

默认示例在独立临时目录执行，结束后删除目录。需要检查持久文件时可执行 `python3 -m reachsets --root state`。测试自己创建临时目录，不依赖固定端口、外部服务或其他用例顺序。

## 容器

```sh
docker build -t retractable-reach:local .
docker run --rm --network none retractable-reach:local
docker run --rm --network none retractable-reach:local python3 -m reachsets
```

容器构建负责预装锁定依赖，启动不下载包。示例输出是真实接入、查询和发布的数据，不读取预制输出。测试及示例可分别运行，退出码非零表示失败。

## 交换口径

进一步的字段含义、范围及兼容边界见 `docs/data-contract.md`。
