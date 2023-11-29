# nacos-sdk-python

nacos sdk for 2.x

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

1. 可用
2. 稳定

- 参考 java agent， 贴合 python 特性
- naming / config 分为两个不同的服务
- grpc / 网络连接 / 断开重连优化 / 控制好服务状态
- 优化日志：debug/info/warning/error, 尤其异常
- 异常细化分类, 异常向外抛出,逻辑不考虑异常为None情况
- 最外层，自定义传参(高优先级) + env, 命名空间、分组、集群等 可默认、可传入
- naming订阅/config监听 , 命名空间、分组等可区分开
- List-Watch 兜底逻辑
- Auth 账户密码 / ak/sk
    - Auth 任务周期可以使用 过期时间 (其他认证方式是否有此时间)
- shutdown
- 配置解密
- redo service
    - update task 等
    - 启动时不进行 redo
- failover reactor
- 内存缓存、磁盘缓存（加载）(多进程)
- 服务防推空, config 需要吗
- 初始化各种参数检查，优化提示
- 充分测试网络连接、断开、重连 等功能
- 监控 metrics
- 启动过程为 同步方式
- Client 端限流阈值
- 断网时 read_server_request_task 分析

- 语法
    - 文档字符串其主要逻辑

- license
- CHANGELOG.md
- poetry
- pytest
- CI
    - check
    - pytest
    - doc gen
    - test/pypi
    - tag
    - release
- doc
- cibuildwheel
- check
    - flake8 pylint
    - black isort
    - mypy
- pre-commit
- pypi
- README.md 中英