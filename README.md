# 市政饮用水污染响应

模块化纯 Python 3.9.6+ 标准库项目，默认端口 `8334`。

- `app.py`：服务生命周期和依赖组装。
- `src/domain.py`：水源、污染物、区域和来源校验。
- `src/rules.py`：污染评分、通知去重、停水、切换水源、冲洗、消毒、复检和恢复状态机。
- `src/repository.py`：SQLite、事务、重复保护、乐观版本和审计链。
- `src/service.py`：身份、角色和用例编排。
- `src/http_api.py`：JSON 接口与静态首页。
- `src/audit.py`：审计哈希。

```bash
python3 app.py --init --db ./data.db
python3 app.py --db ./data.db --port 8334
python3 -m unittest discover -s tests -v
```

接口包括 `GET /health`、`GET /api/state`、`GET /api/readings`、`POST /api/readings`、`POST /api/readings/<id>/reopen`、`POST /api/items`、`POST /api/items/<id>/sources`、`POST /api/items/<id>/actions` 和审计查询。

水源读数由上游监测站按水源编号推送：同一读数只入账一次，观测时刻更旧的丢弃；新读数会自动关联仍在跟进的片区重算等级，已恢复片区按新读数超标的退回待复检，恢复结论超期失效的需重新确认。复检容量有限，满员时先排队、空出后自动补入。监管可按新读数批量重开片区；现场人员跨片区放行会被拒绝。升级前的旧数据按未关联兼容，处置与审计记录照旧可查。

内置规则不替代真实水质模型、法定通报渠道或供水控制系统的联锁。
