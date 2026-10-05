# 市政饮用水污染响应

模块化纯 Python 3.9.6+ 标准库项目，默认端口 `8334`。

- `app.py`：服务生命周期和依赖组装。
- `src/domain.py`：水源、污染物、区域、水源读数和来源校验。
- `src/rules.py`：污染评分、通知去重、停水、切换水源、冲洗、消毒、复检、放行和恢复状态机。
- `src/repository.py`：SQLite、事务、重复保护、乐观版本和审计链。
- `src/service.py`：身份、角色和用例编排（含读数联动重算、退回复检、排队和批量重开）。
- `src/http_api.py`：JSON 接口与静态首页。
- `src/audit.py`：审计哈希。

```bash
python3 app.py --init --db ./data.db
python3 app.py --db ./data.db --port 8334
python3 -m unittest discover -s tests -v
```

接口包括 `GET /health`、`GET /api/state`、`POST /api/items`、`POST /api/items/<id>/sources`、`POST /api/items/<id>/actions`、`POST /api/readings`、`GET /api/readings`、`POST /api/readings/<id>/reopen` 和审计查询。

上游监测站推送的水源读数按 `(source_id, reading_id)` 去重，同一读数只入账一次；观测时刻早于该水源已有读数的直接丢弃。读数入库的同一事务内：仍在跟进的污染事件按水源编号重算等级；已恢复且新读数超标的事件退回 `recheck` 待复检，原恢复结论移入 `previous_restorations` 备查。片区复检容量有限（`RECHECK_CAPACITY`），超出容量的事件进入 `recheck_queued` 排队，有片区放行后按序提拔。退回后必须重新取样并 `release` 放行确认才能再次恢复。监管角色可按读数批量重开已恢复片区；`restore`/`release` 校验管辖区域，跨区越权放行会被拒绝，无区域标记的旧数据按未关联兼容。测试覆盖完整响应流程、重复事件、重复通知、复检阈值、读数去重与丢弃、等级重算、退回排队与提拔、批量重开、权限、区域越权和版本冲突。内置规则不替代真实水质模型、法定通报渠道或供水控制系统的联锁。
