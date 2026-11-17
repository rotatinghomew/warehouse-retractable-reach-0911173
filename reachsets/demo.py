from .marts import ReachMart
from .events import ingest
from .identity import register


def run(root):
    mart=ReachMart(root)
    try:
        register(mart.db,'渠道A:设备1','匿名人1')
        register(mart.db,'渠道B:设备2','匿名人1')
        ingest(mart.db,'曝光A',[
            {'event_id':'E1','source_key':'渠道A:设备1','day':'2026-11-01','channel':'直播'},
            {'event_id':'E2','source_key':'渠道B:设备2','day':'2026-11-01','channel':'直播'}])
        if not mart.db.execute("SELECT 1 FROM editions WHERE name='日度摘录'").fetchone():
            mart.publish('日度摘录')
        return {'日度结果':mart.daily('日度摘录'),'明细':mart.detail('日度摘录')}
    finally:
        mart.close()
