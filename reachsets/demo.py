from .events import ingest, retract
from .identity import merge, register, unmerge
from .marts import ReachMart

WEEK45=[
    {'event_id':'E1','source_key':'渠道A:设备1','day':'2026-11-02','channel':'直播'},
    {'event_id':'E2','source_key':'渠道A:设备1','day':'2026-11-03','channel':'回放'},
    {'event_id':'E3','source_key':'渠道B:设备2','day':'2026-11-03','channel':'直播'},
    {'event_id':'E4','source_key':'渠道A:设备1','day':'2026-11-03','channel':'直播'},
    {'event_id':'E5','source_key':'渠道C:设备3','day':'2026-11-04','channel':'直播'}]
WEEK46=[
    {'event_id':'E6','source_key':'渠道B:设备2','day':'2026-11-09','channel':'直播'}]


def run(root):
    mart=ReachMart(root)
    try:
        register(mart.db,'渠道A:设备1','匿名人1')
        register(mart.db,'渠道B:设备2','匿名人2')
        register(mart.db,'渠道C:设备3','匿名人3')
        ingest(mart.db,'曝光-第1批',WEEK45)
        first=mart.refresh('current')
        try:
            mart.report('2026-W45周报')
        except KeyError:
            mart.seal('2026-W45周报',version=first['version'])
        ingest(mart.db,'曝光-第2批',WEEK46)
        mart.refresh('current')
        merge(mart.db,['渠道A:设备1','渠道B:设备2'],'匿名人1')
        merged=mart.refresh('current')
        merge_impact=mart.impact('current')
        retract(mart.db,'撤销-第1批',['E5'])
        retracted=mart.refresh('current')
        retract_impact=mart.impact('current')
        ingest(mart.db,'曝光-重传批',[dict(WEEK45[4])])
        replayed=mart.refresh('current')
        unmerge(mart.db,{'渠道B:设备2':'匿名人2'})
        restored=mart.refresh('current')
        sealed=mart.report('2026-W45周报')
        return {
            '第45周日度':first['daily'],
            '第45周周度':first['weekly'],
            '第45周汇总':first['summary'],
            '合并后版本':merged['version'],
            '合并影响范围':merge_impact,
            '撤销事件后版本':retracted['version'],
            '撤销影响范围':retract_impact,
            '撤销事件换批次重传':{'产生新版本':not replayed['unchanged'],'版本':replayed['version']},
            '撤回合并后版本':restored['version'],
            '渠道组合_直播加回放_第45周':mart.reach('current',channels=['直播','回放'],start='2026-11-02',end='2026-11-08'),
            '封存周报保持当时口径':sealed['weekly'],
            '当前周度已更正':mart.weekly_at('current'),
            '相对封存口径的总影响':mart.impact('current',base=sealed['version'])}
    finally:
        mart.close()
