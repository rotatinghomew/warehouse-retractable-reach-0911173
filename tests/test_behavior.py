import tempfile
import unittest


class WorkspaceTest(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory()
        self.directory=self.temporary.name
        self.resources=[]

    def open(self, factory):
        resource=factory(self.directory)
        self.resources.append(resource)
        return resource

    def tearDown(self):
        for resource in reversed(self.resources):
            resource.close()
        self.temporary.cleanup()

from reachsets.marts import ReachMart
from reachsets.identity import merge, register, resolve, unmerge
from reachsets.events import ingest, retract


def exposure(event='E1',key='A',day='2026-11-01',channel='直播'):
    return {'event_id':event,'source_key':key,'day':day,'channel':channel}


class Behavior(WorkspaceTest):
    def test_stable_identity(self):
        mart=self.open(ReachMart)
        register(mart.db,'A','P1')
        self.assertFalse(register(mart.db,'A','P1'))
        self.assertEqual(resolve(mart.db,'A'),'P1')
        with self.assertRaises(ValueError):
            register(mart.db,'A','P2')

    def test_event_not_transport_grain(self):
        mart=self.open(ReachMart)
        ingest(mart.db,'B1',[exposure()])
        ingest(mart.db,'B2',[exposure()])
        self.assertEqual(mart.db.execute('SELECT COUNT(*) FROM exposures').fetchone()[0],1)
        with self.assertRaises(ValueError):
            ingest(mart.db,'B3',[exposure('E2'),exposure(channel='回放')])
        self.assertEqual(mart.db.execute('SELECT COUNT(*) FROM exposures').fetchone()[0],1)

    def test_unknown_does_not_publish(self):
        mart=self.open(ReachMart)
        ingest(mart.db,'B1',[exposure()])
        with self.assertRaises(ValueError):
            mart.publish('日报')
        self.assertIsNone(mart.db.execute('SELECT 1 FROM editions').fetchone())

    def test_daily_distinct_and_frozen(self):
        mart=self.open(ReachMart)
        register(mart.db,'A','P1')
        register(mart.db,'B','P1')
        ingest(mart.db,'B1',[exposure(),exposure('E2','B')])
        rows=mart.publish('日报')
        self.assertEqual(rows[0]['people'],1)
        self.assertEqual(rows[0]['exposures'],2)
        ingest(mart.db,'B2',[exposure('E3','A','2026-11-02')])
        reopened=self.open(ReachMart)
        self.assertEqual(len(reopened.detail('日报')),2)
        with self.assertRaises(ValueError):
            reopened.publish('日报')


class Correctable(WorkspaceTest):
    def seed(self,mart):
        register(mart.db,'A','P1')
        register(mart.db,'B','P2')
        register(mart.db,'C','P3')
        ingest(mart.db,'T1',[
            exposure('E1','A','2026-11-02','直播'),
            exposure('E2','A','2026-11-03','回放'),
            exposure('E3','B','2026-11-03','直播'),
            exposure('E4','A','2026-11-03','直播'),
            exposure('E5','C','2026-11-04','直播')])
        ingest(mart.db,'T2',[exposure('E6','B','2026-11-09','直播')])

    def test_retract_removes_exactly_and_blocks_replay(self):
        mart=self.open(ReachMart)
        self.seed(mart)
        mart.refresh('current')
        retract(mart.db,'R1',['E5'])
        state=mart.refresh('current')
        self.assertEqual(state['version'],2)
        self.assertEqual(state['summary']['exposures'],5)
        self.assertEqual(state['summary']['retracted_events'],1)
        self.assertEqual(mart.reach('current',start='2026-11-04',end='2026-11-04'),{'exposures':0,'people':0})
        ingest(mart.db,'T3',[exposure('E5','C','2026-11-04','直播')])
        again=mart.refresh('current')
        self.assertTrue(again['unchanged'])
        self.assertEqual(again['version'],2)
        self.assertEqual(mart.reach('current',start='2026-11-04',end='2026-11-04'),{'exposures':0,'people':0})

    def test_retract_before_ingest_still_invalid(self):
        mart=self.open(ReachMart)
        retract(mart.db,'R1',['E9'])
        register(mart.db,'A','P1')
        ingest(mart.db,'T1',[exposure('E9','A')])
        self.assertEqual(mart.db.execute('SELECT COUNT(*) FROM exposures').fetchone()[0],0)
        state=mart.refresh('current')
        self.assertEqual(state['summary']['exposures'],0)
        self.assertEqual(mart.reach('current'),{'exposures':0,'people':0})

    def test_retract_batch_idempotent_and_conflict(self):
        mart=self.open(ReachMart)
        self.assertTrue(retract(mart.db,'R1',['E1','E2']))
        self.assertFalse(retract(mart.db,'R1',['E2','E1']))
        with self.assertRaises(ValueError):
            retract(mart.db,'R1',['E1'])

    def test_merge_and_unmerge_change_people_not_exposures(self):
        mart=self.open(ReachMart)
        self.seed(mart)
        mart.refresh('current')
        self.assertTrue(merge(mart.db,['A','B'],'P1'))
        self.assertFalse(merge(mart.db,['A','B'],'P1'))
        merged=mart.refresh('current')
        self.assertEqual(merged['summary']['exposures'],6)
        self.assertEqual(merged['summary']['people'],2)
        day=[row for row in merged['daily'] if row['day']=='2026-11-03' and row['channel']=='直播'][0]
        self.assertEqual((day['exposures'],day['people']),(2,1))
        self.assertTrue(unmerge(mart.db,{'B':'P2'}))
        restored=mart.refresh('current')
        self.assertEqual(restored['summary']['exposures'],6)
        self.assertEqual(restored['summary']['people'],3)
        log=[tuple(row) for row in mart.db.execute('SELECT source_key,old_person,new_person,correction FROM identity_log ORDER BY seq')]
        self.assertEqual(log,[('B','P2','P1','merge'),('B','P1','P2','unmerge')])
        with self.assertRaises(ValueError):
            unmerge(mart.db,{})

    def test_day_week_channel_share_identity_and_not_additive(self):
        mart=self.open(ReachMart)
        self.seed(mart)
        mart.refresh('current')
        weekly=mart.weekly_at('current')
        row=[row for row in weekly if row['week']=='2026-W45' and row['channel']=='直播'][0]
        self.assertEqual((row['exposures'],row['people']),(4,3))
        self.assertEqual(sum(row['people'] for row in mart.daily_at('current')),6)
        self.assertEqual(mart.reach('current'),{'exposures':6,'people':3})
        union=mart.reach('current',channels=['直播','回放'])
        self.assertEqual(union,{'exposures':6,'people':3})
        parts=sum(mart.reach('current',channels=[channel])['people'] for channel in ('直播','回放'))
        self.assertEqual(parts,4)
        self.assertGreater(parts,union['people'])
        grouped=mart.reach('current',channels=['直播','回放'],group='week')
        self.assertEqual([(row['week'],row['exposures'],row['people']) for row in grouped],
                         [('2026-W45',5,3),('2026-W46',1,1)])

    def test_blocked_refresh_keeps_old_complete_version(self):
        mart=self.open(ReachMart)
        self.seed(mart)
        first=mart.refresh('current')
        ingest(mart.db,'T3',[exposure('E7','未确认','2026-11-05','直播')])
        with self.assertRaises(ValueError):
            mart.refresh('current')
        self.assertEqual(mart.versions('current'),[1])
        self.assertEqual(mart.daily_at('current'),first['daily'])
        register(mart.db,'未确认','P4')
        second=mart.refresh('current')
        self.assertEqual(second['version'],2)
        days={row['day'] for row in second['daily']}
        self.assertEqual(days,{'2026-11-02','2026-11-03','2026-11-04','2026-11-05','2026-11-09'})
        self.assertEqual(mart.daily_at('current',version=1),first['daily'])

    def test_sealed_report_frozen_current_corrected(self):
        mart=self.open(ReachMart)
        self.seed(mart)
        mart.refresh('current')
        mart.seal('W45周报',version=1)
        with self.assertRaises(ValueError):
            mart.seal('W45周报',version=1)
        merge(mart.db,['A','B'],'P1')
        retract(mart.db,'R1',['E5'])
        mart.refresh('current')
        frozen=mart.report('W45周报')
        self.assertEqual(frozen['version'],1)
        row=[row for row in frozen['weekly'] if row['week']=='2026-W45' and row['channel']=='直播'][0]
        self.assertEqual((row['exposures'],row['people']),(4,3))
        current=[row for row in mart.weekly_at('current') if row['week']=='2026-W45' and row['channel']=='直播'][0]
        self.assertEqual((current['exposures'],current['people']),(3,1))
        impact=mart.impact('current',base=1)
        self.assertEqual((impact['base'],impact['target']),(1,2))
        self.assertEqual(impact['daily'],[
            {'day':'2026-11-03','channel':'直播','old_exposures':2,'new_exposures':2,'old_people':2,'new_people':1},
            {'day':'2026-11-04','channel':'直播','old_exposures':1,'new_exposures':0,'old_people':1,'new_people':0}])
        self.assertEqual(impact['weekly'],[
            {'week':'2026-W45','channel':'直播','old_exposures':4,'new_exposures':3,'old_people':3,'new_people':1}])
        with self.assertRaises(KeyError):
            mart.report('未封存')

    def test_versions_keep_full_history_across_reopen(self):
        mart=self.open(ReachMart)
        self.seed(mart)
        mart.refresh('current')
        retract(mart.db,'R1',['E5'])
        mart.refresh('current')
        self.assertEqual(len(mart.snapshot('current',version=1)),6)
        self.assertEqual(len(mart.snapshot('current')),5)
        reopened=self.open(ReachMart)
        self.assertEqual(len(reopened.snapshot('current',version=1)),6)
        self.assertEqual(reopened.versions('current'),[1,2])

    def test_publish_excludes_retracted_and_ignores_unknown_retracted(self):
        mart=self.open(ReachMart)
        register(mart.db,'A','P1')
        ingest(mart.db,'T1',[exposure('E1','A'),exposure('E2','A','2026-11-02'),exposure('E3','未确认','2026-11-03')])
        retract(mart.db,'R1',['E2','E3'])
        rows=mart.publish('日报')
        self.assertEqual(rows,[{'day':'2026-11-01','channel':'直播','exposures':1,'people':1}])
        self.assertEqual(len(mart.detail('日报')),1)
