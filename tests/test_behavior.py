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
from reachsets.identity import register, resolve
from reachsets.events import ingest


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
