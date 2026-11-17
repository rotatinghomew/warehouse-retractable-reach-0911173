import json
from pathlib import Path

from .parquet import query, write_rows
from .storage import atomic, connect, records

DDL="""
CREATE TABLE IF NOT EXISTS identities(source_key TEXT PRIMARY KEY,person TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS batches(name TEXT PRIMARY KEY,checksum TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS exposures(event_id TEXT PRIMARY KEY,day TEXT,channel TEXT,source_key TEXT,batch TEXT,checksum TEXT);
CREATE TABLE IF NOT EXISTS editions(name TEXT PRIMARY KEY,path TEXT,daily TEXT);
"""


class ReachMart:
    def __init__(self,root):
        self.root=Path(root)
        self.db=connect(root,DDL)

    def close(self):
        self.db.close()

    def publish(self,name):
        with atomic(self.db):
            if self.db.execute('SELECT 1 FROM editions WHERE name=?',(name,)).fetchone():
                raise ValueError("集市名称已经发布")
            unknown=self.db.execute('SELECT e.source_key FROM exposures e LEFT JOIN identities i USING(source_key) WHERE i.person IS NULL LIMIT 1').fetchone()
            if unknown:
                raise ValueError('来源标识尚未确认：'+unknown[0])
            rows=records(self.db,'SELECT e.event_id,e.day,e.channel,e.source_key,i.person FROM exposures e JOIN identities i USING(source_key) ORDER BY e.event_id')
            path=write_rows(self.root/'exposures',rows)
            daily=query([path],'SELECT day,channel,COUNT(*) exposures,COUNT(DISTINCT person) people FROM payload GROUP BY day,channel ORDER BY day,channel') if path else []
            self.db.execute('INSERT INTO editions VALUES(?,?,?)',(name,path,json.dumps(daily)))
        return daily

    def edition(self,name):
        row=self.db.execute('SELECT * FROM editions WHERE name=?',(name,)).fetchone()
        if not row:
            raise KeyError(name)
        return row

    def daily(self,name):
        return json.loads(self.edition(name)['daily'])

    def detail(self,name):
        path=self.edition(name)['path']
        return query([path],'SELECT * FROM payload ORDER BY event_id') if path else []
