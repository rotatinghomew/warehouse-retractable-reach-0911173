import json
from datetime import date
from pathlib import Path

from .parquet import literal, query, write_rows
from .storage import atomic, connect, fingerprint, records, require_text

DDL="""
CREATE TABLE IF NOT EXISTS identities(source_key TEXT PRIMARY KEY,person TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS batches(name TEXT PRIMARY KEY,checksum TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS exposures(event_id TEXT PRIMARY KEY,day TEXT,channel TEXT,source_key TEXT,batch TEXT,checksum TEXT);
CREATE TABLE IF NOT EXISTS editions(name TEXT PRIMARY KEY,path TEXT,daily TEXT);
CREATE TABLE IF NOT EXISTS tombstones(event_id TEXT PRIMARY KEY,batch TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS identity_log(seq INTEGER PRIMARY KEY AUTOINCREMENT,source_key TEXT NOT NULL,old_person TEXT,new_person TEXT NOT NULL,correction TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS versions(mart TEXT NOT NULL,version INTEGER NOT NULL,checksum TEXT NOT NULL,path TEXT,daily TEXT NOT NULL,weekly TEXT NOT NULL,summary TEXT NOT NULL,PRIMARY KEY(mart,version));
CREATE TABLE IF NOT EXISTS reports(name TEXT PRIMARY KEY,mart TEXT NOT NULL,version INTEGER NOT NULL,daily TEXT NOT NULL,weekly TEXT NOT NULL);
"""

VALID="""
SELECT e.event_id,e.day,e.channel,e.source_key,i.person
FROM exposures e JOIN identities i USING(source_key)
WHERE e.event_id NOT IN (SELECT event_id FROM tombstones)
"""

UNKNOWN="""
SELECT e.source_key FROM exposures e LEFT JOIN identities i USING(source_key)
WHERE e.event_id NOT IN (SELECT event_id FROM tombstones) AND i.person IS NULL
LIMIT 1
"""

DAILY_SQL='SELECT day,channel,COUNT(*) exposures,COUNT(DISTINCT person) people FROM payload GROUP BY day,channel ORDER BY day,channel'

WEEKLY_SQL='SELECT strftime(CAST(day AS DATE),\'%G-W%V\') "week",channel,COUNT(*) exposures,COUNT(DISTINCT person) people FROM payload GROUP BY "week",channel ORDER BY "week",channel'

PEOPLE_SQL='SELECT COUNT(DISTINCT person) people FROM payload'


def _diff(before,after,keys):
    def index(rows):
        return {tuple(row[key] for key in keys):row for row in rows}
    old=index(before)
    new=index(after)
    changes=[]
    for key in sorted(set(old)|set(new)):
        prior=old.get(key)
        current=new.get(key)
        pair=lambda row:(row['exposures'],row['people']) if row else (0,0)
        if pair(prior)!=pair(current):
            change=dict(zip(keys,key))
            change.update({'old_exposures':pair(prior)[0],'new_exposures':pair(current)[0],
                           'old_people':pair(prior)[1],'new_people':pair(current)[1]})
            changes.append(change)
    return changes


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
            unknown=self.db.execute(UNKNOWN).fetchone()
            if unknown:
                raise ValueError('来源标识尚未确认：'+unknown[0])
            rows=records(self.db,VALID+' ORDER BY e.event_id')
            path=write_rows(self.root/'exposures',rows)
            daily=query([path],DAILY_SQL) if path else []
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

    def refresh(self,mart='current'):
        require_text(mart,'集市')
        with atomic(self.db):
            unknown=self.db.execute(UNKNOWN).fetchone()
            if unknown:
                raise ValueError('来源标识尚未确认：'+unknown[0])
            rows=records(self.db,VALID+' ORDER BY e.event_id')
            checksum=fingerprint(rows)
            latest=self.db.execute('SELECT MAX(version) FROM versions WHERE mart=?',(mart,)).fetchone()[0]
            if latest is not None:
                prior=self.db.execute('SELECT checksum FROM versions WHERE mart=? AND version=?',(mart,latest)).fetchone()
                if prior[0]==checksum:
                    payload=self._payload(mart,latest)
                    payload['unchanged']=True
                    return payload
            path=write_rows(self.root/'versions'/mart,rows)
            daily=query([path],DAILY_SQL) if path else []
            weekly=query([path],WEEKLY_SQL) if path else []
            summary={'exposures':len(rows),
                     'people':query([path],PEOPLE_SQL)[0]['people'] if path else 0,
                     'summed_daily_people':sum(row['people'] for row in daily),
                     'retracted_events':self.db.execute('SELECT COUNT(*) FROM tombstones').fetchone()[0]}
            version=(latest or 0)+1
            self.db.execute('INSERT INTO versions VALUES(?,?,?,?,?,?,?)',(mart,version,checksum,path,json.dumps(daily),json.dumps(weekly),json.dumps(summary)))
            payload={'version':version,'daily':daily,'weekly':weekly,'summary':summary}
        payload['unchanged']=False
        return payload

    def versions(self,mart='current'):
        return [row[0] for row in self.db.execute('SELECT version FROM versions WHERE mart=? ORDER BY version',(mart,))]

    def _row(self,mart,version):
        if version is None:
            row=self.db.execute('SELECT * FROM versions WHERE mart=? ORDER BY version DESC LIMIT 1',(mart,)).fetchone()
        else:
            row=self.db.execute('SELECT * FROM versions WHERE mart=? AND version=?',(mart,version)).fetchone()
        if not row:
            raise KeyError(mart if version is None else mart+'@'+str(version))
        return dict(row)

    def _payload(self,mart,version=None):
        row=self._row(mart,version)
        return {'version':row['version'],'daily':json.loads(row['daily']),
                'weekly':json.loads(row['weekly']),'summary':json.loads(row['summary'])}

    def daily_at(self,mart='current',version=None):
        return self._payload(mart,version)['daily']

    def weekly_at(self,mart='current',version=None):
        return self._payload(mart,version)['weekly']

    def summary(self,mart='current',version=None):
        return self._payload(mart,version)['summary']

    def snapshot(self,mart='current',version=None):
        path=self._row(mart,version)['path']
        return query([path],'SELECT * FROM payload ORDER BY event_id') if path else []

    def reach(self,mart='current',version=None,channels=None,start=None,end=None,group=None):
        row=self._row(mart,version)
        if group not in (None,'day','week'):
            raise ValueError('不支持的粒度：'+str(group))
        if not row['path']:
            return [] if group else {'exposures':0,'people':0}
        select='COUNT(*) exposures,COUNT(DISTINCT person) people'
        label={'day':'day','week':'"week"'}
        if group=='day':
            select='day,'+select
        elif group=='week':
            select="strftime(CAST(day AS DATE),'%G-W%V') \"week\","+select
        sql='SELECT '+select+' FROM payload WHERE 1=1'
        if channels:
            sql+=' AND channel IN ('+','.join(literal(require_text(channel,'渠道')) for channel in channels)+')'
        if start:
            date.fromisoformat(start)
            sql+=' AND CAST(day AS DATE)>=CAST('+literal(start)+' AS DATE)'
        if end:
            date.fromisoformat(end)
            sql+=' AND CAST(day AS DATE)<=CAST('+literal(end)+' AS DATE)'
        if group:
            sql+=' GROUP BY '+label[group]+' ORDER BY '+label[group]
        rows=query([row['path']],sql)
        return rows if group else rows[0]

    def impact(self,mart='current',base=None,target=None):
        available=self.versions(mart)
        if not available:
            raise KeyError(mart)
        if target is None:
            target=available[-1]
        if base is None:
            base=available[-2] if len(available)>1 else target
        before=self._payload(mart,base)
        after=self._payload(mart,target)
        return {'base':base,'target':target,
                'daily':_diff(before['daily'],after['daily'],('day','channel')),
                'weekly':_diff(before['weekly'],after['weekly'],('week','channel'))}

    def seal(self,name,mart='current',version=None):
        require_text(name,'周报')
        payload=self._payload(mart,version)
        with atomic(self.db):
            if self.db.execute('SELECT 1 FROM reports WHERE name=?',(name,)).fetchone():
                raise ValueError('周报已经封存：'+name)
            self.db.execute('INSERT INTO reports VALUES(?,?,?,?,?)',(name,mart,payload['version'],json.dumps(payload['daily']),json.dumps(payload['weekly'])))
        return {'name':name,'mart':mart,**payload}

    def report(self,name):
        row=self.db.execute('SELECT * FROM reports WHERE name=?',(name,)).fetchone()
        if not row:
            raise KeyError(name)
        return {'name':row['name'],'mart':row['mart'],'version':row['version'],
                'daily':json.loads(row['daily']),'weekly':json.loads(row['weekly'])}
