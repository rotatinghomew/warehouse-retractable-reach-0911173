from datetime import date

from .storage import atomic, fingerprint, require_text


def ingest(database,batch,events):
    checksum=fingerprint(events)
    with atomic(database):
        old=database.execute('SELECT checksum FROM batches WHERE name=?',(batch,)).fetchone()
        if old:
            if old[0]!=checksum:
                raise ValueError("批次内容不一致")
            return False
        for event in events:
            date.fromisoformat(event['day'])
            for field in ('event_id','channel','source_key'):
                require_text(event[field],field)
            identity=fingerprint(event)
            if database.execute('SELECT 1 FROM tombstones WHERE event_id=?',(event['event_id'],)).fetchone():
                continue
            prior=database.execute('SELECT checksum FROM exposures WHERE event_id=?',(event['event_id'],)).fetchone()
            if prior:
                if prior[0]!=identity:
                    raise ValueError("曝光事件键冲突")
                continue
            database.execute('INSERT INTO exposures VALUES(?,?,?,?,?,?)',(event['event_id'],event['day'],event['channel'],event['source_key'],batch,identity))
        database.execute('INSERT INTO batches VALUES(?,?)',(batch,checksum))
    return True


def retract(database,batch,event_ids):
    event_ids=sorted({require_text(event_id,'event_id') for event_id in event_ids})
    checksum=fingerprint({'retract':event_ids})
    with atomic(database):
        old=database.execute('SELECT checksum FROM batches WHERE name=?',(batch,)).fetchone()
        if old:
            if old[0]!=checksum:
                raise ValueError("批次内容不一致")
            return False
        for event_id in event_ids:
            database.execute('INSERT OR IGNORE INTO tombstones VALUES(?,?)',(event_id,batch))
        database.execute('INSERT INTO batches VALUES(?,?)',(batch,checksum))
    return True
