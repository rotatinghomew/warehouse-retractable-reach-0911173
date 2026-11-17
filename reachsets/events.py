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
            prior=database.execute('SELECT checksum FROM exposures WHERE event_id=?',(event['event_id'],)).fetchone()
            if prior:
                if prior[0]!=identity:
                    raise ValueError("曝光事件键冲突")
                continue
            database.execute('INSERT INTO exposures VALUES(?,?,?,?,?,?)',(event['event_id'],event['day'],event['channel'],event['source_key'],batch,identity))
        database.execute('INSERT INTO batches VALUES(?,?)',(batch,checksum))
    return True
