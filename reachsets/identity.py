from .storage import atomic, require_text


def register(database,source_key,person):
    require_text(source_key,'来源标识')
    require_text(person,'分析主体')
    with atomic(database):
        row=database.execute('SELECT person FROM identities WHERE source_key=?',(source_key,)).fetchone()
        if row:
            if row[0]!=person:
                raise ValueError("已经使用的映射不可直接改写")
            return False
        database.execute('INSERT INTO identities VALUES(?,?)',(source_key,person))
    return True


def resolve(database,source_key):
    row=database.execute('SELECT person FROM identities WHERE source_key=?',(source_key,)).fetchone()
    return row[0] if row else None


def _remap(database,assignments,correction):
    changes=0
    with atomic(database):
        for source_key,person in sorted(assignments.items()):
            require_text(source_key,'来源标识')
            require_text(person,'分析主体')
            row=database.execute('SELECT person FROM identities WHERE source_key=?',(source_key,)).fetchone()
            if row and row[0]==person:
                continue
            if row:
                database.execute('UPDATE identities SET person=? WHERE source_key=?',(person,source_key))
            else:
                database.execute('INSERT INTO identities VALUES(?,?)',(source_key,person))
            database.execute('INSERT INTO identity_log(source_key,old_person,new_person,correction) VALUES(?,?,?,?)',(source_key,row[0] if row else None,person,correction))
            changes+=1
    return changes>0


def merge(database,source_keys,person):
    require_text(person,'分析主体')
    keys={require_text(key,'来源标识') for key in source_keys}
    if not keys:
        raise ValueError("合并需要至少一个来源标识")
    return _remap(database,{key:person for key in keys},'merge')


def unmerge(database,assignments):
    if not assignments:
        raise ValueError("撤回合并需要明确的标识关系")
    return _remap(database,dict(assignments),'unmerge')
