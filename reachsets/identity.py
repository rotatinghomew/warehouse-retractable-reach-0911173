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
