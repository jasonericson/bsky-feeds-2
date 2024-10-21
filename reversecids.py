import sqlite3

con = sqlite3.connect('firehose.db', detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
cur = con.cursor()

con.create_function('strrev', 1, lambda s: s[::-1])

# res = con.execute('SELECT cid_rev FROM post LIMIT 10')
# print(res.fetchall())

con.execute('UPDATE post SET cid_rev = strrev(cid)')
con.commit()
