from atproto import AtUri, CAR, firehose_models, FirehoseSubscribeReposClient, models, parse_subscribe_repos_message
from dataclasses import dataclass, field
from dateutil import parser
from enum import auto, Enum
import sqlite3
from threading import Lock, Thread
from time import sleep, time, time_ns
from types import ModuleType

class RecordType(Enum):
    Post = 0
    Like = 1
    Follow = 2
    Repost = 3

@dataclass
class RecordInfo:
    record_type: RecordType
    record_module: ModuleType
    record_nsid: str

_INTERESTED_RECORDS = [
    RecordInfo(RecordType.Post, models.AppBskyFeedPost, models.ids.AppBskyFeedPost),
    RecordInfo(RecordType.Like, models.AppBskyFeedLike, models.ids.AppBskyFeedLike),
    RecordInfo(RecordType.Follow, models.AppBskyGraphFollow, models.ids.AppBskyGraphFollow),
    RecordInfo(RecordType.Repost, models.AppBskyFeedRepost, models.ids.AppBskyFeedRepost),
]

@dataclass
class RecordCollection:
    created: list = field(default_factory=lambda: [])
    deleted: list = field(default_factory=lambda: [])

num_record_collections = 3
curr_record_collection_idx = 0
record_collections_cycle: list[list[RecordCollection]] = []
for i in range(num_record_collections):
    record_collections = [RecordCollection() for _ in RecordType]
    record_collections_cycle.append(record_collections)
cycle_mutex = Lock()

last_time = time()
curr_num_records = 0

def process_events():
    con = sqlite3.connect('firehose.db', detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cur = con.cursor()

    cur.execute(
        """CREATE TABLE IF NOT EXISTS person(
            did TEXT PRIMARY KEY,
            follows_primed BOOLEAN
        )""")
    cur.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_person_did ON person (did)')
    cur.execute(
        """CREATE TABLE IF NOT EXISTS post(
            uri TEXT PRIMARY KEY,
            cid TEXT,
            reply_parent TEXT,
            reply_root TEXT,
            repost_uri TEXT,
            created_at TEXT,
            author TEXT,
            FOREIGN KEY (author)
                REFERENCES person (did)
                    ON DELETE SET NULL
        )""")
    cur.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_post_uri ON post (uri)')
    cur.execute(
        """CREATE TABLE IF NOT EXISTS follow(
            uri TEXT PRIMARY KEY,
            follower TEXT,
            followee TEXT,
            FOREIGN KEY (follower)
                REFERENCES person (did)
                    ON DELETE SET NULL
            FOREIGN KEY (followee)
                REFERENCES person (did)
                    ON DELETE SET NULL
        )"""
    )
    cur.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_follow_uri ON follow (uri)')
    con.commit()

    db_update_interval = 2.0
    while True:
        sleep(db_update_interval)

        start_time = time_ns()

        global curr_record_collection_idx
        last_record_collection_idx = curr_record_collection_idx
        with cycle_mutex:
            curr_record_collection_idx = (last_record_collection_idx + 1) % num_record_collections
        record_collections = record_collections_cycle[last_record_collection_idx]
        
        # Posts
        post_collection = record_collections[RecordType.Post.value]
        authors = []
        created_post_infos = []
        for created_post in post_collection.created:
            author = created_post['author']
            record = created_post['record']

            reply_root = reply_parent = None
            if record.reply:
                reply_root = record.reply.root.uri
                reply_parent = record.reply.parent.uri

            authors.append((
                author,
                False,
            ))
            created_post_infos.append((
                created_post['uri'],
                created_post['cid'],
                reply_parent,
                reply_root,
                None,
                record.created_at,
                author,
            ))

        if len(created_post_infos) > 0:
            cur.executemany('INSERT OR IGNORE INTO person VALUES(?, ?)', authors)
            cur.executemany('INSERT OR IGNORE INTO post VALUES(?, ?, ?, ?, ?, ?, ?)', created_post_infos)
            print(f'Inserted {len(created_post_infos)} posts into database.')

        deleted_post_infos = []
        for deleted_post in post_collection.deleted:
            deleted_post_infos.append((
                deleted_post['uri'],
            ))

        if len(deleted_post_infos) > 0:
            cur.executemany('DELETE FROM post WHERE uri = ?', deleted_post_infos)
            print(f'Deleted {len(deleted_post_infos)} posts from database.')

        # Reposts
        repost_collection = record_collections[RecordType.Repost.value]
        authors = []
        created_repost_infos = []
        for created_repost in repost_collection.created:
            record = created_repost['record']
            author = created_repost['author']

            authors.append((
                author,
                False,
            ))
            created_repost_infos.append((
                created_repost['uri'],
                created_repost['cid'],
                None,
                None,
                record.subject.uri,
                record.created_at,
                author,
            ))

        if len(created_repost_infos) > 0:
            cur.executemany('INSERT OR IGNORE INTO person VALUES(?, ?)', authors)
            cur.executemany('INSERT OR IGNORE INTO post VALUES(?, ?, ?, ?, ?, ?, ?)', created_repost_infos)
            print(f'Inserted {len(created_repost_infos)} reposts into database.')

        deleted_repost_infos = []
        for deleted_repost in repost_collection.deleted:
            deleted_repost_infos.append((
                deleted_repost['uri'],
            ))

        if len(deleted_repost_infos) > 0:
            cur.executemany('DELETE FROM post WHERE uri = ?', deleted_repost_infos)
            print(f'Deleted {len(deleted_repost_infos)} reposts from database.')

        # Follows
        follow_collection = record_collections[RecordType.Follow.value]
        authors = []
        created_follow_infos = []
        for created_follow in follow_collection.created:
            author = created_follow['author']

            authors.append((
                author,
                False,
            ))
            created_follow_infos.append((
                created_follow['uri'],
                author,
                created_follow['record'].subject
            ))

        if len(created_follow_infos) > 0:
            cur.executemany('INSERT OR IGNORE INTO person VALUES(?, ?)', authors)
            cur.executemany('INSERT OR IGNORE INTO follow VALUES(?, ?, ?)', created_follow_infos)
            print(f'Inserted {len(created_follow_infos)} follows into database.')

        deleted_follow_infos = []
        for deleted_follow in follow_collection.deleted:
            deleted_follow_infos.append((
                deleted_follow['uri'],
            ))

        if len(deleted_follow_infos) > 0:
            cur.executemany('DELETE FROM follow WHERE uri = ?', deleted_follow_infos)
            print(f'Deleted {len(deleted_follow_infos)} follows from database.')

        con.commit()

        # print(f'Number of events for idx {last_record_collection_idx}')
        # total_events = 0
        for idx, record_collection in enumerate(record_collections_cycle[last_record_collection_idx]):
            # print(f'{RecordType(idx)} Created: {len(record_collection.created)}')
            # total_events += len(record_collection.created)
            # print(f'{RecordType(idx)} Deleted: {len(record_collection.deleted)}')
            # total_events += len(record_collection.deleted)
            record_collection.created.clear()
            record_collection.deleted.clear()
        # print(f'TOTAL: {total_events}')

        end_time = time_ns()
        elapsed_time_ms = (end_time - start_time) // 1_000_000
        print(f'Time to update db: {elapsed_time_ms} ms. ({elapsed_time_ms / (db_update_interval * 1000) * 100:.3}% of {db_update_interval} seconds.)')

def main():
    params = models.ComAtprotoSyncSubscribeRepos.Params()
    client = FirehoseSubscribeReposClient(params)

    num_records_history: list[int] = []

    def on_message_handler(message: firehose_models.MessageFrame) -> None:
        commit = parse_subscribe_repos_message(message)
        if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
            return
        
        if not commit.blocks:
            return

        car = CAR.from_bytes(commit.blocks)
        global curr_num_records
        curr_num_records += len(commit.ops)
        for op in commit.ops:
            if op.action == 'update':
                continue

            uri = AtUri.from_str(f'at://{commit.repo}/{op.path}')

            if op.action == 'create':
                if not op.cid:
                    continue

                record_raw_data = car.blocks.get(op.cid)
                if not record_raw_data:
                    continue

                record = models.get_or_create(record_raw_data, strict=False)
                for record_info in _INTERESTED_RECORDS:
                    if uri.collection == record_info.record_nsid and models.is_record_type(record, record_info.record_module):
                        create_info = {'record': record, 'uri': str(uri), 'cid': str(op.cid), 'author': commit.repo}
                        with cycle_mutex:
                            record_collections_cycle[curr_record_collection_idx][record_info.record_type.value].created.append(create_info)
                        break

            if op.action == 'delete':
                for record_info in _INTERESTED_RECORDS:
                    if uri.collection == record_info.record_nsid:
                        delete_info = {'uri': str(uri)}
                        with cycle_mutex:
                            record_collections_cycle[curr_record_collection_idx][record_info.record_type.value].deleted.append(delete_info)
                        break

        curr_time = time()
        global last_time
        if curr_time - last_time >= 1.0:
            num_records_history.append(curr_num_records)
            while len(num_records_history) > 20:
                num_records_history.pop()
            avg = 0
            for num_records in num_records_history:
                avg += num_records
            avg /= len(num_records_history)

            curr_num_records = 0
            last_time = curr_time

    t = Thread(target = process_events)
    t.start()

    client.start(on_message_handler)

if __name__ == "__main__":
    main()
    # con = sqlite3.connect('test.db')
    # cur = con.cursor()

    # cur.execute(
    #     """CREATE TABLE IF NOT EXISTS post(
    #         uri TEXT PRIMARY KEY,
    #         label TEXT,
    #         desc TEXT
    #     )""")

    # created_post_infos = []
    # created_post_infos.append((
    #     'a',
    #     'b',
    #     'c',
    # ))

    # cur.executemany('INSERT INTO post VALUES(?, ?, ?)', [('a', 'b', 'c')])
    # cur.executemany('DELETE FROM post WHERE uri = ?', [('a')])
    # con.commit()
