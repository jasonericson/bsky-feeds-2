from atproto import AtUri, CAR, firehose_models, FirehoseSubscribeReposClient, models, parse_subscribe_repos_message
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from dateutil import parser
from enum import auto, Enum
import fcntl
import os
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

last_cycle_time = time()
last_purge_time = 0
curr_num_records = 0

def process_events():
    con = sqlite3.connect('firehose.db', detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cur = con.cursor()

    cur.execute(
        """CREATE TABLE IF NOT EXISTS people(
            did TEXT PRIMARY KEY,
            follows_primed BOOLEAN
        )""")
    cur.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_people_did ON people (did)')
    cur.execute(
        """CREATE TABLE IF NOT EXISTS posts(
            uri TEXT PRIMARY KEY,
            cid_rev TEXT,
            reply_parent TEXT,
            reply_root TEXT,
            repost_uri TEXT,
            created_at INTEGER,
            author TEXT
        )""")
    cur.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_posts_uri ON posts (uri)')
    cur.execute(
        """CREATE TABLE IF NOT EXISTS follows(
            uri TEXT PRIMARY KEY,
            follower TEXT,
            followee TEXT
        )"""
    )
    cur.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_follows_uri ON follows (uri)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_follows_follower ON follows (follower)')
    con.commit()

    last_update_time = time()
    db_update_interval = 2.0
    while True:
        time_since_last_update = time() - last_update_time
        wait_time = db_update_interval - time_since_last_update
        if wait_time >= 0.0:
            sleep(wait_time)
        last_update_time = time()

        start_time = time_ns()

        global curr_record_collection_idx
        last_record_collection_idx = curr_record_collection_idx
        with cycle_mutex:
            curr_record_collection_idx = (last_record_collection_idx + 1) % num_record_collections
        record_collections = record_collections_cycle[last_record_collection_idx]
        
        # Acquire lock on db
        lock_file = open('./firehose.db.lock', 'w+')
        print('Acquiring db lock')
        fcntl.flock(lock_file, fcntl.LOCK_EX)

        # Posts
        post_collection = record_collections[RecordType.Post.value]
        authors = []
        created_post_infos = []
        for created_post in post_collection.created:
            author = created_post['author']
            record = created_post['record']

            reply_root = reply_parent = None
            if hasattr(record, 'reply') and record.reply:
                if record.reply.root is None or record.reply.parent is None:
                    continue
                reply_root = record.reply.root.uri
                reply_parent = record.reply.parent.uri

            cid: str = created_post['cid']

            authors.append((
                author,
                False,
            ))
            created_post_infos.append((
                created_post['uri'],
                cid[::-1],
                reply_parent,
                reply_root,
                None,
                int(parser.isoparse(record.created_at).timestamp()),
                author,
            ))

        if len(created_post_infos) > 0:
            cur.executemany('INSERT OR IGNORE INTO people VALUES(?, ?)', authors)
            cur.executemany('INSERT OR IGNORE INTO posts VALUES(?, ?, ?, ?, ?, ?, ?)', created_post_infos)
            print(f'Inserted {len(created_post_infos)} posts into database.')

        deleted_post_infos = []
        for deleted_post in post_collection.deleted:
            deleted_post_infos.append((
                deleted_post['uri'],
            ))

        if len(deleted_post_infos) > 0:
            cur.executemany('DELETE FROM posts WHERE uri = ?', deleted_post_infos)
            print(f'Deleted {len(deleted_post_infos)} posts from database.')

        # Reposts
        repost_collection = record_collections[RecordType.Repost.value]
        authors = []
        created_repost_infos = []
        for created_repost in repost_collection.created:
            record = created_repost['record']
            author = created_repost['author']
            
            if not hasattr(record, 'subject') or record.subject is None:
                continue

            cid: str = created_post['cid']

            authors.append((
                author,
                False,
            ))
            created_repost_infos.append((
                created_repost['uri'],
                cid[::-1],
                None,
                None,
                record.subject.uri,
                int(parser.isoparse(record.created_at).timestamp()),
                author,
            ))

        if len(created_repost_infos) > 0:
            cur.executemany('INSERT OR IGNORE INTO people VALUES(?, ?)', authors)
            cur.executemany('INSERT OR IGNORE INTO posts VALUES(?, ?, ?, ?, ?, ?, ?)', created_repost_infos)
            print(f'Inserted {len(created_repost_infos)} reposts into database.')

        deleted_repost_infos = []
        for deleted_repost in repost_collection.deleted:
            deleted_repost_infos.append((
                deleted_repost['uri'],
            ))

        if len(deleted_repost_infos) > 0:
            cur.executemany('DELETE FROM posts WHERE uri = ?', deleted_repost_infos)
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
            cur.executemany('INSERT OR IGNORE INTO people VALUES(?, ?)', authors)
            cur.executemany('INSERT OR IGNORE INTO follows VALUES(?, ?, ?)', created_follow_infos)
            print(f'Inserted {len(created_follow_infos)} follows into database.')

        deleted_follow_infos = []
        for deleted_follow in follow_collection.deleted:
            deleted_follow_infos.append((
                deleted_follow['uri'],
            ))

        if len(deleted_follow_infos) > 0:
            cur.executemany('DELETE FROM follows WHERE uri = ?', deleted_follow_infos)
            print(f'Deleted {len(deleted_follow_infos)} follows from database.')

        global last_purge_time
        time_since_last_purge = time() - last_purge_time
        if time_since_last_purge >= 30.0 * 60.0:
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=12)
            cur.execute(f"DELETE FROM posts WHERE created_at <= {int(cutoff_time.timestamp())}")
            print('Purged old posts.')
            last_purge_time = time()

        print('Committing queries')
        con.commit()
        print('Unlocking db')
        fcntl.flock(lock_file, fcntl.LOCK_UN)
        lock_file.close()

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
        global last_cycle_time
        if curr_time - last_cycle_time >= 1.0:
            num_records_history.append(curr_num_records)
            while len(num_records_history) > 20:
                num_records_history.pop()
            avg = 0
            for num_records in num_records_history:
                avg += num_records
            avg /= len(num_records_history)

            curr_num_records = 0
            last_cycle_time = curr_time

    t = Thread(target = process_events)
    t.start()

    client.start(on_message_handler)

if __name__ == "__main__":
    main()
