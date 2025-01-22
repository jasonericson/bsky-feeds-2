from atproto import AtUri, CAR, firehose_models, FirehoseSubscribeReposClient, models, parse_subscribe_repos_message
from queue import SimpleQueue
import config
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from dateutil import parser
from enum import auto, Enum
import psycopg2
from psycopg2.extras import execute_batch
from threading import Thread
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

class ActionType:
    Created = 0
    Deleted = 1

@dataclass
class Record:
    record_type: RecordType
    action_type: ActionType
    record_info: dict

@dataclass
class RecordCollection:
    created: list = field(default_factory=lambda: [])
    deleted: list = field(default_factory=lambda: [])

record_queue = SimpleQueue()

last_purge_time = 0

def process_events():
    con = psycopg2.connect(database='bluesky',
                           host='localhost',
                           user='postgres',
                           password=config.DB_PASSWORD,
                           port=5432)
    cur = con.cursor()

    cur.execute(
        """CREATE TABLE IF NOT EXISTS follows_primed(
            did TEXT PRIMARY KEY
        )""")
    cur.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_people_did ON follows_primed (did)')
    cur.execute(
        """CREATE TABLE IF NOT EXISTS posts(
            uri TEXT,
            cid_rev TEXT,
            repost_uri TEXT,
            created_at TIMESTAMPTZ NOT NULL,
            author TEXT
        ) PARTITION BY RANGE(created_at)""")
    cur.execute('CREATE INDEX IF NOT EXISTS idx_posts_uri ON posts (uri)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts (created_at)')

    cur.execute('CREATE INDEX IF NOT EXISTS idx_posts_author ON posts (author)')
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
    last_successful_update_time = time()
    update_success_threshhold = 30.0
    while True:
        time_since_last_update = time() - last_update_time
        wait_time = db_update_interval - time_since_last_update
        if wait_time >= 0.0:
            sleep(wait_time)
        last_update_time = time()

        time_since_last_successful_update = time() - last_successful_update_time
        if time_since_last_successful_update >= update_success_threshhold:
            print(f"Error: It's been {update_success_threshhold} seconds since the last successful firehose update. Restarting...")
            exit(1)

        start_time = time_ns()

        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=13)
        now_time = datetime.now(timezone.utc) + timedelta(minutes=10) # padding "now" time in case firehose is out of sync with computer system time

        record_collections: list[RecordCollection] = [RecordCollection() for _ in RecordType]
        global record_queue
        if record_queue.empty():
            continue

        while not record_queue.empty():
            record: Record = record_queue.get_nowait()
            if record.action_type == ActionType.Created:
                record_collections[record.record_type.value].created.append(record.record_info)
            else:
                record_collections[record.record_type.value].deleted.append(record.record_info)

        queue_finished_time = time_ns()
        elapsed_time_ms = (queue_finished_time - start_time) / 1_000_000
        print(f'Time to pull from queue: {elapsed_time_ms} ms.')

        # Posts
        post_collection = record_collections[RecordType.Post.value]
        times_to_create: set[datetime] = set()
        created_post_infos = []
        for created_post in post_collection.created:
            author = created_post['author']
            record = created_post['record']

            # Posts can be given custom created_at dates - if it's too old, or in the future, we ignore it
            created_at_dt = parser.isoparse(record.created_at).astimezone(timezone.utc)
            if created_at_dt < cutoff_time or created_at_dt > now_time:
                continue

            # Ignoring replies
            if hasattr(record, 'reply') and record.reply:
                continue

            # Log each hour block that a post has been created in, for table partitioning
            created_at_hour = datetime(year=created_at_dt.year, month=created_at_dt.month, day=created_at_dt.day, hour=created_at_dt.hour, tzinfo=created_at_dt.tzinfo)
            times_to_create.add(created_at_hour)

            cid: str = created_post['cid']
            
            created_post_infos.append((
                created_post['uri'],
                cid[::-1], # Reversed, for more random sorting
                None,
                created_at_dt,
                author,
            ))

        # Add partitions to the table for each hour (if they don't exist)
        for table_time in times_to_create:
            cur.execute(f"CREATE TABLE IF NOT EXISTS posts_{table_time.strftime('y%Ym%md%dh%H')} PARTITION OF posts\
                            FOR VALUES FROM (%s) TO (%s)", (table_time, table_time + timedelta(hours=1)))

        # Add posts to db
        if len(created_post_infos) > 0:
            execute_batch(cur, 'INSERT INTO posts VALUES(%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING', created_post_infos)
            print(f'Inserted {len(created_post_infos)} posts into database.')

        # Collect deleted posts
        deleted_post_infos = []
        for deleted_post in post_collection.deleted:
            deleted_post_infos.append((
                deleted_post['uri'],
            ))

        # Delete posts from db
        if len(deleted_post_infos) > 0:
            execute_batch(cur, 'DELETE FROM posts WHERE uri = %s', deleted_post_infos)
            print(f'Deleted {len(deleted_post_infos)} posts from database.')

        # Reposts
        repost_collection = record_collections[RecordType.Repost.value]
        times_to_create: set[datetime] = set()
        created_repost_infos = []
        for created_repost in repost_collection.created:
            record = created_repost['record']
            author = created_repost['author']

            # Posts can be given custom created_at dates - if it's too old, or in the future, we ignore it
            created_at_dt = parser.isoparse(record.created_at).astimezone(timezone.utc)
            if created_at_dt < cutoff_time or created_at_dt > datetime.now(timezone.utc) + timedelta(minutes=5):
                continue
            
            # Ignore empty reposts
            if not hasattr(record, 'subject') or record.subject is None:
                continue

            # Log each hour block that a post has been created in, for table partitioning
            created_at_hour = datetime(year=created_at_dt.year, month=created_at_dt.month, day=created_at_dt.day, hour=created_at_dt.hour, tzinfo=created_at_dt.tzinfo)
            times_to_create.add(created_at_hour)

            cid: str = created_post['cid']

            created_repost_infos.append((
                created_repost['uri'],
                cid[::-1], # Reversed for more random sorting
                record.subject.uri,
                created_at_dt,
                author,
            ))

        # Add partitions to the table for each hour (if they don't exist)
        for table_time in times_to_create:
            cur.execute(f"CREATE TABLE IF NOT EXISTS posts_{table_time.strftime('y%Ym%md%dh%H')} PARTITION OF posts\
                            FOR VALUES FROM (%s) TO (%s)", (table_time, table_time + timedelta(hours=1)))

        # Add reposts to db
        if len(created_repost_infos) > 0:
            execute_batch(cur, 'INSERT INTO posts VALUES(%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING', created_repost_infos)
            print(f'Inserted {len(created_repost_infos)} reposts into database.')

        # Collect deleted reposts
        deleted_repost_infos = []
        for deleted_repost in repost_collection.deleted:
            deleted_repost_infos.append((
                deleted_repost['uri'],
            ))

        # Delete reposts from db
        if len(deleted_repost_infos) > 0:
            execute_batch(cur, 'DELETE FROM posts WHERE uri = %s', deleted_repost_infos)
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
            execute_batch(cur, 'INSERT INTO follows VALUES(%s, %s, %s) ON CONFLICT DO NOTHING', created_follow_infos)
            print(f'Inserted {len(created_follow_infos)} follows into database.')

        deleted_follow_infos = []
        for deleted_follow in follow_collection.deleted:
            deleted_follow_infos.append((
                deleted_follow['uri'],
            ))

        if len(deleted_follow_infos) > 0:
            execute_batch(cur, 'DELETE FROM follows WHERE uri = %s', deleted_follow_infos)
            print(f'Deleted {len(deleted_follow_infos)} follows from database.')

        global last_purge_time
        time_since_last_purge = time() - last_purge_time
        if time_since_last_purge >= 60.0 * 60.0:
            # Query to collect all partitions of 'posts' table
            cur.execute(
                """ SELECT child.relname AS name
                    FROM pg_inherits
                        JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
                        JOIN pg_class child ON pg_inherits.inhrelid = child.oid
                        JOIN pg_namespace nmsp_parent ON nmsp_parent.oid = parent.relnamespace
                        JOIN pg_namespace nmsp_child ON nmsp_child.oid = child.relnamespace
                    WHERE parent.relname = 'posts'
                    ORDER BY name ASC
                """)
            for row in cur.fetchall():
                # Parse the datetime from the table name - if it's older than the threshhold, drop it
                table_name = row[0]
                table_time_str = f'{table_name}+0000'
                table_time = datetime.strptime(table_time_str, 'posts_y%Ym%md%dh%H%z')
                if table_time < cutoff_time:
                    print(f'Dropping partition {table_name}')
                    cur.execute(f"DROP TABLE {table_name}")
                else:
                    print(f'Leaving partition {table_name} in the db')

            print('Purged old posts.')
            last_purge_time = time()

        print('Committing queries')
        con.commit()

        end_time = time_ns()
        elapsed_time_ms = (end_time - start_time) // 1_000_000
        print(f'Time to update db: {elapsed_time_ms} ms. ({elapsed_time_ms / (db_update_interval * 1000) * 100:.3}% of {db_update_interval} seconds.)')

        last_successful_update_time = time()

def main():
    params = models.ComAtprotoSyncSubscribeRepos.Params()
    client = FirehoseSubscribeReposClient(params)

    def on_message_handler(message: firehose_models.MessageFrame) -> None:
        commit = parse_subscribe_repos_message(message)
        if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
            return
        
        if not commit.blocks:
            return

        car = CAR.from_bytes(commit.blocks)
        global record_queue
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
                        record_queue.put(Record(record_info.record_type, ActionType.Created, create_info))

            if op.action == 'delete':
                for record_info in _INTERESTED_RECORDS:
                    if uri.collection == record_info.record_nsid:
                        delete_info = {'uri': str(uri)}
                        record_queue.put(Record(record_info.record_type, ActionType.Deleted, delete_info))

    def on_error_handler(ex: BaseException) -> None:
        print(f'Firehose error! {ex}')
        exit(1)

    t = Thread(target = process_events)
    t.start()

    client.start(on_message_handler, on_error_handler)

if __name__ == "__main__":
    main()
