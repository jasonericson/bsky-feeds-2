from atproto import Client, DidInMemoryCache, IdResolver, models, verify_jwt
from atproto.exceptions import TokenInvalidSignatureError
import config
from flask import Flask, jsonify, request
from random import Random
import psycopg2
from psycopg2.extras import execute_batch
from time import time_ns
from waitress import serve

SERVICE_DID = f'did:web:{config.HOSTNAME}'

CACHE = DidInMemoryCache()
ID_RESOLVER = IdResolver(cache=CACHE)

user_last_seeds: dict[str, int] = {}

class AuthorizationError(Exception):
    ...

def hashcode(text: str, r: Random) -> int:
    l = list(text)
    r.shuffle(l)
    return hash(''.join(l))

app = Flask(__name__)

@app.route('/')
def index():
    return 'ATProto Feed Generator powered by The AT Protocol SDK for Python (https://github.com/MarshalX/atproto).'

@app.route('/.well-known/did.json', methods=['GET'])
def did_json():
    return jsonify({
        '@context': ['https://www.w3.org/ns/did/v1'],
        'id': SERVICE_DID,
        'service': [
            {
                'id': '#bsky_fg',
                'type': 'BskyFeedGenerator',
                'serviceEndpoint': f'https://{config.HOSTNAME}'
            }
        ]
    })

@app.route('/xrpc/app.bsky.feed.describeFeedGenerator', methods=['GET'])
def describe_feed_generator():
    return jsonify({
        'encoding': 'application/json',
        'body': {
            'did': SERVICE_DID,
            'feeds': [{'uri': feed['uri']} for feed in config.FEEDS.values()],
        }
    })

@app.route('/xrpc/app.bsky.feed.getFeedSkeleton', methods=['GET'])
def get_feed_skeleton():
    feed = request.args.get('feed', default=None, type=str)
    include_reposts = feed.endswith('chaos')
    print(f'Include reposts: {include_reposts}')

    # Get requester DID
    authorization = request.headers.get('Authorization')
    if not authorization:
        return 'Authorization header is missing', 401
    if not authorization.startswith('Bearer '):
        return 'Invalid authorization header', 401
    jwt = authorization[len('Bearer '):].strip()
    try:
        requester_did = verify_jwt(jwt, ID_RESOLVER.did.resolve_atproto_key).iss
    except TokenInvalidSignatureError as ex:
        return f'Invalid signature: {ex}', 401

    try:
        cursor = request.args.get('cursor', default=None, type=str)
    except ValueError:
        return 'Malformed cursor', 400

    limit = request.args.get('limit', default=20, type=int)
    print(f'Feed refreshed by {requester_did} - cursor = {cursor} - limit = {limit}:')

    db_con = psycopg2.connect(database='bluesky',
                           host='db',
                           user='postgres',
                           password=config.DB_PASSWORD,
                           port=5432)
    db_cursor = db_con.cursor()

    # If necessary, populate the requester's following list
    # (for any people they followed before this feed service started running)
    db_cursor.execute('SELECT 1 FROM follows WHERE follower = %s', (requester_did, ))
    if db_cursor.rowcount <= 0:
        print(f'Priming follows for {requester_did}.')

        start_time = time_ns()

        did_doc = ID_RESOLVER.did.resolve(requester_did)
        client = Client(did_doc.get_pds_endpoint())
        follows = client.com.atproto.repo.list_records(models.ComAtprotoRepoListRecords.Params(
            repo=requester_did,
            collection=models.ids.AppBskyGraphFollow,
            limit=100,
        ))
        follows_cursor = None
        authors = []
        follow_infos = []
        while True:
            follows = client.com.atproto.repo.list_records(models.ComAtprotoRepoListRecords.Params(
                repo=requester_did,
                collection=models.ids.AppBskyGraphFollow,
                limit=100,
                cursor=follows_cursor,
            ))

            for follow in follows.records:
                authors.append((
                    follow.value.subject,
                    False,
                ))
                follow_infos.append((
                    follow.uri,
                    requester_did,
                    follow.value.subject,
                ))

            follows_cursor = follows.cursor
            if follows_cursor == None:
                break

        # Add all follows
        if len(follow_infos) > 0:
            db_cursor.execute('ALTER TABLE follows DISABLE TRIGGER check_follows_primed_trigger')
            execute_batch(db_cursor, 'INSERT INTO follows VALUES(%s, %s, %s) ON CONFLICT DO NOTHING', follow_infos)
            db_cursor.execute('ALTER TABLE follows ENABLE TRIGGER check_follows_primed_trigger')
            print(f'Inserted {len(follow_infos)} follows into database.')

        print('Committing queries')
        db_con.commit()

        end_time = time_ns()
        elapsed_time_ms = (end_time - start_time) // 1_000_000

        print(f'Time to update follows: {elapsed_time_ms} ms.)')

    cursor_rand_id: int = None
    if cursor is not None:
        try:
            rand_id_str, did = cursor.split('::')
            cursor_rand_id = int(rand_id_str)
        except ValueError as ex:
            return f'Malformed cursor "{cursor}"', 400
        if did != requester_did:
            return f'JWT and cursor DID do not match', 400
    limit = limit if limit < 600 else 600

    start_time = time_ns()
    # Collect posts
    db_cursor.execute(f"""
                        SELECT uri, repost_uri, cid_rev
                        FROM posts
                        WHERE author IN
                            (SELECT followee FROM follows WHERE follower = '{requester_did}')
                        {'AND repost_uri IS NULL' if not include_reposts else ''}
                        ORDER BY cid_rev
			LIMIT 1000
                        """)
    posts = db_cursor.fetchall()
    print(f'Num posts: {len(posts)}')
    end_time = time_ns()
    elapsed_time_ms = (end_time - start_time) // 1_000_000
    print(f'Query time: {elapsed_time_ms} ms.')

    # Get the sorting seed
    seed = user_last_seeds.get(requester_did, 0)

    # An undefined cursor and larger limit indicates a full refresh, so we'll update the seed to reorder everything
    if cursor is None and limit > 20:
        seed += 1
        user_last_seeds[requester_did] = seed

    r = Random(seed)

    start_time = time_ns()
    feed = []
    for uri, repost_uri, cid_rev in posts:
        post: dict
        if include_reposts and repost_uri is not None and repost_uri != '':
            post = {
                'post': repost_uri,
                'reason': {
                    '$type': 'app.bsky.feed.defs#skeletonReasonRepost',
                    'repost': uri,
                },
                'rand_id': hashcode(cid_rev, r),
            }
        else:
            post = {
                'post': uri,
                'rand_id': hashcode(cid_rev, r),
            }
        
        feed.append(post)
    
    feed.sort(key=lambda p: p['rand_id'])

    # Find position based on rand_id from cursor
    position = 0
    if cursor_rand_id is not None:
        for post in feed:
            if post['rand_id'] > cursor_rand_id:
                break
            position += 1

    # Slice feed from position to limit
    feed_slice = feed[position:position+limit]
    if len(feed_slice) > 0:
        cursor_rand_id = feed_slice[-1]['rand_id']

    cursor = f'{cursor_rand_id}::{requester_did}'

    end_time = time_ns()
    elapsed_time_ms = (end_time - start_time) // 1_000_000
    print(f'Sort time: {elapsed_time_ms} ms.')

    body = { 'cursor': cursor, 'feed': feed_slice }
    
    return jsonify(body)

if __name__ == '__main__':
    print('Server started!')
    serve(app, host='0.0.0.0', port=5000)
