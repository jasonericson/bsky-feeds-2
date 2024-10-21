from atproto import Client, DidInMemoryCache, IdResolver, models, verify_jwt
from atproto.exceptions import TokenInvalidSignatureError
import bisect
import fcntl
from flask import Flask, jsonify, request
import random
import sqlite3
from time import time_ns
from waitress import serve

HANDLE: str = 'singpigtest.bsky.social'
PASSWORD: str = 'mczv-ak5r-3st6-hpkr'
HOSTNAME = 'bskyfeed.jasonericson.com'
# HOSTNAME = 'bee-upward-physically.ngrok-free.app'
SERVICE_DID = f'did:web:{HOSTNAME}'
URI = 'at://did:plc:da4qrww7zq3flsr2zialldef/app.bsky.feed.generator/chaos'
# URI = 'at://did:plc:str7htuk7asez6oi2pxgknci/app.bsky.feed.generator/chaos'

CACHE = DidInMemoryCache()
ID_RESOLVER = IdResolver(cache=CACHE)

user_last_seeds: dict[str, int] = {}

class AuthorizationError(Exception):
    ...

def hashcode(text: str) -> int:
    l = list(text)
    random.shuffle(l)
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
                'serviceEndpoint': f'https://{HOSTNAME}'
            }
        ]
    })

@app.route('/xrpc/app.bsky.feed.describeFeedGenerator', methods=['GET'])
def describe_feed_generator():
    return jsonify({
        'encoding': 'application/json',
        'body': {
            'did': SERVICE_DID,
            'feeds': [{'uri': URI}]
        }
    })

@app.route('/xrpc/app.bsky.feed.getFeedSkeleton', methods=['GET'])
def get_feed_skeleton():
    feed = request.args.get('feed', default=None, type=str)

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
    print(f'Feed refreshed by {requester_did} - limit = {limit}:')

    db_con = sqlite3.connect('firehose.db', detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    db_cursor = db_con.cursor()

    # If necessary, populate the requester's following list
    # (for any people they followed before this feed service started running)
    res = db_con.execute(f"SELECT follows_primed FROM people WHERE did='{requester_did}'")
    follows_primed_tuple = res.fetchone()
    if follows_primed_tuple is None or follows_primed_tuple[0] == 0:
        print(f'Priming follows for {requester_did}.')

        start_time = time_ns()

        client = Client()
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

        # Acquire lock file for writing to db
        lock_file = open('./firehose.db.lock', 'w+')
        print('Acquiring db lock')
        fcntl.flock(lock_file, fcntl.LOCK_EX)

        # Add the requester to the person table, if they're not there already
        if follows_primed_tuple is None:
            db_cursor.execute(f"INSERT OR IGNORE INTO people (did, follows_primed) VALUES ('{requester_did}', {False})")

        # Add all authors and follows
        if len(follow_infos) > 0:
            db_cursor.executemany('INSERT OR IGNORE INTO people VALUES(?, ?)', authors)
            db_cursor.executemany('INSERT OR IGNORE INTO follows VALUES(?, ?, ?)', follow_infos)
            print(f'Inserted {len(follow_infos)} follows into database.')
        db_cursor.execute(f"UPDATE people SET follows_primed = {True} WHERE did = '{requester_did}'")

        print('Committing queries')
        db_con.commit()
        print('Unlocking db')
        fcntl.flock(lock_file, fcntl.LOCK_UN)
        lock_file.close()

        end_time = time_ns()
        elapsed_time_ms = (end_time - start_time) // 1_000_000

        print(f'Time to update follows: {elapsed_time_ms} ms.)')

    if cursor is not None:
        try:
            rand_id_str, did = cursor.split('::')
            rand_id = int(rand_id_str)
        except ValueError as ex:
            return f'Malformed cursor "{cursor}"', 400
        if did != requester_did:
            return f'JWT and cursor DID do not match', 400
    limit = limit if limit < 600 else 600

    start_time = time_ns()
    res = db_con.execute(f"""
                        SELECT uri, repost_uri, cid_rev
                        FROM posts
                        WHERE reply_parent is NULL AND author IN
                            (SELECT followee FROM follows WHERE follower = '{requester_did}')
                        ORDER BY cid_rev
                        """)
    posts = res.fetchall()
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

    random.seed(seed)

    start_time = time_ns()
    feed = []
    for uri, repost_uri, cid_rev in posts:
        post: dict
        if repost_uri is not None and repost_uri != '':
            post = {
                'post': repost_uri,
                'reason': {
                    '$type': 'app.bsky.feed.defs#skeletonReasonRepost',
                    'repost': uri,
                },
                'rand_id': hashcode(cid_rev),
            }
        else:
            post = {
                'post': uri,
                'rand_id': hashcode(cid_rev),
            }
        
        feed.append(post)
    
    feed.sort(key=lambda p: p['rand_id'])

    end_time = time_ns()
    elapsed_time_ms = (end_time - start_time) // 1_000_000
    print(f'Sort time: {elapsed_time_ms} ms.')

    body = { 'feed': feed }
    
    return jsonify(body)

if __name__ == '__main__':
    print('Server started!')
    serve(app, host='0.0.0.0', port=5000)
