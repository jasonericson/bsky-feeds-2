from atproto import Client, DidInMemoryCache, IdResolver, models, verify_jwt
from atproto.exceptions import TokenInvalidSignatureError
import fcntl
from flask import Flask, jsonify, request
import sqlite3
from time import time_ns
from waitress import serve

HANDLE: str = 'singpigtest.bsky.social'
PASSWORD: str = 'mczv-ak5r-3st6-hpkr'
# HOSTNAME = 'bskyfeed.jasonericson.com'
HOSTNAME = 'bee-upward-physically.ngrok-free.app'
SERVICE_DID = f'did:web:{HOSTNAME}'
# URI = 'at://did:plc:da4qrww7zq3flsr2zialldef/app.bsky.feed.generator/chaos'
URI = 'at://did:plc:str7htuk7asez6oi2pxgknci/app.bsky.feed.generator/chaos'

CACHE = DidInMemoryCache()
ID_RESOLVER = IdResolver(cache=CACHE)

class AuthorizationError(Exception):
    ...

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
        raise AuthorizationError('Authorization header is missing')
    if not authorization.startswith('Bearer '):
        raise AuthorizationError('Invalid authorization header')
    jwt = authorization[len('Bearer '):].strip()
    try:
        requester_did = verify_jwt(jwt, ID_RESOLVER.did.resolve_atproto_key).iss
    except TokenInvalidSignatureError as ex:
        raise AuthorizationError('Invalid signature') from ex

    try:
        cursor = request.args.get('cursor', default=None, type=str)
    except ValueError:
        return 'Malformed cursor', 400

    limit = request.args.get('limit', default=20, type=int)
    print(f'Feed refreshed by {requester_did} - limit = {limit}:')

    con = sqlite3.connect('firehose.db', detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cur = con.cursor()

    # If necessary, populate the requester's following list
    # (for any people they followed before this feed service started running)
    res = con.execute(f"SELECT follows_primed FROM person WHERE did='{requester_did}'")
    follows_primed_tuple = res.fetchone()
    if follows_primed_tuple is None or follows_primed_tuple[0] == 0:
        print(f'Priming follows for {requester_did}.')

        start_time = time_ns()

        client = Client()
        client.login(HANDLE, PASSWORD)
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
            cur.execute(f'INSERT OR IGNORE INTO person ({requester_did}, {False})')

        # Add all authors and follows
        if len(follow_infos) > 0:
            cur.executemany('INSERT OR IGNORE INTO person VALUES(?, ?)', authors)
            cur.executemany('INSERT OR IGNORE INTO follow VALUES(?, ?, ?)', follow_infos)
            print(f'Inserted {len(follow_infos)} follows into database.')
        cur.execute(f"UPDATE person SET follows_primed = {True} WHERE did = '{requester_did}'")

        print('Committing queries')
        con.commit()
        print('Unlocking db')
        fcntl.flock(lock_file, fcntl.LOCK_UN)
        lock_file.close()

        end_time = time_ns()
        elapsed_time_ms = (end_time - start_time) // 1_000_000

        print(f'Time to update follows: {elapsed_time_ms} ms.)')

    body = { 'feed': [{'post': 'at://did:plc:da4qrww7zq3flsr2zialldef/app.bsky.feed.post/3l3w3xxig4o2h'}] }
    
    return jsonify(body)

if __name__ == '__main__':
    print('Server started!')
    serve(app, host='0.0.0.0', port=5000)
