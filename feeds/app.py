from atproto import DidInMemoryCache, IdResolver, verify_jwt
from atproto.exceptions import TokenInvalidSignatureError
from flask import Flask, jsonify, request
from waitress import serve

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

    # If necessary, populate the requester's following list

    body = { 'feed': [{'post': 'at://did:plc:da4qrww7zq3flsr2zialldef/app.bsky.feed.post/3l3w3xxig4o2h'}] }
    
    return jsonify(body)

if __name__ == '__main__':
    print('Server started!')
    serve(app, host='0.0.0.0', port=5000)
