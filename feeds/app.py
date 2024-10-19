from flask import Flask, jsonify, request

HOSTNAME = 'bskyfeed.jasonericson.com'
SERVICE_DID = f'did:web:{HOSTNAME}'
URI = 'at://did:plc:da4qrww7zq3flsr2zialldef/app.bsky.feed.generator/chaos'

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

    try:
        cursor = request.args.get('cursor', default=None, type=str)
        limit = request.args.get('limit', default=20, type=int)
        body = { 'cursor': cursor, 'feed': [{'post': 'at://did:plc:da4qrww7zq3flsr2zialldef/app.bsky.feed.post/3l6qmib2wct2s'}] }
    except ValueError:
        return 'Malformed cursor', 400
    
    return jsonify(body)
