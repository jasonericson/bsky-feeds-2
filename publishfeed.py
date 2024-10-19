from atproto import Client, models

HANDLE: str = 'singpigtest.bsky.social'
PASSWORD: str = 'mczv-ak5r-3st6-hpkr'
HOSTNAME: str = 'bee-upward-physically.ngrok-free.app'
RECORD_NAME: str = 'chaos'
DISPLAY_NAME: str = 'Random From Follows'
DESCRIPTION: str = 'Random collection of posts from your follows, from the last 12 hours.'

def main():
    client = Client()
    client.login(HANDLE, PASSWORD)

    feed_did = f'did:web:{HOSTNAME}'

    # response = client.com.atproto.repo.delete_record(models.ComAtprotoRepoDeleteRecord.Data(
    #     repo=client.me.did,
    #     collection=models.ids.AppBskyFeedGenerator,
    #     rkey=RECORD_NAME,
    # ))
    response = client.com.atproto.repo.put_record(models.ComAtprotoRepoPutRecord.Data(
        repo=client.me.did,
        collection=models.ids.AppBskyFeedGenerator,
        rkey=RECORD_NAME,
        record=models.AppBskyFeedGenerator.Record(
            did=feed_did,
            display_name=DISPLAY_NAME,
            description=DESCRIPTION,
            avatar=None,
            created_at=client.get_current_time_iso(),
        )
    ))

    print('Successfully published!')
    print('Feed URI:', response.uri)

if __name__ == '__main__':
    main()
