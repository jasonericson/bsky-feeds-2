from atproto import Client, models
import config

feed_config = config.FEEDS['random_from_follows']

def main():
    client = Client()
    client.login(config.HANDLE, config.PASSWORD)

    feed_did = f'did:web:{config.HOSTNAME}'

    # response = client.com.atproto.repo.delete_record(models.ComAtprotoRepoDeleteRecord.Data(
    #     repo=client.me.did,
    #     collection=models.ids.AppBskyFeedGenerator,
    #     rkey=RECORD_NAME,
    # ))
    response = client.com.atproto.repo.put_record(models.ComAtprotoRepoPutRecord.Data(
        repo=client.me.did,
        collection=models.ids.AppBskyFeedGenerator,
        rkey=feed_config['record_name'],
        record=models.AppBskyFeedGenerator.Record(
            did=feed_did,
            display_name=feed_config['display_name'],
            description=feed_config['description'],
            avatar=None,
            created_at=client.get_current_time_iso(),
        )
    ))

    print('Successfully published!')
    print('Feed URI:', response.uri)

if __name__ == '__main__':
    main()
