from atproto import Client, models
import config

feed_config = config.FEEDS['random_onlyposts']

def main():
    client = Client()
    client.login(config.HANDLE, config.PASSWORD)

    feed_did = f'did:web:{config.HOSTNAME}'

    avatar_blob = None
    avatar_path = feed_config.get('avatar_path', None)
    if avatar_path is not None and avatar_path != '':
        with open(avatar_path, 'rb') as file:
            avatar_data = file.read()
            avatar_blob = client.com.atproto.repo.upload_blob(avatar_data).blob

    # response = client.com.atproto.repo.delete_record(models.ComAtprotoRepoDeleteRecord.Data(
    #     repo=client.me.did,
    #     collection=models.ids.AppBskyFeedGenerator,
    #     rkey='favorites',
    # ))
    response = client.com.atproto.repo.put_record(models.ComAtprotoRepoPutRecord.Data(
        repo=client.me.did,
        collection=models.ids.AppBskyFeedGenerator,
        rkey=feed_config['record_name'],
        record=models.AppBskyFeedGenerator.Record(
            did=feed_did,
            display_name=feed_config['display_name'],
            description=feed_config['description'],
            avatar=avatar_blob,
            created_at=client.get_current_time_iso(),
        )
    ))

    print('Successfully published!')
    # print('Feed URI:', response.uri)

if __name__ == '__main__':
    main()
