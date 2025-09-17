# Random Feeds

Source code for the feeds Random From Follows (https://bsky.app/profile/did:plc:da4qrww7zq3flsr2zialldef/feed/chaos) and Random - Only Posts (https://bsky.app/profile/did:plc:da4qrww7zq3flsr2zialldef/feed/rffposts).

To run:
- Create a `.env` file in the root folder (based on `.env.template`) and fill it out.
  - The ngrok values are optional, only for testing from a local computer.
- Create a `config.yml` file in the `src` folder (based on `config.yml.template`) and fill it out.
  - `handle` and `password` are the handle and app password for the account you wish to publish the feed on.
  - `hostname` = URL where the feed will be published.
  - `db_password` = postgres database password (same as `POSTGRES_PASSWORD` in `.env`).
  - `feeds` = used for `publishfeed.py`.
- Run `docker compose up -d`.
