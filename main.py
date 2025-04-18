import requests as http
import urllib
from mastodon import Mastodon
from datetime import datetime


def create_app(source_instances):
    for source_instance in source_instances:
        Mastodon.create_app(
            source_instance,
            api_base_url=f"https://{source_instance}",
            to_file=f"{source_instance}_clientcred.secret",
        )


def login(source_instances):
    for source_instance in source_instances:
        mastodon = Mastodon(
            client_id=f"{source_instance}_clientcred.secret",
        )
        print(mastodon.auth_request_url())

        # open the URL in the browser and paste the code you get
        mastodon.log_in(
            code=input("Enter the OAuth authorization code: "),
            to_file=f"{source_instance}_clientcred.secret",
        )


def get_user_statuses_from_remotes(accounts, source_instances, target_instance):
    accounts_statuses = []
    for source_instance in source_instances:
        mastodon = Mastodon(access_token=f"{source_instance}_clientcred.secret")
        for account in accounts:
            print(f"Searching for posts for {account}@{source_instance}")
            account = mastodon.account_lookup(f"@{account}@{target_instance}")
            statuses = mastodon.account_statuses(account)
            accounts_statuses.append(statuses)
    return accounts_statuses


def create_status(status, media_attachment_ids):

    return "EXECUTE backfill_statuses ({}, '{}', '{}', {}, {}, {}, {}, '{}', {}, {}, '{}', {}, '{}', {}, {}, {}, {}, {}, {}, {}, {},{});\n".format(
        status["id"],
        status["uri"].replace("'", r"\'"),
        status["content"].replace("'", r"\'"),
        int(datetime.timestamp(status["created_at"])),
        (
            int(datetime.timestamp(status["edited_at"]))
            if status["edited_at"]
            else datetime.timestamp(status["created_at"])
        ),
        (status["in_reply_to_id"] if status["in_reply_to_id"] else "null"),
        (status["reblog"]["id"] if status["reblog"] else "null"),
        status["url"].replace("'", r"\'") if status["url"] else "null",
        status["sensitive"],
        get_visibility(status["visibility"]),
        status["spoiler_text"].replace("'", r"\'"),
        True if status["in_reply_to_id"] else False,
        (status["language"].replace("'", r"\'") if status["language"] else "null"),
        "null",
        "True",
        status["account"]["id"],
        "null",
        (
            status["in_reply_to_account_id"]
            if status["in_reply_to_account_id"]
            else "null"
        ),
        "null",
        "null",
        (
            int(datetime.timestamp(status["edited_at"]))
            if status["edited_at"]
            else "null"
        ),
        "False",
        str(media_attachment_ids),
    )


def get_media_attachment_ids(status):
    media_attachment_ids = []
    if status.media_attachments and len(status["media_attachments"]) > 0:
        for attachment in status["media_attachments"]:
            media_attachment_ids.append(attachment.id)
    return media_attachment_ids


def generate_statuses_sql(accounts_statuses):
    commands = []
    commands.append(
        "PREPARE backfill_statuses as INSERT INTO (id,uri,text,created_at,updated_at,in_reply_to_id,reblog_of_id,url,sensitive,visibility,spoiler_text,reply,language,conversation_id,local,account_id,application_id,in_reply_to_account_id,poll_id,deleted_at,edited_at,trendable,ordered_media_attachment_ids) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23);\n"
    )
    for account_statuses in accounts_statuses:
        for status in account_statuses:
            # if status["in_reply_to_id"]:
            #     reply = mastodon.status(status["in_reply_to_id"])
            #     commands.append(create_status(reply, get_media_attachment_ids(reply)))
            media_attachment_ids = get_media_attachment_ids(status)
            commands.append(create_status(status, media_attachment_ids))

    print("\n".join(media_attachment_ids))
    print(len(commands))
    print(commands)
    return commands


def write_commands(commands):
    with open("commands.sql", "w") as f:
        f.writelines(commands)


def get_visibility(visibility_str):
    mapping = {"public": 0, "unlisted": 1, "private": 2, "direct": 3, "limited": 4}
    return mapping[visibility_str]


def main():
    accounts = ["hhwerbefrei"]
    target_instance = "bewegung.social"
    source_instances = ["digitalcourage.social"]

    create_app(source_instances)
    login(source_instances)
    statuses = get_user_statuses_from_remotes(
        accounts, source_instances, target_instance
    )
    commands = generate_statuses_sql(statuses)
    write_commands(commands)


if __name__ == "__main__":
    main()
