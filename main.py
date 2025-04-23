import requests as http
import urllib
from mastodon import Mastodon
from datetime import datetime
from ratelimit import limits, RateLimitException
from backoff import on_exception, expo

FIVE_MINUTES = 300


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


def get_all_replies(status, mastodon):
    replies = []
    while status and "in_reply_to_id" in status and status["in_reply_to_id"]:
        status = get_status(status["in_reply_to_id"], mastodon)
        if status and "in_reply_to_id" in status:
            replies.append(create_status(status, get_media_attachment_ids(status)))
        else:
            break
    return replies


@on_exception(expo, RateLimitException, max_tries=10)
@limits(calls=300, period=FIVE_MINUTES)
def get_status(id, mastodon):
    mastodon.status(id)


@on_exception(expo, RateLimitException, max_tries=10)
@limits(calls=300, period=FIVE_MINUTES)
def get_account_statuses(account, mastodon):
    statuses = mastodon.account_statuses(account)
    return mastodon.fetch_remaining(statuses)


def get_user_statuses_from_remotes(accounts, source_instances, target_instance):
    accounts_statuses = []
    for source_instance in source_instances:
        mastodon = Mastodon(
            access_token=f"{source_instance}_clientcred.secret", ratelimit_method="pace"
        )
        for account in accounts:
            print(f"Searching for posts for {account}@{source_instance}")
            account = mastodon.account_lookup(f"@{account}@{target_instance}")
            statuses = get_account_statuses(account, mastodon)
            print("Account-Statuses: " + str(len(statuses)))
            final_statuses = statuses.copy()
            for status in statuses:
                final_statuses += get_all_replies(status, mastodon)

            accounts_statuses.append(final_statuses)
    return accounts_statuses


def create_status(status, media_attachment_ids):

    return "EXECUTE backfill_statuses ({}, '{}', '{}', {}, {}, {}, {}, '{}', {}, {}, '{}', {}, '{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, {});\n".format(
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
        # (
        #     "{" + ",".join(media_attachment_ids)[:-1] + "}"
        #     if len(media_attachment_ids) > 0
        #     else "null"
        # ),
        "null",
    )


def get_media_attachment_ids(status):
    media_attachment_ids = []
    if (
        "media_attachments" in status
        and status["media_attachments"]
        and len(status["media_attachments"]) > 0
    ):
        for attachment in status["media_attachments"]:
            media_attachment_ids.append(attachment.id)
    return media_attachment_ids


def generate_statuses_sql(statuses):
    commands = []
    commands.append(
        "PREPARE backfill_statuses as INSERT INTO statuses (id,uri,text,created_at,updated_at,in_reply_to_id,reblog_of_id,url,sensitive,visibility,spoiler_text,reply,language,conversation_id,local,account_id,application_id,in_reply_to_account_id,poll_id,deleted_at,edited_at,trendable,ordered_media_attachment_ids) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23) ON CONFLICT DO NOTHING;\n"
    )
    for status in statuses:
        media_attachment_ids = get_media_attachment_ids(status)
        try:
            commands.append(create_status(status, media_attachment_ids))
        except:
            print("Exception: " + status)

    return commands


def write_commands(commands):
    with open("commands.sql", "w") as f:
        f.writelines(commands)


def get_visibility(visibility_str):
    mapping = {"public": 0, "unlisted": 1, "private": 2, "direct": 3, "limited": 4}
    return mapping[visibility_str]


def cleanup_statuses(accounts_statuses):
    cleaned_accounts_statuses = []
    for account in accounts_statuses:
        result = account.copy()
        for status in account:
            if not status:
                result.remove(status)
                continue
            id_exists = False
            for reference in account:
                if (
                    "in_reply_to_id" in status
                    and status["in_reply_to_id"]
                    and reference
                    and "id" in reference
                    and reference["id"] == status["in_reply_to_id"]
                ):
                    id_exists = True
                    print("Id exists in reply")
            if not id_exists:
                result.remove(status)
                print("Id does not exist in reply")
        cleaned_accounts_statuses += result
    return cleaned_accounts_statuses


def main():
    accounts = ["hhwerbefrei"]
    target_instance = "bewegung.social"
    source_instances = ["digitalcourage.social"]

    create_app(source_instances)
    login(source_instances)
    statuses = get_user_statuses_from_remotes(
        accounts, source_instances, target_instance
    )
    print(len(statuses))
    statuses = cleanup_statuses(statuses)
    print(len(statuses))
    commands = generate_statuses_sql(statuses)
    write_commands(commands)


if __name__ == "__main__":
    main()
