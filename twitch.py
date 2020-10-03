"""
Accepts command line arguments and manipulates data for Twitch Streaming platform based on those arguments.
"""
from argparse import *
from streaming_platform import *


def process_arguments(arguments, database_name, logging_file_name):
    """Process arguments and call appropriate function based on their type."""
    twitch = StreamingPlatform(logging_file_name)
    channel_dao = ChannelDaoSqlLiteImplementation(database_name)
    spam_dao = SpamDaoSqlLiteImplementation("twitch.db")
    chat_log_dao = ChatLogDaoSqlLiteImplementation("twitch.db")
    twitch.set_channel_dao(channel_dao)
    twitch.set_spam_dao(spam_dao)
    twitch.set_chat_log_dao(chat_log_dao)

    if arguments.command == "createchannel":
        twitch.create_channel(arguments.id, arguments.name)

    elif arguments.command == "parsetopspam":
        try:
            comment_dao = CommentDaoJSONImpl(arguments.file)
            twitch.set_comment_dao(comment_dao)
            twitch.parse_top_spam()
        except IndexError:
            return -1

    elif arguments.command == "gettopspam":
        try:
            twitch.get_top_spam(arguments.channel_id, arguments.stream_id)
        except AttributeError:
            return -1

    elif arguments.command == "storechatlog":
        try:
            comment_dao = CommentDaoJSONImpl(arguments.file)
            twitch.set_comment_dao(comment_dao)
            twitch.store_chat_log()
        except IndexError:
            return -1

    elif arguments.command == "querychatlog":
        try:
            twitch.query_chat_log(arguments.filters)
        except AttributeError:
            return -1

    # added for enhancement
    elif arguments.command == "gettopspam2":  # pragma: no cover
        twitch.get_top_spam2(arguments.channel_id, arguments.stream_id)  # pragma: no cover

    elif arguments.command == "viewership":  # pragma: no cover
        twitch.viewership_metrics(arguments.channel_id, arguments.stream_id)  # pragma: no cover

    return 0


def setup_parsers(sub_parsers):
    """Add parsers to sub_parsers to handle different arguments."""
    create_channel = sub_parsers.add_parser("createchannel")
    create_channel.add_argument("name")
    create_channel.add_argument("id", type=int)

    parse_top_spam = sub_parsers.add_parser("parsetopspam")
    parse_top_spam.add_argument("file")

    get_top_spam = sub_parsers.add_parser("gettopspam")
    get_top_spam.add_argument("channel_id", type=int)
    get_top_spam.add_argument("stream_id", type=int)

    store_chat_log = sub_parsers.add_parser("storechatlog")
    store_chat_log.add_argument("file")

    query_char_log = sub_parsers.add_parser("querychatlog")
    query_char_log.add_argument("filters", nargs="+")

    get_top_spam = sub_parsers.add_parser("gettopspam2")
    get_top_spam.add_argument("channel_id", type=int)
    get_top_spam.add_argument("stream_id", type=int)

    get_top_spam = sub_parsers.add_parser("viewership")
    get_top_spam.add_argument("channel_id", type=int)
    get_top_spam.add_argument("stream_id", type=int)


def main():  # pragma: no cover
    """
    starting point of the application
    """
    argument_parser = ArgumentParser(description="Parse Twitch chatlogs")  # pragma: no cover
    sub_parsers = argument_parser.add_subparsers(dest="command")  # pragma: no cover
    setup_parsers(sub_parsers)  # pragma: no cover

    process_arguments(argument_parser.parse_args(), "twitch.db", "twitch.log")  # pragma: no cover


if __name__ == "__main__":  # pragma: no cover
    main()  # pragma: no cover
