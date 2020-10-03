"""Class for Streaming Platform."""

import logging
from dao import *
from models import *

logging.basicConfig(level=logging.INFO, filename='twitch.log')


class StreamingPlatform:
    """A streaming platform."""

    def __init__(self, log_file_name):
        # set DAO objects to None
        self.comment_dao = None
        self.channel_dao = None
        self.spam_dao = None
        self.chat_log_dao = None

    def set_comment_dao(self, comment_dao):
        """Set the value of comment DAO used by streaming platform"""
        self.comment_dao = comment_dao

    def set_channel_dao(self, channel_dao):
        """Set the value of channel DAO used by streaming platform"""
        self.channel_dao = channel_dao

    def set_spam_dao(self, spam_dao):
        """Set the value of spam DAO used by streaming platform"""
        self.spam_dao = spam_dao

    def set_chat_log_dao(self, chat_log_dao):
        """Set the value of chat log DAO used by streaming platform"""
        self.chat_log_dao = chat_log_dao

    def create_channel(self, id, name):
        """
        Create channel based on the given arguments - id and name.
        """
        self.channel_dao.start_session()
        channel = Channel(id, name)
        self.channel_dao.insert(channel)

        channels = self.channel_dao.find_by_id(id)
        for row in channels:
            print(row)

        logging.info("created channel id: {} name: {}".format(id, name))
        self.channel_dao.save_changes()
        self.channel_dao.close_session()

    def parse_top_spam(self):
        """
        Process messages and store top spam messages.
        """
        comments_count, comments_user_count = self.comment_dao.count_comments_and_users()
        channel_id, stream_id = self.comment_dao.get_channel_and_stream_id(0)
        self.spam_dao.start_session()
        self.spam_dao.delete_with_channel_and_stream_id(channel_id, stream_id)

        count = self.spam_dao.sort_and_insert_spam(comments_count, comments_user_count, channel_id, stream_id)
        self.spam_dao.save_changes()
        self.spam_dao.close_session()
        print("inserted {} top spam records for stream {} on channel {}".format(count, stream_id, channel_id))
        logging.info("inserted {} top spam records for stream {} on channel {}".format(count, stream_id,
                                                                                       channel_id))

    def get_top_spam(self, channel_id, stream_id):
        """
        Outputs top spam.
        """
        spam_key_value_list = []
        self.spam_dao.start_session()

        spam_list = self.spam_dao.get_all_wth_channel_and_stream_id(channel_id, stream_id)
        for spam in spam_list:
            spam_key_value_list.append({"spam_text": spam.get_text(), "occurrences": spam.get_occurences(),
                                        "user_count": spam.get_user_count()})

        self.spam_dao.close_session()

        logging.info((json.dumps(spam_key_value_list, sort_keys=True)))
        print(json.dumps(spam_key_value_list, sort_keys=True))

    def store_chat_log(self):
        """
        Generate and store chat log for comments.
        """
        self.chat_log_dao.start_session()
        chatlog_comments = self.comment_dao.get_all_comments()
        channel_id, stream_id = self.comment_dao.get_channel_and_stream_id(0)
        self.chat_log_dao.delete_with_channel_id_stream_id(channel_id, stream_id)

        for comment in chatlog_comments:
            self.chat_log_dao.insert(self.comment_dao.get_chat_log_from_comment(channel_id, stream_id, comment))
        self.chat_log_dao.save_changes()
        self.chat_log_dao.close_session()
        print("inserted {} records to chat log for stream {} on channel {}".format(len(chatlog_comments), stream_id,
                                                                                   channel_id))
        logging.info(
            "inserted {} records to chat log for stream {} on channel {}".format(len(chatlog_comments), stream_id,
                                                                                 channel_id))

    def query_chat_log(self, filters):
        """
        Outputs chat logs that satisfy given arguments.
        """
        self.chat_log_dao.start_session()
        chat_logs = self.chat_log_dao.select_where_filter_conditions_are_satisfied(filters)
        list_of_chat_log_dict = []
        for chat_log in chat_logs:
            chat_log_dict = chat_log.convert_to_dict()
            list_of_chat_log_dict.append(chat_log_dict)

        self.chat_log_dao.close_session()
        logging.info((json.dumps(list_of_chat_log_dict, sort_keys=True)))
        print(json.dumps(list_of_chat_log_dict, sort_keys=True))

    def get_top_spam2(self, channel_id, stream_id):
        """Takes channel_id and stream_id and produces the same output as get_top_spam, provided the data for the
        channel and stream has already been loaded. If no data has been loaded, empty list will appear in the output.
        """
        self.chat_log_dao.start_session()
        spam_list = self.chat_log_dao.get_spam_list(channel_id, stream_id, 10)

        spam_key_value_list = []
        for spam in spam_list:
            spam_key_value_list.append({"occurrences": spam.get_occurences(),
                                        "spam_text": spam.get_text(),
                                        "user_count": spam.get_user_count()})

        self.chat_log_dao.close_session()

        logging.info(eval((json.dumps(spam_key_value_list))))
        print(eval(json.dumps(json.dumps(spam_key_value_list))))

    def viewership_metrics(self, channel_id, stream_id):  # pragma: no cover
        """Outputs per minute message and viewer counts for the specified stream and channel that
        has been persisted via the storechatlog command. If no such data has been persisted, outputs empty list.
        """
        self.chat_log_dao.start_session()  # pragma: no cover
        key_value_list = self.chat_log_dao.get_viewership_metrics(channel_id, stream_id)  # pragma: no cover

        self.chat_log_dao.close_session()  # pragma: no cover
        logging.info(json.dumps(key_value_list))  # pragma: no cover
        print(json.dumps(key_value_list))  # pragma: no cover
