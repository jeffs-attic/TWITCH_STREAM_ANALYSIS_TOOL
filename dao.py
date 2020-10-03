"""
Abstract DAO classes and their implementations for twitch.py
"""
import json
import sqlite3
from models import *
import datetime


class CommentDao:
    """
    Abstract class for tool that allows to manage persistent state of comments.
    """

    def __init__(self):  # pragma: no cover
        raise NotImplementedError  # pragma: no cover

    def get_all_comments(self):  # pragma: no cover
        """Retrieve all the comments from the database"""  # pragma: no cover
        raise NotImplementedError  # pragma: no cover

    def get_chat_log_from_comment(self, channel_id, stream_id, comment):  # pragma: no cover
        """Create and return chat log from given comment, and with given channel_id and stream_id."""
        raise NotImplementedError  # pragma: no cover

    def get_channel_and_stream_id(self, comment_index=0):  # pragma: no cover
        """Return channel_id and stream_id of comment at comment_index."""
        raise NotImplementedError  # pragma: no cover

    def count_comments_and_users(self):  # pragma: no cover
        """
        Return two dictionaries - first that maps messages to number of occurences and
        second that maps messages to number of users.
        """
        raise NotImplementedError  # pragma: no cover


class CommentDaoJSONImpl(CommentDao):
    """Extends CommentDao abstract class."""

    def __init__(self, filename):
        self.filename = filename

    def get_all_comments(self):
        """
        Overriden from CommentDao.
        """
        try:
            with open(self.filename) as file:
                json_file = json.load(file)
                comments = json_file["comments"]
                return comments

        except FileNotFoundError:
            return []

    def get_chat_log_from_comment(self, channel_id, stream_id, comment):
        """Create and return chat log from given comment, and with given channel_id and stream_id."""
        return ChatLog(channel_id, stream_id, comment["message"]["body"],
                       comment["commenter"]["display_name"],
                       comment["created_at"],
                       comment["content_offset_seconds"])

    def get_channel_and_stream_id(self, comment_index=0):
        """Return channel_id and stream_id of comment at comment_index."""
        all_comments = self.get_all_comments()
        first_comment = all_comments[comment_index]
        channel_id, stream_id = first_comment["channel_id"], first_comment["content_id"]
        return channel_id, stream_id

    def count_comments_and_users(self):
        """
        Return two dictionaries - first that maps messages to number of occurences and
        second that maps messages to number of users.
        """
        comments_count = {}
        comments_user_count = {}
        all_comments = self.get_all_comments()

        for c in all_comments:
            user = c["commenter"]["display_name"]

            comment_body = c["message"]["body"]
            count = comments_count.get(comment_body, 0)
            comments_count[comment_body] = count + 1
            user_count = comments_user_count.get(comment_body, set())
            user_count.add(user)
            comments_user_count[comment_body] = user_count

        return comments_count, comments_user_count


class ChannelDao:  # pragma: no cover
    """
    Abstract class for tool that allows to manage persistent state of channels.
    """

    def __init__(self):  # pragma: no cover
        raise NotImplementedError  # pragma: no cover

    def insert(self, channel):  # pragma: no cover
        """Store given channel in database.
        """
        raise NotImplementedError  # pragma: no cover

    def find_by_id(self, channel_id):  # pragma: no cover
        """Retrieve and return channel with given channel_id."""
        raise NotImplementedError  # pragma: no cover

    def start_session(self):  # pragma: no cover
        """Start current database connection session."""
        raise NotImplementedError  # pragma: no cover

    def close_session(self):  # pragma: no cover
        """Close current database connection session."""
        raise NotImplementedError  # pragma: no cover

    def save_changes(self):  # pragma: no cover
        """Commit changes to the database"""
        raise NotImplementedError  # pragma: no cover


class ChannelDaoSqlLiteImplementation(ChannelDao):
    """
    Extends ChannelDao abstract class.
    """

    def __init__(self, database_name):
        self.database_name = database_name
        self.database_connection = None
        self.cursor = None

    def save_changes(self):
        """Overriden from ChannelDao."""
        self.database_connection.commit()

    def close_session(self):
        """
        Overriden from ChannelDao.
        """
        self.database_connection.close()

    def start_session(self):
        """
        Overriden from ChannelDao.
        """
        self.database_connection = sqlite3.connect(self.database_name)
        self.cursor = self.database_connection.cursor()

    def __create_table_if_not_exists(self):
        """
        Overriden from ChannelDao.
        """
        self.cursor.execute("""CREATE TABLE if not exists channels
            (channel_id integer primary key, channel_name text)""")

    def insert(self, channel):
        """
        Overriden from ChannelDao.
        """
        self.__create_table_if_not_exists()

        self.cursor.execute("INSERT INTO CHANNELS VALUES ({},'{}')".format(channel.id, channel.name))

    def find_by_id(self, channel_id):
        """
        Overriden from ChannelDao.
        """
        self.__create_table_if_not_exists()

        rows = self.cursor.execute("select * from channels where channel_id = {}".format(channel_id))
        return rows


class SpamDao:  # pragma: no cover
    """
    Abstract class for tool that allows to manage persistent state of spam.
    """

    def __init__(self):  # pragma: no cover
        raise NotImplementedError  # pragma: no cover

    def get_all_wth_channel_and_stream_id(self, channel_id, stream_id):  # pragma: no cover
        """Retrieve and return spam messages with given channel_id and stream_id."""
        raise NotImplementedError  # pragma: no cover

    def insert(self, spam):  # pragma: no cover
        """Store given spam message in database."""
        raise NotImplementedError  # pragma: no cover

    def delete_with_channel_and_stream_id(self, channel_id, stream_id):  # pragma: no cover
        """Delete spam message with channel_id and stream_id from database."""
        raise NotImplementedError  # pragma: no cover

    def start_session(self):  # pragma: no cover
        """Start current database connection session."""
        raise NotImplementedError  # pragma: no cover

    def close_session(self):  # pragma: no cover
        """Close current database connection session."""
        raise NotImplementedError  # pragma: no cover

    def save_changes(self):  # pragma: no cover
        """Commit changes to the database"""
        raise NotImplementedError  # pragma: no cover

    def sort_and_insert_spam(self, comments_count, comments_user_count, channel_id, stream_id, threshold=10,
                             reverse=True):
        """Sort and store spam based on the key-value pairs comments_count, comments_user_count, and also
        channel_id and stream_id. Spam should be sorted in the reverse or non=reverse order based on the value of
        reverse. Spam should be created and sorted only iff the number of occurences of the comment in comments_count
        is greater than provided threshold.

        Preconditions:
        1) comments_count must have the following format: {"comment": count}, count should correspond to the number of
        occurences of the comment.
        2) comments_user_count must have the following format: {"comment": user_count}, user_count must correspond
        to the number of users who are related to this comment.

        """
        raise NotImplementedError


class SpamDaoSqlLiteImplementation(SpamDao):
    """Extends SpamDao abstract class."""

    def __init__(self, database_name):
        self.database_name = database_name
        self.database_connection = None
        self.cursor = None
        self.spam_factory = SpamFactory()

    def save_changes(self):
        """Overriden from SpamDao."""
        self.database_connection.commit()

    def __create_table_if_not_exists(self):
        self.cursor.execute("""create table if not exists top_spam (channel_id integer NOT NULL,
        stream_id integer NOT NULL, spam_text string, spam_occurrences integer,
        spam_user_count integer, FOREIGN KEY(channel_id) REFERENCES channels(channel_id))""")

    def get_all_wth_channel_and_stream_id(self, channel_id, stream_id):
        """
        Overriden from SpamDao.
        """
        self.__create_table_if_not_exists()
        rows = self.cursor.execute(("""select * from top_spam where channel_id = {} and stream_id = {}
            order by spam_occurrences desc, spam_user_count desc, spam_text""").format(
            channel_id, stream_id))

        spam_list = []
        for row in rows:
            spam = self.spam_factory.from_vector(row)
            spam_list.append(spam)
        return spam_list

    def insert(self, spam):
        """
        Overriden from SpamDao.
        """
        self.__create_table_if_not_exists()

        self.cursor.execute("insert into top_spam values(?,?,?,?,?)",
                            (spam.channel_id, spam.stream_id, spam.spam_text, spam.spam_occurences,
                             spam.spam_user_count))

    def delete_with_channel_and_stream_id(self, channel_id, stream_id):
        """
        Overriden from SpamDao.
        """
        self.__create_table_if_not_exists()

        self.cursor.execute("delete from top_spam where channel_id = ? and stream_id = ?", (channel_id, stream_id))

    def sort_and_insert_spam(self, comments_count, comments_user_count, channel_id, stream_id, threshold=10,
                             reverse=True):
        """Overriden from SpamDao.
        """
        sorted_comments_count = sorted(comments_count.items(), key=lambda kv: kv[1], reverse=reverse)
        count = 0

        for i, (key, value) in enumerate(sorted_comments_count):
            comment_user_number = len(comments_user_count[key])
            if value > threshold:
                new_spam = Spam(channel_id, stream_id, key, value, comment_user_number)
                self.insert(new_spam)
                count += 1

        return count

    def close_session(self):
        """
        Overriden from SpamDao.
        """
        self.database_connection.close()

    def start_session(self):
        """
        Overriden from SpamDao.
        """
        self.database_connection = sqlite3.connect(self.database_name)
        self.cursor = self.database_connection.cursor()


class ChatLogDao:  # pragma: no cover
    """
    Abstract class for tool that allows to manage persistent state of chat log.
    """

    def __init__(self):  # pragma: no cover
        raise NotImplementedError  # pragma: no cover

    def insert(self, chat_log):  # pragma: no cover
        """Store given chat log in the database."""
        raise NotImplementedError  # pragma: no cover

    def delete_with_channel_id_stream_id(self, channel_id, stream_id):  # pragma: no cover
        """Delete chat log with given channel_id and stream_id from the database."""
        raise NotImplementedError  # pragma: no cover

    def select_where_filter_conditions_are_satisfied(self, arguments):  # pragma: no cover
        """Generate and execute query based on the given arguments."""
        raise NotImplementedError  # pragma: no cover

    def get_all_with_channel_and_stream_id(self, channel_id, stream_id):  # pragma: no cover
        """Return all the rows from chat_log table where channel id and
        stream id match given channel_id and stream_id."""
        raise NotImplementedError  # pragma: no cover

    def start_session(self):  # pragma: no cover
        """Start current database connection session."""
        raise NotImplementedError  # pragma: no cover

    def close_session(self):  # pragma: no cover
        """Close current database connection session."""
        raise NotImplementedError  # pragma: no cover

    def save_changes(self):  # pragma: no cover
        """Commit changes to the database"""
        raise NotImplementedError  # pragma: no cover

    def get_viewership_metrics(self, channel_id, stream_id):  # pragma: no cover
        """Returns per minute message and viewer counts for the specified stream and channel that
        has been persisted via the storechatlog command. If no such data has been persisted, returns empty list.
        """
        raise NotImplementedError  # pragma: no cover

    def parse_chat_times(self, chat_logs):  # pragma: no cover
        """Convert chat_time into a datetime object in each chat log and return updated chat logs."""
        raise NotImplementedError  # pragma: no cover


class ChatLogDaoSqlLiteImplementation(ChatLogDao):
    """Extends ChatLogDao abstract class"""

    def __init__(self, database_name):
        self.database_name = database_name
        self.database_connection = None
        self.cursor = None
        self.operation_keyword_mapping = {"eq": " = ", "gt": " > ", "lt": " < ", "gteq": " >= ", "lteq": " <= ",
                                          "like": " like "}
        self.chat_log_factory = ChatLogFactory()
        self.spam_factory = SpamFactory()

    def __create_table_if_not_exists(self):
        self.cursor.execute("""create table if not exists chat_log (channel_id integer
        NOT NULL, stream_id integer NOT NULL, text string, user string, chat_time
        datetime, offset int, FOREIGN KEY(channel_id) REFERENCES channels(channel_id))""")

    def get_spam_list(self, channel_id, stream_id, threshold):
        """Implemented for enhancement get_top_spam2. Generate and return spam based on the data stored for chat
        logs."""
        chat_logs = self.get_all_with_channel_and_stream_id(channel_id, stream_id)
        spam_occurences = {}
        seen_messages_and_users = []
        spam_user_count = {}
        for chat_log in chat_logs:
            if chat_log.text not in spam_occurences:
                spam_occurences[chat_log.text] = 1
            else:
                spam_occurences[chat_log.text] += 1
            if (chat_log.user, chat_log.text) not in seen_messages_and_users:
                seen_messages_and_users.append((chat_log.user, chat_log.text))

                if chat_log.text not in spam_user_count:
                    spam_user_count[chat_log.text] = 1
                else:
                    spam_user_count[chat_log.text] += 1
        self.start_session()
        self.cursor.execute("""create table if not exists temp_spam (channel_id integer NOT NULL,
        stream_id integer NOT NULL, spam_text string, spam_occurrences integer,
        spam_user_count integer, FOREIGN KEY(channel_id) REFERENCES channels(channel_id))""")
        seen_messages = []
        for chat_log in chat_logs:
            if spam_occurences[chat_log.text] > threshold:
                if chat_log.text not in seen_messages:
                    spam = Spam(channel_id, stream_id, chat_log.text, spam_occurences[chat_log.text],
                                spam_user_count[chat_log.text])
                    seen_messages.append(chat_log.text)
                    self.cursor.execute("insert into temp_spam values(?,?,?,?,?)",
                                        (spam.channel_id, spam.stream_id, spam.spam_text, spam.spam_occurences,
                                         spam.spam_user_count))
        self.save_changes()
        spam_list = self.cursor.execute(("""select * from temp_spam order by spam_occurrences desc, 
        spam_user_count desc, spam_text"""))
        result = []
        for spam in spam_list:
            spam_instance = self.spam_factory.from_vector(spam)
            result.append(spam_instance)
        self.cursor.execute("drop table if exists temp_spam")
        self.close_session()
        return result

    def parse_chat_times(self, chat_logs):  # pragma: no cover
        """Convert chat_time into a datetime object in each chat log and return updated chat logs."""
        for i in range(len(chat_logs)):  # pragma: no cover
            chat_time = chat_logs[i].chat_time  # pragma: no cover

            prefix = chat_time[:chat_time.index("T")]  # pragma: no cover
            suffix = chat_time[chat_time.index("T") + 1:][0:8]  # pragma: no cover
            chat_time = prefix + ' ' + suffix  # pragma: no cover

            chat_logs[i].chat_time = datetime.datetime.strptime(chat_time, '%Y-%m-%d %H:%M:%S')  # pragma: no cover

        return chat_logs  # pragma: no cover

    def __sort_chat_log_by_minute(self, chat_logs, channel_id, stream_id):  # pragma: no cover
        per_minute_dict = {}  # pragma: no cover
        seen_users = {}  # pragma: no cover
        current_offset = 1  # pragma: no cover
        cur_total_seconds = (chat_logs[0].chat_time.time().minute * 60 +  # pragma: no cover
                             chat_logs[0].chat_time.time().second)  # pragma: no cover
        for chat_log in chat_logs:  # pragma: no cover
            if (chat_log.chat_time.time().minute * 60 +   # pragma: no cover
                    chat_log.chat_time.time().second - cur_total_seconds <= 59):  # pragma: no cover
                pass  # pragma: no cover
            else:  # pragma: no cover
                current_offset += 1  # pragma: no cover
                cur_total_seconds = (chat_log.chat_time.time().minute * 60 +   # pragma: no cover
                                     chat_log.chat_time.time().second)  # pragma: no cover

            if current_offset not in per_minute_dict:  # pragma: no cover
                per_minute_dict[current_offset] = {"offset": current_offset, "viewers": 1,   # pragma: no cover
                                                   "messages": 1}  # pragma: no cover
                seen_users[current_offset] = [chat_log.user]  # pragma: no cover
            else:  # pragma: no cover
                per_minute_dict[current_offset]["messages"] += 1  # pragma: no cover
                if chat_log.user not in seen_users[current_offset]:  # pragma: no cover
                    per_minute_dict[current_offset]["viewers"] += 1  # pragma: no cover
                    seen_users[current_offset].append(chat_log.user)  # pragma: no cover

        per_minute_list = []  # pragma: no cover
        for item in per_minute_dict.items():  # pragma: no cover
            per_minute_list.append(item[1])  # pragma: no cover

        result = [{"channel_id": channel_id, "stream_id": stream_id, "starttime": str(  # pragma: no cover
            chat_logs[0].chat_time),  # pragma: no cover
                   "per_minute": per_minute_list  # pragma: no cover
                   }]  # pragma: no cover

        return result  # pragma: no cover

    def get_viewership_metrics(self, channel_id, stream_id):  # pragma: no cover
        """Returns per minute message and viewer counts for the specified stream and channel that
        has been persisted via the storechatlog command. If no such data has been persisted, returns empty list.
        """
        chat_logs = self.get_all_with_channel_and_stream_id(channel_id, stream_id)  # pragma: no cover
        chat_logs = self.parse_chat_times(chat_logs)  # pragma: no cover
        chat_logs.sort()  # pragma: no cover
        viewership_stats = self.__sort_chat_log_by_minute(chat_logs, channel_id, stream_id)  # pragma: no cover
        return viewership_stats  # pragma: no cover

    def get_all_with_channel_and_stream_id(self, channel_id, stream_id):
        """implemented for enhancement get_top_spam2. Return all the rows from chat_log table where channel id and
        stream id match given channel_id and stream_id."""
        self.__create_table_if_not_exists()
        rows = self.cursor.execute("select * from chat_log where channel_id = {} and stream_id = {}".
                                   format(channel_id, stream_id))
        chat_logs = []
        for row in rows:
            chat_log = self.chat_log_factory.from_vector(row)
            chat_logs.append(chat_log)
        return chat_logs

    def save_changes(self):
        """Overriden from ChatLogDao."""
        self.database_connection.commit()

    def insert(self, chat_log):
        """
        Overriden from ChatLogDao.
        """
        self.__create_table_if_not_exists()

        self.cursor.execute("insert into chat_log VALUES (?,?,?,?,?,?)", (chat_log.channel_id, chat_log.stream_id,
                                                                          chat_log.text, chat_log.user,
                                                                          chat_log.chat_time, chat_log.offset))

    def delete_with_channel_id_stream_id(self, channel_id, stream_id):
        """
        Overriden from ChatLogDao.
        """
        self.__create_table_if_not_exists()

        self.cursor.execute("delete from chat_log where channel_id = ? and stream_id = ?", (channel_id, stream_id))

    def close_session(self):
        """
        Overriden from ChatLogDao.
        """
        self.database_connection.close()

    def start_session(self):
        """
        Overriden from ChatLogDao.
        """
        self.database_connection = sqlite3.connect(self.database_name)
        self.cursor = self.database_connection.cursor()

    def __append_comparisons_from_filters(self, filters, query, string_column_names, operation_keyword_mapping):
        for filter_arg in filters:
            first_pos = filter_arg.index(" ")
            last_pos = filter_arg.rindex(" ")
            column = filter_arg[0:first_pos]
            value = filter_arg[last_pos + 1:]
            operation = filter_arg[first_pos + 1:last_pos]

            fragment = column
            fragment += operation_keyword_mapping[operation]

            if column in string_column_names:
                fragment += "'" + value + "' AND "
            else:
                fragment += value + " AND "
            query += fragment

        return query

    def select_where_filter_conditions_are_satisfied(self, filters):
        """
        Overriden from ChatLogDao.
        """
        self.__create_table_if_not_exists()

        query = "select * from chat_log "
        if len(filters) > 0:
            query += "where "

        query = self.__append_comparisons_from_filters(filters, query, ["text", "user"],
                                                       self.operation_keyword_mapping)
        if len(filters) > 0:
            query = query[:-4]

        query += " order by chat_time"
        rows = self.cursor.execute(query)

        chat_logs = []
        for row in rows:
            chat_log = self.chat_log_factory.from_vector(row)
            chat_logs.append(chat_log)
        return chat_logs
