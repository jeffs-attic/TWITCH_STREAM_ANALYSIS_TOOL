"""Tests for twitch.py"""
import unittest
from twitch import *
import sqlite3


def clean_up():
    """Helper function. Only used for testing. Clean the database"""
    conn = sqlite3.connect('twitch.db')
    c = conn.cursor()
    c.execute("drop table if exists chat_log")
    print("dropped chat_log")
    c.execute("drop table if exists top_spam")
    print("dropped top_spam")
    c.execute("drop table if exists channels")
    print("channels dropped")
    conn.close()


def clean_log_file(log_file_name):
    """Helper function. Only used for testing. Clean log file."""

    # clean log file
    resource = logging.FileHandler(log_file_name, mode='w')
    resource.close()


def setup_twitch(database_name, log_file_name, json_file_name=None):
    """Returns instance of Streaming Platform class initialized with given values"""
    twitch = StreamingPlatform("twitch.log")

    channel_dao = ChannelDaoSqlLiteImplementation("twitch.db")
    spam_dao = SpamDaoSqlLiteImplementation("twitch.db")
    chat_log_dao = ChatLogDaoSqlLiteImplementation("twitch.db")
    twitch.set_channel_dao(channel_dao)
    twitch.set_spam_dao(spam_dao)
    twitch.set_chat_log_dao(chat_log_dao)

    if json_file_name:
        comment_dao = CommentDaoJSONImpl(json_file_name)
        twitch.set_comment_dao(comment_dao)

    return twitch


class TestCreateChannel(unittest.TestCase):
    """Test functionality of create channel."""
    def setUp(self):
        """Clean up the database, set up new connection and obtain cursor."""
        clean_up()
        self.twitch = setup_twitch("twitch.db", "twitch.log")
        self.twitch.create_channel(1500, 'CallOfDuty')
        self.database_connection = sqlite3.connect("./twitch.db")
        self.cursor = self.database_connection.cursor()

    def test_single_channel_created(self):
        """test creation of a single channel."""
        rows = self.cursor.execute("select * from channels where channel_id = {}".format(1500))
        count = 0
        row_list = []
        for row in rows:
            row_list.append(row)
            count += 1

        # check the number of channels
        self.assertEqual(count, 1)

        # check whether values match
        self.assertEqual(row_list[0][0], 1500)
        self.assertEqual(row_list[0][1], 'CallOfDuty')
        self.test_multiple_channel_created()

    def test_multiple_channel_created(self):
        """Test creation and storage of multiple channels."""
        self.twitch.create_channel(1502, 'LeaugueofLegends')
        self.database_connection = sqlite3.connect("./twitch.db")
        self.cursor = self.database_connection.cursor()
        self.cursor = self.database_connection.cursor()

        rows = self.cursor.execute("select * from channels")
        count = 0
        row_list = []
        for row in rows:
            print(row)
            row_list.append(row)
            count += 1

        # check the number of channels
        self.assertEqual(count, 2)

        # check whether values match
        self.assertEqual(row_list[0][0], 1500)
        self.assertEqual(row_list[0][1], 'CallOfDuty')
        self.assertEqual(row_list[1][0], 1502)
        self.assertEqual(row_list[1][1], 'LeaugueofLegends')

    def tearDown(self):
        """Close current database connection."""
        self.database_connection.close()


class TestParseTopSpam(unittest.TestCase):
    """Test functionality of parse top spam."""

    def setUp(self):
        """Clean current logging file and call parse_top_spam on json file."""

        clean_up()
        self.twitch = setup_twitch("twitch.db", "twitch.log", "test_league.json")

        # clean log file
        resource = logging.FileHandler('twitch.log', mode='w')
        resource.close()

        self.twitch.parse_top_spam()

    def test_corrent_operation_one(self):

        with open("twitch.log") as file:
            content = file.readlines()

            self.assertEqual(['INFO:root:inserted 1 top spam records for stream 497295395 on channel 36029255\n'],
                             content)

    def test_correct_operations_many(self):

        with open("twitch.log") as file:
            content = file.readlines()

            self.assertEqual(content, ['INFO:root:inserted 1 top spam records for stream 497295395 ' +
                                       'on channel 36029255\n'])


class TestGetTopSpam(unittest.TestCase):
    """Test functionality of get top spam."""
    def setUp(self):
        """Clean the log file, call parse_top_spam and clean the logging file again."""

        clean_up()
        clean_log_file('twitch.log')

        self.twitch = setup_twitch("twitch.db", "twitch.log", "test_league2.json")
        self.twitch.parse_top_spam()
        resource = logging.FileHandler('twitch.log', mode='w')
        resource.close()

        self.twitch.get_top_spam(36029255, 497295395)

        self.database_connection = sqlite3.connect("./twitch.db")
        self.cursor = self.database_connection.cursor()

    def test_correct_elements(self):

        with open("twitch.log") as file:
            content = file.readlines()

            self.assertEqual(content, ['INFO:root:[{"occurrences": 18, "spam_text": "!drop", "user_count": 15}]\n'])

    def test_correct_num_elements(self):

        with open("twitch.log") as file:
            content = file.readlines()
            self.assertEqual(len(content), 1)

    def test_invalid_channel_and_stream_id(self):
        clean_up()
        clean_log_file('twitch.log')
        self.twitch.get_top_spam(-1, -1)

        with open("twitch.log") as file:
            content = file.readlines()
            self.assertEqual(content, ['INFO:root:[]\n'])

    def tearDown(self):
        """Close current database connection."""
        self.database_connection.close()


class TestStoreChatLog(unittest.TestCase):
    """Test functionality of store chat log."""
    def setUp(self):
        """clean up the database, call store chat log, establish new database connection."""

        clean_up()

        self.twitch = setup_twitch("twitch.db", "twitch.log", "test_league2.json")
        self.twitch.store_chat_log()
        self.database_connection = sqlite3.connect("./twitch.db")
        self.cursor = self.database_connection.cursor()

    def test_check_database_content(self):
        rows = self.cursor.execute("select * from chat_log")
        count = 0
        row_list = []
        for row in rows:
            row_list.append(row)
            count += 1

        self.assertEqual(count, 133)

    def tearDown(self):
        """Close the current database sesssion"""
        self.database_connection.close()


class TestQueryChatLog(unittest.TestCase):
    """Test functionality of query chat log"""
    def setUp(self):
        """Clean up the database, clean up the log, set up the database connection."""

        clean_up()
        # clean log file
        clean_log_file('twitch.log')

        self.twitch = setup_twitch("twitch.db", "twitch.log", "test_league2.json")
        self.twitch.store_chat_log()

        self.database_connection = sqlite3.connect("./twitch.db")
        self.cursor = self.database_connection.cursor()

        resource = logging.FileHandler('twitch.log', mode='w')
        resource.close()

    def test_correct_operation_performed(self):
        self.twitch.query_chat_log(['stream_id eq 497295395', 'user eq seabunnei'])

        with open("twitch.log") as file:
            content = file.readlines()

            res_str = 'INFO:root:[{"channel_id": 36029255, "chat_time": "2019-10-23T11:51:19.859498086Z", ' + \
                      '"offset": 29, "stream_id": 497295395, "text": "VoHiYo", "user": "seabunnei"}]\n'

            self.assertEqual(res_str, content[0])

    def test_invalid_arguments_provided(self):
        self.twitch.query_chat_log(['stream_id eq -1', 'user eq doesnotexist'])
        with open("twitch.log") as file:
            content = file.readlines()
            self.assertEqual(content, ['INFO:root:[]\n'])

    def tearDown(self):
        """Close the current database session."""
        self.database_connection.close()


class MockArgument:
    """Class for creating mock arguments."""
    def __init__(self, id, name, command):
        self.command = command
        self.id = id
        self.name = name
        self.file = "unexisting file"


class TestSetupParsers(unittest.TestCase):
    """Tests functionality of setup_parsers."""
    def setUp(self):
        """Clean up the database and log file."""

        clean_up()
        # clean log file
        clean_log_file('twitch.log')

    def test_createchannel_argument(self):
        argument_parser = ArgumentParser(description="Parse Twitch chatlogs")
        sub_parsers = argument_parser.add_subparsers(dest="command")
        sub_parsers.command = "createchannel"
        setup_parsers(sub_parsers)
        return_value = process_arguments(MockArgument(0, "random", "createchannel"), "twitch.db", "twitch.log")

        # we're passing valid arguments, not exceptions should be raised
        self.assertEqual(return_value, 0)

    def test_parsetopspam_argument(self):
        argument_parser = ArgumentParser(description="Parse Twitch chatlogs")
        sub_parsers = argument_parser.add_subparsers(dest="command")
        sub_parsers.command = "parsetopspam"
        setup_parsers(sub_parsers)
        return_value = process_arguments(MockArgument(0, "random", "parsetopspam"), "twitch.db", "twitch.log")
        self.assertEqual(return_value, -1)

    def test_get_top_spam(self):
        argument_parser = ArgumentParser(description="Parse Twitch chatlogs")
        sub_parsers = argument_parser.add_subparsers(dest="command")
        sub_parsers.command = "gettopspam"
        setup_parsers(sub_parsers)
        return_value = process_arguments(MockArgument(0, "random", "gettopspam"), "twitch.db", "twitch.log")

        # we're passing invalid arugments, exceptions should be raised
        self.assertEqual(return_value, -1)

    def test_storechatlog(self):
        argument_parser = ArgumentParser(description="Parse Twitch chatlogs")
        sub_parsers = argument_parser.add_subparsers(dest="command")
        sub_parsers.command = "storechatlog"
        setup_parsers(sub_parsers)
        return_value = process_arguments(MockArgument(0, "random", "storechatlog"), "twitch.db", "twitch.log")

        # we're passing invalid arugments, exceptions should be raised
        self.assertEqual(return_value, -1)

    def test_query_chat_log(self):
        argument_parser = ArgumentParser(description="Parse Twitch chatlogs")
        sub_parsers = argument_parser.add_subparsers(dest="command")
        sub_parsers.command = "querychatlog"
        setup_parsers(sub_parsers)
        return_value = process_arguments(MockArgument(0, "random", "querychatlog"), "twitch.db", "twitch.log")

        # we're passing invalid arugments, exceptions should be raised
        self.assertEqual(return_value, -1)


class TestGetTopSpam2(unittest.TestCase):
    """Test functionality of gettopspam2."""

    def setUp(self):
        """Clean up database, log file, and call method."""
        clean_up()
        clean_log_file('twitch.log')

        self.twitch = setup_twitch("twitch.db", "twitch.log", "test_league2.json")

    def test_compare_log_with_get_top_spam(self):

        self.twitch.store_chat_log()

        clean_log_file('twitch.log')

        self.twitch.get_top_spam2(36029255, 497295395)

        content_spam2 = None
        with open("twitch.log") as file:
            content_spam2 = file.readlines()

        clean_log_file('twitch.log')

        self.assertEqual(["INFO:root:[{'occurrences': 18, 'spam_text': '!drop', 'user_count': 15}]\n"],
                         content_spam2)


class TestFactoryClasses(unittest.TestCase):
    """Test functionality of ChatLogFactory and SpamFactory."""
    def setUp(self):
        """Create instances of chat log factory and spam factory."""
        self.chat_log_factory = ChatLogFactory()
        self.spam_factory = SpamFactory()

    def test_chat_log_factory(self):
        chat_log = self.chat_log_factory.from_vector([0, 0, "hello", "unicorn", "1980/01/01, 00:00:00", 0])
        self.assertEqual(str(chat_log), "ChatLog(0, 0, hello, unicorn, 1980/01/01, 00:00:00, 0)")

    def test_spam_factory(self):
        spam = self.spam_factory.from_vector([100, 100, "hello", 100, 100])
        self.assertEqual(str(spam), "Spam(100, 100, hello, 100, 100)")

    def test_chat_log_factory_type_error(self):
        chat_log = self.chat_log_factory.from_vector([0, 0, "hello", "unicorn"])
        self.assertEqual(chat_log, None)

    def test_spam_factory_type_error(self):
        spam = self.spam_factory.from_vector([100, 100, "hello"])
        self.assertEqual(spam, None)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()  # pragma: no cover
