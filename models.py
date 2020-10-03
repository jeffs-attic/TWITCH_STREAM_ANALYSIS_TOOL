"""models for channel, spam, and chat log."""


class KeyValueModel:
    """Abstract class that models class that can be easily converted to a dictionary of key=value pairs"""
    def __init__(self):  # pragma: no cover
        raise NotImplementedError  # pragma: no cover

    def convert_to_dict(self):
        """Convert channel class to a set of key-value pairs."""
        return self.__dict__


class Channel(KeyValueModel):  # pragma: no cover
    """Models Twitch channel."""

    def __init__(self, id, name):  # pragma: no cover
        self.id = id  # pragma: no cover
        self.name = name  # pragma: no cover


class ChatLog(KeyValueModel):
    """Models chat log for Twitch.
    """

    def __init__(self, channel_id, stream_id, text, user, chat_time, offset):
        self.channel_id = channel_id
        self.stream_id = stream_id
        self.text = text
        self.user = user
        self.chat_time = chat_time
        self.offset = offset

    def __ge__(self, other):  # pragma: no cover
        return self.chat_time >= other.chat_time  # pragma: no cover

    def __gt__(self, other):  # pragma: no cover
        return self.chat_time > other.chat_time  # pragma: no cover

    def __le__(self, other):  # pragma: no cover
        return self.chat_time <= other.chat_time  # pragma: no cover

    def __lt__(self, other):  # pragma: no cover
        return self.chat_time < other.chat_time  # pragma: no cover

    def __eq__(self, other):  # pragma: no cover
        return self.chat_time == other.chat_time  # pragma: no cover

    def __str__(self):
        return "ChatLog({}, {}, {}, {}, {}, {})".format(self.channel_id, self.stream_id, self.text, self.user,
                                                        self.chat_time, self.offset)


class Spam(KeyValueModel):
    """Models spam message from Twitch."""
    def __init__(self, channel_id, stream_id, spam_text, spam_occurences, spam_user_count):
        self.channel_id = channel_id
        self.stream_id = stream_id
        self.spam_text = spam_text
        self.spam_occurences = spam_occurences
        self.spam_user_count = spam_user_count

    def get_text(self):
        """Return spam's text."""
        return self.spam_text

    def get_occurences(self):
        """Return spam's number of occurences."""
        return self.spam_occurences

    def get_user_count(self):
        """Return spam's user count."""
        return self.spam_user_count

    def __str__(self):
        """Return a string representation of Spam object."""
        return "Spam({}, {}, {}, {}, {})".format(self.channel_id, self.stream_id, self.spam_text,
                                                 self.spam_occurences, self.spam_user_count)


class ChatLogFactory:
    """Creates chat log instance from given values"""

    def from_vector(self, vector):
        """Create Chat log from vector of values"""
        try:
            return ChatLog(*vector)
        except TypeError:
            return None


class SpamFactory:
    """Create spam instance from given values"""

    def from_vector(self, vector):
        """Create Spam from vector values"""
        try:
            return Spam(*vector)
        except TypeError:
            return None
