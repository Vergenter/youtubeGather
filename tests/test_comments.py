import json
import unittest
from models.Comment import fromJson


class TestJsonParsing(unittest.TestCase):
    """Main Unit tests"""

    def test_parse_basic(self):
        with open('src//tests//resources//simpleComment.json', 'r', encoding='utf-8') as file:
            basic_data = json.load(file)
        fromJson(basic_data)

    def test_parse_with_reply(self):
        with open('src//tests//resources//commentWithReply.json', 'r', encoding='utf-8') as file:
            with_reply = json.load(file)
        fromJson(with_reply)


if __name__ == '__main__':
    unittest.main()
