"""채널 ID 찾기 — 빈 결과를 '채널 없음' 으로 읽지 않는가."""
import unittest
import telegram_chat_id as T


class ExtractTests(unittest.TestCase):
    def test_channel_post_extracted(self):
        u = [{"channel_post": {"chat": {"id": -1001234567890, "type": "channel",
                                        "title": "오마카세 레이더"}}}]
        self.assertEqual(T.chats_from_updates(u),
                         [(-1001234567890, "channel", "오마카세 레이더")])

    def test_duplicates_collapsed(self):
        c = {"id": -100123, "type": "channel", "title": "X"}
        u = [{"channel_post": {"chat": c}}, {"edited_channel_post": {"chat": c}}]
        self.assertEqual(len(T.chats_from_updates(u)), 1)

    def test_my_chat_member_counts(self):
        """봇을 채널에 **추가한 순간**도 업데이트로 온다 — 메시지가 없어도 잡힌다."""
        u = [{"my_chat_member": {"chat": {"id": -100999, "type": "channel",
                                          "title": "새 채널"}}}]
        self.assertEqual(T.chats_from_updates(u)[0][0], -100999)

    def test_empty_and_malformed_safe(self):
        self.assertEqual(T.chats_from_updates([]), [])
        self.assertEqual(T.chats_from_updates(None), [])
        self.assertEqual(T.chats_from_updates([{}, {"message": {}},
                                               {"message": {"chat": "문자열"}}]), [])

    def test_private_chat_labelled_not_channel(self):
        u = [{"message": {"chat": {"id": 555, "type": "private",
                                   "first_name": "혁기"}}}]
        cid, kind, title = T.chats_from_updates(u)[0]
        self.assertEqual(kind, "private")
        self.assertEqual(title, "혁기")


class CallTests(unittest.TestCase):
    def patch(self, fn):
        self.addCleanup(setattr, T.requests, "get", T.requests.get)
        T.requests.get = fn

    def test_api_error_reason_preserved(self):
        class R:
            status_code = 401
            def json(self):
                return {"ok": False, "description": "Unauthorized"}
        self.patch(lambda *a, **k: R())
        res, why = T.call("t", "getMe")
        self.assertIsNone(res)
        self.assertIn("Unauthorized", why)

    def test_network_error_reason_preserved(self):
        def boom(*a, **k):
            raise TimeoutError("read timed out")
        self.patch(boom)
        res, why = T.call("t", "getMe")
        self.assertIsNone(res)
        self.assertIn("TimeoutError", why)


if __name__ == "__main__":
    unittest.main()
