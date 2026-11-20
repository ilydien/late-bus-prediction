import unittest
import uuid

from governance.run_log import new_run_id


class RunLogTest(unittest.TestCase):
    def test_new_run_id_is_uuid(self):
        value = new_run_id()
        self.assertEqual(str(uuid.UUID(value)), value)

    def test_new_run_id_unique(self):
        self.assertNotEqual(new_run_id(), new_run_id())


if __name__ == "__main__":
    unittest.main()
