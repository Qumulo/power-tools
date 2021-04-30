import unittest

from api_tree_walk import FileEntry


class FileEntryTest(unittest.TestCase):
    def test_is_directory(self):
        entry = FileEntry(name="foo", size=0, file_type="FS_FILE_TYPE_DIRECTORY", child_count=0)
        self.assertTrue(entry.is_directory())

    def test_is_not_directory(self):
        entry = FileEntry(name="foo", size=0, file_type="Not a Dir", child_count=0)
        self.assertFalse(entry.is_directory())

    def test_has_children(self):
        entry = FileEntry(name="foo", size=0, file_type="bar", child_count=1)
        self.assertTrue(entry.has_children())

    def test_does_not_have_children(self):
        entry = FileEntry(name="foo", size=0, file_type="bar", child_count=0)
        self.assertFalse(entry.has_children())


class WorkerTest(unittest.TestCase):
    pass


class ApiTreeWalkerTest(unittest.TestCase):
    pass
