import unittest
from unittest.mock import Mock, patch
from qupgrade import get_version_num, qumulo_api, qumulo_release_mgr
from qumulo.rest_client import RestClient

qr = qumulo_release_mgr()


class QupgradeTest(unittest.TestCase):
    def test_get_version_num(self):
        assert get_version_num("2.7.8") == 2070800
        assert get_version_num("2.7.8.1") == 2070801
        assert get_version_num("2.14.3") >= 2140205
        assert get_version_num("2.14.2") <= 2140205

    def test_valid_releases(self):
        global qr
        assert qr.is_valid_release("2.12.6") == True
        assert qr.is_valid_release("2.12.7") == False
        assert qr.is_valid_release("2.14.0.3") == True
        assert qr.is_valid_release("2.14.0.4") == False
        assert qr.is_valid_release("2.14.0.X") == False

    def test_get_next(self):
        global qr
        assert qr.get_next_q("2.10.1") == 2110000
        assert qr.get_next_q("2.9.0") == 2100000
        assert qr.get_next_q("2.12.4") == 2130000

    def test_is_quarterly(self):
        global qr
        assert qr.is_quarterly("4.0.0.2") == True
        assert qr.is_quarterly("4.3.0") == True
        assert qr.is_quarterly("5.0.0.1") == True
        assert qr.is_quarterly("5.0.1") == False

    def test_upgrade_regular(self):
        global qr
        qr.get_path("2.12.2", "2.13.2")
        steps = qr.print_qimg_list()
        assert len(steps) == 6
        assert steps[0] == " 1:    2.12.3  |  qimg: qumulo_upgrade_2.12.3.qimg"
        assert steps[5] == " 6:    2.13.2  |  qimg: qumulo_upgrade_2.13.2.qimg"

    def test_upgrade_2121_302_hpe(self):
        global qr
        qr.get_path("2.12.1", "3.0.2", is_hpe=True)
        steps = qr.print_qimg_list()
        assert len(steps) == 5
        assert steps[0] == " 1:    2.12.2  |  qimg: qumulo_upgrade_2.12.2.qimg"
        assert steps[1] == " 2:    2.13.0  |  qimg: qumulo_upgrade_hpe_2.13.0.qimg"
        assert steps[2] == " 3:  2.14.0.3  |  qimg: qumulo_upgrade_2.14.0.3.qimg"
        assert steps[4] == " 5:     3.0.2  |  qimg: qumulo_upgrade_3.0.2.qimg"

    def test_upgrade_2121_302(self):
        global qr
        qr.get_path("2.12.1", "3.0.2")
        steps = qr.print_qimg_list()
        assert len(steps) == 5
        assert steps[0] == " 1:    2.12.2  |  qimg: qumulo_upgrade_2.12.2.qimg"
        assert steps[1] == " 2:    2.13.0  |  qimg: qumulo_upgrade_2.13.0.qimg"
        assert steps[2] == " 3:  2.14.0.3  |  qimg: qumulo_upgrade_2.14.0.3.qimg"
        assert steps[4] == " 5:     3.0.2  |  qimg: qumulo_upgrade_3.0.2.qimg"

    def test_upgrade_cloud(self):
        global qr
        qr.get_path("2.10.0", "2.14.3", is_cloud=True)
        steps = qr.print_qimg_list()
        assert len(steps) == 5
        assert steps[0] == " 1:    2.11.0  |  qimg: qumulo_upgrade_cloud_2.11.0.qimg"
        assert steps[3] == " 4:  2.14.0.3  |  qimg: qumulo_upgrade_cloud_2.14.0.3.qimg"
        assert steps[4] == " 5:    2.14.3  |  qimg: qumulo_upgrade_cloud_2.14.3.qimg"


class QUpgradeVersionTest(unittest.TestCase):
    def setUp(self):
        self.api = qumulo_api()
        self.api.rc = Mock(spec=RestClient)
        self.api.login = Mock(spec=self.api.login)
        self.api.test_login("host", "port", "user", "password")

    def test_upgrade_uses_old_api(self):
        target_version = "4.1.0.1"

        self.api.rc.request.return_value = {
            "state": "UPGRADE_PREPARED",
            "error_state": ""
        }
        self.api.rc.version.version.return_value = {
            "revision_id": "Qumulo Core 4.1.0.1"
        }
        self.api.rc.upgrade.status_get.side_effect = [
            {"state": "UPGRADE_IDLE", "error_state": "UPGRADE_ERROR_NO_ERROR"},
            {"state": "UPGRADE_PREPARING", "error_state": "UPGRADE_ERROR_NO_ERROR"},
            {"state": "UPGRADE_PREPARED", "error_state": "UPGRADE_ERROR_NO_ERROR"},
            {"state": "UPGRADE_PREPARED", "error_state": "UPGRADE_ERROR_NO_ERROR"},
        ]

        self.api.upgrade_to(target_version, "placeholder/path.qimg")

    def test_uses_new_api(self):
        target_version = "4.1.1"
        self.api.rc.version.version.return_value = {"revision_id": "Qumulo Core 4.1.1"}
        self.api.rc.upgrade_v2.status.side_effect = [
            {"state": "UPGRADE_STATE_IDLE"},
            {"state": "UPGRADE_STATE_PREPARING"},
            {"state": "UPGRADE_STATE_PREPARED"},
            {"state": "UPGRADE_STATE_PREPARED"},
        ]
        self.api.upgrade_to(target_version, "placeholder/path.qimg")
        self.api.rc.upgrade_v2.prepare.assert_called_once_with(
            "placeholder/path.qimg", auto_commit=False
        )
        self.api.rc.upgrade_v2.commit.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
