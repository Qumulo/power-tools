from qupgrade import *

qr = qumulo_release_mgr()

def test_get_version_num():
    assert get_version_num('2.7.8') == 2070800
    assert get_version_num('2.7.8.1') == 2070801
    assert get_version_num('2.14.3') >= 2140205
    assert get_version_num('2.14.2') <= 2140205

def test_valid_releases():
    global qr
    assert qr.is_valid_release('2.12.6') == True
    assert qr.is_valid_release('2.12.7') == False
    assert qr.is_valid_release('2.14.0.3') == True
    assert qr.is_valid_release('2.14.0.4') == False
    assert qr.is_valid_release('2.14.0.X') == False

def test_get_next():
    global qr
    assert qr.get_next_q('2.10.1') == 2110000
    assert qr.get_next_q('2.9.0') == 2100000
    assert qr.get_next_q('2.12.4') == 2130000

def test_upgrade_regular():
    global qr
    qr.get_path('2.12.2', '2.13.2')
    steps = qr.print_qimg_list()
    assert len(steps) == 6
    assert steps[0] == " 1:    2.12.3  |  qimg: qumulo_install_2.12.3.qimg"
    assert steps[5] == " 6:    2.13.2  |  qimg: qumulo_install_2.13.2.qimg"

def test_upgrade_hpe():
    global qr
    qr.get_path('2.10.0', '2.14.3', is_hpe=True)
    steps = qr.print_qimg_list()
    assert len(steps) == 5
    assert steps[0] == " 1:    2.11.0  |  qimg: qumulo_install_2.11.0.qimg"
    assert steps[2] == " 3:    2.13.0  |  qimg: qumulo_install_hpe_2.13.0.qimg"

def test_upgrade_cloud():
    global qr
    qr.get_path('2.10.0', '2.14.3', is_cloud=True)
    steps = qr.print_qimg_list()
    assert len(steps) == 5
    assert steps[0] == " 1:    2.11.0  |  qimg: qumulo_install_cloud_2.11.0.qimg"
    assert steps[3] == " 4:  2.14.0.3  |  qimg: qumulo_install_cloud_2.14.0.3.qimg"
    assert steps[4] == " 5:    2.14.3  |  qimg: qumulo_install_cloud_2.14.3.qimg"
