from parsing_weather_desktop import *
from add_day_window import *
from PyQt5.QtWidgets import QMessageBox, QComboBox
from PyQt5 import QtWidgets
import sys
from pandasModel import pandasModel

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..', 'telegram_bot')))
from library.telegram_constants import *
import library.additional_functions as lib
import library.airflow_functions as afl
import library.logger as mylogger
from secret.auth_data import *


def msg_error_():
    msg_error = QMessageBox()
    msg_error.setWindowTitle("ERROR!")
    msg_error.setText("Ошибка!")
    msg_error.setIcon(QMessageBox.Critical)
    msg_error.exec_()


def msg_error_website():
    msg_error = QMessageBox()
    msg_error.setWindowTitle("ERROR!")
    msg_error.setText("Выберите сайт!")
    msg_error.setIcon(QMessageBox.Critical)
    msg_error.exec_()


def msg_error_city():
    msg_error = QMessageBox()
    msg_error.setWindowTitle("ERROR!")
    msg_error.setText("Выберите Город!")
    msg_error.setIcon(QMessageBox.Critical)
    msg_error.exec_()


def msg_error_dataset():
    msg_error = QMessageBox()
    msg_error.setWindowTitle("ERROR!")
    msg_error.setText("Не удалось обновить данные! Попробуйте позже!")
    msg_error.setIcon(QMessageBox.Critical)
    msg_error.exec_()


def msg_error_view():
    msg_error = QMessageBox()
    msg_error.setWindowTitle("ERROR!")
    msg_error.setText("Не удалось вывести данные!")
    msg_error.setIcon(QMessageBox.Critical)
    msg_error.exec_()


def msg_error_backup():
    msg_error = QMessageBox()
    msg_error.setWindowTitle("ERROR!")
    msg_error.setText("Не удалось сделать backup!")
    msg_error.setIcon(QMessageBox.Critical)
    msg_error.exec_()

def msg_error_connect():
    msg_error = QMessageBox()
    msg_error.setWindowTitle("ERROR!")
    msg_error.setText("Не удалось подключиться к базе данных!")
    msg_error.setIcon(QMessageBox.Critical)
    msg_error.exec_()


def msg_good_backup():
    msg_error = QMessageBox()
    msg_error.setWindowTitle("GOOD!")
    msg_error.setText("backup Сделан!")
    msg_error.setIcon(QMessageBox.Information)
    msg_error.exec_()

def msg_good_add_day():
    msg_error = QMessageBox()
    msg_error.setWindowTitle("GOOD!")
    msg_error.setText("День добавлен!")
    msg_error.setIcon(QMessageBox.Information)
    msg_error.exec_()

def msg_good_connect():
    msg_error = QMessageBox()
    msg_error.setWindowTitle("GOOD!")
    msg_error.setText("Подключение к БД установлено!")
    msg_error.setIcon(QMessageBox.Information)
    msg_error.exec_()


def on_click_Moscow():
    if not ui.radioButton.isChecked() and not ui.radioButton_2.isChecked():
        msg_error_website()
    elif ui.radioButton.isChecked():
        try:
            afl.update("Moscow", "GisMeteo", connect)
            log.info("update Moscow GisMeteo")
            ui.pushButton.setEnabled(False)
        except Exception as e:
            log.error(e)
            msg_error_dataset()
    elif ui.radioButton_2.isChecked():
        try:
            afl.update("Moscow", "Yandex", connect)
            ui.pushButton.setEnabled(False)
        except Exception as e:
            log.error(e)
            msg_error_dataset()
    if ui.radioButton_3.isChecked():
        on_click_rd_Moscow()


def on_click_Krasnodar():
    if not ui.radioButton.isChecked() and not ui.radioButton_2.isChecked():
        msg_error_website()
    elif ui.radioButton.isChecked():
        try:
            afl.update("Krasnodar", "GisMeteo", connect)
            ui.pushButton_2.setEnabled(False)
        except Exception as e:
            log.error(e)
            msg_error_dataset()
    elif ui.radioButton_2.isChecked():
        try:
            afl.update("Krasnodar", "Yandex", connect)
            ui.pushButton_2.setEnabled(False)
        except Exception as e:
            log.error(e)
            msg_error_dataset()
    if ui.radioButton_4.isChecked():
        on_click_rd_Krasnodar()


def on_click_Ekaterinburg():
    if not ui.radioButton.isChecked() and not ui.radioButton_2.isChecked():
        msg_error_website()
    elif ui.radioButton.isChecked():
        try:
            afl.update("Ekaterinburg", "GisMeteo", connect)
            ui.pushButton_3.setEnabled(False)
        except Exception as e:
            log.error(e)
            msg_error_dataset()
    elif ui.radioButton_2.isChecked():
        try:
            afl.update("Ekaterinburg", "Yandex", connect)
            ui.pushButton_3.setEnabled(False)
        except Exception as e:
            log.error(e)
            msg_error_dataset()
    if ui.radioButton_5.isChecked():
        on_click_rd_Ekaterinburg()


def on_click_backup():
    try:
        # table.backup()
        msg_good_backup()
    except Exception as e:
        log.error(e)
        msg_error_backup()


def on_click_rd_Moscow():
    if not ui.radioButton.isChecked() and not ui.radioButton_2.isChecked():
        msg_error_website()
    elif ui.radioButton.isChecked():
        try:
            model = pandasModel(lib.view("Moscow", "GisMeteo", connection=connect, schema='prom', key="all"))
            ui.tableView.setModel(model)
        except Exception as e:
            log.error(e)
            msg_error_view()
    elif ui.radioButton_2.isChecked():
        try:
            model = pandasModel(lib.view("Moscow", "Yandex", connection=connect, schema='prom', key="all"))
            ui.tableView.setModel(model)
        except Exception as e:
            log.error(e)
            msg_error_view()


def on_click_rd_Krasnodar():
    if not ui.radioButton.isChecked() and not ui.radioButton_2.isChecked():
        msg_error_website()
    elif ui.radioButton.isChecked():
        try:
            model = pandasModel(lib.view("Krasnodar", "GisMeteo", connection=connect, schema='prom', key="all"))
            ui.tableView.setModel(model)
        except Exception as e:
            print(e)
            msg_error_view()
    elif ui.radioButton_2.isChecked():
        try:
            model = pandasModel(lib.view("Krasnodar", "Yandex", connection=connect, schema='prom', key="all"))
            ui.tableView.setModel(model)
        except Exception as e:
            print(e)
            msg_error_view()


def on_click_rd_Ekaterinburg():
    if not ui.radioButton.isChecked() and not ui.radioButton_2.isChecked():
        msg_error_website()
    elif ui.radioButton.isChecked():
        try:
            model = pandasModel(lib.view("Ekaterinburg", "GisMeteo", connection=connect, schema='prom', key="all"))
            ui.tableView.setModel(model)
        except Exception as e:
            log.error(e)
            msg_error_view()
    elif ui.radioButton_2.isChecked():
        try:
            model = pandasModel(lib.view("Ekaterinburg", "Yandex", connection=connect, schema='prom', key="all"))
            ui.tableView.setModel(model)
        except Exception as e:
            log.error(e)
            msg_error_view()


def on_click_rd_Yandex():
    ui.pushButton.setEnabled(True)
    ui.pushButton_2.setEnabled(True)
    ui.pushButton_3.setEnabled(True)

    if ui.radioButton_3.isChecked():
        try:
            model = pandasModel(lib.view("Moscow", "Yandex", connection=connect, schema='prom', key="all"))
            ui.tableView.setModel(model)
        except Exception as e:
            log.error(e)
            msg_error_view()
    elif ui.radioButton_4.isChecked():
        try:
            model = pandasModel(lib.view("Krasnodar", "Yandex", connection=connect, schema='prom', key="all"))
            ui.tableView.setModel(model)
        except Exception as e:
            log.error(e)
            msg_error_view()
    elif ui.radioButton_5.isChecked():
        try:
            model = pandasModel(lib.view("Ekaterinburg", "Yandex", connection=connect, schema='prom', key="all"))
            ui.tableView.setModel(model)
        except Exception as e:
            log.error(e)
            msg_error_view()


def on_click_rd_GisMeteo():
    ui.pushButton.setEnabled(True)
    ui.pushButton_2.setEnabled(True)
    ui.pushButton_3.setEnabled(True)

    if ui.radioButton_3.isChecked():
        try:
            model = pandasModel(lib.view("Moscow", "GisMeteo", connection=connect, schema='prom', key="all"))
            ui.tableView.setModel(model)
        except Exception as e:
            log.error(e)
            msg_error_view()
    elif ui.radioButton_4.isChecked():
        try:
            model = pandasModel(lib.view("Krasnodar", "GisMeteo", connection=connect, schema='prom', key="all"))
            ui.tableView.setModel(model)
        except Exception as e:
            print(e)
            msg_error_view()
    elif ui.radioButton_5.isChecked():
        try:
            model = pandasModel(lib.view("Ekaterinburg", "GisMeteo", connection=connect, schema='prom', key="all"))
            ui.tableView.setModel(model)
        except Exception as e:
            log.error(e)
            msg_error_view()


def on_click_add_day():
    Add_Day_window.show()


def on_click_cancel():
    Add_Day_window.close()


def on_click_OK():
    if not ui_2.radioButton_Yandex.isChecked() and not ui_2.radioButton_GisMeteo.isChecked():
        msg_error_website()
    elif not ui_2.radioButton_Moscow.isChecked() and not ui_2.radioButton_Krasnodar.isChecked() and not ui_2.radioButton_Ekaterinburg.isChecked():
        msg_error_city()
    else:
        try:
            list_days = ui_2.lineEdit_4.text().split()
            list_nights = ui_2.lineEdit_5.text().split()
            list_weathers = list()
            for comboBox in COMBO_BOXES:
                list_weathers.append(comboBox.currentText())

            Year = ui_2.spinBox.value()
            Month = ui_2.spinBox_2.value()
            Day = ui_2.spinBox_3.value()

            if ui_2.radioButton_Yandex.isChecked():
                type = "Yandex"
            else:
                type = "GisMeteo"

            if ui_2.radioButton_Moscow.isChecked():
                city = "Moscow"
            elif ui_2.radioButton_Krasnodar.isChecked():
                city = "Krasnodar"
            else:
                city = "Ekaterinburg"

            afl.create_new_day(city, type, Year, Month, Day, list_days, list_nights, list_weathers, connect, 'prom')
            msg_good_add_day()
            Add_Day_window.close()
        except Exception as e:
            log.error(e)
            msg_error_()


def on_click_rd_2_GisMeteo():
    for comboBox in COMBO_BOXES:
        comboBox.clear()
        comboBox.addItems(WEATHERS_GISMETEO)


def on_click_rd_2_Yandex():
    for comboBox in COMBO_BOXES:
        comboBox.clear()
        comboBox.addItems(WEATHERS_YANDEX)


if __name__ == "__main__":
    # table = table()
    log = mylogger.Logger().get_logger()

    connect, _ = lib.create_connect(external_host, external_port, user, password, database)

    WEATHERS_GISMETEO = WEATHER_GISMETEO_SMILE.keys()
    WEATHERS_YANDEX = WEATHER_YANDEX_SMILE.keys()

    app = QtWidgets.QApplication(sys.argv)

    Parsing_Weather_window = QtWidgets.QMainWindow()
    ui = Ui_Parsing_Weather()
    ui.setupUi(Parsing_Weather_window)

    Add_Day_window = QtWidgets.QMainWindow()
    ui_2 = Ui_MainWindow()
    ui_2.setupUi(Add_Day_window)

    COMBO_BOXES = [Add_Day_window.findChild(QComboBox, f'comboBox_{i}') for i in range(1, 11)]

    Parsing_Weather_window.show()

    ui.pushButton.clicked.connect(on_click_Moscow)
    ui.pushButton_2.clicked.connect(on_click_Krasnodar)
    ui.pushButton_3.clicked.connect(on_click_Ekaterinburg)
    ui.pushButton_4.clicked.connect(on_click_backup)
    ui.pushButton_4.setEnabled(False) # backup выключен
    ui.pushButton_5.clicked.connect(on_click_add_day)

    ui.radioButton.clicked.connect(on_click_rd_GisMeteo)
    ui.radioButton_2.clicked.connect(on_click_rd_Yandex)

    ui.radioButton_3.clicked.connect(on_click_rd_Moscow)
    ui.radioButton_4.clicked.connect(on_click_rd_Krasnodar)
    ui.radioButton_5.clicked.connect(on_click_rd_Ekaterinburg)

    ui_2.pushButton.clicked.connect(on_click_OK)
    ui_2.pushButton_2.clicked.connect(on_click_cancel)

    ui_2.radioButton_GisMeteo.clicked.connect(on_click_rd_2_GisMeteo)
    ui_2.radioButton_Yandex.clicked.connect(on_click_rd_2_Yandex)

    log.info("app start!")

    if connect:
        msg_good_connect()
    else:
        log.critical("refused to connect")
        msg_error_connect()
        sys.exit('refused to connect')

    sys.exit(app.exec_())
