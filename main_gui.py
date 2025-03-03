from strategy.RSIStrategy import RSIStrategy
from strategy.BollingerMACDStrategy import BollingerMACDStrategy
from strategy.TripleScreenStrategy import TripleScreenStrategy
from PyQt5.QtWidgets import QApplication, QMainWindow, QWidget, QVBoxLayout, QPushButton, QLabel
import sys


class StrategySelector(QMainWindow):
    def __init__(self):
        super().__init__()
        self.initUI()
        self.strategy = None

    def initUI(self):
        self.setWindowTitle('Trading Strategy Selector')
        self.setGeometry(300, 300, 400, 200)

        # 중앙 위젯 생성
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)

        # 레이블 추가
        label = QLabel('거래 전략을 선택하세요:')
        layout.addWidget(label)

        # RSI 전략 버튼
        rsi_btn = QPushButton('RSI 전략', self)
        rsi_btn.clicked.connect(self.start_rsi_strategy)
        layout.addWidget(rsi_btn)

        # 볼린저+MACD 전략 버튼
        boll_macd_btn = QPushButton('볼린저+MACD 전략', self)
        boll_macd_btn.clicked.connect(self.start_bollinger_macd_strategy)
        layout.addWidget(boll_macd_btn)

        # 삼지창 전략 버튼
        TripleScreen_btn = QPushButton('삼지창 전략', self)
        TripleScreen_btn.clicked.connect(self.start_TripleScreen_strategy)
        layout.addWidget(TripleScreen_btn)

        # 전략 중지 버튼
        stop_btn = QPushButton('전략 중지', self)
        stop_btn.clicked.connect(self.stop_strategy)
        layout.addWidget(stop_btn)

        self.status_label = QLabel('상태: 대기중')
        layout.addWidget(self.status_label)

    def start_rsi_strategy(self):
        if self.strategy:
            self.stop_strategy()

        self.strategy = RSIStrategy()
        self.strategy.start()
        self.status_label.setText('상태: RSI 전략 실행 중')

    def start_bollinger_macd_strategy(self):
        if self.strategy:
            self.stop_strategy()

        self.strategy = BollingerMACDStrategy()
        self.strategy.start()
        self.status_label.setText('상태: 볼린저+MACD 전략 실행 중')

    def start_TripleScreen_strategy(self):
        if self.strategy:
            self.stop_strategy()

        self.strategy = TripleScreenStrategy()
        self.strategy.start()
        self.status_label.setText('상태: 삼지창 전략 실행 중')

    def stop_strategy(self):
        if self.strategy:
            self.strategy.terminate()
            self.strategy = None
            self.status_label.setText('상태: 중지됨')


if __name__ == '__main__':
    app = QApplication(sys.argv)

    window = StrategySelector()
    window.show()

    sys.exit(app.exec_())