from api.Kiwoom import *
from util.make_up_universe import *
from util.db_helper import *
from util.time_helper import *
from util.notifier import *
import math
import traceback
import numpy as np
import pandas as pd


class BollingerMACDStrategy(QThread):
    def __init__(self):
        QThread.__init__(self)
        self.strategy_name = "BollingerMACDStrategy"
        self.kiwoom = Kiwoom()

        # 유니버스 정보를 담을 딕셔너리
        self.universe = {}

        # 계좌 예수금
        self.deposit = 0

        # 초기화 함수 성공 여부 확인 변수
        self.is_init_success = False

        self.init_strategy()

    def init_strategy(self):
        """전략 초기화 기능을 수행하는 함수"""
        try:
            # 유니버스 조회, 없으면 생성
            self.check_and_get_universe()

            # 가격 정보를 조회, 필요하면 생성
            self.check_and_get_price_data()

            # Kiwoom > 주문정보 확인
            self.kiwoom.get_order()

            # Kiwoom > 잔고 확인
            self.kiwoom.get_balance()

            # Kiwoom > 예수금 확인
            self.deposit = self.kiwoom.get_deposit()

            # 유니버스 실시간 체결정보 등록
            self.set_universe_real_time()

            self.is_init_success = True

        except Exception as e:
            print(traceback.format_exc())
            send_message(traceback.format_exc(), BOLMA_STRATEGY_MESSAGE_TOKEN)

    def check_and_get_universe(self):
        """유니버스가 존재하는지 확인하고 없으면 생성하는 함수"""
        if not check_table_exist(self.strategy_name, 'universe'):
            universe_list = get_universe()
            print(universe_list)
            universe = {}
            now = datetime.now().strftime("%Y%m%d")

            kospi_code_list = self.kiwoom.get_code_list_by_market("0")
            kosdaq_code_list = self.kiwoom.get_code_list_by_market("10")

            for code in kospi_code_list + kosdaq_code_list:
                code_name = self.kiwoom.get_master_code_name(code)
                if code_name in universe_list:
                    universe[code] = code_name

            universe_df = pd.DataFrame({
                'code': universe.keys(),
                'code_name': universe.values(),
                'created_at': [now] * len(universe.keys())
            })

            insert_df_to_db(self.strategy_name, 'universe', universe_df)

        sql = "select * from universe"
        cur = execute_sql(self.strategy_name, sql)
        universe_list = cur.fetchall()
        for item in universe_list:
            idx, code, code_name, created_at = item
            self.universe[code] = {
                'code_name': code_name
            }
        print(self.universe)

    def check_and_get_price_data(self):
        """일봉 데이터가 존재하는지 확인하고 없다면 생성하는 함수"""
        for idx, code in enumerate(self.universe.keys()):
            print("({}/{}) {}".format(idx + 1, len(self.universe), code))

            if check_transaction_closed() and not check_table_exist(self.strategy_name, code):
                price_df = self.kiwoom.get_price_data(code)
                insert_df_to_db(self.strategy_name, code, price_df)
            else:
                if check_transaction_closed():
                    sql = "select max(`{}`) from `{}`".format('index', code)
                    cur = execute_sql(self.strategy_name, sql)
                    last_date = cur.fetchone()
                    now = datetime.now().strftime("%Y%m%d")

                    if last_date[0] != now:
                        price_df = self.kiwoom.get_price_data(code)
                        insert_df_to_db(self.strategy_name, code, price_df)
                else:
                    sql = "select * from `{}`".format(code)
                    cur = execute_sql(self.strategy_name, sql)
                    cols = [column[0] for column in cur.description]

                    price_df = pd.DataFrame.from_records(data=cur.fetchall(), columns=cols)
                    price_df = price_df.set_index('index')
                    self.universe[code]['price_df'] = price_df

    def run(self):
        """실질적 수행 역할을 하는 함수"""
        while self.is_init_success:
            try:
                if not check_transaction_open():
                    print("장시간이 아니므로 5분간 대기합니다.")
                    time.sleep(5 * 60)
                    continue

                for idx, code in enumerate(self.universe.keys()):
                    print('[{}/{}_{}]'.format(idx + 1, len(self.universe), self.universe[code]['code_name']))
                    time.sleep(0.5)

                    if code in self.kiwoom.order.keys():
                        print('접수 주문', self.kiwoom.order[code])

                        if self.kiwoom.order[code]['미체결수량'] > 0:
                            pass

                    elif code in self.kiwoom.balance.keys():
                        print('보유 종목', self.kiwoom.balance[code])
                        if self.check_sell_signal(code):
                            self.order_sell(code)

                    else:
                        self.check_buy_signal_and_order(code)

            except Exception as e:
                print(traceback.format_exc())
                send_message(traceback.format_exc(), BOLMA_STRATEGY_MESSAGE_TOKEN)

    def set_universe_real_time(self):
        """유니버스 실시간 체결정보 수신 등록하는 함수"""
        fids = get_fid("체결시간")
        codes = self.universe.keys()
        codes = ";".join(map(str, codes))
        self.kiwoom.set_real_reg("9999", codes, fids, "0")

    def calculate_indicators(self, df):
        """볼린저밴드와 MACD 지표를 계산하는 함수"""
        # 볼린저밴드 계산 (20일 기준)
        df['middle_band'] = df['close'].rolling(window=20).mean()
        df['std'] = df['close'].rolling(window=20).std()
        df['upper_band'] = df['middle_band'] + (df['std'] * 2)
        df['lower_band'] = df['middle_band'] - (df['std'] * 2)

        # MACD 계산 (12, 26, 9)
        df['ema12'] = df['close'].ewm(span=12, adjust=False).mean()
        df['ema26'] = df['close'].ewm(span=26, adjust=False).mean()
        df['macd'] = df['ema12'] - df['ema26']
        df['signal'] = df['macd'].ewm(span=9, adjust=False).mean()
        df['macd_hist'] = df['macd'] - df['signal']

        # 거래량 20일 이동평균
        df['volume_ma20'] = df['volume'].rolling(window=20).mean()

        return df

    def check_sell_signal(self, code):
        """매도대상인지 확인하는 함수"""
        universe_item = self.universe[code]

        if code not in self.kiwoom.universe_realtime_transaction_info.keys():
            print("매도대상 확인 과정에서 아직 체결정보가 없습니다.")
            return False

        open = self.kiwoom.universe_realtime_transaction_info[code]['시가']
        high = self.kiwoom.universe_realtime_transaction_info[code]['고가']
        low = self.kiwoom.universe_realtime_transaction_info[code]['저가']
        close = self.kiwoom.universe_realtime_transaction_info[code]['현재가']
        volume = self.kiwoom.universe_realtime_transaction_info[code]['누적거래량']

        today_price_data = [open, high, low, close, volume]
        df = universe_item['price_df'].copy()
        df.loc[datetime.now().strftime('%Y%m%d')] = today_price_data

        df = self.calculate_indicators(df)

        # 현재 값 추출
        current_price = df['close'].iloc[-1]
        upper_band = df['upper_band'].iloc[-1]
        macd = df['macd'].iloc[-1]
        signal = df['signal'].iloc[-1]
        prev_macd = df['macd'].iloc[-2]
        prev_signal = df['signal'].iloc[-2]

        # 매도 조건:
        # 1. 현재가가 상단 밴드에 접근 (95% 이상)
        # 2. MACD가 시그널 라인을 하향 돌파
        band_condition = current_price >= upper_band * 0.95
        macd_condition = (prev_macd > prev_signal) and (macd < signal)

        # 매도 조건 충족 여부
        return band_condition and macd_condition

    def order_sell(self, code):
        """매도 주문 접수 함수"""
        quantity = self.kiwoom.balance[code]['보유수량']
        ask = self.kiwoom.universe_realtime_transaction_info[code]['(최우선)매도호가']
        order_result = self.kiwoom.send_order('send_sell_order', '1001', 2, code, quantity, ask, '00')

        message = "[{}]sell order is done! quantity:{}, ask:{}, order_result:{}".format(
            code, quantity, ask, order_result)
        send_message(message, BOLMA_STRATEGY_MESSAGE_TOKEN)

    def check_buy_signal_and_order(self, code):
        """매수 대상인지 확인하고 주문을 접수하는 함수"""
        if not check_adjacent_transaction_closed():
            return False

        universe_item = self.universe[code]

        if code not in self.kiwoom.universe_realtime_transaction_info.keys():
            print("매수대상 확인 과정에서 아직 체결정보가 없습니다.")
            return

        open = self.kiwoom.universe_realtime_transaction_info[code]['시가']
        high = self.kiwoom.universe_realtime_transaction_info[code]['고가']
        low = self.kiwoom.universe_realtime_transaction_info[code]['저가']
        close = self.kiwoom.universe_realtime_transaction_info[code]['현재가']
        volume = self.kiwoom.universe_realtime_transaction_info[code]['누적거래량']

        today_price_data = [open, high, low, close, volume]
        df = universe_item['price_df'].copy()
        df.loc[datetime.now().strftime('%Y%m%d')] = today_price_data

        df = self.calculate_indicators(df)

        # 현재 값 추출
        current_price = df['close'].iloc[-1]
        lower_band = df['lower_band'].iloc[-1]
        macd = df['macd'].iloc[-1]
        signal = df['signal'].iloc[-1]
        prev_macd = df['macd'].iloc[-2]
        prev_signal = df['signal'].iloc[-2]
        volume_ratio = volume / df['volume_ma20'].iloc[-1]

        # 매수 조건:
        # 1. 현재가가 하단 밴드에 근접 (105% 이하)
        # 2. MACD가 시그널 라인을 상향 돌파
        # 3. 거래량이 20일 평균 대비 150% 이상
        band_condition = current_price <= lower_band * 1.05
        macd_condition = (prev_macd < prev_signal) and (macd > signal)
        volume_condition = volume_ratio >= 1.5

        if band_condition and macd_condition and volume_condition:
            if (self.get_balance_count() + self.get_buy_order_count()) >= 10:
                return

            budget = self.deposit / (10 - (self.get_balance_count() + self.get_buy_order_count()))
            bid = self.kiwoom.universe_realtime_transaction_info[code]['(최우선)매수호가']
            quantity = math.floor(budget / bid)

            if quantity < 1:
                return

            amount = quantity * bid
            self.deposit = math.floor(self.deposit - amount * 1.00015)

            if self.deposit < 0:
                return

            order_result = self.kiwoom.send_order('send_buy_order', '1001', 1, code, quantity, bid, '00')
            self.kiwoom.order[code] = {'주문구분': '매수', '미체결수량': quantity}

            message = "[{}]buy order is done! quantity:{}, bid:{}, order_result:{}, deposit:{}, get_balance_count:{}, get_buy_order_count:{}, balance_len:{}".format(
                code, quantity, bid, order_result, self.deposit, self.get_balance_count(), self.get_buy_order_count(),
                len(self.kiwoom.balance))
            send_message(message, BOLMA_STRATEGY_MESSAGE_TOKEN)

    def get_balance_count(self):
        """매도 주문이 접수되지 않은 보유 종목 수를 계산하는 함수"""
        balance_count = len(self.kiwoom.balance)
        # kiwoom balance에 존재하는 종목이 매도 주문 접수되었다면 보유 종목에서 제외시킴
        for code in self.kiwoom.order.keys():
            if code in self.kiwoom.balance and self.kiwoom.order[code]['주문구분'] == "매도" and self.kiwoom.order[code][
                '미체결수량'] == 0:
                balance_count = balance_count - 1
        return balance_count

    def get_buy_order_count(self):
        """매수 주문 종목 수를 계산하는 함수"""
        buy_order_count = 0
        # 아직 체결이 완료되지 않은 매수 주문
        for code in self.kiwoom.order.keys():
            if code not in self.kiwoom.balance and self.kiwoom.order[code]['주문구분'] == "매수":
                buy_order_count = buy_order_count + 1
        return buy_order_count