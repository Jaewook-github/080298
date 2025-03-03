from api.Kiwoom import *
from util.make_up_universe import *
from util.db_helper import *
from util.time_helper import *
from util.notifier import *
import math
import traceback
import numpy as np
import pandas as pd
from datetime import datetime, timedelta


class TripleScreenStrategy(QThread):
    def __init__(self):
        QThread.__init__(self)
        self.strategy_name = "TripleScreenStrategy"
        self.kiwoom = Kiwoom()

        # 유니버스 정보를 담을 딕셔너리
        self.universe = {}

        # 계좌 예수금
        self.deposit = 0

        # 초기화 함수 성공 여부 확인 변수
        self.is_init_success = False

        # 주간 데이터 저장용 딕셔너리
        self.weekly_data = {}

        self.init_strategy()

    def init_strategy(self):
        """전략 초기화 기능을 수행하는 함수"""
        try:
            # 유니버스 조회, 없으면 생성
            self.check_and_get_universe()

            # 가격 정보를 조회, 필요하면 생성
            self.check_and_get_price_data()

            # 주간 데이터 생성
            self.create_weekly_data()

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
            send_message(traceback.format_exc(), TripleScreen_STRATEGY_MESSAGE_TOKEN)

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

            # (1)케이스: 일봉 데이터가 아예 없는지 확인(장 종료 이후)
            if check_transaction_closed() and not check_table_exist(self.strategy_name, code):
                # API를 이용해 조회한 가격 데이터 price_df에 저장
                price_df = self.kiwoom.get_price_data(code)
                # 코드를 테이블 이름으로 해서 데이터베이스에 저장
                insert_df_to_db(self.strategy_name, code, price_df)
                # price_df를 universe에도 저장
                self.universe[code]['price_df'] = price_df
            else:
                # (2), (3), (4) 케이스: 일봉 데이터가 있는 경우
                # (2)케이스: 장이 종료된 경우 API를 이용해 얻어온 데이터를 저장
                if check_transaction_closed():
                    # 저장된 데이터의 가장 최근 일자를 조회
                    sql = "select max(`{}`) from `{}`".format('index', code)
                    cur = execute_sql(self.strategy_name, sql)
                    # 일봉 데이터를 저장한 가장 최근 일자를 조회
                    last_date = cur.fetchone()
                    # 오늘 날짜를 20210101 형태로 지정
                    now = datetime.now().strftime("%Y%m%d")

                    # 최근 저장 일자가 오늘이 아닌지 확인
                    if last_date[0] != now:
                        price_df = self.kiwoom.get_price_data(code)
                        # 코드를 테이블 이름으로 해서 데이터베이스에 저장
                        insert_df_to_db(self.strategy_name, code, price_df)
                        # price_df를 universe에도 저장
                        self.universe[code]['price_df'] = price_df

                # 데이터베이스에서 데이터를 조회하여 저장
                sql = "select * from `{}`".format(code)
                cur = execute_sql(self.strategy_name, sql)
                cols = [column[0] for column in cur.description]

                # 데이터베이스에서 조회한 데이터를 DataFrame으로 변환해서 저장
                price_df = pd.DataFrame.from_records(data=cur.fetchall(), columns=cols)
                price_df = price_df.set_index('index')
                # 가격 데이터를 self.universe에서 접근할 수 있도록 저장
                self.universe[code]['price_df'] = price_df

    def create_weekly_data(self):
        """일봉 데이터를 바탕으로 주간 데이터 생성"""
        for code in self.universe.keys():
            df = self.universe[code]['price_df'].copy()
            df.index = pd.to_datetime(df.index)
            weekly_df = df.resample('W').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            })
            self.weekly_data[code] = weekly_df

    def calculate_indicators(self, df, timeframe='daily'):
        """기술적 지표 계산 함수"""
        # MACD (장기 추세)
        exp1 = df['close'].ewm(span=12, adjust=False).mean()
        exp2 = df['close'].ewm(span=26, adjust=False).mean()
        df['macd'] = exp1 - exp2
        df['signal'] = df['macd'].ewm(span=9, adjust=False).mean()
        df['macd_hist'] = df['macd'] - df['signal']

        # Stochastic (중기 조정)
        period = 14
        df['lowest_low'] = df['low'].rolling(window=period).min()
        df['highest_high'] = df['high'].rolling(window=period).max()
        df['%K'] = ((df['close'] - df['lowest_low']) / (df['highest_high'] - df['lowest_low'])) * 100
        df['%D'] = df['%K'].rolling(window=3).mean()

        # Force Index (단기 진입)
        df['force_index'] = df['close'].diff(1) * df['volume']
        df['force_index_ema'] = df['force_index'].ewm(span=13, adjust=False).mean()

        return df

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
                send_message(traceback.format_exc(), TripleScreen_STRATEGY_MESSAGE_TOKEN)

    def set_universe_real_time(self):
        """유니버스 실시간 체결정보 수신 등록하는 함수"""
        fids = get_fid("체결시간")
        codes = self.universe.keys()
        codes = ";".join(map(str, codes))
        self.kiwoom.set_real_reg("9999", codes, fids, "0")

    def check_sell_signal(self, code):
        """매도대상인지 확인하는 함수"""
        if code not in self.kiwoom.universe_realtime_transaction_info.keys():
            print("매도대상 확인 과정에서 아직 체결정보가 없습니다.")
            return False

        open = self.kiwoom.universe_realtime_transaction_info[code]['시가']
        high = self.kiwoom.universe_realtime_transaction_info[code]['고가']
        low = self.kiwoom.universe_realtime_transaction_info[code]['저가']
        close = self.kiwoom.universe_realtime_transaction_info[code]['현재가']
        volume = self.kiwoom.universe_realtime_transaction_info[code]['누적거래량']

        daily_df = self.universe[code]['price_df'].copy()
        daily_df.loc[datetime.now().strftime('%Y%m%d')] = [open, high, low, close, volume]

        weekly_df = self.weekly_data[code].copy()

        daily_df = self.calculate_indicators(daily_df, 'daily')
        weekly_df = self.calculate_indicators(weekly_df, 'weekly')

        # 매도 신호 확인
        weekly_macd_down = weekly_df['macd'].iloc[-1] < weekly_df['signal'].iloc[-1]
        daily_stoch_overbought = daily_df['%K'].iloc[-1] > 80 and daily_df['%D'].iloc[-1] > 80
        daily_force_negative = daily_df['force_index_ema'].iloc[-1] < 0

        return weekly_macd_down and daily_stoch_overbought and daily_force_negative

    def order_sell(self, code):
        """매도 주문 접수 함수"""
        quantity = self.kiwoom.balance[code]['보유수량']
        ask = self.kiwoom.universe_realtime_transaction_info[code]['(최우선)매도호가']
        order_result = self.kiwoom.send_order('send_sell_order', '1001', 2, code, quantity, ask, '00')

        message = "[{}]sell order is done! quantity:{}, ask:{}, order_result:{}".format(
            code, quantity, ask, order_result)
        send_message(message, TripleScreen_STRATEGY_MESSAGE_TOKEN)

    def check_buy_signal_and_order(self, code):
        """매수 대상인지 확인하고 주문을 접수하는 함수"""
        if not check_adjacent_transaction_closed():
            return False

        if code not in self.kiwoom.universe_realtime_transaction_info.keys():
            print("매수대상 확인 과정에서 아직 체결정보가 없습니다.")
            return

        open = self.kiwoom.universe_realtime_transaction_info[code]['시가']
        high = self.kiwoom.universe_realtime_transaction_info[code]['고가']
        low = self.kiwoom.universe_realtime_transaction_info[code]['저가']
        close = self.kiwoom.universe_realtime_transaction_info[code]['현재가']
        volume = self.kiwoom.universe_realtime_transaction_info[code]['누적거래량']

        daily_df = self.universe[code]['price_df'].copy()
        daily_df.loc[datetime.now().strftime('%Y%m%d')] = [open, high, low, close, volume]

        weekly_df = self.weekly_data[code].copy()

        daily_df = self.calculate_indicators(daily_df, 'daily')
        weekly_df = self.calculate_indicators(weekly_df, 'weekly')

        # 매수 신호 확인
        weekly_macd_up = weekly_df['macd'].iloc[-1] > weekly_df['signal'].iloc[-1]
        daily_stoch_oversold = daily_df['%K'].iloc[-1] < 20 and daily_df['%D'].iloc[-1] < 20
        daily_force_positive = daily_df['force_index_ema'].iloc[-1] > 0

        if weekly_macd_up and daily_stoch_oversold and daily_force_positive:
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

            message = "[{}]buy order is done! quantity:{}, bid:{}, order_result:{}, deposit:{}".format(
                code, quantity, bid, order_result, self.deposit)
            send_message(message, TripleScreen_STRATEGY_MESSAGE_TOKEN)

    def get_balance_count(self):
        """매도 주문이 접수되지 않은 보유 종목 수를 계산하는 함수"""
        balance_count = len(self.kiwoom.balance)
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