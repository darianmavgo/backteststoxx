import pandas as pd
import yfinance as yf
from backtesting import Backtest, Strategy
from datetime import datetime
import sqlite3

# Load signals from SQLite database
def load_signals_from_db(db_path='backteststoxx_emails.db'):
    """Load trade signals from SQLite database"""
    conn = sqlite3.connect(db_path)
    
    query = """
    SELECT 
        id,
        email_id,
        ticker,
        signal_date,
        entry_date,
        buy_price,
        stop_price,
        target_price
    FROM trade_signals_v1_1 
    WHERE ticker IS NOT NULL 
      AND buy_price IS NOT NULL 
      AND stop_price IS NOT NULL 
      AND target_price IS NOT NULL
    ORDER BY signal_date
    """
    
    df_signals = pd.read_sql_query(query, conn)
    conn.close()
    
    # Convert UNIX timestamps to datetime
    df_signals['entry_date'] = pd.to_datetime(df_signals['entry_date'], unit='ms')
    df_signals['signal_date'] = pd.to_datetime(df_signals['signal_date'], unit='ms')
    
    return df_signals

# Function to get next batch number
def get_next_batch_number(db_path='backteststoxx_emails.db'):
    """Get the next batch number from the sequence table"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get current batch number and increment it
    cursor.execute("SELECT next_batch_number FROM backtest_batch_sequence WHERE id = 1")
    result = cursor.fetchone()
    
    if result:
        batch_number = result[0]
        # Increment for next time
        cursor.execute("UPDATE backtest_batch_sequence SET next_batch_number = ? WHERE id = 1", (batch_number + 1,))
        conn.commit()
    else:
        # Initialize if not exists
        batch_number = 1
        cursor.execute("INSERT INTO backtest_batch_sequence (id, next_batch_number) VALUES (1, 2)")
        conn.commit()
    
    conn.close()
    return batch_number

# Function to store backtest results in database
def store_backtest_results(ticker, signals_df, stats, trade_details, batch_number, db_path='backteststoxx_emails.db'):
    """Store backtest results in the backtest_results table"""
    if stats is None or signals_df.empty:
        return
    
    conn = sqlite3.connect(db_path)
    
    # Get signal details for this ticker
    ticker_signals = signals_df[signals_df['ticker'] == ticker]
    if ticker_signals.empty:
        conn.close()
        return
    
    # Calculate aggregated signal details
    signal_date = ticker_signals['signal_date'].iloc[0].strftime('%Y-%m-%d')
    entry_date = ticker_signals['entry_date'].iloc[0].strftime('%Y-%m-%d')
    buy_price_limit = float(ticker_signals['buy_price'].iloc[0])
    stop_loss_price = float(ticker_signals['stop_price'].iloc[0])
    target_price = float(ticker_signals['target_price'].iloc[0])
    
    # Extract trade execution details
    signal_triggered_date = trade_details.get('signal_triggered_date', entry_date)
    market_price_at_signal = trade_details.get('market_price_at_signal', buy_price_limit)
    actual_entry_price = trade_details.get('actual_entry_price', buy_price_limit)
    exit_date = trade_details.get('exit_date', '')
    exit_price = trade_details.get('exit_price', 0.0)
    exit_reason = trade_details.get('exit_reason', 'NO TRADE')
    trade_duration_days = trade_details.get('trade_duration_days', 0)
    individual_trade_return_pct = trade_details.get('individual_trade_return_pct', 0.0)
    
    # Insert the results
    insert_query = """
    INSERT INTO backtest_results (
        ticker, signal_date, entry_date, buy_price_limit, stop_loss_price, target_price,
        signal_triggered_date, market_price_at_signal, actual_entry_price,
        exit_date, exit_price, exit_reason, trade_duration_days, individual_trade_return_pct,
        overall_portfolio_return_pct, win_rate_pct, max_drawdown_pct, sharpe_ratio,
        number_of_trades, trade_return_pct, exposure_time_pct, buy_hold_return_pct,
        starting_capital, final_equity, backtest_start_date, backtest_end_date, backtest_duration_days,
        backtest_batch
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    values = (
        ticker, signal_date, entry_date, buy_price_limit, stop_loss_price, target_price,
        signal_triggered_date, market_price_at_signal, actual_entry_price,
        exit_date, exit_price, exit_reason, trade_duration_days, individual_trade_return_pct,
        float(stats['Return [%]']), float(stats['Win Rate [%]']), float(stats['Max. Drawdown [%]']), 
        float(stats['Sharpe Ratio']), int(stats['# Trades']), 
        float(stats['Best Trade [%]']) if stats['# Trades'] > 0 else 0.0,
        float(stats['Exposure Time [%]']), float(stats['Buy & Hold Return [%]']),
        100000.0, float(stats['Equity Final [$]']),
        str(stats['Start'])[:10], str(stats['End'])[:10], int(stats['Duration'].days),
        batch_number
    )
    
    conn.execute(insert_query, values)
    conn.commit()
    conn.close()
    print(f"✅ Stored {ticker} results in database")

# Load signals from database
df_signals = load_signals_from_db()

print(f"Loaded {len(df_signals)} signals from database")
print(f"Date range: {df_signals['signal_date'].min()} to {df_signals['signal_date'].max()}")
print(f"Unique tickers: {df_signals['ticker'].nunique()}")

# Global variables to store current ticker and trade details
current_ticker = None
current_trade_details = {}

# Define the trading strategy
class SignalStrategy(Strategy):
    def init(self):
        # Store signals for the current ticker
        self.ticker = current_ticker
        self.signals = df_signals[df_signals['ticker'] == self.ticker].set_index('entry_date')
        self.position_opened = False
        self.buy_price = None
        self.stop_price = None
        self.target_price = None
        self.entry_price = None
        self.exit_reason = None
        self.trade_start_date = None

    def next(self):
        global current_trade_details
        
        # Get current date
        current_date = self.data.index[-1]

        # Convert to date for comparison
        current_date_str = str(current_date)[:10]  # Get YYYY-MM-DD format
        
        # Find matching signal by iterating through signals
        signal = None
        for idx, row in self.signals.iterrows():
            idx_str = str(idx)[:10]  # Get YYYY-MM-DD format
            if idx_str == current_date_str:
                signal = row
                break
        
        if signal is not None:
            self.buy_price = float(signal['buy_price'])
            self.stop_price = float(signal['stop_price'])
            self.target_price = float(signal['target_price'])

            # Enter position if not already in one
            if not self.position_opened:
                # Place buy order at buy_price (limit order simulation)
                current_price = self.data.Close[-1]
                print(f"Signal triggered on {current_date_str} for {self.ticker}")
                print(f"Current market price: ${current_price:.2f}")
                print(f"Signal buy price: ${self.buy_price:.2f}")
                
                # Store trade details
                current_trade_details['signal_triggered_date'] = current_date_str
                current_trade_details['market_price_at_signal'] = current_price
                
                if current_price <= self.buy_price:
                    self.buy(size=10, sl=self.stop_price, tp=self.target_price)
                    self.position_opened = True
                    self.entry_price = current_price
                    self.trade_start_date = current_date
                    current_trade_details['actual_entry_price'] = current_price
                    print(f"✅ BOUGHT {self.ticker} at ${current_price:.2f}")
                    print(f"Stop Loss: ${self.stop_price:.2f}")
                    print(f"Target: ${self.target_price:.2f}")
                else:
                    print(f"❌ No entry for {self.ticker} - market price ${current_price:.2f} > buy limit ${self.buy_price:.2f}")
                    current_trade_details['actual_entry_price'] = 0.0
                    current_trade_details['exit_reason'] = 'NO ENTRY'

        # Exit position if stop-loss or take-profit is hit
        if self.position_opened and self.stop_price is not None and self.target_price is not None:
            current_price = self.data.Close[-1]
            # Check stop-loss
            if self.data.Low[-1] <= self.stop_price:
                self.position.close()
                self.position_opened = False
                self.exit_reason = "STOP LOSS"
                current_trade_details['exit_date'] = current_date_str
                current_trade_details['exit_price'] = self.stop_price
                current_trade_details['exit_reason'] = "STOP LOSS"
                if self.trade_start_date is not None:
                    duration = (current_date - self.trade_start_date).days
                    current_trade_details['trade_duration_days'] = duration
                if self.entry_price is not None:
                    loss_pct = ((self.stop_price - self.entry_price) / self.entry_price) * 100
                    current_trade_details['individual_trade_return_pct'] = loss_pct
                    print(f"🛑 STOP LOSS HIT for {self.ticker} at ${self.stop_price:.2f} on {current_date_str}")
                    print(f"Loss: {loss_pct:.2f}%")
            # Check take-profit
            elif self.data.High[-1] >= self.target_price:
                self.position.close()
                self.position_opened = False
                self.exit_reason = "TARGET HIT"
                current_trade_details['exit_date'] = current_date_str
                current_trade_details['exit_price'] = self.target_price
                current_trade_details['exit_reason'] = "TARGET HIT"
                if self.trade_start_date is not None:
                    duration = (current_date - self.trade_start_date).days
                    current_trade_details['trade_duration_days'] = duration
                if self.entry_price is not None:
                    gain_pct = ((self.target_price - self.entry_price) / self.entry_price) * 100
                    current_trade_details['individual_trade_return_pct'] = gain_pct
                    print(f"🎯 TARGET HIT for {self.ticker} at ${self.target_price:.2f} on {current_date_str}")
                    print(f"Gain: {gain_pct:.2f}%")

# Function to fetch historical data and run backtest for a ticker
def run_backtest(ticker, signals_df):
    global current_ticker, current_trade_details
    current_ticker = ticker
    current_trade_details = {}  # Reset for each ticker
    
    try:
        # Fetch historical data from yfinance with wider date range
        start_date = signals_df['entry_date'].min() - pd.Timedelta(days=60)  # More buffer before signal
        end_date = signals_df['entry_date'].max() + pd.Timedelta(days=180)   # More buffer after signal
        
        print(f"Downloading {ticker} data from {start_date.date()} to {end_date.date()}...")
        data = yf.download(ticker, start=start_date, end=end_date, interval='1d', auto_adjust=False)

        # Ensure data is not empty and is a DataFrame
        if data is None or data.empty or not isinstance(data, pd.DataFrame):
            print(f"No valid data available for {ticker}")
            return None

        # Handle MultiIndex columns from yfinance
        if isinstance(data.columns, pd.MultiIndex):
            # Flatten MultiIndex columns by taking the first level
            data.columns = data.columns.get_level_values(0)
        
        # Ensure we have the required columns
        required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        if not all(col in data.columns for col in required_columns):
            print(f"Missing required columns for {ticker}: {data.columns.tolist()}")
            return None

        # Prepare data for Backtesting.py
        data = data[required_columns]
        data.index.name = 'Date'

        # Remove any rows with NaN values
        data = data.dropna()
        
        if data.empty:
            print(f"No valid data after cleaning for {ticker}")
            return None

        print(f"Data loaded: {len(data)} days of {ticker} price data")

        # Ensure data is a DataFrame for the Backtest constructor
        if not isinstance(data, pd.DataFrame):
            print(f"Error: Data is not a DataFrame for {ticker}")
            return None

        # Run backtest
        bt = Backtest(data, SignalStrategy, cash=100000, commission=0.002, exclusive_orders=True)
        stats = bt.run()
        
        # Store results in database
        store_backtest_results(ticker, signals_df, stats, current_trade_details, current_batch_number)
        
        return stats

    except Exception as e:
        print(f"Error backtesting {ticker}: {str(e)}")
        return None

# Main execution
if __name__ == "__main__":
    results = {}
    unique_tickers = df_signals['ticker'].unique()
    
    # Get batch number for this run
    current_batch_number = get_next_batch_number()
    
    print(f"\n🚀 Starting backtest for {len(unique_tickers)} tickers...")
    print(f"📊 Batch Number: {current_batch_number}")
    print("="*60)

    for i, ticker in enumerate(unique_tickers, 1):
        print(f"\n[{i}/{len(unique_tickers)}] Backtesting {ticker}...")
        stats = run_backtest(ticker, df_signals[df_signals['ticker'] == ticker])
        if stats is not None:
            results[ticker] = stats
        print("-" * 40)

    # Aggregate and display results
    print(f"\n📊 BACKTEST RESULTS SUMMARY")
    print("="*60)
    
    for ticker, stats in results.items():
        print(f"\n{ticker}:")
        print(f"  Return: {stats['Return [%]']:.2f}%")
        print(f"  Win Rate: {stats['Win Rate [%]']:.2f}%")
        print(f"  Max Drawdown: {stats['Max. Drawdown [%]']:.2f}%")
        print(f"  Sharpe Ratio: {stats['Sharpe Ratio']:.2f}")
        print(f"  Number of Trades: {stats['# Trades']}")

    # Aggregate portfolio-level metrics
    if results:
        total_trades = sum(stats['# Trades'] for stats in results.values())
        total_return = sum(stats['Return [%]'] * stats['# Trades'] for stats in results.values()) / total_trades if total_trades > 0 else 0
        
        print(f"\n🎯 PORTFOLIO SUMMARY:")
        print("="*30)
        print(f"Total Tickers: {len(results)}")
        print(f"Total Trades: {total_trades}")
        print(f"Average Return: {total_return:.2f}%")
        print(f"Successful Backtests: {len(results)}/{len(unique_tickers)}")
        
        print(f"\n✅ All results stored in backtest_results table")
    else:
        print("❌ No successful backtests completed")