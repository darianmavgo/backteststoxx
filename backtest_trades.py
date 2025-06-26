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

# Load signals from database
df_signals = load_signals_from_db()

print(f"Loaded {len(df_signals)} signals from database")
print(f"Date range: {df_signals['signal_date'].min()} to {df_signals['signal_date'].max()}")
print(f"Unique tickers: {df_signals['ticker'].nunique()}")

# Define the trading strategy
class SignalStrategy(Strategy):
    def init(self):
        # Store signals for the current ticker
        self.ticker = self.data._name
        self.signals = df_signals[df_signals['ticker'] == self.ticker].set_index('entry_date')
        self.position_opened = False
        self.buy_price = None
        self.stop_price = None
        self.target_price = None

    def next(self):
        # Get current date
        current_date = self.data.index[-1]

        # Check if there's a signal for the current date
        try:
            current_date_only = current_date.date()
        except AttributeError:
            # Handle case where current_date doesn't have date() method
            current_date_only = pd.to_datetime(current_date).date()
        
        # Find matching signal by iterating through signals
        signal = None
        for idx, row in self.signals.iterrows():
            try:
                if hasattr(idx, 'date'):
                    idx_date = idx.date()
                else:
                    idx_date = pd.to_datetime(idx).date()
                
                if idx_date == current_date_only:
                    signal = row
                    break
            except (AttributeError, TypeError):
                continue
        
        if signal is not None:
            self.buy_price = float(signal['buy_price'])
            self.stop_price = float(signal['stop_price'])
            self.target_price = float(signal['target_price'])

            # Enter position if not already in one
            if not self.position_opened:
                # Place buy order at buy_price (limit order simulation)
                if self.data.Close[-1] <= self.buy_price:
                    self.buy(size=1, sl=self.stop_price, tp=self.target_price)
                    self.position_opened = True

        # Exit position if stop-loss or take-profit is hit
        if self.position_opened and self.stop_price is not None and self.target_price is not None:
            # Check stop-loss
            if self.data.Low[-1] <= self.stop_price:
                self.position.close()
                self.position_opened = False
            # Check take-profit
            elif self.data.High[-1] >= self.target_price:
                self.position.close()
                self.position_opened = False

# Function to fetch historical data and run backtest for a ticker
def run_backtest(ticker, signals_df):
    try:
        # Fetch historical data from yfinance
        start_date = signals_df['entry_date'].min() - pd.Timedelta(days=30)  # Buffer before first signal
        end_date = signals_df['entry_date'].max() + pd.Timedelta(days=30)    # Buffer after last signal
        data = yf.download(ticker, start=start_date, end=end_date, interval='1d', auto_adjust=False)

        # Ensure data is not empty and is a DataFrame
        if data is None or data.empty or not isinstance(data, pd.DataFrame):
            print(f"No valid data available for {ticker}")
            return None

        # Prepare data for Backtesting.py
        data = data[['Open', 'High', 'Low', 'Close', 'Volume']]
        data.index.name = 'Date'

        # Rename the DataFrame for Backtesting.py
        data._name = ticker

        # Run backtest
        bt = Backtest(data, SignalStrategy, cash=100000, commission=0.002, exclusive_orders=True)
        stats = bt.run()
        return stats

    except Exception as e:
        print(f"Error backtesting {ticker}: {str(e)}")
        return None

# Main execution
results = {}
unique_tickers = df_signals['ticker'].unique()

for ticker in unique_tickers:
    print(f"Backtesting {ticker}...")
    stats = run_backtest(ticker, df_signals[df_signals['ticker'] == ticker])
    if stats is not None:
        results[ticker] = stats

# Aggregate and display results
for ticker, stats in results.items():
    print(f"\nResults for {ticker}:")
    print(f"Return: {stats['Return [%]']:.2f}%")
    print(f"Win Rate: {stats['Win Rate [%]']:.2f}%")
    print(f"Max Drawdown: {stats['Max. Drawdown [%]']:.2f}%")
    print(f"Sharpe Ratio: {stats['Sharpe Ratio']:.2f}")
    print(f"Number of Trades: {stats['# Trades']}")

# Aggregate portfolio-level metrics
total_trades = sum(stats['# Trades'] for stats in results.values())
total_return = sum(stats['Return [%]'] * stats['# Trades'] for stats in results.values()) / total_trades if total_trades > 0 else 0
print(f"\nPortfolio Summary:")
print(f"Total Trades: {total_trades}")
print(f"Average Return: {total_return:.2f}%")