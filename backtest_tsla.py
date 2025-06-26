import pandas as pd
import yfinance as yf
from backtesting import Backtest, Strategy
from datetime import datetime
import sqlite3

# Load signals from SQLite database
def load_tsla_signals_from_db(db_path='backteststoxx_emails.db'):
    """Load TSLA trade signals from SQLite database"""
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
    WHERE ticker = 'TSLA'
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

# Load TSLA signals from database
df_signals = load_tsla_signals_from_db()

print(f"=== TESLA (TSLA) BACKTEST ===")
print(f"Loaded {len(df_signals)} TSLA signals from database")
if len(df_signals) > 0:
    print(f"Signal Date: {df_signals['signal_date'].iloc[0]}")
    print(f"Entry Date: {df_signals['entry_date'].iloc[0]}")
    print(f"Buy Price: ${df_signals['buy_price'].iloc[0]}")
    print(f"Stop Price: ${df_signals['stop_price'].iloc[0]}")
    print(f"Target Price: ${df_signals['target_price'].iloc[0]}")
    print()

# Global variable to store current ticker being backtested
current_ticker = None

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

    def next(self):
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
                print(f"Signal triggered on {current_date_str}")
                print(f"Current market price: ${current_price:.2f}")
                print(f"Signal buy price: ${self.buy_price:.2f}")
                
                if current_price <= self.buy_price:
                    self.buy(size=1, sl=self.stop_price, tp=self.target_price)
                    self.position_opened = True
                    self.entry_price = current_price
                    print(f"✅ BOUGHT at ${current_price:.2f}")
                    print(f"Stop Loss: ${self.stop_price:.2f}")
                    print(f"Target: ${self.target_price:.2f}")
                else:
                    print(f"❌ No entry - market price ${current_price:.2f} > buy limit ${self.buy_price:.2f}")

        # Exit position if stop-loss or take-profit is hit
        if self.position_opened and self.stop_price is not None and self.target_price is not None:
            current_price = self.data.Close[-1]
            # Check stop-loss
            if self.data.Low[-1] <= self.stop_price:
                self.position.close()
                self.position_opened = False
                self.exit_reason = "STOP LOSS"
                print(f"🛑 STOP LOSS HIT at ${self.stop_price:.2f} on {current_date_str}")
                if self.entry_price:
                    loss_pct = ((self.stop_price - self.entry_price) / self.entry_price) * 100
                    print(f"Loss: {loss_pct:.2f}%")
            # Check take-profit
            elif self.data.High[-1] >= self.target_price:
                self.position.close()
                self.position_opened = False
                self.exit_reason = "TARGET HIT"
                print(f"🎯 TARGET HIT at ${self.target_price:.2f} on {current_date_str}")
                if self.entry_price:
                    gain_pct = ((self.target_price - self.entry_price) / self.entry_price) * 100
                    print(f"Gain: {gain_pct:.2f}%")

# Function to fetch historical data and run backtest for TSLA
def run_tsla_backtest():
    global current_ticker
    current_ticker = 'TSLA'
    
    try:
        # Get date range for TSLA signals
        if df_signals.empty:
            print("No TSLA signals found!")
            return None
            
        # Fetch historical data from yfinance with wider date range
        start_date = df_signals['entry_date'].min() - pd.Timedelta(days=60)  # More buffer before signal
        end_date = df_signals['entry_date'].max() + pd.Timedelta(days=180)   # More buffer after signal
        
        print(f"Downloading TSLA data from {start_date.date()} to {end_date.date()}...")
        data = yf.download('TSLA', start=start_date, end=end_date, interval='1d', auto_adjust=False)

        # Ensure data is not empty and is a DataFrame
        if data is None or data.empty or not isinstance(data, pd.DataFrame):
            print(f"No valid data available for TSLA")
            return None

        # Handle MultiIndex columns from yfinance
        if isinstance(data.columns, pd.MultiIndex):
            # Flatten MultiIndex columns by taking the first level
            data.columns = data.columns.get_level_values(0)
        
        # Ensure we have the required columns
        required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        if not all(col in data.columns for col in required_columns):
            print(f"Missing required columns for TSLA: {data.columns.tolist()}")
            return None

        # Prepare data for Backtesting.py
        data = data[required_columns]
        data.index.name = 'Date'

        # Remove any rows with NaN values
        data = data.dropna()
        
        if data.empty:
            print(f"No valid data after cleaning for TSLA")
            return None

        print(f"Data loaded: {len(data)} days of TSLA price data")
        print()

        # Ensure data is a DataFrame for the Backtest constructor
        if not isinstance(data, pd.DataFrame):
            print("Error: Data is not a DataFrame")
            return None

        # Run backtest
        bt = Backtest(data, SignalStrategy, cash=100000, commission=0.002, exclusive_orders=True)
        stats = bt.run()
        return stats

    except Exception as e:
        print(f"Error backtesting TSLA: {str(e)}")
        return None

# Main execution for TSLA only
if __name__ == "__main__":
    if df_signals.empty:
        print("No TSLA signals found in trade_signals_v1_1")
    else:
        print("Running TSLA backtest...")
        print()
        
        stats = run_tsla_backtest()
        
        if stats is not None:
            print("\n" + "="*50)
            print("📊 TSLA BACKTEST RESULTS")
            print("="*50)
            print(f"Return: {stats['Return [%]']:.2f}%")
            print(f"Win Rate: {stats['Win Rate [%]']:.2f}%")
            print(f"Max Drawdown: {stats['Max. Drawdown [%]']:.2f}%")
            print(f"Sharpe Ratio: {stats['Sharpe Ratio']:.2f}")
            print(f"Number of Trades: {stats['# Trades']}")
            print(f"Best Trade: {stats['Best Trade [%]']:.2f}%")
            print(f"Worst Trade: {stats['Worst Trade [%]']:.2f}%")
            print(f"Avg Trade: {stats['Avg. Trade [%]']:.2f}%")
            print(f"Max Trade Duration: {stats['Max. Trade Duration']}")
            print(f"Avg Trade Duration: {stats['Avg. Trade Duration']}")
            
            print("\n📈 DETAILED STATISTICS:")
            print(f"Start: {stats['Start']}")
            print(f"End: {stats['End']}")
            print(f"Duration: {stats['Duration']}")
            print(f"Exposure Time: {stats['Exposure Time [%]']:.1f}%")
            print(f"Equity Final: ${stats['Equity Final [$]']:,.2f}")
            print(f"Buy & Hold Return: {stats['Buy & Hold Return [%]']:.2f}%")
            
        else:
            print("❌ Backtest failed - no results generated")