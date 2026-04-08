"""
update_data.py
──────────────
Chạy script này sau lần đầu chạy vn30_analysis.ipynb để append dữ liệu mới.
Workflow:
  1. Đọc ngày cuối cùng trong mỗi file CSV
  2. Gọi API chỉ từ ngày đó đến hôm nay
  3. Append vào file CSV gốc (không ghi đè toàn bộ)
  4. Power BI Refresh là xong

Cách dùng:
  python update_data.py

Nâng cấp tier (mặc định Guest = 20 req/phút):
  Đổi BATCH = 60 và DELAY = 62 nếu dùng Community tier.
"""

import time, os
from datetime import date
import pandas as pd
from vnstock import Listing, Quote

# ── Config (giữ giống notebook) ──────────────────────────────────
SOURCE = 'KBS'
BATCH  = 20
DELAY  = 62
OUT    = 'output'

# ── Helper: tag giai đoạn ────────────────────────────────────────
def tag_phase(d):
    if d < pd.Timestamp('2020-03-25'): return '1_COVID_Crash'
    if d < pd.Timestamp('2022-01-01'): return '2_COVID_Recovery'
    if d < pd.Timestamp('2022-11-16'): return '3_TPDN_Crisis'
    if d < pd.Timestamp('2025-01-01'): return '4_Rebound'
    return '5_YTD_2025'

# ── Helper: tính indicators (rút gọn — chỉ cần append đủ lịch sử) 
import numpy as np

def recalc_indicators(g):
    """Tính lại indicator trên toàn chuỗi sau khi append."""
    c = g['close']
    g['ret_1d']   = c.pct_change()
    g['ret_1w']   = c.pct_change(5)
    g['ret_1m']   = c.pct_change(21)
    g['ret_3m']   = c.pct_change(63)
    g['ret_6m']   = c.pct_change(126)
    g['ret_1y']   = c.pct_change(252)
    g['cum_ret']  = (c / c.iloc[0] - 1) * 100
    g['ytd_ret']  = g.groupby(g['date'].dt.year, group_keys=False)['close'] \
                     .transform(lambda x: (x / x.iloc[0] - 1) * 100)
    for w in [20, 50, 200]:
        g[f'ma_{w}'] = c.rolling(w).mean()
    std20       = c.rolling(20).std()
    g['bb_up']  = g['ma_20'] + 2 * std20
    g['bb_lo']  = g['ma_20'] - 2 * std20
    g['bb_pct'] = (c - g['bb_lo']) / (g['bb_up'] - g['bb_lo'])
    d = c.diff()
    gain = d.clip(lower=0).rolling(14).mean()
    loss = (-d.clip(upper=0)).rolling(14).mean()
    g['rsi']      = 100 - 100 / (1 + gain / loss.replace(0, np.nan))
    e12 = c.ewm(span=12, adjust=False).mean()
    e26 = c.ewm(span=26, adjust=False).mean()
    g['macd']     = e12 - e26
    g['macd_sig'] = g['macd'].ewm(span=9, adjust=False).mean()
    g['macd_hist']= g['macd'] - g['macd_sig']
    g['vol_ann']  = g['ret_1d'].rolling(20).std() * np.sqrt(252) * 100
    if 'volume' in g.columns:
        g['vol_ratio'] = g['volume'] / g['volume'].rolling(20).mean()
    return g


def main():
    today = date.today().strftime('%Y-%m-%d')
    print(f'🔄 update_data.py — target date: {today}\n')

    # ── 1. Đọc dữ liệu hiện tại ──────────────────────────────────
    price_path = f'{OUT}/price_history.csv'
    index_path = f'{OUT}/market_index.csv'

    if not os.path.exists(price_path):
        print('❌ Không tìm thấy output/price_history.csv')
        print('   Hãy chạy vn30_analysis.ipynb trước.')
        return

    df_old = pd.read_csv(price_path, parse_dates=['date'])
    df_old['date'] = pd.to_datetime(df_old['date'])

    last_date  = df_old['date'].max()
    last_str   = last_date.strftime('%Y-%m-%d')
    print(f'📅 Dữ liệu hiện tại đến: {last_str}')

    if last_str >= today:
        print('✅ Dữ liệu đã cập nhật. Không cần làm gì thêm.')
        return

    print(f'   Sẽ fetch từ {last_str} → {today}\n')

    # ── 2. Lấy danh sách ticker ──────────────────────────────────
    TICKERS = df_old['ticker'].unique().tolist()
    SECTOR  = dict(zip(df_old['ticker'], df_old.get('sector', pd.Series())))
    print(f'🎯 Cập nhật {len(TICKERS)} mã\n')

    # ── 3. Fetch dữ liệu mới từng ticker ─────────────────────────
    new_frames = []
    errors     = []

    for i, tkr in enumerate(TICKERS):
        try:
            df_new = Quote(symbol=tkr, source=SOURCE).history(
                start=last_str, end=today, interval='d'
            )
            df_new.columns = [c.lower() for c in df_new.columns]
            t_col = [c for c in df_new.columns if 'time' in c or 'date' in c][0]
            df_new.rename(columns={t_col: 'date'}, inplace=True)
            df_new['date']   = pd.to_datetime(df_new['date'])
            df_new['ticker'] = tkr
            df_new['sector'] = SECTOR.get(tkr, 'Khác')

            # Chỉ lấy ngày mới hơn last_date
            df_new = df_new[df_new['date'] > last_date]

            if not df_new.empty:
                new_frames.append(df_new)
                print(f'  [{i+1:2d}/{len(TICKERS)}] ✅ {tkr}: +{len(df_new)} ngày mới')
            else:
                print(f'  [{i+1:2d}/{len(TICKERS)}] ℹ️  {tkr}: không có ngày mới')

        except Exception as e:
            errors.append(tkr)
            print(f'  [{i+1:2d}/{len(TICKERS)}] ❌ {tkr}: {e}')

        # Rate limit
        if (i + 1) % BATCH == 0 and (i + 1) < len(TICKERS):
            print(f'  ⏳ Batch {(i+1)//BATCH} xong — nghỉ {DELAY}s...')
            time.sleep(DELAY)

    if not new_frames:
        print('\nℹ️  Không có dữ liệu mới để cập nhật.')
        return

    df_new_all = pd.concat(new_frames, ignore_index=True)
    print(f'\n📥 {len(df_new_all)} dòng mới | lỗi: {errors or "không có"}')

    # ── 4. Merge & recalculate indicators ────────────────────────
    print('🔄 Recalculating indicators...')
    df_combined = pd.concat([df_old, df_new_all], ignore_index=True)
    df_combined = df_combined.drop_duplicates(subset=['ticker','date'])
    df_combined = df_combined.sort_values(['ticker','date']).reset_index(drop=True)

    # Metadata
    df_combined['year']    = df_combined['date'].dt.year
    df_combined['quarter'] = df_combined['date'].dt.quarter
    df_combined['month']   = df_combined['date'].dt.month
    df_combined['week']    = df_combined['date'].dt.isocalendar().week.astype(int)
    df_combined['phase']   = df_combined['date'].apply(tag_phase)

    # Recalc indicators trên toàn chuỗi (cần 200 ngày lookback)
    df_combined = df_combined.groupby('ticker', group_keys=False).apply(recalc_indicators)

    # ── 5. Ghi đè CSV ────────────────────────────────────────────
    df_combined.to_csv(price_path, index=False, encoding='utf-8-sig')
    print(f'✅ price_history.csv cập nhật: {len(df_combined):,} rows → {df_combined["date"].max().date()}')

    # ── 6. Cập nhật market_index.csv ─────────────────────────────
    if os.path.exists(index_path):
        idx_old     = pd.read_csv(index_path, parse_dates=['date'])
        idx_last    = idx_old['date'].max()
        idx_frames  = []

        for sym in idx_old['symbol'].unique():
            try:
                d = Quote(symbol=sym, source=SOURCE).history(
                    start=idx_last.strftime('%Y-%m-%d'), end=today, interval='d'
                )
                d.columns = [c.lower() for c in d.columns]
                tc = [c for c in d.columns if 'time' in c or 'date' in c][0]
                d.rename(columns={tc: 'date'}, inplace=True)
                d['date']   = pd.to_datetime(d['date'])
                d['symbol'] = sym
                d = d[d['date'] > idx_last]
                if not d.empty:
                    idx_frames.append(d)
                    print(f'  ✅ {sym}: +{len(d)} ngày')
                time.sleep(2)
            except Exception as e:
                print(f'  ❌ {sym}: {e}')

        if idx_frames:
            idx_new = pd.concat([idx_old] + idx_frames, ignore_index=True)
            idx_new = idx_new.drop_duplicates(subset=['symbol','date'])
            idx_new['ret_1d']  = idx_new.groupby('symbol')['close'].pct_change()
            idx_new['cum_ret'] = idx_new.groupby('symbol')['close'].transform(
                lambda x: (x / x.iloc[0] - 1) * 100
            )
            idx_new['vol_ann'] = idx_new.groupby('symbol')['ret_1d'].transform(
                lambda x: x.rolling(20).std() * np.sqrt(252) * 100
            )
            idx_new['phase']   = idx_new['date'].apply(tag_phase)
            idx_new.to_csv(index_path, index=False, encoding='utf-8-sig')
            print(f'✅ market_index.csv cập nhật → {idx_new["date"].max().date()}')

    # ── 7. Cập nhật summary_stats.csv ────────────────────────────
    print('🔄 Cập nhật summary_stats.csv...')
    summary = (
        df_combined.groupby(['ticker','sector'])
        .apply(lambda g: pd.Series({
            'total_ret'    : g.sort_values('date')['cum_ret'].iloc[-1],
            'volatility'   : g['ret_1d'].std() * np.sqrt(252) * 100,
            'sharpe'       : (g['ret_1d'].mean()*252)/(g['ret_1d'].std()*np.sqrt(252)+1e-9),
            'max_dd'       : ((g.sort_values('date')['close']/g.sort_values('date')['close'].cummax())-1).min()*100,
            'calmar'       : (g['ret_1d'].mean()*252)/abs((g.sort_values('date')['close']/g.sort_values('date')['close'].cummax()-1).min()+1e-9),
            'latest_close' : g.sort_values('date')['close'].iloc[-1],
            'latest_rsi'   : g.sort_values('date')['rsi'].iloc[-1],
            'latest_ytd'   : g.sort_values('date')['ytd_ret'].iloc[-1],
            'latest_ret_3m': g.sort_values('date')['ret_3m'].iloc[-1],
            'latest_vol'   : g.sort_values('date')['vol_ann'].iloc[-1],
            'data_from'    : g['date'].min().strftime('%Y-%m-%d'),
            'data_to'      : g['date'].max().strftime('%Y-%m-%d'),
            'n_days'       : len(g),
        }))
        .reset_index().round(4)
    )
    summary.to_csv(f'{OUT}/summary_stats.csv', index=False, encoding='utf-8-sig')
    print(f'✅ summary_stats.csv cập nhật: {len(summary)} mã')

    # ── 8. Cập nhật phase_perf.csv ───────────────────────────────
    phase_perf = (
        df_combined.groupby(['ticker','sector','phase'])
        .apply(lambda g: (g.sort_values('date')['close'].iloc[-1]/
                          g.sort_values('date')['close'].iloc[0]-1)*100)
        .reset_index(name='phase_ret')
    )
    phase_perf.to_csv(f'{OUT}/phase_perf.csv', index=False, encoding='utf-8-sig')
    print(f'✅ phase_perf.csv cập nhật')

    print(f'\n🎉 Cập nhật hoàn tất! Refresh Power BI để xem dữ liệu mới.')
    if errors:
        print(f'⚠️  Lỗi {len(errors)} mã (cần fetch thủ công): {errors}')


if __name__ == '__main__':
    main()
