#!/usr/bin/python3

import datetime as dt
import argparse
import dateutil.parser as dp
import zmq
import pandas as pd
import os
import tetrion.clientcore
import _thread
import time
import tetrion.options
import ujson
import redis
import sys
import tetrion.db
import collections
import numpy as np
from tetrion.core import Instrument
import tetrion.config

REDIS_QUERY_REFRESH = 10 # seconds
TIMEOUT = 36000 # seconds (10 hours)
USE_INSTR_FX_RATE = False # use per instrument fxrate

# Use mkt mid instead of theo to calculate pnl
MKT_MID_INSTR_LIST = ['FUT_CFFEX_IH', 'FUT_CME_GC', 'FUT_CFFEX_IC', 'FUT_CFFEX_IF',
                      'FUT_SHFE_HC', 'FUT_SHFE_RB',
                      'FUT_SHFE_AU', 'FUT_SHFE_AG',
                      'FUT_SHFE_SN', 'FUT_SHFE_AL',
                      'FUT_SHFE_CU', 'FUT_SHFE_NI',
                      'FUT_DCE_Y', 'FUT_DCE_P',
                      'FUT_DCE_M', 'FUT_CZCE_RM',
                      'OPT_CFFEX_IO', 'OPT_SZSE_159919']

INSTR_FX_MAP = {}
latest_ffwd = 0

class ZMQ_Publisher():
    def __init__(self, pub_zmq_conn_str):
        print("Publishing to {}".format(pub_zmq_conn_str))
        pubcxt = zmq.Context()
        self.pub_subsok = pubcxt.socket(zmq.PUB)
        self.pub_subsok.bind( pub_zmq_conn_str )

    # takes df and converts to a dictionary
    def publish_trd_df(self, strategy, df, fut_fill_count, opt_fill_count):
        # print("pub: df: {}".format(df))
        try:
            if df is not None and not df.empty:
                data_dict = df.to_dict('records')[-1]
            else:
                data_dict = {}
                data_dict['pnl'] = 0
            data_dict['strategy'] = strategy
            data_dict['fut_fills'] = fut_fill_count
            data_dict['opt_fills'] = opt_fill_count
            data_dict['type'] = 'trade'
            data = ujson.dumps(data_dict)
            self.pub_subsok.send_string(data)
        except Exception as e:
            print("Exception: {}".format(e))
            return

    # takes df and converts to a dictionary
    def publish_pos_df(self, strategy, df):
        try:
            if df is not None and not df.empty:
                data_dict = df.to_dict('records')[-1]
            else:
                data_dict = {}
                data_dict['pnl'] = 0
            data_dict['strategy'] = strategy
            data_dict['type'] = 'pos'
            data = ujson.dumps(data_dict)
            self.pub_subsok.send_string(data)
        except Exception as e:
            print("Exception: {}".format(e))
            return

# for futures quoting
def report_trd_fut_quote(fills, ovdf,
                  start_tm=dt.time(8,44,30), end_tm=dt.time(13,44,30), details=False):
    '''trade attribution
    returns a tuple of the marking time and the report
    '''
    from tetrion.table import Table
    import datetime as dt
    import numpy as np
    import pandas as pd

    opt_fill_count = 0
    fut_fill_count = 0

    times=ovdf.index.unique()
    last_tm = times[times<end_tm][-1]
    # CC (11/28/18) - zmq publish should only contain one unique time, so don't need to do this.
    #
    # ovs1 = ovdf.loc[last_tm]
    ovs1 = ovdf

    t = pd.DataFrame()
    id = 0

    for msg in fills:
        id += 1
        id_str = 't'+str(id)
        instr = msg['instr']
        ts = msg['ts']
        fill_ts = dt.datetime.fromtimestamp(ts/(1 if isinstance(ts, float) else 1000000.0))
        #fill_ts = dt.datetime.fromtimestamp(msg['ts']/1000000)
        if (fill_ts < start_tm):
            continue
        if isinstance(ts, float) or isinstance(ts, int) or isinstance(ts, long):
            tt = dt.datetime.fromtimestamp(ts/(1 if isinstance(ts, float) else 1000000.0))
            #tt = dt.datetime.fromtimestamp(tt/1000000.0)
        sz = msg['sz']
        if instr.startswith('FUT'):
            fut_fill_count = fut_fill_count + abs(sz)
        else:
            opt_fill_count = opt_fill_count + abs(sz)

        px = msg['px']
        mult = msg['mult']
        theo0 = px
        fwd0 = px
        if msg['ffwd'] == 'NaN':
            ffwd0 = 0
        else:
            ffwd0 = msg['ffwd'] if not np.isnan(msg['ffwd']) else fwd0
        vol0 = msg['vol']*100
        fee = msg['fee'] if not np.isnan(msg['fee']) else 0
        delta = msg['delta']
        vega  = msg['vega']

        ov = ovs1[ovs1['instrument']==instr]
        if len(ov)==0:
            print("Error: missing instrument {} while loading optionvalues".format(instr))
            continue

        for i in MKT_MID_INSTR_LIST:
            if instr.startswith(i):
                mkt_bid = 0.0 if np.isnan(float(ov['mkt_bid'])) else float(ov['mkt_bid'])
                mkt_ask = 0.0 if np.isnan(float(ov['mkt_ask'])) else float(ov['mkt_ask'])
                # on limit up/down use the other side as the theo
                print(mkt_bid)
                print(mkt_ask)
                if mkt_bid == 0 and mkt_ask > 0:
                    mkt_bid = mkt_ask
                elif mkt_ask == 0 and mkt_bid > 0:
                    mkt_ask = mkt_bid
                theo1 = (mkt_bid + mkt_ask)/2
                ffwd1 = fwd1 = theo0 = theo1
                fwd0 = ffwd0 = px
                break
            else:
                theo1 = float(ov['forward_theo'])
                ffwd1 = ffwd0 if np.isnan(float(ov['ffwd'])) else float(ov['ffwd'])
                fwd1 = float(ov['forward_theo'])

        vol1 = float(ov['theo_vol'].values[-1])

        if (theo1 == 0):
            theo1 = theo0 if np.isnan(float(ov['value'])) else float(ov['value'])
            fwd1 = fwd0 if np.isnan(float(ov['forward'])) else float(ov['forward'])

        # if instr.startswith('FUT_TAIFEX_EXF') or instr.startswith('FUT_TAIFEX_TXF') or instr.startswith('FUT_TAIFEX_FXF'):
        #     fees_pnl  = sz * mult * ( theo0 - px ) - abs(sz) * 200 * 0.35721
        # elif instr.startswith('FUT_HKFE_GDU'):
        #     fees_pnl  = sz * mult * ( theo0 - px ) - abs(sz) * 1.07
        # elif instr.startswith('FUT_CME_GC'):
        #     fees_pnl  = sz * mult * ( theo0 - px ) - abs(sz) * 3.1
        # else:
        fees_pnl  = sz * mult * ( theo0 - px ) - abs(sz) * mult * fee

        currency_rate = fxrate
        if USE_INSTR_FX_RATE and instr:
            if instr not in INSTR_FX_MAP:
                instr_pytrion = Instrument.fromString(str(instr))
                INSTR_FX_MAP[instr] = tetrion.db.get_currency_rate('production', instr_pytrion.currency.to_string())
            currency_rate = INSTR_FX_MAP[instr]

        fees_pnl = fees_pnl/currency_rate
        gross_pnl   = (sz * mult * ( theo1 - theo0 ))/currency_rate
        res_pnl = 0
        pnl = fees_pnl + gross_pnl

        tid=msg['tid']        
        t.at[id_str, 'tt']=tt
        t.at[id_str, 'instr']=instr
        t.at[id_str, 'exp']=instr.split(':')[1]
        t.at[id_str, 'sz']=sz
        t.at[id_str, 'px']=px
        t.at[id_str, 'theo0']=theo0
        t.at[id_str, 'theo1']=theo1
        t.at[id_str, 'delta']=delta
        t.at[id_str, 'ffwd0']=ffwd0
        t.at[id_str, 'ffwd1']=ffwd1
        t.at[id_str, 'fwd0']=fwd0
        t.at[id_str, 'fwd1']=fwd1
        t.at[id_str, 'vega']=vega
        t.at[id_str, 'vol0']=vol0
        t.at[id_str, 'vol1']=vol1
        t.at[id_str, 'pnl']=pnl
        t.at[id_str, 'gross_pnl']=gross_pnl
        t.at[id_str, 'fees_pnl']=fees_pnl
        t.at[id_str, 'res_pnl']=res_pnl
        t.at[id_str, 'tid']=tid

    if t.empty:
        return last_tm, t, fut_fill_count, opt_fill_count

    values = [col for col in t.columns if col.endswith('pnl')]

    if not details:
        t = pd.pivot_table(t, index='exp', values=values, aggfunc=sum)
        sum_row = {col: t[col].sum() for col in t}
        sum_df = pd.DataFrame(sum_row, index=["Total"])
        t = t.append(sum_df)
        t = t.reindex(columns=values)
        t.index.name=None
    else:
        t['ts'] = t['tt'].apply(lambda t: t.strftime('%H:%M:%S'))
        t['dffwd']=t['ffwd1']-t['ffwd0']
        t['dfwd']=t['fwd1']-t['fwd0']
        for c in ['pnl']:
            t[c+'_cum']=t[c].cumsum()
        t=t[['ts', 'instr', 'sz', 'px', 'theo1', 'theo0']+values+['pnl_cum']]

    return last_tm, t, fut_fill_count, opt_fill_count

def calc_trd_pnl(fill, ov, last_tm, trading_day_secs):
    global latest_ffwd
    # if not validate_fill(fill):
    #     return None

    ##############################################################################################
    # cc 20211005 - trade messages sending out NaN theo, fwd and edge values, working around it
    # by setting the theo and fwd to the fill px
    ##############################################################################################
    if fill['theo'] == 'NaN':
        fill['theo'] = fill['px']

    if fill['edge'] == 'NaN':
        fill['edge'] = 0
    ##############################################################################################

    if fill['ffwd'] == 'NaN':
        fill['ffwd'] = latest_ffwd

    if fill['fwd'] == 'NaN':
        fill['fwd'] = 0

    if fill['fee'] == 'NaN':
        fill['fee'] = 0

    fill_pnl = {}

    instr = fill['instr']
    ts = fill['ts']
    if isinstance(ts, float) or isinstance(ts, int) or isinstance(ts, long):
        tt = dt.datetime.fromtimestamp(ts/(1 if isinstance(ts, float) else 1000000.0))
    sz = fill['sz']
    px = fill['px']
    mult = fill['mult']
    if mult == 1 and (instr.startswith('OPT_SHSE_510050') or instr.startswith('OPT_SHSE_510300')):
        mult = 10000.
    theo0 = fill['theo'] if not np.isnan(fill['theo']) else px
    fwd0 = fill['fwd'] if not np.isnan(fill['fwd']) else px
    ffwd0 = fill['ffwd'] if not np.isnan(fill['ffwd']) else fwd0
    vol0 = fill['vol']
    if fill['label'] == 'centralbook':
        fill['fee'] = 0
    fee = fill['fee'] if not np.isnan(fill['fee']) else 0
    delta = fill['delta']
    gamma = fill['gamma']
    vega  = fill['vega']
    theta = fill['theta']

    tt2 = last_tm
    for i in MKT_MID_INSTR_LIST:
        if instr.startswith(i):
            mkt_bid = 0.0 if np.isnan(float(ov['mkt_bid'])) else float(ov['mkt_bid'])
            mkt_ask = 0.0 if np.isnan(float(ov['mkt_ask'])) else float(ov['mkt_ask'])
            # limit up/down: use the other side as the theo.
            if mkt_bid == 0 and mkt_ask > 0:
                mkt_bid = mkt_ask
            elif mkt_ask == 0 and mkt_bid > 0:
                mkt_ask = mkt_bid
            theo1 = (mkt_bid + mkt_ask)/2
            ffwd1 = fwd1 = theo0 = theo1
            fwd0 = ffwd0 = px
            break
        else:
            theo1 = theo0 if np.isnan(float(ov['value'])) else float(ov['value'])
            ffwd1 = ffwd0 if np.isnan(float(ov['ffwd'])) else float(ov['ffwd'])
            fwd1 = fwd0 if np.isnan(float(ov['forward'])) else float(ov['forward'])
            theo0 = fill['theo'] if not np.isnan(fill['theo']) else px1

    #theo1 = theo0 if np.isnan(float(ov['value'])) else float(ov['value'])
    #ffwd1 = ffwd0 if np.isnan(float(ov['ffwd'])) else float(ov['ffwd'])
    #fwd1 = fwd0 if np.isnan(float(ov['forward'])) else float(ov['forward'])
    vol1 = float(ov['theo_vol'].values[-1])

    elapse = 0.6 * (tt2 - tt).total_seconds() / trading_day_secs

    if 'seq' in fill:
        seq = float(fill['seq'])
        if not seq.is_integer(): # this is how to determine if the fill is part of a spread leg
            if theo1 == 0:
                theo1 = theo0

    sprd_pnl  = sz * mult * ( theo0 - px ) - abs(sz) * mult * fee
    pos_pnl   = sz * mult * ( theo1 - theo0 )
    delta_pnl = sz * mult * delta * ( ffwd1 - ffwd0 )
    roll_pnl  = sz * mult * delta * ( (fwd1 - fwd0) - (ffwd1 - ffwd0) )
    gamma_pnl = sz * mult * gamma * ( fwd1 - fwd0 ) * ( fwd1 - fwd0 ) * 0.5
    vega_pnl  = sz * mult * vega  * ( vol1 - vol0 ) 
    theta_pnl = sz * mult * theta * elapse
    res_pnl = pos_pnl - delta_pnl - roll_pnl - gamma_pnl - vega_pnl - theta_pnl
    pnl = sprd_pnl + pos_pnl

    # currency conversion
    sprd_pnl = sprd_pnl / fxrate
    pos_pnl = pos_pnl / fxrate
    delta_pnl = delta_pnl / fxrate
    roll_pnl = roll_pnl / fxrate
    gamma_pnl = gamma_pnl / fxrate
    vega_pnl = vega_pnl / fxrate
    theta_pnl = theta_pnl / fxrate
    res_pnl = res_pnl / fxrate
    pnl = pnl / fxrate

    fill_pnl['tt'] = tt
    fill_pnl['instr']=instr
    fill_pnl['exp']=instr.split(':')[1]
    fill_pnl['sz']=sz
    fill_pnl['px']=px
    fill_pnl['theo0']=theo0
    fill_pnl['theo1']=theo1
    fill_pnl['delta']=delta
    fill_pnl['ffwd0']=ffwd0
    fill_pnl['ffwd1']=ffwd1
    fill_pnl['fwd0']=fwd0
    fill_pnl['fwd1']=fwd1
    fill_pnl['vega']=vega
    fill_pnl['vol0']=vol0
    fill_pnl['vol1']=vol1
    fill_pnl['pnl']=pnl
    fill_pnl['sprd_pnl']=sprd_pnl
    fill_pnl['delta_pnl']=delta_pnl
    fill_pnl['roll_pnl']=roll_pnl
    fill_pnl['gamma_pnl']=gamma_pnl
    fill_pnl['vega_pnl']=vega_pnl
    fill_pnl['theta_pnl']=theta_pnl
    fill_pnl['res_pnl']=res_pnl

    if ffwd0 != 0:
        latest_ffwd = ffwd0

    return fill_pnl

def report_trd(fills, ovdf,
                    start_tm=dt.time(8,44,30), end_tm=dt.time(13,44,30),
                    skipfuture=True, details=False):
    '''trade attribution
    returns a tuple of the marking time and the report
    '''
    import datetime as dt
    import numpy as np
    import pandas as pd

    opt_fill_count = 0
    fut_fill_count = 0
    trading_day_secs = float((end_tm - start_tm).total_seconds())

    times=ovdf.index.unique()
    last_tm = times[times<end_tm][-1]
    # CC (11/28/18) - zmq publish should only contain one unique time, so don't need to do this.
    #                 for the 50etf dividend, we have combined options values from two different
    #                 traders, so there will infact be two different unique times, but we want to
    #                 ignore it anyway because there are different instruments in both.
    # ovs1 = ovdf.loc[last_tm]

    pnl_list = []

    for f in fills:
        instr = f['instr']
        ov = ovdf[ovdf['instrument']==instr]
        if len(ov)==0:
            print("Error: missing instrument {} while loading optionvalues".format(instr))
            continue
        if ov.shape[0] > 1:
            ov = ov.head(1)
        f_pnl = calc_trd_pnl(f, ov, last_tm, trading_day_secs)
        sz = f['sz']
        if instr.startswith('FUT'):
            fut_fill_count = fut_fill_count + abs(sz)
        else:
            opt_fill_count = opt_fill_count + abs(sz)
        if f_pnl:
            pnl_list.append(f_pnl)

    df = pd.DataFrame(pnl_list)

    if df.empty:
        return last_tm, df, fut_fill_count, opt_fill_count

    values = [col for col in df.columns if col.endswith('pnl')]

    if not details:
        df = pd.pivot_table(df, index='exp', values=values, aggfunc=sum)
        sum_row = {col: df[col].sum() for col in df}
        sum_df = pd.DataFrame(sum_row, index=["Total"])
        df = df.append(sum_df)
        df = df.reindex(columns=values)
        df.index.name=None
        df = df[['pnl', 'sprd_pnl', 'delta_pnl', 'roll_pnl', 'gamma_pnl', 'vega_pnl', 'theta_pnl', 'res_pnl']] # reorder columns
    else:
        df['ts'] = df['tt'].apply(lambda t: t.strftime('%H:%M:%S'))
        df['dffwd']=df['ffwd1']-df['ffwd0']
        df['dfwd']=df['fwd1']-df['fwd0']
        for c in ['delta_pnl', 'roll_pnl','pnl']:
            df[c+'_cum']=df[c].cumsum()
        df=df[['ts', 'instr', 'sz', 'px', 'theo1', 'theo0']+values+['delta_pnl_cum', 'roll_pnl_cum', 'pnl_cum']]

    return last_tm, df, fut_fill_count, opt_fill_count

def calc_pos_pnl(instr, pos, ov0, ov1, mult, intradayFrac):
    pos_pnl = {}
    exp = instr.split(":")[1]
    fwd1=float(ov1['forward'])
    fwd0=ov0['forward']
    fwd1=fwd0 if np.isnan(fwd1) else fwd1
    dfwd = fwd1-fwd0
    dvol = float(ov1['theo_vol'])-ov0['theo_vol']
    for i in MKT_MID_INSTR_LIST:
        if instr.startswith(i):
            mkt_bid = 0.0 if np.isnan(float(ov1['mkt_bid'])) else float(ov1['mkt_bid'])
            mkt_ask = 0.0 if np.isnan(float(ov1['mkt_ask'])) else float(ov1['mkt_ask'])
            ov1_theo = (mkt_bid + mkt_ask)/2
            break
        else:
            ov1_theo = ov0['value'] if np.isnan(float(ov1['value'])) else float(ov1['value'])

    pnl = pos*mult*(ov1_theo-ov0['value'])
    delta_pnl = pos*mult*ov0['delta']*dfwd
    gamma_pnl = pos*mult*ov0['gamma']*dfwd*dfwd*0.5
    vega_pnl  = pos*mult*ov0['vega']*dvol
    theta_pnl = pos*mult*ov0['theta']*intradayFrac
    res_pnl   = pnl-delta_pnl-gamma_pnl-vega_pnl-theta_pnl

    # currency conversion
    pnl = pnl / fxrate
    delta_pnl = delta_pnl / fxrate
    gamma_pnl = gamma_pnl / fxrate
    vega_pnl = vega_pnl / fxrate
    theta_pnl = theta_pnl / fxrate
    res_pnl = res_pnl / fxrate

    pos_pnl['exp']= exp
    pos_pnl['pos']= pos
    pos_pnl['mult']= mult
    pos_pnl['px0']= float(ov0['value'])
    pos_pnl['px1']= ov1_theo
    pos_pnl['pnl']= pnl
    pos_pnl['delta_pnl']= delta_pnl
    pos_pnl['gamma_pnl']= gamma_pnl
    pos_pnl['vega_pnl']=  vega_pnl
    pos_pnl['theta_pnl']= theta_pnl
    pos_pnl['res_pnl'] =  res_pnl

    return pos_pnl

def report_pos(pos, ovdf, ovd0, start_tm=dt.time(8,45), end_tm=dt.time(13,44,30),
              intraday_var=0.6, details=False, as_dataframe=True,conn='production'):
    '''
    intraday_var: intraday variance, amount of theta decay intraday
    returns a tuple of the marking time and the report
    '''
    now = dt.datetime.now()

    elapse = (now - start_tm).total_seconds() / (end_tm - start_tm).total_seconds()
    intradayFrac = (1 - intraday_var) + intraday_var * max(0, min(1, elapse))

    times=ovdf.index.unique()
    last_tm = times[times<end_tm][-1]
    # ovs1 = ovdf.loc[last_tm] 
    ovs1 = ovdf.copy()  # We assume that only one time snapshot is captured for each symbol. If ovdf contains two symbols from two symbols' optionvalues, the timestamp will be different, so cannot use ovdf.loc[last_tm]. # This is a temporary solution.

    pnl_list = []
    conn=tetrion.db.get_conn(conn)
    
    for instr, pos in sorted(list(pos.items())):
        ov0 = ovd0[instr]
        ov1 = ovs1[ovs1['instrument']==instr]
        if not ov0 or ov1.empty:
            print("warning: could not find values for {}, pos:{}".format(instr, pos))
            continue
        ov1=ov1.iloc[0]
        mult = tetrion.db.get_multiplier(conn,instr,date)
        p_pnl = calc_pos_pnl(instr, pos, ov0, ov1, mult, intradayFrac)
        if p_pnl:
            pnl_list.append(p_pnl)

    df = pd.DataFrame(pnl_list)

    if not details:
        pnlcols = [col for col in df if col.endswith('pnl')]
        df = pd.pivot_table(df, index='exp', values=pnlcols, aggfunc=np.sum)
        sum_row = {col: df[col].sum() for col in df}
        sum_df = pd.DataFrame(sum_row, index=["Total"])
        df = df.append(sum_df)
        df = df.reindex(columns=pnlcols)
        df.index.name=None
        df = df[['pnl', 'delta_pnl', 'gamma_pnl', 'vega_pnl', 'theta_pnl', 'res_pnl']] # reorder columns

    return last_tm, df

# for futures quoting
def report_pos_fut_quote(pos, ovdf, ovs0,
              start_tm=dt.time(8,45), end_tm=dt.time(13,44,30), intraday_var=0.6,
              details=False, as_dataframe=True, conn='production'):
    '''
    intraday_var: intraday variance, amount of theta decay intraday
    returns a tuple of the marking time and the report
    '''
    import numpy as np
    now = dt.datetime.now()

    elapse = (now - start_tm).total_seconds() / (end_tm - start_tm).total_seconds()
    intradayFrac = (1 - intraday_var) + intraday_var * max(0, min(1, elapse))

    times=ovdf.index.unique()
    last_tm = times[times<end_tm][-1]
    ovs1 = ovdf.loc[last_tm]
    
    if as_dataframe:
        import pandas as pd
        t=pd.DataFrame()
    else:
        from tetrion.table import Table
        t = Table()
        t.set_value = t.set

    conn=tetrion.db.get_conn(conn)
    for k,v in sorted(list(pos.items())):
        ov0 = ovs0[k]
        ov1 = ovs1[ovs1['instrument']==k]
        if not ov0 or ov1.empty:
            print("warning: could not find values for {}".format(k))
            continue
        ov1=ov1.iloc[0]

        exp = k.split(":")[1]
        mult = tetrion.db.get_multiplier(conn,k,date)
        fwd1=float(ov1['forward_theo'])
        if fwd1 == 0:
            fwd1=float(ov1['forward'])
        fwd0=ov0['forward']
        fwd1=fwd0 if np.isnan(fwd1) else fwd1
        dfwd = fwd1-fwd0
        #dfwd = float(ov1['ffwd'])-ov0['ffwd']
        dvol = float(ov1['theo_vol'])-ov0['theo_vol']

        px0 = float(ov0['value'])
        px1 = fwd1

        for i in MKT_MID_INSTR_LIST:
            if k.startswith(i):
                mkt_bid = 0.0 if np.isnan(float(ov1['mkt_bid'])) else float(ov1['mkt_bid'])
                mkt_ask = 0.0 if np.isnan(float(ov1['mkt_ask'])) else float(ov1['mkt_ask'])
                ov1_theo = (mkt_bid + mkt_ask)/2
                px1 = ov1_theo
                break

        pnl = v*mult*(px1 - px0)
        delta_pnl = v*mult*ov0['delta']*dfwd
        gamma_pnl = v*mult*ov0['gamma']*dfwd*dfwd*0.5
        vega_pnl  = v*mult*ov0['vega']*dvol
        theta_pnl = v*mult*ov0['theta']*intradayFrac
        res_pnl   = pnl-delta_pnl-gamma_pnl-vega_pnl-theta_pnl

        currency_rate = fxrate
        if USE_INSTR_FX_RATE and k:
            if k not in INSTR_FX_MAP:
                instr_pytrion = Instrument.fromString(str(k))
                INSTR_FX_MAP[k] = tetrion.db.get_currency_rate('production', instr_pytrion.currency.to_string())
            currency_rate = INSTR_FX_MAP[k]

        # currency conversion
        pnl = pnl / currency_rate
        delta_pnl = delta_pnl / currency_rate
        gamma_pnl = gamma_pnl / currency_rate
        vega_pnl = vega_pnl / currency_rate
        theta_pnl = theta_pnl / currency_rate
        res_pnl = res_pnl / currency_rate
        if(as_dataframe):
            t.at[k, 'exp']= exp
            t.at[k, 'pos']= v
            t.at[k, 'mult']= mult
            t.at[k, 'px0']= px0
            t.at[k, 'px1']= px1
            t.at[k, 'pnl']= pnl
            t.at[k, 'delta_pnl']= delta_pnl
            t.at[k, 'gamma_pnl']= gamma_pnl
            t.at[k, 'vega_pnl']=  vega_pnl
            t.at[k, 'theta_pnl']= theta_pnl
            t.at[k, 'res_pnl']=   res_pnl
        else:
            t.set_value(k, 'exp', exp)
            t.set_value(k, 'pos', v)
            t.set_value(k, 'mult', mult)
            t.set_value(k, 'px0', px0)
            t.set_value(k, 'px1', px1)
            t.set_value(k, 'pnl', pnl)
            t.set_value(k, 'delta_pnl', delta_pnl)
            t.set_value(k, 'gamma_pnl', gamma_pnl)
            t.set_value(k, 'vega_pnl',  vega_pnl)
            t.set_value(k, 'theta_pnl', theta_pnl)
            t.set_value(k, 'res_pnl',   res_pnl)

    if not details:
        import pandas as pd
        import numpy as np
        pnlcols = [col for col in t if col.endswith('pnl')]
        t = pd.pivot_table(t, index='exp', values=pnlcols, aggfunc=np.sum)
        sum_row = {col: t[col].sum() for col in t}
        sum_df = pd.DataFrame(sum_row, index=["Total"])
        t = t.append(sum_df)
        t = t.reindex(columns=pnlcols)
        t.index.name=None

    return (last_tm,t)

def send_email(recipients, subject, body):
    import tetrion.email
    tetrion.email.send_message("pnl@tetrioncapital.com", recipients, subject, body)    
    print("Sent EOD Email")

# 11/8/2018 - created to handle new spread order format:
# 'CAL:MXF:1811w2x1811' vs old 'sprd:CAL:FUT_TAIFEX_MXF:FUT_TAIFEX_MXF:201811w2x201811'
def split_spread_fill_v2(fill):
    import copy
    import re
    instr = fill['instr']
    m = re.match('CAL:([A-Z_]+):([0-9]+[w]*[0-9]*)x([0-9]+[w]*[0-9]*)', instr)
    if m:
        # Currently hardcoded for TAIFEX spreads
        fl_instr = 'FUT_TAIFEX_{}:20{}'.format(m.group(1), m.group(2))
        fl_exp_str = '20{}'.format(m.group(2))
        sl_instr = 'FUT_TAIFEX_{}:20{}'.format(m.group(1), m.group(3))
        sl_exp_str = '20{}'.format(m.group(3))
    else:
        print("Invalid spread instrument")
        return []

    instr_sz = fill['sz']
    seq_no = fill['seq']
    # cc (20190807) - to handle unable to coerce error, maybe rewrite this snippet in the future?
    try:
        ffwd = fill['ffwd']
        fwd = ffwd+fill['px']
    except:
        ffwd = 0
        fwd = ffwd+fill['px']
    #ffwd = fill['ffwd']
    #fwd = ffwd + fill['px']
    fill['vol'] = 0 # set to 0 because trader sends out NaN
    fl_fwd = ffwd 
    sl_fwd = fwd

    # if weekly is the first leg, then we assume front month forward is the second
    # since spread orders are labeled chronologically
    if fl_exp_str.find('w') >= 0:
        fwd = ffwd - fill['px']
        fl_fwd = fwd
        sl_fwd = ffwd

    # first leg
    fl = fill.copy()
    fl['fee'] = 0.14 + (ffwd * 0.00002) # approximation of fees, remove this once fees are passed correctly
    fl['instr'] = fl_instr
    fl['sz'] = instr_sz * -1
    fl['seq'] = seq_no + .1
    fl['theo'] = fl_fwd
    fl['px'] = fl_fwd
    fl['fwd'] = fl_fwd

    # second leg
    sl = fill.copy()
    sl['instr'] = sl_instr
    sl['sz'] = instr_sz
    sl['seq'] = seq_no + .2
    sl['fee'] = 0
    sl['theo'] = sl_fwd
    sl['px'] = sl_fwd
    sl['fwd'] = sl_fwd
    return [fl, sl]

def split_spread_fill(fill):
    import copy
    import re
    instr = fill['instr']
    m = re.match('sprd:CAL:([A-Z_]+):([A-Z_]+):([0-9]+[w]*[0-9]*)x([0-9]+[w]*[0-9]*)', instr)
    if m:
        fl_instr = '{}:{}'.format(m.group(1), m.group(3))
        fl_exp_str = m.group(3)
        sl_instr = '{}:{}'.format(m.group(2), m.group(4))
        sl_exp_str = m.group(4)
    else:
        print("Invalid spread instrument")
        return []

    instr_sz = fill['sz']
    seq_no = fill['seq']
    ffwd = fill['ffwd']
    fwd = ffwd + fill['px']
    fl_fwd = ffwd 
    sl_fwd = fwd

    # if weekly is the first leg, then we assume front month forward is the second
    # since spread orders are labeled chronologically
    if fl_exp_str.find('w') >= 0:
        fwd = ffwd - fill['px']
        fl_fwd = fwd
        sl_fwd = ffwd

    # first leg
    fl = fill.copy()
    fl['fee'] = 0.14 + (ffwd * 0.00002) # approximation of fees, remove this once fees are passed correctly
    fl['instr'] = fl_instr
    fl['sz'] = instr_sz * -1
    fl['seq'] = seq_no + .1
    fl['theo'] = fl_fwd
    fl['px'] = fl_fwd
    fl['fwd'] = fl_fwd
    sl = fill.copy()

    # second leg
    sl['instr'] = sl_instr
    sl['sz'] = instr_sz
    sl['seq'] = seq_no + .2
    sl['fee'] = 0
    sl['theo'] = sl_fwd
    sl['px'] = sl_fwd
    sl['fwd'] = sl_fwd
    return [fl, sl]

def process_fills(fills, filters):
    processed_fills = []
    for f in fills:
        if len(filters) > 0:
            if f['label'] not in filters:
                continue

        if f['label'] == 'centralbook':
            if f['theo'] == 0:
                f['theo'] = f['px']

        if f['instr'].startswith('sprd:'):
            processed_fills.extend(split_spread_fill(f))
        elif f['instr'].startswith('CAL:'):
            processed_fills.extend(split_spread_fill_v2(f))
        else:
            processed_fills.append(f)

    return processed_fills

def update_fills_redis(redisdb, trades_key_list, filters):
    global fills
    global ltt
    global error_msg

    new_fills = []
    for trades_key in trades_key_list:
        new_fills.extend([ujson.loads(nf.replace(b'NaN,',b'"NaN",')) for nf in redisdb.lrange(trades_key, 0, -1)])

    fills = process_fills(new_fills, filters)
    if len(fills) > 0:
        ts = fills[-1]['ts']
        ltt = dt.datetime.fromtimestamp(ts/(1 if isinstance(ts, float) else 1000000.0))
        #ltt = dt.datetime.fromtimestamp(fills[-1]['ts']/1000000.0)

    while True:
        try:
            new_fills = []
            for trades_key in trades_key_list:
                new_fills.extend([ujson.loads(nf.replace(b'NaN,',b'"NaN",')) for nf in redisdb.lrange(trades_key, 0, -1)])

            fills = process_fills(new_fills, filters)
            if len(fills) > 0:
                ts = fills[-1]['ts']
                ltt = dt.datetime.fromtimestamp(ts/(1 if isinstance(ts, float) else 1000000.0))
        except Exception as e:
            print("Exception in update_fills_redis: {}".format(e))
        finally:
            time.sleep(REDIS_QUERY_REFRESH)

def get_optionvalues(date, instrs, simulate=False, conn='production'):
    '''returns dictionary of option values prior to beginning of 'date' '''
    result = collections.defaultdict(dict)
    conn = tetrion.db.get_conn(conn)

    cond = 'date<="{}"'
    cond = cond.format(date)

    cond_instr=''
    if instrs:
        instrs = [instrs] if isinstance(instrs, str) else instrs
        cond_instr = 'and instrument in ("{}")'.format('","'.join(instrs))
        cond = cond + ' ' + cond_instr

    cursor = conn.cursor()
    stmt='''
    select date,instrument,value,delta,gamma,vega,theta,theo_vol,forward,maturity,discount,vega_tw,front_forward
    from marketdata.dayinfo_optionvalues
    where
    date=(select max(date) from marketdata.dayinfo_optionvalues where {}) {}
    '''.format(cond, cond_instr)
    
    instr_missing_px = []
    if simulate:
        return stmt
    cursor.execute( stmt )
    if cursor:
        #cols=[x[0] for x in cursor.description]
        #result=[dict(zip(cols,r)) for r in cursor]
        for p in cursor:
            v = result[p[1]]            #.update({c: p[cols.index(c)] for c in cols[2:]})
            # print(p[1], p[2])
            if float(p[2]) == 0:
                instr_missing_px.append(p[1])
            v['value'] = float(p[2])
            v['delta'] = float(p[3])
            v['gamma'] = float(p[4])
            v['vega']  = float(p[5])
            v['theta'] = float(p[6])
            v['theo_vol'] = float(p[7])
            v['forward']  = float(p[8])
            v['maturity']  = float(p[9]) if p[9] else None
            v['discount']  = float(p[10]) if p[10] else None
            v['vega_tw']  = float(p[11])
            v['front_forward']  = float(p[12]) if p[12] else None
            v['ffwd'] = v['front_forward']
    cursor.close()

    instr_missing_px = list(set(instr_missing_px).union(set(instrs).difference(set(result.keys()))))
    if len(instr_missing_px) > 0:
        prices = get_dayinfo_prices(date, instr_missing_px, conn)
        for k, v in prices.items():
            if k in result.keys():
                result[k]['value'] = v['value']
            else:
                result[k]['value'] = v['value']
                result[k]['forward'] = v['value']
                result[k]['theo_vol'] = 0
                result[k]['delta'] = 1
                result[k]['gamma'] = 0
                result[k]['vega'] = 0
                result[k]['theta'] = 0

    # if there's no data, try from dayinfo and initialize greeks
    if len(result) == 0:
        prices = get_dayinfo_prices(date, instrs, conn)
        for k, v in prices.items():
            result[k]['value'] = v['value']
            result[k]['forward'] = v['value']
            result[k]['theo_vol'] = 0
            result[k]['delta'] = 1
            result[k]['gamma'] = 0
            result[k]['vega'] = 0
            result[k]['theta'] = 0

    return result

def get_dayinfo_prices(date, instrs, conn):
    prices = collections.defaultdict(dict)
    cond = 'trade_date<"{}"'
    cond = cond.format(date)

    cond_instr=''
    if instrs:
        cond_instr = 'and instrument_name in ("{}")'.format('","'.join(instrs))

    cursor = conn.cursor()
    stmt='''
    select trade_date, instrument_name, close_price, last_price, theo_price from marketdata.instrument_dayinfo
    where
    trade_date=(select max(trade_date) from marketdata.instrument_dayinfo where {} {})
    {}
    '''.format(cond, cond_instr, cond_instr)

    cursor.execute( stmt )

    if cursor:
        for p in cursor:
            v = prices[p[1]]
            try:
                if p[4]: # if theo is blank, use close price
                    v['value'] = float(p[4])
                else:
                    v['value'] = float(p[2])
            except:
                v['value'] = 0.0
    cursor.close()
    return prices

# check if we're currently in t1 session and return start_dt, end_dt
def is_t1(start_time, end_time):
    cur_dt = dt.datetime.now()
    if cur_dt.time() < end_time: # if we run this past 12AM, we need to subtract a day
        t1_dt = cur_dt-dt.timedelta(1)
    else:
        t1_dt = cur_dt

    start_dt, end_dt = get_t1_dt(t1_dt, start_time, end_time)
    start_dt2 = start_dt - dt.timedelta(minutes=15) # give 15 min buffer
    if cur_dt >= start_dt2 and cur_dt <= end_dt:
        return True, start_dt, end_dt

    return False, start_dt, end_dt

# return start and end dts for given start date, start time and end time
def get_t1_dt(start_date, start_time, end_time):
    start_dt = dt.datetime.combine(start_date, start_time)
    if end_time < start_time:
        end_dt = dt.datetime.combine((start_date + dt.timedelta(days=1)), end_time)
    else:
        end_dt = dt.datetime.combine(start_date, end_time)
    return start_dt, end_dt

def calc_pnl(account,ovdf, ts0, lut=dt.datetime.now()):
    output = ''
    rpt_trd = None
    rpt_pos = None
    last_tm = dt.datetime.min
    ######################################################################################################
    # TRADE PNL
    ######################################################################################################
    if not fills:
        if not showcursor or not sendemail:
            os.system('setterm -cursor off')
        fut_fill_count = 0
        opt_fill_count = 0
        rpt_trd_str = "No Trades\n{}".format(error_msg)
        trd_df = pd.DataFrame([[0,0,0,0,0,0,0,0]],index = ['Total'],columns = ['pnl', 'sprd_pnl', 'delta_pnl', 'roll_pnl', 'gamma_pnl', 'vega_pnl', 'theta_pnl', 'res_pnl'])
    else:
        if futpnl:
            last_tm, rpt_trd, fut_fill_count, opt_fill_count = report_trd_fut_quote(fills,
                                    ovdf, details = False,
                                    start_tm = start_dt, end_tm = end_dt)
        else:
            last_tm, rpt_trd, fut_fill_count, opt_fill_count = report_trd(fills,
                                    ovdf, skipfuture = True, details = False,
                                    start_tm = start_dt, end_tm = end_dt)
        if not rpt_trd.empty:
            trd_df = rpt_trd.copy()
            #rpt_trd.to_csv(account + 'trade_pnl.csv')
            rpt_trd_str=rpt_trd.to_string(float_format='${:,.0f}'.format)
        else:
            trd_df = pd.DataFrame([[0,0,0,0,0,0,0,0]],index = ['Total'],columns = ['pnl', 'sprd_pnl', 'delta_pnl', 'roll_pnl', 'gamma_pnl', 'vega_pnl', 'theta_pnl', 'res_pnl'])
            rpt_trd_str = ""

            
    
    ######################################################################################################
    # POS PNL
    ######################################################################################################
    if not ignore_pos:
        if not pos:
            if not showcursor or not sendemail:
                os.system('setterm -cursor off')
            rpt_pos_str = "No Positions"
            pos_df = pd.DataFrame([[0,0,0,0,0,0]],index = ['Total'],columns = ['pnl', 'delta_pnl', 'gamma_pnl', 'vega_pnl', 'theta_pnl', 'res_pnl'])
        else:
            if futpnl:
                last_tm, rpt_pos = report_pos_fut_quote(pos, ovdf, ovd0, details = False,
                                start_tm = start_dt, end_tm = end_dt)
            else:
                last_tm, rpt_pos = report_pos(pos, ovdf, ovd0, details = False,
                                start_tm = start_dt, end_tm = end_dt)

            if not rpt_pos.empty:
                rpt_pos_str=rpt_pos.to_string(float_format='${:,.0f}'.format)
                pos_df = rpt_pos.copy()
                #rpt_pos.to_csv(account + 'pos_pnl.csv')
            else:
                pos_df = pd.DataFrame([[0,0,0,0,0,0]],index = ['Total'],columns = ['pnl', 'delta_pnl', 'gamma_pnl', 'vega_pnl', 'theta_pnl', 'res_pnl'])
                rpt_pos_str = ""
    instr_name = account.split('_')[1]
    sso_df = pd.concat([pos_df.tail(1), trd_df.tail(1)],axis = 1)
    sso_df['fut'] = fut_fill_count
    sso_df['opt'] = opt_fill_count
    sso_df['instr'] = instr_name
    sso_df.to_csv(instr_name + '_pnl.csv')
    ######################################################################################################
    # OUTPUT
    ######################################################################################################
    if publish_zmq:
        zmq_publisher.publish_trd_df(strategy, rpt_trd, fut_fill_count, opt_fill_count)
        if not ignore_pos:
            zmq_publisher.publish_pos_df(strategy, rpt_pos)

    ts1=time.time()

    sys.stdout.write(chr(27)+'\033c')
    
    o0 = 'PNL {} {} SESSION ({})\n'.format(account_list, session_str, zmq_conn_str)
    o1 = 'last update tm:{}, last trd tm:{}, values tm:{}, {:.3f}s\n'.format(lut.strftime('%Y-%m-%d %H:%M:%S'),
                                        ltt.strftime('%H:%M:%S.%f')[:-3] if ltt != dt.datetime.min else 'N/A',
                                        last_tm.strftime('%H:%M:%S') if last_tm != dt.datetime.min else 'N/A',
                                        ts1-ts0)
    o2 = '{} to USD: {} {}\n\n'.format(currency, fxrate, filters if len(filters) > 0 else '')
    o3 = '[TRADE PNL] redis: {}:{} ({}), fut: {} opt: {}\n'.format(redishost, redisport, redis_tradeskey, fut_fill_count, opt_fill_count)
    o4 = rpt_trd_str
    o5 = o6 = ''
    if not ignore_pos:
        o5 = '\n\n[POSITION PNL]\n'
        o6 = rpt_pos_str

    output = o0 + o1 + o2 + o3 + o4 + o5 + o6
    print (output)

    if not showcursor or not sendemail:
        os.system('setterm -cursor off')

    return output

if __name__ == "__main__":
    ######################################################################################################
    # DEFAULTS:
    ######################################################################################################
    showcursor = False
    prev = False
    simulate = False
    simt1 = False
    recipients = ['ck10100713@gmail.com']
    cur_dt = dt.datetime.now()
    date = cur_dt.date()

    datestr = date.strftime('%Y%m%d')
    start_time = dt.time(8,45)
    end_time = dt.time(13,44,30)
    start_time_t1 = dt.time(15,0)
    end_time_t1 = dt.time(5,0,0)

    market = 'MKT_TAIFEX'
    session_str = 'T'
    strategy = "UNKNOWN"
    redishost = 'prod2.hk'
    redisport = 6379
    redis_session_str = ''
    sendemail = False
    futpnl = False
    publish_zmq = False

    currency = 'USD'
    zmq_server_lookup = ''
    zmq_pub_port = 10800

    rpts = ''
    error_msg = ''
    fills = []
    filters = []
    ltt = dt.datetime.min
    ignore_pos = False
    ovfile_prefix = ''
    ######################################################################################################
    
    account = 'capital_ojo_mm'

    ######################################################################################################
    # ACCOUNT PROFILE OVERRIDES:
    ######################################################################################################

    additional_accounts = []
    if (account == 'txo_main'): # contains both kavalan and weekly
        zmq_server_lookup = 'txo-ovdfwd'
        account = 'TAIFEX800'
        currency = 'TWD'
        strategy = "TXO"
        zmq_pub_port = 10800
    else:
        strategy_config = {}
        try:
            ident = account
            strategy_config = tetrion.config.get_hive_config_by_ident(ident, ignore_acgrp=True)
            # print(strategy_config)
            account = ident # should be the name of the strategy
            if strategy_config:
                # cc 20220509 - temporary solution until we move capital to the new strategy key convention.
                if 'redis_trades_key' in strategy_config:
                    account = strategy_config['redis_trades_key']

                market_info = strategy_config['market_info']
                start_time = dt.datetime.strptime(market_info['start_time'], '%H:%M:%S').time()
                end_time = dt.datetime.strptime(market_info['end_time'], '%H:%M:%S').time()
                start_time_t1 = dt.datetime.strptime(market_info['start_time_t1'], '%H:%M:%S').time() if 'start_time_t1' in market_info else dt.time(14,59,0)
                end_time_t1 = dt.datetime.strptime(market_info['end_time_t1'], '%H:%M:%S').time() if 'end_time_t1' in market_info else dt.time(5,1,0)
                market = market_info['market']
                currency = market_info['currency']

                zmq_trade = strategy_config['zmq_trade']
                zmq_conn_str = "tcp://{}".format(zmq_trade['ovd_listen_connstr'])
                redishost, redisport = zmq_trade['redis_trade_host'].split(':')
                if 'linked_strategies' in zmq_trade:
                    additional_accounts = zmq_trade['linked_strategies']

                if 'fut_only' in zmq_trade:
                    futpnl = zmq_trade['fut_only']

                if 'zmq_ovd' in strategy_config:
                    zmq_ovd = strategy_config['zmq_ovd']
                    if 'ovdfile_prefix' in zmq_ovd:
                        ovfile_prefix = zmq_ovd['ovdfile_prefix']

                zeus = strategy_config['zeus']
                zeus_ident = zeus['pnl_ident']
                strategy = zeus_ident + ''
                zmq_pub_port = zeus['pub_port']
            else:
                print("Strategy Config is empty or Unable to Find Strategy: {}, exiting".format(ident))
                exit(1)
        except Exception as e:
            print("Error fetching hive config for {}, Exception: {}".format(ident, e))
            exit(1)
    ######################################################################################################

    fxrate = tetrion.db.get_currency_rate('production', currency)

    start_dt = dt.datetime.combine(date, start_time)
    end_dt = dt.datetime.combine(date, end_time)
    pos_date = ovd_date = date
    cur_dt = dt.datetime.now()
    t1, start_dt_t1, end_dt_t1 = is_t1(start_time_t1, end_time_t1)

    if simulate:
        showcursor = True
        ovd_date = ovd_date - dt.timedelta(1)
        if simt1:
            t1 = True
            start_dt_t1, end_dt_t1 = get_t1_dt(date, start_time_t1, end_time_t1)
        else:
            t1 = False

    # prev is only assume prev t+1 not prev t session
    if t1:
        if t1:
            ovd_date = start_dt_t1.date()
            pos_date = pos_date + (dt.timedelta(days=3) if dt.date.isoweekday(dt.date.today()) == 5 else dt.timedelta(days=1))
            next_day = tetrion.clientcore.next_trading_day(date, mkt=market)
            datestr = next_day.strftime('%Y%m%d')
            start_dt = start_dt_t1
            end_dt = end_dt_t1
        else:
            prev_day = tetrion.clientcore.prev_trading_day(date, mkt=market)
            start_dt = dt.datetime.combine(prev_day, start_time_t1)
        redis_session_str = 'E'
        session_str = 'T+1'

    # redis
    redisdb = redis.StrictRedis(host=redishost, port=redisport)
    redis_tradeskey = "{}:{}{}".format(account, datestr, redis_session_str)
    # if we need to listen to multiple redis keys
    redis_keys_list = [redis_tradeskey]
    if len(additional_accounts) > 0:
        for a in additional_accounts:
            key = "{}:{}{}".format(a, datestr, redis_session_str)
            redis_keys_list.append(key)
    # zmq
    if zmq_server_lookup:
        zmq_conn_str = "tcp://{}".format(tetrion.clientcore.get_zserver_port(zmq_server_lookup))
    print("ZMQ Connecting to: {}".format(zmq_conn_str))
    subcxt = zmq.Context()
    subsok = subcxt.socket(zmq.SUB)
    subsok.connect(zmq_conn_str)
    subsok.setsockopt_string(zmq.SUBSCRIBE, "OVWVALUES")

    if publish_zmq:
        zmq_pub_conn_str = "tcp://*:{}".format(zmq_pub_port)
        zmq_publisher = ZMQ_Publisher(zmq_pub_conn_str)
        print("ZMQ Publishing to: {}".format(zmq_pub_conn_str))

    # accounts
    try:
        account_list = tetrion.db.get_accounts_by_tag('production', account)
        if not account_list:
            print("Account {} is not a tag".format(account))
            account_list = [account] + additional_accounts
        # print("Accounts: {}".format(account_list))
    except:
        account_list = account
    print("Trades are being loaded from: {}:{} {}".format(redishost, redisport, redis_keys_list))
    _thread.start_new_thread(update_fills_redis, (redisdb, redis_keys_list, filters))

    if not ignore_pos:
        print("Fetching positions for {} from account(s) {}, using ovd date: {}".format(pos_date, account_list, ovd_date))
        # pos = None
        pos = tetrion.db.get_positions(pos_date, account=account_list, eod=False)
        ovd0 = get_optionvalues(ovd_date, instrs = pos.keys())

    if simulate:
        try:
            print("Simulating PNL for Date: {}{} end date:{}".format(date, ' T1' if simt1 else '', end_dt))
            datestr = date.strftime('%Y%m%d')

            path = '/nfs/public_data/optiontrading/optionvalues/' # change this to get ovfile path when api is ready.
            ovfile_prefix = path + ovfile_prefix

            ts0 = time.time()
            ovfile = '{}-{}.csv'.format(ovfile_prefix, datestr)
            ovdf = tetrion.options.load_optionvalues(ovfile, memory_map=False)
        except:
            print("Options Values file not valid: {}".format(ovfile))
            exit(1)

        if ovdf.empty:
            print("Options Values loaded an empty dataframe: {}".format(ovfile))
            exit(1)
        else:
            print("Loading option values from {}".format(ovfile))

        times = ovdf.index.unique()
        last_tm = times[times<end_dt][-1]
        ovdf = ovdf.loc[last_tm]
        calc_pnl(ovdf, ts0)
        exit(0)

    # print(ovd0)
    # print("start_time_t1_dt: {} start_time: {} end_time_dt: {} end_dt: {}".format(start_time_t1, start_dt, end_time_t1, end_dt))
    # initialization
    output = ''
    while True:
        msg = subsok.recv()
        # get the latest message in case we are queued up
        try:
            while True:
                msg = subsok.recv(flags=zmq.NOBLOCK)
        except:
            pass
        if (msg):
            lut = dt.datetime.now()
            if lut > end_dt:
                if (lut - end_dt).total_seconds() > TIMEOUT:
                    print("End of Session execeeded TIMEOUT. Exiting.")
                if sendemail:
                    if output: 
                        subject = "EOD PNL for {} {} Session".format(account_list, session_str)
                        send_email(recipients, subject, output)
                        print("Exiting")
            ts0=time.time()
            msgtype, ovdf = tetrion.options.parse_zmq_msg(msg)
            output = calc_pnl(account,ovdf, ts0, lut)
