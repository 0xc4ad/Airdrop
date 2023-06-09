import os
from dotenv import load_dotenv
import datetime as dt
import requests
import json
from web3 import Web3
from web3._utils.filters import construct_event_filter_params
from web3._utils.events import get_event_data
import pandas as pd
from pycoingecko import CoinGeckoAPI


load_dotenv()
ALCHEMY_URL = os.environ['ALCHEMY_URL_ARB']
W3  = Web3(Web3.HTTPProvider(ALCHEMY_URL))
ETHERSCAN_API_KEY = os.environ['ARB_ETHERSCAN_API']
CG = CoinGeckoAPI()
FROM_BLOCK = 1   # pair factory creation block

ERC20_ABI = [
    {
        'inputs': [
            {
                'internalType': 'address',
                'name': '',
                'type': 'address'
            }],
        'name': 'balanceOf',
        'outputs': [
            {
                'internalType': 'uint256',
                'name': '',
                'type': 'uint256'
            }],
        'stateMutability': 'view',
        'type': 'function'},
    {
        'inputs': [],
        'name': 'totalSupply',
        'outputs': [
            {
                'internalType': 'uint256',
                'name': '',
                'type': 'uint256'
            }],
        'stateMutability': 'view',
        'type': 'function'},
    {
        'anonymous': False,
        'inputs': [
            {
                'indexed': True,
                'internalType': 'address',
                'name': 'from',
                'type': 'address'
            },
            {
                'indexed': True,
                'internalType': 'address',
                'name': 'to',
                'type': 'address'
            },
            {
                'indexed': False,
                'internalType': 'uint256',
                'name': 'amount',
                'type': 'uint256'
            }],
        'name': 'Transfer',
        'type': 'event'
    }]


def get_coingecko_prices(token_set, time_stamp, blockchain):
    ''' pulls CoinGecko data from set of addresses '''

    # pull list of tokens that have data in CoinGecko
    df = pd.DataFrame(columns=['api_id', 'symbol'])
    df.index.name = 'token'
    for token in token_set:
        print(f'CG get info: {token}')
        try:
            token_data = CG.get_coin_info_from_contract_address_by_id(blockchain, token)
            df.loc[token] = token_data['id'], token_data['symbol']
        except ValueError as e:
            if str(e) == "{'error': 'coin not found'}":
                continue
            else:
                raise

    # enrich with prices as of time_stamp:
    date_str = dt.datetime.fromtimestamp(time_stamp, tz=dt.timezone.utc).strftime('%d-%m-%Y')
    for token in df.index:
        print(f'CG get info: {token}')
        api_id = df.loc[token, 'api_id']
        hist_data = CG.get_coin_history_by_id(api_id, date_str)
        try:
            price = hist_data['market_data']['current_price']['usd']
        except:
            if api_id == 'real-usd':
                price = 1.0
            else:
                price = 0.0
        df.loc[token, 'price'] = price

    return df


def get_reserves(pair_set, block_num):
    ''' gets reserves for set of Pairs '''
    abi = [
        {
            'inputs': [],
            'name': 'getReserves',
            'outputs': [
                {
                    'internalType': 'uint256',
                    'name': '_reserve0',
                    'type': 'uint256'
                },
                {
                    'internalType': 'uint256',
                    'name': '_reserve1',
                    'type': 'uint256'
                },
                {
                    'internalType': 'uint256',
                    'name': '_blockTimestampLast',
                    'type': 'uint256'
                }],
            'stateMutability': 'view',
            'type': 'function'}]
    df = pd.DataFrame(columns=['reserve0', 'reserve1'])
    df.index.name = 'pair'
    for pair in pair_set:
        contract = W3.eth.contract(pair, abi=abi)
        reserve0, reserve1, _ = contract.functions.getReserves().call(block_identifier=block_num)
        df.loc[pair, ['reserve0', 'reserve1']] = reserve0, reserve1

    return df


def get_decimals(token_set, block_num):
    ''' pulls decimals from set of ERC20 tokens '''
    abi = [
        {
            'inputs': [],
            'name': 'decimals',
            'outputs': [
                {
                    'internalType': 'uint8',
                    'name': '',
                    'type': 'uint8'
                }],
            'stateMutability': 'view',
            'type': 'function'}]
    df = pd.DataFrame(columns=['decimals'])
    df.index.name = 'token'
    for token in token_set:
        contract = W3.eth.contract(token, abi=abi)
        decimals = contract.functions.decimals().call(block_identifier=block_num)
        df.loc[token, 'decimals'] = decimals

    return df


def get_all_pairs(time_stamp, block_num):
    ''' gets list of all pairs in Chronos as well as TVL (in USD) as of block_num '''

    # factory contract and event ABI:
    pair_factory = '0xCe9240869391928253Ed9cc9Bcb8cb98CB5B0722'
    pair_created_event_abi = {
        'anonymous': False,
        'inputs': [
            {
                'indexed': True,
                'internalType': 'address',
                'name': 'token0',
                'type': 'address'
            },
            {
                'indexed': True,
                'internalType': 'address',
                'name': 'token1',
                'type': 'address'
            },
            {
                'indexed': False,
                'internalType': 'bool',
                'name': 'stable',
                'type': 'bool'
            },
            {
                'indexed': False,
                'internalType': 'address',
                'name': 'pair',
                'type': 'address'
            },
            {
                'indexed': False,
                'internalType': 'uint256',
                'name': '',
                'type': 'uint256'
            }],
        'name': 'PairCreated',
        'type': 'event'}

    # get all pairs from PairCreated events:
    _, event_filter_params = construct_event_filter_params(
        pair_created_event_abi,
        W3.codec,
        address=pair_factory,
        argument_filters={'address': pair_factory},
        fromBlock=FROM_BLOCK,
        toBlock=block_num
    )
    logs = W3.eth.get_logs(event_filter_params)
    df = pd.DataFrame(columns=['token0', 'token1'])
    df.index.name = 'pair'
    for log in logs:
        evt = get_event_data(W3.codec, pair_created_event_abi, log)
        args = evt['args']
        df.loc[args['pair']] = args['token0'], args['token1']

    # enrich with prices from CoinGecko (ignore anything that's unlisted there):
    token_set = set(df.token0).union(df.token1)
    df_prices = get_coingecko_prices(token_set, time_stamp, 'arbitrum-one')
    df = df.join(df_prices[['price']], on='token0', how='inner').rename(columns={'price': 'price0'})
    df = df.join(df_prices[['price']], on='token1', how='inner').rename(columns={'price': 'price1'})

    # get token amounts:
    df_reserves = get_reserves(df.index, block_num)
    df = df.join(df_reserves)

    # adjust for decimals:
    df_decimals = get_decimals(token_set, block_num)
    df = df.join(df_decimals, on='token0').rename(columns={'decimals': 'decimals0'})
    df = df.join(df_decimals, on='token1').rename(columns={'decimals': 'decimals1'})

    df.reserve0 /= 10 ** df.decimals0
    df.reserve1 /= 10 ** df.decimals1

    df['TVL'] = df.price0 * df.reserve0 + df.price1 * df.reserve1

    return df[['TVL']]


def get_all_gauges(block_num):
    ''' gets set of all Chronos guages '''
    abi = {
        'anonymous': False,
        'inputs': [
            {
                'indexed': True,
                'internalType': 'address',
                'name': 'gauge',
                'type': 'address'
            },
            {
                'indexed': False,
                'internalType': 'address',
                'name': 'creator',
                'type': 'address'
            },
            {
                'indexed': False,
                'internalType': 'address',
                'name': 'internal_bribe',
                'type': 'address'
            },
            {
                'indexed': True,
                'internalType': 'address',
                'name': 'external_bribe',
                'type': 'address'
            },
            {
                'indexed': True,
                'internalType': 'address',
                'name': 'pool',
                'type': 'address'
            }],
        'name': 'GaugeCreated',
        'type': 'event'}

    # get GaugeCreated events from voter contract:
    chronos_voter = '0xC72b5C6D2C33063E89a50B2F77C99193aE6cEe6c'
    _, event_filter_params = construct_event_filter_params(
        abi,
        W3.codec,
        address=chronos_voter,
        argument_filters={'address': chronos_voter},
        fromBlock=FROM_BLOCK,
        toBlock=block_num
    )
    logs = W3.eth.get_logs(event_filter_params)

    # get vault addresses:
    df = pd.DataFrame(columns=['gauge'])
    df.index.name = 'pool'
    for log in logs:
        evt = get_event_data(W3.codec, abi, log)
        args = evt['args']
        df.loc[args['pool']] = args['gauge']

    return df


def get_erc20_owners(erc20, block_num):
    ''' retrieve all owners of ERC20 token that emits transfer events '''
    # pull all transfer events:
    contract = W3.eth.contract(erc20, abi=ERC20_ABI)
    transfer_event = contract.events.Transfer
    transfer_event_abi = transfer_event._get_event_abi()
    _, event_filter_params = construct_event_filter_params(
        transfer_event_abi,
        W3.codec,
        address=erc20,
        argument_filters={'address': erc20},
        fromBlock=FROM_BLOCK,
        toBlock=block_num
    )
    logs = W3.eth.get_logs(event_filter_params)

    # find list of all LPs that have ever been sent LP token:
    owners = set()
    for log in logs:
        evt = get_event_data(W3.codec, transfer_event_abi, log)
        owners.add(evt['args']['to'])

    # get balances:
    df = pd.DataFrame(columns=['balance'])
    df.index.name = 'owner'
    for owner in owners:
        balance = contract.functions.balanceOf(owner).call(block_identifier=block_num)
        df.loc[owner, 'balance'] = balance

    # check to make sure we have the full total:
    total_balance = contract.functions.totalSupply().call(block_identifier=block_num)
    assert df.balance.sum() == total_balance
    
    df['pct_own'] = df.balance / df.balance.sum()
    return df[['pct_own']]
    #return df


def get_gauge_users(gauge, block_num):
    abi = [
        {
            'inputs': [
                {
                    'internalType': 'address',
                    'name': '_user',
                    'type': 'address'
                }],
            'name': 'balanceOf',
            'outputs': [
                {
                    'internalType': 'uint256',
                    'name': '',
                    'type': 'uint256'
                }],
            'stateMutability': 'view',
            'type': 'function'
        },
        {
            'inputs': [],
            'name': 'totalSupply',
            'outputs': [
                {
                    'internalType': 'uint256',
                    'name': '',
                    'type': 'uint256'
                }],
            'stateMutability': 'view',
            'type': 'function'},
        {
            'anonymous': False,
            'inputs': [
                {
                    'indexed': True,
                    'internalType': 'address',
                    'name': 'user',
                    'type': 'address'
                },
                {
                    'indexed': False,
                    'internalType': 'uint256',
                    'name': 'tokenId',
                    'type': 'uint256'
                },
                {
                    'indexed': False,
                    'internalType': 'uint256',
                    'name': 'amount',
                    'type': 'uint256'
                }],
            'name': 'Deposit',
            'type': 'event'
        }]

    # pull all Deposit events:
    contract = W3.eth.contract(gauge, abi=abi)
    deposit_event = contract.events.Deposit
    deposit_event_abi = deposit_event._get_event_abi()
    _, event_filter_params = construct_event_filter_params(
        deposit_event_abi,
        W3.codec,
        address=gauge,
        argument_filters={'address': gauge},
        fromBlock=FROM_BLOCK,
        toBlock=block_num
    )
    logs = W3.eth.get_logs(event_filter_params)

    # assemble sets of users and transactions:
    users = set()
    for log in logs:
        evt = get_event_data(W3.codec, deposit_event_abi, log)
        users.add(evt['args']['user'])

    df = pd.DataFrame(columns=['balance'])
    df.index.name = 'owner'
    for user in users:
        balance = contract.functions.balanceOf(user).call(block_identifier=block_num)
        df.loc[user] = balance

    # for some reason there's a bit of dust in some of these, so doesn't perfectly line-up:
    total_balance = contract.functions.totalSupply().call(block_identifier=block_num)
    balance_sum = df.balance.astype('float').sum()
    if total_balance > 0 or df.balance.sum() > 0:
        if abs(balance_sum / total_balance - 1) > 1e-3:
            print(f'warning: df.balance.sum() = {balance_sum}, total_balance = {total_balance}')
        df['pct_own'] = df.balance / total_balance
    else:
        df['pct_own'] = 1.0

    return df


def get_pair_name(pair):
    abi = [
        {
            'inputs': [],
            'name': 'name',
            'outputs': [
                {
                    'internalType': 'string',
                    'name': '',
                    'type': 'string'
                }],
            'stateMutability': 'view',
            'type': 'function'}]
    contract = W3.eth.contract(pair, abi=abi)
    return contract.functions.name().call()


def get_all_lps(pair, gauge, block_num):
    ''' for a given Pair returns all LP addresses and percent ownership
        (takes into account gauges) '''

    # pull free LPs:
    df_pair_lps = get_erc20_owners(pair, block_num)
    df_pair_lps['type'] = 'LP'

    # pull LPs from gauge:
    if not pd.isnull(gauge) and gauge in df_pair_lps.index:
        gauge_pct_own = df_pair_lps.loc[gauge, 'pct_own']
        df_gauge_lps = get_gauge_users(gauge, block_num)
        df_gauge_lps['type'] = 'GaugeLP'
        df_gauge_lps.pct_own *= gauge_pct_own
        df_pair_lps.drop(gauge, inplace=True)
        df_pair_lps = pd.concat([df_pair_lps, df_gauge_lps])
        df_pair_lps = df_pair_lps.reset_index().groupby(['owner', 'type']) \
            .agg({'balance': sum, 'pct_own': sum}) \
            .reset_index(level='type')

    df_pair_lps['pair'] = get_pair_name(pair)
    return df_pair_lps


if __name__ == '__main__':
    # get block time from time_stamp:
    time_stamp = int(dt.datetime(2023, 6, 1).replace(tzinfo=dt.timezone.utc).timestamp())
    response = requests.get('https://api.arbiscan.io/api',
                            params={'module': 'block',
                                    'action': 'getblocknobytime',
                                    'timestamp': time_stamp,
                                    'closest': 'before',
                                    'apikey': ETHERSCAN_API_KEY})
    response_json = json.loads(response.content)
    block_num = int(response_json['result'])

    df_pairs = get_all_pairs(time_stamp, block_num)
    df_gauges = get_all_gauges(block_num)
    df_pairs = df_pairs.join(df_gauges)

    # find ownership:
    df_lps_list = []
    for pair, row in df_pairs.iterrows():
        print(pair)
        df_lps = get_all_lps(pair, row.gauge, block_num)
        df_lps['TVL'] = row.TVL
        df_lps_list.append(df_lps)

    df_lps_all = pd.concat(df_lps_list)
    df_lps_all['balance'] = df_lps_all.pct_own * df_lps_all.TVL
    df_lps_all.sort_values('balance', ascending=False, inplace=True)
    df_lps_all.groupby('owner')[['balance']].sum().to_csv('chronos_data.csv')
