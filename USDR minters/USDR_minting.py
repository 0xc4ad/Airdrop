from web3 import Web3
from web3._utils.filters import construct_event_filter_params
from web3._utils.events import get_event_data
import json
import datetime as dt
import requests
import os
from dotenv import load_dotenv
import pandas as pd
import re
from collections import defaultdict


load_dotenv()
ETHERSCAN_API_KEY = os.environ['POLY_ETHERSCAN_API']
ALCHEMY_URL = os.environ['ALCHEMY_URL_POLY']
W3 = Web3(Web3.HTTPProvider(ALCHEMY_URL))
FROM_BLOCK = 34737085   # block of first USDR transfer

W3 = Web3(Web3.HTTPProvider(ALCHEMY_URL))

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

USDR_EXCHANGE_ABI = [
    {
        'inputs': [
            {
                'internalType': 'uint256',
                'name': 'amountIn',
                'type': 'uint256'
            },
            {
                'internalType': 'address',
                'name': 'to',
                'type': 'address'
            }],
        'name': 'swapFromUnderlying',
        'outputs': [
            {
                'internalType': 'uint256',
                'name': 'amountOut',
                'type': 'uint256'}],
        'stateMutability': 'nonpayable',
        'type': 'function'
    },
    {
        'inputs': [
            {
                'internalType': 'uint256',
                'name': 'amountIn',
                'type': 'uint256'
            },
            {
                 'internalType': 'address',
                 'name': 'to',
                 'type': 'address'
            }],
        'name': 'swapToUnderlying',
        'outputs': [
            {
                'internalType': 'uint256',
                'name': '',
                'type': 'uint256'
            }],
        'stateMutability': 'nonpayable',
        'type': 'function'}
]


SIMPLE_TX = set()
COMPLEX_TX = set()
def is_simple(tx_hash):
    if tx_hash in SIMPLE_TX:
        return True
    if tx_hash in COMPLEX_TX:
        return False
    usdr_exchange = '0x195F7B233947d51F4C3b756ad41a5Ddb34cEBCe0'
    contract = W3.eth.contract(usdr_exchange, abi=USDR_EXCHANGE_ABI)
    tx_info = W3.eth.get_transaction(tx_hash)
    if tx_info.to == usdr_exchange:
        try:
            function_input = contract.decode_function_input(tx_info.input)
        except:
            COMPLEX_TX.add(tx_hash)
            return False
        if function_input[0].fn_name == contract.functions.swapFromUnderlying.fn_name or \
            function_input[0].fn_name == contract.functions.swapToUnderlying.fn_name:
            SIMPLE_TX.add(tx_hash)
            return True
    COMPLEX_TX.add(tx_hash)
    return False


def get_all_usdr_transfers(token, block_num):
    contract = W3.eth.contract(token, abi=ERC20_ABI)
    transfer_event = contract.events.Transfer
    transfer_event_abi = transfer_event._get_event_abi()

    # pull all transfer events:
    logs_all = []
    start_block = FROM_BLOCK
    end_block = block_num
    while True:
        try:
            print(f'trying block {start_block} to {end_block}...')
            _, event_filter_params = construct_event_filter_params(
                transfer_event_abi,
                W3.codec,
                address=token,
                argument_filters={'address': token
                                  },
                fromBlock=start_block,
                toBlock=end_block
            )
            logs = W3.eth.get_logs(event_filter_params)
        except ValueError as e:
            print('failed...')
            m = re.search('range should work: \[(0x[0-9a-f]*), (0x[0-9a-f]*)\]', str(e))
            end_block = int(m.group(2), 0)
            continue
        except Exception as e:
            print('unhandled exception:', e)
            continue

        logs_all.extend(logs)
        if end_block == block_num:
            break
        start_block = end_block
        end_block = block_num
    print('done pulling logs.')

    # assemble in DataFrame:
    df = []
    for i, log in enumerate(logs_all):
        evt = get_event_data(W3.codec, transfer_event_abi, log)
        tx_hash = evt['transactionHash'].hex()
        print(f'{i}) tx {tx_hash}')
        if not is_simple(tx_hash):
            continue
        args = evt['args']
        args['from']
        df.append({
            'from': args['from'],
            'to': args['to'],
            'amount': args['amount'],
            'tx_hash': tx_hash,
            'block_num': evt['blockNumber']
            })
    df = pd.DataFrame(df)
    return df


def get_all_dai_transfers(token, block_nums):
    contract = W3.eth.contract(token, abi=ERC20_ABI)
    transfer_event = contract.events.Transfer
    transfer_event_abi = transfer_event._get_event_abi()

    # pull all transfer events:
    logs_all = []
    for i, tx_block_num in enumerate(block_nums):
        print(f'{i}) trying block {tx_block_num}')
        _, event_filter_params = construct_event_filter_params(
            transfer_event_abi,
            W3.codec,
            address=token,
            argument_filters={'address': token},
            fromBlock=tx_block_num,
            toBlock=tx_block_num
        )
        logs = W3.eth.get_logs(event_filter_params)
        logs_all.extend(logs)
    print('done pulling logs.')

    # assemble in DataFrame:
    df = []
    for log in logs_all:
        evt = get_event_data(W3.codec, transfer_event_abi, log)
        args = evt['args']
        args['from']
        df.append({
            'from': args['from'],
            'to': args['to'],
            'amount': args['amount'],
            'tx_hash': evt['transactionHash'].hex(),
            'block_num': evt['blockNumber']
            })
    df = pd.DataFrame(df)
    return df


def main():
    # get block time from etherscan API:
    snap_time = dt.datetime(2023, 6, 1).replace(tzinfo=dt.timezone.utc).timestamp()
    response = requests.get('https://api.polygonscan.com/api',
                            params = {'module': 'block',
                                      'action': 'getblocknobytime',
                                      'timestamp': int(snap_time),
                                      'closest': 'before',
                                      'apikey': ETHERSCAN_API_KEY})
    response_json = json.loads(response.content)
    block_num = int(response_json['result'])

    # pull all transfers for both USDR and DAI:
    real_usd = '0xb5DFABd7fF7F83BAB83995E72A52B97ABb7bcf63'
    dai = '0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063'
    df_usdr_xfers = get_all_usdr_transfers(real_usd, block_num)
    block_nums = set(df_usdr_xfers.block_num)
    df_dai_xfers = get_all_dai_transfers(dai, block_nums)
    tx_list = set(df_usdr_xfers.tx_hash)
    df_dai_xfers = df_dai_xfers[df_dai_xfers.tx_hash.isin(tx_list)]

    # loop through all tx:
    null_addr = '0x0000000000000000000000000000000000000000'
    minter_list = []
    for i, tx in enumerate(tx_list):
        # get DAI flow for each address in this tx:
        dai_balances = defaultdict(int)
        for _, dai_tx in df_dai_xfers[df_dai_xfers.tx_hash == tx].iterrows():
            dai_balances[dai_tx['from']] -= dai_tx.amount
            dai_balances[dai_tx['to']] += dai_tx.amount

        print(f'{i}) tx {tx}')
        
        # get mint, distribute credit according to whomever paid for it in DAI:
        df_mint = df_usdr_xfers[(df_usdr_xfers['from'] == null_addr) & (df_usdr_xfers['tx_hash'] == tx)]        
        if len(df_mint) > 0:
            mint_amount = df_mint.amount.sum()
            total_dai = sum(dai_balances[addr] for addr in dai_balances if dai_balances[addr] < 0)        
            for addr in dai_balances:
                balance = dai_balances[addr]
                if balance >= 0:
                    continue
                minter_list.append({
                    'addr': addr,
                    'amount': balance / total_dai * mint_amount
                    })

        # get burn, penalize whomever received DAI:
        df_burn = df_usdr_xfers[(df_usdr_xfers['to'] == null_addr) & (df_usdr_xfers['tx_hash'] == tx)]
        if len(df_burn) > 0:
            burn_amount = -df_burn.amount.sum()
            total_dai = sum(dai_balances[addr] for addr in dai_balances if dai_balances[addr] > 0)
            for addr in dai_balances:
                balance = dai_balances[addr]
                if balance <= 0:
                    continue
                minter_list.append({
                    'addr': addr,
                    'amount': balance / total_dai * burn_amount
                    })

    # summarize, copy to clipboard, do rest in Excel:
    df = pd.DataFrame(minter_list)
    df.groupby('addr')['amount'].sum().to_clipboard()
