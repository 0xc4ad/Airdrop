import os
from dotenv import load_dotenv
import datetime as dt
import requests
import json
from web3 import Web3
from web3._utils.filters import construct_event_filter_params
from web3._utils.events import get_event_data
from collections import defaultdict
import pandas as pd
from pycoingecko import CoinGeckoAPI
import re


load_dotenv()
ETHERSCAN_API_KEY = os.environ['ETHERSCAN_API']
ALCHEMY_URL = os.environ['ALCHEMY_URL_ETH']
W3 = Web3(Web3.HTTPProvider(ALCHEMY_URL))
CG = CoinGeckoAPI()
FROM_BLOCK = 1

BOOSTER_ABI = [
    {
        'anonymous': False,
        'inputs': [
            {
                'indexed': True,
                'internalType': 'address',
                'name': 'user',
                'type': 'address'},
            {
                'indexed': True,
                'internalType': 'uint256',
                'name': 'poolid',
                'type': 'uint256'},
            {
                'indexed': False,
                'internalType': 'uint256',
                'name': 'amount',
                'type': 'uint256'}],
        'name': 'Deposited',
        'type': 'event'},
    {
        'inputs': [
            {
                'internalType': 'uint256',
                'name': '',
                'type': 'uint256'}],
        'name': 'poolInfo',
        'outputs': [
            {
                'internalType': 'address',
                'name': 'lptoken',
                'type': 'address'},
            {
                'internalType': 'address',
                'name': 'token',
                'type': 'address'},
            {
                'internalType': 'address',
                'name': 'gauge',
                'type': 'address'},
            {
                'internalType': 'address',
                'name': 'crvRewards',
                'type': 'address'},
            {
                'internalType': 'address',
                'name': 'stash',
                'type': 'address'},
            {
                'internalType': 'bool',
                'name': 'shutdown',
                'type': 'bool'}],
        'stateMutability': 'view',
        'type': 'function'}]

REWARD_POOL_ABI = [
    {
        'inputs': [],
        'name': 'totalSupply',
        'outputs': [
            {
                'internalType': 'uint256',
                'name': '',
                'type': 'uint256'}],
        'stateMutability': 'view',
        'type': 'function'},
    {
        'inputs': [
            {
                'internalType': 'address',
                'name': 'account',
                'type': 'address'}],
        'name': 'balanceOf',
        'outputs': [
            {
                'internalType': 'uint256',
                'name': '',
                'type': 'uint256'}],
        'stateMutability': 'view',
        'type': 'function'}]



def get_convex_deposits(block_num):
    booster = '0xF403C135812408BFbE8713b5A23a04b3D48AAE31'
    booster_contract = W3.eth.contract(booster, abi=BOOSTER_ABI)
    deposited_event = booster_contract.events.Deposited
    deposited_event_abi = deposited_event._get_event_abi()

    logs_all = []
    start_block = FROM_BLOCK
    end_block = block_num
    while True:
        try:
            print(f'trying block {start_block} to {end_block}...')
            _, event_filter_params = construct_event_filter_params(
                deposited_event_abi,
                W3.codec,
                address=booster,
                argument_filters={'address': booster},
                fromBlock=start_block,
                toBlock=end_block
            )
            logs = W3.eth.get_logs(event_filter_params)
        except ValueError as e:
            print('failed...')
            m = re.search('range should work: \[(0x[0-9a-f]*), (0x[0-9a-f]*)\]', str(e))
            end_block = int(m.group(2), 0)
            continue
        logs_all.extend(logs)
        if end_block == block_num:
            break
        start_block = end_block
        end_block = block_num
    print('done pulling logs.')

    pool_id_to_users = defaultdict(set)
    for log in logs_all:
        evt = get_event_data(W3.codec, deposited_event_abi, log)
        args = evt['args']
        pool_id_to_users[args['poolid']].add(args['user'])

    return pool_id_to_users


def get_token_info(pool_id, block_num):
    ''' get LP token and reward pool '''
    booster = '0xF403C135812408BFbE8713b5A23a04b3D48AAE31'
    booster_contract = W3.eth.contract(booster, abi=BOOSTER_ABI)
    pool_info = booster_contract.functions.poolInfo(pool_id).call(block_identifier=block_num)
    lp_token = pool_info[0]
    reward_pool = pool_info[3]
    return lp_token, reward_pool


def get_pool_ownership(reward_pool, users, block_num):
    ''' from reward pool get balance and total supply: '''
    reward_pool_contract = W3.eth.contract(reward_pool, abi=REWARD_POOL_ABI)
    total_supply = reward_pool_contract.functions.totalSupply().call(block_identifier=block_num)
    df_lp_ownership = pd.DataFrame(columns=['pct_own'], index=list(users))
    if total_supply > 0:
        for user in users:
            user_balance = reward_pool_contract.functions.balanceOf(user).call(block_identifier=block_num)
            df_lp_ownership.loc[user, 'pct_own'] = user_balance / total_supply
    return df_lp_ownership


def get_decimals(token, block_num):
    ''' pulls decimals from set of ERC20 tokens '''
    abi = [
        {
            'inputs': [],
            'name': 'decimals',
            'outputs': [
                {
                    'internalType': 'uint8',
                    'name': '',
                    'type': 'uint8'}],
            'stateMutability': 'view',
            'type': 'function'}]
    contract = W3.eth.contract(token, abi=abi)
    decimals = contract.functions.decimals().call(block_identifier=block_num)
    return decimals


def main():
    # get block number:
    time_stamp = int(dt.datetime(2023, 6, 1).replace(tzinfo=dt.timezone.utc).timestamp())
    response = requests.get('https://api.etherscan.io/api',
                            params={'module': 'block',
                                    'action': 'getblocknobytime',
                                    'timestamp': time_stamp,
                                    'closest': 'before',
                                    'apikey': ETHERSCAN_API_KEY})
    response_json = json.loads(response.content)
    block_num = int(response_json['result'])

    # get Convex deposits:
    pool_id_to_users = get_convex_deposits(block_num)
    df_pool_ownership_list = []
    for pool_id in pool_id_to_users:
        print(pool_id)
        lp_token, reward_pool = get_token_info(pool_id, block_num)
        df_pool_ownership = get_pool_ownership(reward_pool, pool_id_to_users[pool_id], block_num)
        df_pool_ownership['crv_pool'] = lp_token
        df_pool_ownership['convex_pool_id'] = pool_id
        df_pool_ownership_list.append(df_pool_ownership)

    df_pool_ownership_all = pd.concat(df_pool_ownership_list)
    df_pool_ownership_all.to_clipboard()
