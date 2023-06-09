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


load_dotenv()
ETHERSCAN_API_KEY = os.environ['ARB_ETHERSCAN_API']
ALCHEMY_URL = os.environ['ALCHEMY_URL_ARB']
W3 = Web3(Web3.HTTPProvider(ALCHEMY_URL))
FROM_BLOCK = 1

# Setup
W3 = Web3(Web3.HTTPProvider(ALCHEMY_URL))

VECHR_ABI = [
    {
        'anonymous': False,
        'inputs': [
            {
                'indexed': True,
                'internalType': 'address',
                'name': 'from',
                'type': 'address'},
            {
                'indexed': True,
                'internalType': 'address',
                'name': 'to',
                'type': 'address'},
            {
                'indexed': True,
                'internalType': 'uint256',
                'name': 'tokenId',
                'type': 'uint256'}],
        'name': 'Transfer',
        'type': 'event'},
    {
        'inputs': [
            {
                'internalType': 'uint256',
                'name': '_tokenId',
                'type': 'uint256'}],
        'name': 'ownerOf',
        'outputs': [
            {
                'internalType': 'address',
                'name': '',
                'type': 'address'}],
        'stateMutability': 'view',
        'type': 'function'},
    {
        'inputs': [
            {
                'internalType': 'uint256',
                'name': '_tokenId',
                'type': 'uint256'}],
        'name': 'balanceOfNFT',
        'outputs': [
            {
                'internalType': 'uint256',
                'name': '',
                'type': 'uint256'}],
        'stateMutability': 'view',
        'type': 'function'}]

AIRDROP_EVENT_ABI = {
    'anonymous': False,
    'inputs': [
        {
            'indexed': False,
            'internalType': 'address',
            'name': '_who',
            'type': 'address'},
        {
            'indexed': False,
            'internalType': 'uint256',
            'name': 'amount',
            'type': 'uint256'}],
    'name': 'Claimed',
    'type': 'event'}


def get_all_creation_events(block_num):
    vechr = '0x9A01857f33aa382b1d5bb96C3180347862432B0d'
    vechr_contract = W3.eth.contract(vechr, abi=VECHR_ABI)
    transfer_event = vechr_contract.events.Transfer
    transfer_event_abi = transfer_event._get_event_abi()

    logs_all = []
    start_block = FROM_BLOCK
    end_block = block_num
    while True:
        try:
            print(f'trying block {start_block} to {end_block}...')
            _, event_filter_params = construct_event_filter_params(
                transfer_event_abi,
                W3.codec,
                address=vechr,
                argument_filters={'address': vechr},
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

    df = pd.DataFrame(columns=['hash', 'block_num'])
    null_addr = '0x0000000000000000000000000000000000000000'
    for log in logs_all:
        evt = get_event_data(W3.codec, transfer_event_abi, log)
        args = evt['args']
        if args['from'] == null_addr:
            token_id = args['tokenId']
            tx_hash = evt['transactionHash'].hex()
            evt_block_num = evt['blockNumber']
            df.loc[token_id, 'hash'] = tx_hash
            df.loc[token_id, 'block_num'] = evt_block_num
    df.block_num = df.block_num.astype(int)
    return df


def get_all_claim_events(block_num):
    airdrop_claim = '0xCA830F6d34D03c07b2A79021186C2eE4E0A3Da58'
    _, event_filter_params = construct_event_filter_params(
        AIRDROP_EVENT_ABI,
        W3.codec,
        address=airdrop_claim,
        argument_filters={'address': airdrop_claim},
        fromBlock=FROM_BLOCK,
        toBlock=block_num
    )
    logs = W3.eth.get_logs(event_filter_params)

    tx_set = set()
    for log in logs:
        evt = get_event_data(W3.codec, AIRDROP_EVENT_ABI, log)
        tx_set.add(evt['transactionHash'].hex())

    return tx_set


def get_amounts(df_creation):
    vechr = '0x9A01857f33aa382b1d5bb96C3180347862432B0d'
    vechr_contract = W3.eth.contract(vechr, abi=VECHR_ABI)

    for token_id, row in df_creation.iterrows():
        token_block_num = int(row.block_num)
        owner = vechr_contract.functions.ownerOf(token_id).call(block_identifier=token_block_num)
        balance = vechr_contract.functions.balanceOfNFT(token_id).call(block_identifier=token_block_num)
        df_creation.loc[token_id, 'owner'] = owner
        df_creation.loc[token_id, 'balance'] = balance


def main():
    # get block time from etherscan API:
    snap_time = dt.datetime(2023, 6, 1).replace(tzinfo=dt.timezone.utc).timestamp()
    response = requests.get('https://api.arbiscan.io/api',
                            params = {'module': 'block',
                                      'action': 'getblocknobytime',
                                      'timestamp': int(snap_time),
                                      'closest': 'before',
                                      'apikey': ETHERSCAN_API_KEY})
    response_json = json.loads(response.content)
    block_num = int(response_json['result'])

    # get (non-airdrop) veCHR creation events:
    df_creation = get_all_creation_events(block_num)
    airdropped_tx = get_all_claim_events(block_num)
    df_creation = df_creation[~df_creation.hash.isin(airdropped_tx)]

    # enrich with amount:
    get_amounts(df_creation)
    df_creation.groupby('owner')[['balance']].sum().to_clipboard()
