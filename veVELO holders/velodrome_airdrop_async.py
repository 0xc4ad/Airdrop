from web3 import AsyncWeb3, AsyncHTTPProvider
import json
import datetime as dt
import requests
from collections import defaultdict
import asyncio
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()
ETHERSCAN_API_KEY = os.environ['OPT_ETHERSCAN_API']
ALCHEMY_URL = os.environ['ALCHEMY_URL_OPT']

global_lock = asyncio.Lock()
w3 = AsyncWeb3(AsyncHTTPProvider(ALCHEMY_URL))

# Voting Escrow contract:
abi = [
    {
        'inputs': [
            {
                'internalType': 'uint256',
                'name': '_tokenId',
                'type': 'uint256'},
            {
                'internalType': 'uint256',
                'name': '_block',
                'type': 'uint256'}],
        'name': 'balanceOfAtNFT',
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
                'internalType': 'uint256',
                'name': '_tokenId',
                'type': 'uint256'}],
        'name': 'ownerOf',
        'outputs': [{
            'internalType': 'address',
            'name': '',
            'type': 'address'}],
        'stateMutability': 'view',
        'type': 'function'}]
venft = '0x9c7305eb78a432ced5C4D14Cac27E8Ed569A2e26'
venft_contract = w3.eth.contract(venft, abi=abi)

max_token_id = 26500
token_id_generator = (x for x in range(max_token_id))
snap_time = dt.datetime(2023, 6, 1).replace(tzinfo=dt.timezone.utc).timestamp()

# get block time from etherscan API:
response = requests.get('https://api-optimistic.etherscan.io/api',
                        params = {'module': 'block',
                                  'action': 'getblocknobytime',
                                  'timestamp': int(snap_time),
                                  'closest': 'before',
                                  'apikey': ETHERSCAN_API_KEY})
response_json = json.loads(response.content)
block_num = int(response_json['result'])

null_addr = '0x0000000000000000000000000000000000000000'
voter_balances = defaultdict(int)


async def get_voter_balance(thread_id):
    while True:
        async with global_lock:
            try:
                token_id = next(token_id_generator)
            except:
                return

        while True:
            try:
                print(f'token_id {token_id} thread {thread_id}')
                voter_address = await venft_contract.functions.ownerOf(token_id).call(block_identifier=block_num)
                if voter_address == null_addr:
                    voter_vevelo = 0
                else:
                    voter_vevelo = await venft_contract.functions.balanceOfAtNFT(token_id, block_num).call()
                break
            except Exception as e:
                print(f'exception: {e}')
                asyncio.sleep(1)

        async with global_lock:
            voter_balances[voter_address] += voter_vevelo


async def main():
    task_list = []
    for thread_id in range(6):
        task_list.append(asyncio.create_task(get_voter_balance(thread_id)))
    for task in task_list:
        await task

    df = pd.DataFrame.from_dict(voter_balances, orient='index', columns=['voting_power'])
    df.sort_values('voting_power', ascending=False)
    df.index.name = 'address'
    df.to_csv('velodrome_voters.csv')
    print('file written.')


asyncio.run(main())
