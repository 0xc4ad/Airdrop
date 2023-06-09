import os
import requests
import json
import pandas as pd
from web3 import Web3
from web3.middleware import geth_poa_middleware
from dotenv import load_dotenv
from ratelimiter import RateLimiter

# Load environment variables
load_dotenv()
API_KEY = os.getenv('COVALENT_API_KEY')

# Define file names
holders_filename = 'holders.json'
airdrop_filename = 'airdrop_data.json'
intermediary_filename = 'intermediary_data.json'

# Check if holders data exists
if os.path.exists(holders_filename):
    # Load holders data from file
    with open(holders_filename, 'r') as file:
        holders = json.load(file)
    print(f"Loaded holders data from {holders_filename}")
else:
    # Fetch holders data from Covalent API
    holders = []
    page_number = 0
    while True:
        response = requests.get(
            'https://api.covalenthq.com/v1/137/tokens/0xDc7ee66c43f35aC8C1d12Df90e61f05fbc2cd2c1/token_holders/',
            params={
                'block-height': 43386770,
                'page-number': page_number,
                'format': 'JSON',
                'key': API_KEY
            }
        )
        data = response.json()
        holders.extend(data['data']['items'])
        if not data['data']['pagination']['has_more']:
            break
        page_number += 1
    # Save holders data to file
    with open(holders_filename, 'w') as file:
        json.dump(holders, file)
    print(f"Saved holders data to {holders_filename}")

# Connect to Polygon Network
w3 = Web3(Web3.HTTPProvider('https://polygon-rpc.com'))
w3.middleware_onion.inject(geth_poa_middleware, layer=0)

# ABI for NFT contract
ABI = [
    {
        "constant": True,
        "inputs": [
            {"name": "owner", "type": "address"}
        ],
        "name": "balanceOf",
        "outputs": [
            {"name": "", "type": "uint256"}
        ],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [
            {"name": "owner", "type": "address"},
            {"name": "index", "type": "uint256"}
        ],
        "name": "tokenOfOwnerByIndex",
        "outputs": [
            {"name": "", "type": "uint256"}
        ],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [
            {"name": "tokenId", "type": "uint256"}
        ],
        "name": "locks",
        "outputs": [
            {"name": "startTime", "type": "uint256"},
            {"name": "endTime", "type": "uint256"},
            {"name": "lockedAmount", "type": "uint256"},
            {"name": "multiplier", "type": "uint256"},
            {"name": "claimed", "type": "uint256"},
            {"name": "maxPayout", "type": "uint256"}
        ],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    }
]

# Define the contract
contract = w3.eth.contract(address=w3.to_checksum_address('0xDc7ee66c43f35aC8C1d12Df90e61f05fbc2cd2c1'), abi=ABI)

# Define the addresses to exclude
addresses_to_exclude = [
    "0x3d41487a3c5662ede90d0ee8854f3cc59e8d66ad",
    "0xaf8a1548fd69a59ce6a2a5f308bcc4698e1db2e5",
    "0x100fcc635acf0c22dcdcef49dd93ca94e55f0c71",
    "0x2c9cb092625d9be2c359af155f0a2f95ef58514e",
    "0x43e656716cF49C008435A8196d8f825f66f37254",
    "0x65b86882D5bCf9AD1C3F8Fd09C92F834f92E32B4",
    "0xd214c3b23203C02C356D14c0FB655da5682d0c26",
    "0x88a07267495a51D0d19b86b184F974590A834F61",
    "0x51F6B11f8D46DD6843C2b7a8Dc5252236E39fad6",
    "0xF5BDe94E98E20ade36FB720E8A0f8b5394bcEc93",
    "0xe6a1EDC64C7860f5B20b56eb89F16b279D0972e1",
    "0x29613FbD3e695a669C647597CEFd60bA255cc1F8",
    "0x0000000000000000000000000000000000000000"
]

# Convert to checksum addresses
addresses_to_exclude = [w3.to_checksum_address(address) for address in addresses_to_exclude]

# Check if intermediary data exists
if os.path.exists(intermediary_filename):
    # Load intermediary data from file
    with open(intermediary_filename, 'r') as file:
        airdrop_data, totalValue, start_index = json.load(file)
    print(f"Loaded intermediary data from {intermediary_filename}")
else:
    # Initialize intermediary data
    airdrop_data = []
    totalValue = 0
    start_index = 0

# Fetch airdrop data from blockchain
rate_limiter = RateLimiter(max_calls=5, period=1)  # 5 requests per second
for i, holder in enumerate(holders[start_index:], start=start_index):
    print(i)
    address = w3.to_checksum_address(holder['address'])
    if address not in addresses_to_exclude:
        with rate_limiter:
            balance = contract.functions.balanceOf(address).call(block_identifier=43386770)
            for index in range(balance):
                tokenId = contract.functions.tokenOfOwnerByIndex(address, index).call(block_identifier=43386770)
                lock = contract.functions.locks(tokenId).call(block_identifier=43386770)
                value = lock[2] + lock[5]  # lockedAmount + maxPayout
                totalValue += value
                airdrop_data.append((address, value))
            if i % 10 == 0:
                # Save intermediary data to file
                with open(intermediary_filename, 'w') as file:
                    json.dump((airdrop_data, totalValue, i), file)
                print(f"Processed {i} holders out of {len(holders)}. Intermediary data saved to {intermediary_filename}")

# Save final airdrop data to file
with open(airdrop_filename, 'w') as file:
    json.dump(airdrop_data, file)
print(f"Saved airdrop data to {airdrop_filename}")

# Calculate airdrop amounts
airdrop_amounts = [(address, value * 5000000000000000000000000 // totalValue) for address, value in airdrop_data]

# Group by address
address_to_amount = {}
for address, amount in airdrop_amounts:
    if address in address_to_amount:
        address_to_amount[address] += amount
    else:
        address_to_amount[address] = amount

# Sort by amount and write to CSV
dataframe = pd.DataFrame(address_to_amount.items(), columns=['ADDRESS', 'AMOUNT'])
dataframe['CHAIN'] = 'polygon'
dataframe = dataframe[['CHAIN', 'ADDRESS', 'AMOUNT']]
dataframe.sort_values(by='AMOUNT', ascending=False, inplace=True)
dataframe.to_csv('33.csv', index=False)
print('Saved airdrop amounts to 33.csv')
