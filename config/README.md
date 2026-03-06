# Network Configuration

SliceStream defaults to **CKB Testnet**.

## Default
- `network = "testnet"`
- `ckb_address_prefix = "ckt"`

## Mainnet-ready switch
To prepare for mainnet configuration, set:
- `network = "mainnet"`
- `mainnet_ready = true`
- use `mainnet_address_prefix = "ckb"`

> Runtime and examples should stay on testnet unless an explicit deployment change is made.
