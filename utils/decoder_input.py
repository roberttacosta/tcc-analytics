from eth_abi import decode_abi
from hexbytes import HexBytes


def decode_flash_loan_transaction(input_data):
    method_map = {
        "0x5cffe9de": "flashLoanSimple",
        "0xab9c4b5d": "flashLoan"
    }

    # Verificar o Method ID
    method_id = input_data[:10]
    if method_id not in method_map:
        raise ValueError(f"Method ID não suportado: {method_id}")

    method = method_map[method_id]
    input_data = input_data[10:]
    input_bytes = HexBytes(input_data)

    if method == "flashLoan":
        # Verificar se o tamanho é suficiente para `flashLoan`
        if len(input_bytes) < 32 * 7:
            raise ValueError("Input insuficiente para flashLoan")

        decoded = decode_abi(
            ['address', 'address[]', 'uint256[]', 'uint256[]', 'address', 'bytes', 'uint16'],
            input_bytes
        )

        # Extraindo os dados
        receiver_address = decoded[0]
        assets = decoded[1]
        amounts = decoded[2]
        return {
            "method": method,
            "receiver_address": receiver_address,
            "assets": assets,
            "amounts": amounts
        }

    elif method == "flashLoanSimple":
        # Verificar se o tamanho é suficiente para `flashLoanSimple`
        if len(input_bytes) < 32 * 5:
            raise ValueError("Input insuficiente para flashLoanSimple")

        decoded = decode_abi(
            ['address', 'address', 'uint256', 'bytes', 'uint16'],
            input_bytes
        )

        # Extraindo os dados
        receiver_address = decoded[0]
        asset = decoded[1]
        amount = decoded[2]
        return {
            "method": method,
            "receiver_address": receiver_address,
            "asset": asset,
            "amount": amount
        }
