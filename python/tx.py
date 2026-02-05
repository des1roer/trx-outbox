import getopt
import sys
import uuid
import requests

G = "\033[1;32m"
R = "\033[1;31m"
C = "\033[0m"

def main(argv):
    user_id = None
    amount = None
    transaction_id = None

    try:
        opts, args = getopt.getopt(argv, "u:a:i:", ["user=", "amount=", "id="])
    except getopt.GetoptError as err:
        print(str(err))
        print('Usage: tx.py -u <user_id> -a <amount> [-i <transaction_id>]')
        sys.exit(2)

    for opt, arg in opts:
        if opt in ("-u", "--user"):
            user_id = int(arg)
        elif opt in ("-a", "--amount"):
            amount = float(arg)
        elif opt in ("-i", "--id"):
            transaction_id = arg

    if user_id is None or amount is None:
        print('Usage: tx.py -u <user_id> -a <amount> [-i <transaction_id>]')
        sys.exit(2)

    if transaction_id is None:
        transaction_id = str(uuid.uuid4())

    transaction_type = "DEPOSIT" if amount > 0 else "WITHDRAWAL"
    url = "http://localhost:8000/transactions/"
    headers = {"Content-Type": "application/json"}
    payload = {
        "account_id": user_id,
        "amount": "{:0.2f}".format(abs(amount)),
        "transaction_type": transaction_type,
        "external_id": transaction_id
    }

    print(url, payload)

    response = requests.post(url, json=payload, headers=headers)

    if response.status_code == 200:
        print(f"{G}Transaction processed successfully{C}:", response.json())
    else:
        print(f"{R}Failed to process transaction{C}:", response.status_code, response.text)

if __name__ == "__main__":
    main(sys.argv[1:])
