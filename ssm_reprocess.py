from multiprocessing import Pool
from numbers import Number
import requests,json

# Script Config
DEBUG = False
POOL_SIZE = 1 if DEBUG else 100
INPUT_FILENAME = 'shipment_ids_light.txt'

# Rest Config
TIMEOUT = 150
HEADERS = {
    "Content-Type": "application/json",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "X-Tiger-Token":"Bearer tuToken"
}

#----------------------- generic requests --------------------------------------------------------

def generic_post(url, data, headers):
    try:
        response = requests.post(url=url, data=data, headers=headers, timeout=TIMEOUT)
        if response.status_code == 200:
            return response.json()
        else:
            errorName = "error_1_calling_" + url
            return  {"error": errorName}
    except requests.exceptions.RequestException as e:
        errorName = "error_2_calling_" + url
        return  {"error": errorName}

#----------------------- specific requests --------------------------------------------------------

def post_ssm_message(shipment_id):
    data = json.dumps({
        "msg":{
            "shipment_id": int(shipment_id),
            "status":"delivered"
        }
    })
    url ='http://prod-ENDPOINT'
    return generic_post(url, data, HEADERS) 

def get_shipments_ids():
    ids = []
    with open(INPUT_FILENAME, 'r') as archivo:
        for line in archivo:
            if line.startswith('#'):
                continue
            if len(line) < 3:
                break
            request_id = line.strip()
            ids.append(request_id)
    return ids

#----------------------- main code --------------------------------------------------------

def check(shipment_id):
    try:
        # send ssm msg
        messageRep = post_ssm_message(shipment_id)
        if messageRep is None:
            print(f"{shipment_id},PROCESADO")
        elif 'error' in messageRep:
            raise ValueError(f'UNPROCESSED_ERROR_POST_SSM_MESSAGE - {messageRep}')
        
    except ValueError as error:
        print(f"{shipment_id},{error}")

#--------------------------------------------------------------------------------------------------

def main():
    print("START")
    print(f"SHIPMENT_ID,RESULT")
    shipments_ids = get_shipments_ids()
    with Pool(POOL_SIZE) as p:
        results = p.map(check, shipments_ids)
    print("END")

if __name__ == '__main__':
    main()