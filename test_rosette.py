
import argparse
import json
import os

from rosette.api import API, NameDeduplicationParameters, RosetteException


def run(key="b68ed921660dca2fbb79a4a481e931ff", alt_url='https://api.rosette.com/rest/v1/'):
    """ Run the example """
    # Create an API instance
    api = API(user_key=key, service_url=alt_url)

    #name_dedupe_data = "AMERICAN HEYER-SCHULTE CORPORATION,AMERICAN HEYER- SCHULTE CORPORATION,AMERICAN HOECHST CORPORATION,Betty Gable,Norma Shearer,Norm Shearer,Brigitte Helm,Bridget Helem,Judy Holliday,Julie Halliday"
    #name_dedupe_data = "ALADDIN INDUSTRIES,ALADDIN INDUSTRIES LIMITED,ALADDIN INDUSTRIES INCORPORATED,ALFA-LAVAL AB.,AMAX MAGNESIUM,AMAX MAGNESIUM CORPORATION"
    name_dedupe_data = "IBM,I.B.M,ibm,IBM corporate,International Business Machines"
    threshold = 0.7
    params = NameDeduplicationParameters()
    params["names"] = name_dedupe_data.split(',')
    params["threshold"] = threshold
    try:
        return api.name_deduplication(params)
    except RosetteException as exception:
        print(exception)


PARSER = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                 description='Calls the ' +
                                 os.path.splitext(os.path.basename(__file__))[0] + ' endpoint')
PARSER.add_argument('-k', '--key', help='Rosette API Key', default="b68ed921660dca2fbb79a4a481e931ff")
PARSER.add_argument('-u', '--url', help="Alternative API URL",
                    default='https://api.rosette.com/rest/v1/')

if __name__ == '__main__':
    ARGS = PARSER.parse_args()
    RESULT = run(ARGS.key, ARGS.url)
    print(RESULT)
    #print(json.dumps(RESULT, indent=2, ensure_ascii=False, sort_keys=True).encode("utf8"))