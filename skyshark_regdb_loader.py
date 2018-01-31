#!/usr/bin/env python
# vim: tabstop=4:softtabstop=4:shiftwidth=4:expandtab:

import pymongo
import csv
import sys
import os
import logging
import arrow
import config

def log_config(lvl):
    logging_format = '%(levelname)s: %(message)s'
    if lvl > 1:
        logging.basicConfig(format=logging_format, level=logging.DEBUG)
    elif lvl > 0:
        logging.basicConfig(format=logging_format, level=logging.INFO)
    else:
        logging.basicConfig(format=logging_format, level=logging.WARN)

def clean_record(rec, table):
    rec.pop('_junk_', None)

    for key in rec.keys():
        tmp = rec[key]
        tmp = tmp.strip()

        if tmp is None or len(tmp) == 0:
            rec.pop(key, None)
            continue

        # boolean conversions
        if table == 'DEALER' and key == 'EXPIRATION_FLAG':
            rec[key] = True if tmp == '*' else False
            continue

        # int conversions
        if ((table == 'DOCINDEX' and key in ['TYPE_COLLATERAL']) or
            (table == 'ENGINE' and key in ['CODE', 'THRUST', 'HORSEPOWER', 'TYPE']) or
            (table == 'ACFTREF' and key in ['AC_CAT', 'BUILD_CERT_IND', 'SPEED', 'NO_ENG', 'NO_SEATS', 'TYPE_ENG', 'TYPE_ACFT']) or 
            (table == 'DEALER' and key in ['OWNERSHIP', 'CERTIFICATE_ISSUE_COUNT']) or 
            (table == 'DEREG' and key in ['INDICATOR_GROUP', 'MODE_S_CODE', 'ENG_MFR_MDL', 'MFR_MDL_CODE', 'YEAR_MFR', ]) or 
            (table == 'MASTER' and key in ['YEAR_MFR', 'MODE_S_CODE', 'ENG_MFR_MDL', 'TYPE_AIRCRAFT', 'TYPE_ENGINE', 'TYPE_REGISTRANT'])):
                try:
                    rec[key] = int(tmp, 10)
                except ValueError:
                    pass
                continue

        # date conversions
        if ((table == 'RESERVED' and key in ['RSV_DATE', 'EXP_DATE']) or
            (table == 'DOCINDEX' and key in ['PROCESSING_DATE', 'DRDATE', 'CORR_DATE' ]) or
            (table == 'DEALER' and key in ['EXPIRATION_DATE', 'CERTIFICATE_DATE']) or
            (table == 'DEREG' and key in ['INDICATOR_GROUP', 'ENG_MFR_MDL', 'YEAR_MFR', 'CANCEL_DATE', 'CERT_ISSUE_DATE', 'AIR_WORTH_DATE', 'LAST_ACT_DATE']) or
            (table == 'MASTER' and key in ['LAST_ACTION_DATE', 'EXPIRATION_DATE', 'CERT_ISSUE_DATE', 'AIR_WORTH_DATE' ])):
                try:    
                    rec[key] = arrow.get(tmp, "YYYYMMDD").datetime
                except arrow.parser.ParserError:
                    pass
                continue
        rec[key] = tmp

    # Whole Record Munging
    if table == 'DEALER':
        n = rec.pop('OTHER_NAMES_COUNT', 0)
        other_names = []
        aliases = filter(lambda x: x.startswith('OTHER_NAMES_'), rec.keys())
        for alias in aliases:
            tmp = rec.pop(alias, '').strip()
            if tmp:
                other_names.append(tmp)

        if len(other_names):
            rec['OTHER_NAMES'] = other_names
        rec['_id'] = rec['CERTIFICATE_NUMBER']

    if table == 'MASTER':
        rec['_id'] = rec['UNIQUE_ID']

    if table == 'ACFTREF':
        rec['_id'] = rec['CODE']

    if table == 'ENGINE':
        rec['_id'] = rec['CODE']

    if table == 'RESERVED':
        rec['_id'] = rec['N_NUMBER']


def fix_field_names(cvr):
    if cvr.fieldnames[-1] == '':
        cvr.fieldnames[-1] = '_junk_';
    cvr.fieldnames = map(lambda x: x.strip().replace(' ','_').replace('-','_').replace('(','').replace(')',''), cvr.fieldnames)

def dbConnect(db=None, check_index=True):
    '''connect to database, and optionally verify the indexes
    If `db` is unspecified, read the mongodb url from `mongo_url_faa.txt`
    '''
    if db is None:
        db = 'mongodb://localhost:27017/'

    mc = pymongo.MongoClient(db)
    dbh = mc['skyshark']

    if check_index is True:
        logging.info("checking indexes")
        dbh['ENGINE'].create_index('CODE', unique=True)

        dbh['RESERVED'].create_index('N_NUMBER', unique=True)
        dbh['RESERVED'].create_index('RSV_DATE')
        dbh['RESERVED'].create_index('TR')

        dbh['DEALER'].create_index('CERTIFICATE_NUMBER', unique=True)
        dbh['DEALER'].create_index('CERTIFICATE_ISSUE_COUNT')

        dbh['DEREG'].create_index('N_NUMBER')
        dbh['DEREG'].create_index('MODE_S_CODE_HEX')
        dbh['DEREG'].create_index( [('N_NUMBER',1), ('SERIAL_NUMBER',1), ('YEAR_MFR',1), ('MFR_MDL_CODE',1), ('ENG_MFR_MDL',1), ('STATUS_CODE',1), ('LAST_ACT_DATE',1), ('CERT_ISSUE_DATE',1), ('CANCEL_DATE',1), ], unique=True, name="deduper")

        dbh['ACFTREF'].create_index('CODE', unique=True)
        dbh['ACFTREF'].create_index( [('MFR',1), ('MODEL',1)])

        dbh['DOCINDEX'].create_index('DOC_ID')
        dbh['DOCINDEX'].create_index('DRDATE')
        dbh['DOCINDEX'].create_index('PARTY')
        dbh['DOCINDEX'].create_index('COLLATERAL')

        dbh['MASTER'].create_index('N_NUMBER', unique=True)
        dbh['MASTER'].create_index('MODE_S_CODE_HEX')
        dbh['MASTER'].create_index('UNIQUE_ID', unique=True)
        dbh['MASTER'].create_index('ENG_MFR_MDL')
        dbh['MASTER'].create_index('MFR_MDL_CODE')
        dbh['MASTER'].create_index('TYPE_ENGINE')
        dbh['MASTER'].create_index('TYPE_AIRCRAFT')
        dbh['MASTER'].create_index('EXPIRATION_DATE')
        dbh['MASTER'].create_index('CERT_ISSUE_DATE')
        dbh['MASTER'].create_index('AIR_WORTH_DATE')
        dbh['MASTER'].create_index('LAST_ACTION_DATE')
        dbh['MASTER'].create_index('YEAR_MFR')

    #logging.debug(str(dbh.command('dbStats')))
    return dbh

def main():
    log_config(2)

    dbh = dbConnect(config.mongo_url)

    if len(sys.argv) != 2:
        logging.fatal("Usage: %s <datadir>", sys.argv[0])
        sys.exit(1)

    d = sys.argv[1]
    if not os.path.isdir(d):
        logging.fatal('"%s" is not a directory', d)
        sys.exit(1)

    files = sorted(os.listdir(d))
    for f in files:
        if not f.endswith('.txt'):
            continue

        with open(os.path.join(d, f)) as fd:
            logging.info("loading %s", f)
            table = f.replace('.txt', '')

            # byte-order mark. There's gotta be a better way...
            _ = fd.read(3)

            reader = csv.DictReader(fd)
            fix_field_names(reader)
            for row in reader:
                clean_record(row, table)
                try:
                    dbh[table].insert(row)
                except pymongo.errors.DuplicateKeyError:
                    pass



if __name__ == '__main__':
    main()
