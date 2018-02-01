#!/usr/bin/env python
# vim: tabstop=4:softtabstop=4:shiftwidth=4:expandtab:

import pymongo
import logging
import argparse
import socket
import config
import json
import arrow

from expn import *


def dbConnect(db='mongodb://localhost:27017/', check_index=True):
    '''connect to database, and optionally verify the indexes'''
    mc = pymongo.MongoClient(db, connectTimeoutMS=3000, serverSelectionTimeoutMS=3000)
    dbh = mc['skyshark']
    dbh.command('dbStats') # explode if auth was wrong :)
    logging.debug("connected to %s", db)

    if check_index is True:
        logging.debug("checking indexes")
        cols = ['rxfreq', 'country', 'callsign', 'block_id', 'date', 'mode', 'rssi', 'reg',
                'errors', 'tail', 'flight', 'label', 'ack', 'expn', 'icao', 'msgno', 'iata', ]
        for c in cols:
            dbh['acars'].create_index(c)
        dbh['acars'].create_index([ ('coordinates', pymongo.GEOSPHERE) ])
        dbh['acars'].create_index([ ('rxfreq',1), ('date',1), ('msgno',1), ('ack',1), ('block_id',1), ('tail',1), ('flight',1), ('label',1) ], unique=True)

    return dbh


def log_config(lvl):
    logging_format = '%(levelname)s: %(message)s'
    if lvl > 1:
        logging.basicConfig(format=logging_format, level=logging.DEBUG)
    elif lvl > 0:
        logging.basicConfig(format=logging_format, level=logging.INFO)
    else:
        logging.basicConfig(format=logging_format, level=logging.WARN)


def process_acars(msg):
    '''Dispatcher for message parsers, fixups, etc.'''
    #try:
    try:
        if msg['error'] > config.acars_max_errors: # don't even try process excessively errored messages
            return False
    except NameError:
        pass

    try:
        if msg['label'] in config.acars_ignored_labels: # OK, so it's not corrupt, but is it one to drop?
            return False
    except NameError:
        pass

    msg['date'] = arrow.get(msg['timestamp']).datetime # and now we can start doing the hard work with "expensive" functions
    msg['rxfreq'] = msg.pop('freq', 0)

    #except (TypeError, ValueError, KeyError):
    #    return False

    try:
        # strip the dot-padded right-justification. It messes up other database joins
        msg['tail'] = msg['tail'].replace('.','').strip()
    except KeyError:
        pass

    try:
        # strip whitespace here too.
        msg['flight'] = msg['flight'].strip()
    except KeyError:
        pass

    msg['label'] = msg['label'].upper()
    msg['expn'] = arinc620.get(msg['label'], 'unknown_{}'.format(msg['label']))
    if len(msg['expn']) == 1:
        msg['expn'] = msg['expn'][0]
    return True


def main():
    descr = 'load ACARS logs into a mongodb instance'

    parser = argparse.ArgumentParser(description=descr, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-b', '--bind', dest='bind', type=str, metavar='IP', default='localhost', help='hostname or IP address to listen on')
    parser.add_argument('-p', '--port', dest='port', type=int, metavar='PORT', default=5555, help='port to listen on')
    parser.add_argument('-m', '--mongodb', dest='db', metavar='MONGO', default=None, help='MongoDB server url')
    parser.add_argument('-v', '--verbose', dest='verbose', action='count', default=0, help='increase verbosity')
    args = parser.parse_args()

    log_config(args.verbose)
    if args.db is None:
        try:
            args.db = config.mongo_url
        except AttributeError:
            pass
    dbh = dbConnect(args.db)

    ip = socket.gethostbyname_ex(args.bind)[-1][0]
    logging.info("listening on %s:%d (%s)", ip, args.port, args.bind)
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    s.bind((ip, args.port))
    while True:
        try:
            line = s.recv(1024)
            parsed = json.loads(line)
            if process_acars(parsed) is False:
                continue
            logging.debug("%s", parsed)
            dbh.acars.insert_one(parsed)
        except pymongo.errors.DuplicateKeyError:
            pass
        except pymongo.errors.WriteError, e: # What.everrrrrrrr...
            logging.info("MongoDB exception: %s", e)
            logging.info("%s", parsed)
        except KeyboardInterrupt:
            logging.debug("Caught ^C - shutting down" )
            exit(0)
        except ValueError:  # Invalid JSON
            logging.info("PARSE ERROR: %s", line)


if __name__ == '__main__':
    main()
