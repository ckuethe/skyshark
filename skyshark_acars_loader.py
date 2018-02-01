#!/usr/bin/env python
# vim: tabstop=4:softtabstop=4:shiftwidth=4:expandtab:

import pymongo
import logging
import argparse
import socket
import config
import json


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
    '''Dispatcher for message parsers, fixups, etc. For now this is a no-op'''
    pass


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
            process_acars(parsed)
            logging.debug("%s", parsed)
            dbh.acars.insert_one(parsed)
        except pymongo.errors.DuplicateKeyError:
            pass
        except KeyboardInterrupt:
            logging.debug("Caught ^C - shutting down" )
            exit(0)
        except ValueError:  # Invalid JSON
            logging.info("PARSE ERROR: %s", line)


if __name__ == '__main__':
    main()
