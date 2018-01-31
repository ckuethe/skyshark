#!/usr/bin/env python
# vim: tabstop=4:softtabstop=4:shiftwidth=4:expandtab:

import re
import csv
from dateutil.parser import parse as dateparser
from dateutil.tz import tzlocal
import pymongo
import logging
import bz2
import gzip
import argparse
import socket
import cPickle
import os
import config

# http://woodair.net/SBS/Article/Barebones42_Socket_Data.htm
# https://github.com/wiseman/node-sbs1
fields=['message_type', 'transmission_type', 'session_id', 'aircraft_id',
        'icao24', 'flight_id', 'gen_date', 'gen_time', 'log_date', 'log_time',
        'callsign', 'altitude', 'ground_speed', 'track', 'lat', 'lon',
        'vertical_rate', 'squawk', 'alert', 'emergency', 'spi', 'is_on_ground']

msg_types = {1:'ES_IDENT_AND_CATEGORY', 2: 'ES_SURFACE_POS', 3:'ES_AIRBORNE_POS',
        4:'ES_AIRBORNE_VEL', 5:'SURVEILLANCE_ALT', 6:'SURVEILLANCE_ID',
        7:'AIR_TO_AIR', 8:'ALL_CALL_REPLY'}

def timefix(date_or_datetime_str, time_str=''):
    '''convert date and time formats into a datetime()'''
    return dateparser(date_or_datetime_str + ' ' + time_str).replace(tzinfo=tzlocal())

def resolve_icao(icao_cache_dict, message):
    '''inject the callsign into a position report'''
    icao = message['icao24'].strip().upper()
    if icao in icao_cache_dict: # and (icao_cache_dict[icao]['lastseen'] > icao_cache_dict[icao]['firstseen']):
        message['callsign'] = icao_cache_dict[icao]['callsign']

def process_position(message, dbh):
    rv = { 'icao24': message['icao24']}
    if len(message['squawk']):
        rv['squawk'] = int(message['squawk'])
    
    for field in ['alert', 'emergency', 'spi', 'is_on_ground']:
        rv[field] = False if message[field] in ['', '0', 0] else True

    if len(message['altitude']):
        rv['altitude'] = float(message['altitude'])
        
    if len(message['lat']) and len(message['lon']):
        rv['loc'] = {'type':'Point', 'coordinates': [float(message['lon']),float(message['lat'])]}
    
    rv['timestamp'] = timefix(message['gen_date'], message['gen_time'])
    rv['callsign'] = message['callsign'] # populated by resolve_icao() 

    selector = {'icao24': rv['icao24'], 'timestamp': rv['timestamp']}
    dbh['adsb_positions'].update(selector, rv, upsert=True)
    return rv

def process_ident(icao_cache_dict, dbh, line):
    '''stash the ident message into the ident cache for later use by the position reporter'''
    # FIXME: data structure should have time ranges bounding on the translations, i.e.
    # ICAO24 A2B3C4 might arrive as UAL712 and 2 hours later turn around as UAL355
    # UAL355 might be served by A2B3C4 today and A2B9C8 tomorrow
    if line['transmission_type'] != '1':
        return None
    
    icao24 = line['icao24'].strip().upper()
    message_time = timefix(line['gen_date'], line['gen_time'])
    callsign = line['callsign'].strip().upper() # or '*NONE*'
    if len(callsign) == 0 or re.search('[^0-9A-Z]', callsign):
        return None #reject line noise
    
    if icao24 in icao_cache_dict and icao_cache_dict[icao24]['callsign'] == callsign:
        # only update the lastseen time if we've seen this mapping before, thus
        # we don't stash a single corrupt mapping (or the mapping needs to be
        # corrupted the same way twice in a row)
        icao_cache_dict[icao24]['lastseen'] = message_time
        if 'idents' not in icao_cache_dict[icao24]:
            icao_cache_dict[icao24]['idents'] = 0
        icao_cache_dict[icao24]['idents'] = icao_cache_dict[icao24]['idents'] + 1
    else:
        icao_cache_dict[icao24] = {
            'icao24': icao24,
            'callsign': callsign,
            'lastseen': message_time,
            'firstseen':message_time,
            'idents': 1}
    
    selector = {'icao24': icao24, 'callsign': callsign }
    dbh['adsb_ident'].update(selector, {'$set': icao_cache_dict[icao24]}, upsert=True)
    return "{0} => {1}".format(icao24, callsign)

def handle_line(icao_cache, dbh, message):
    if message['transmission_type'] == '1':
        process_ident(icao_cache, dbh, message)
    elif message['transmission_type'] in ['2', '3']:
        resolve_icao(icao_cache, message)
        process_position(message, dbh)
    else:
        pass

def dbConnect(db='mongodb://localhost:27017/', check_index=True):
    '''connect to database, and optionally verify the indexes'''
    mc = pymongo.MongoClient(db)
    dbh = mc['skyshark']
    _ = dbh.command('dbStats') # explode if auth was wrong :)

    if check_index is True:
        logging.debug("checking indexes")
        dbh['adsb_positions'].create_index('icao24')
        dbh['adsb_positions'].create_index('squawk')
        dbh['adsb_positions'].create_index('callsign')
        dbh['adsb_positions'].create_index('timestamp')
        dbh['adsb_positions'].create_index([('loc', pymongo.GEOSPHERE)])
        dbh['adsb_positions'].create_index([('loc', pymongo.GEOSPHERE), ('altitude', 1)])
        dbh['adsb_positions'].create_index('altitude')
        dbh['adsb_ident'].create_index('icao24')
        dbh['adsb_ident'].create_index('callsign')
        dbh['adsb_ident'].create_index([('icao24', 1), ('callsign', 1)], unique=True)

    return dbh

def log_config(lvl):
    logging_format = '%(levelname)s: %(message)s'
    if lvl > 1:
        logging.basicConfig(format=logging_format, level=logging.DEBUG)
    elif lvl > 0:
        logging.basicConfig(format=logging_format, level=logging.INFO)
    else:
        logging.basicConfig(format=logging_format, level=logging.WARN)

def do_argparse():
    '''parse arguments and flags to the program'''
    descr = 'load SBS1 logs into a mongodb instance'

    parser = argparse.ArgumentParser(description=descr, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-c', '--cache', dest='cache', metavar='FILE', default=None, help='used as the pickle of persistent ICAO mappings')
    parser.add_argument('-s', '--sbs', dest='server', metavar='SERVER', default='localhost', help='SBS1 server for streaming live results. Not used if files given.')
    parser.add_argument('-p', '--port', dest='port', metavar='PORT', default=30003, help='SBS1 port')
    parser.add_argument('-m', '--mongodb', dest='db', metavar='MONGO', default=None, help='MongoDB server url')
    parser.add_argument('-v', '--verbose', dest='verbose', action='count', default=0, help='increase verbosity')
    parser.add_argument(dest='files', metavar='FILE', nargs='*', help='If specified, load data from files rather than live streaming')
    args = parser.parse_args()
    return args

def do_network_io(icao_cache, dbh, args):
    c = (args.server, args.port)
    logging.debug("connecting to %s:%d", args.server, args.port)
    fd = socket.create_connection(c).makefile('r')
    reader = csv.DictReader(fd,fields)
    try:
        for line in reader:
            logging.debug("%s", line)
            handle_line(icao_cache, dbh, line)
    except KeyboardInterrupt:
        logging.info("saving cache")
        save_icao_cache(args, icao_cache)

def load_icao_cache(args):
    if args.cache is None:
        return {}

    try:
        with open(args.cache, 'r') as fd:
            c = cPickle.load(fd)
            if not isinstance(c, dict):
                return {}
            logging.info( "loaded %d entries from icao cache", len(c.keys()) )
            return c

    except (EOFError, IOError):
        return {}

def save_icao_cache(args, icao_cache):
    if args.cache:
        with open(args.cache, 'w') as fd:
            cPickle.dump(icao_cache, fd, 2)
            logging.info( "dumped %d entries to cache", len(icao_cache.keys()))

def open_datafile(f):
    '''Automatically handle compressed files'''
    
    if f.lower().endswith('.bz2'):
        fd = bz2.BZ2File(f, 'r')
    elif f.lower().endswith('.gz'):
        fd = gzip.open(f)
    elif f.lower().endswith('.csv'):
        fd = open(f, 'rU')
    elif f.lower().endswith('.txt'):
        fd = open(f, 'rU')
    else:
        raise ValueError("not sure what to do with file")

    fd.readline() # throw first line away in case it's got junk in it
    reader = csv.DictReader(fd, fields)
    return reader

def do_file_io(icao_cache, dbh, args):
    n = len(args.files)
    m = 0
    for f in args.files:
        f = os.path.realpath(f)
        m += 1
        try:
            logging.info("Processing file: %s (%d/%d)", f, m, n)
            reader = open_datafile(f)
            nr = 0
            if dbh.loaded.find({'_id': f}).count():
                logging.debug("file already loaded")
                continue
            for line in reader:
                try:
                    handle_line(icao_cache, dbh, line)
                except KeyboardInterrupt:
                    raise KeyboardInterrupt()
                except Exception:
                    pass
                    # continue ? abort file?
                nr += 1
                if nr % 50000 == 0:
                    logging.debug("processed %d lines from %s", nr, f)
        except csv.Error: # probably EOF or truncated file. keep calm and carry on
            pass
        dbh.loaded.insert_one({'_id': f})
        logging.debug("completed processing %d lines from %s", nr, f)
        save_icao_cache(args, icao_cache)

def main():
    args = do_argparse()
    log_config(args.verbose)
    if args.db is None:
        args.db = config.mongo_url
    dbh = dbConnect(args.db)

    icao_cache = load_icao_cache(args)

    try: # wrap the IO stuff
        if len(args.files):
            do_file_io(icao_cache, dbh, args)
        else:
            do_network_io(icao_cache, dbh, args)

    except Exception as e:
        logging.debug(str(e))
    
    save_icao_cache(args, icao_cache)

if __name__ == '__main__':
    main()
