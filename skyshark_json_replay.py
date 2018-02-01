#!/usr/bin/env python
# vim: tabstop=4:softtabstop=4:shiftwidth=4:expandtab:

import json
from time import sleep
import logging
import bz2
import gzip
import argparse
import socket

def log_config(lvl):
    logging_format = '%(levelname)s: %(message)s'
    if lvl > 1:
        logging.basicConfig(format=logging_format, level=logging.DEBUG)
    elif lvl > 0:
        logging.basicConfig(format=logging_format, level=logging.INFO)
    else:
        logging.basicConfig(format=logging_format, level=logging.WARN)

def main():
    descr = 'replay acarsdec JSON log'
    parser = argparse.ArgumentParser(description=descr, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-f', '--file',    dest='file', type=str,   metavar='FILE', default='acarsdec.json', help='file to replay')
    parser.add_argument('-d', '--dest',    dest='dest', type=str,   metavar='IP', default='localhost', help='destination host')
    parser.add_argument('-p', '--port',    dest='port', type=int,   metavar='PORT', default=5555, help='destination port')
    parser.add_argument('-r', '--rate',    dest='rate', type=float, metavar='RATE', default=1, help='scale replay rate')
    parser.add_argument('-q', '--quick',   dest='quick', action='store_true', default=False, help='scale replay rate')
    parser.add_argument('-v', '--verbose', dest='verbose', action='count', default=0, help='increase verbosity')
    args = parser.parse_args()

    log_config(args.verbose)

    ip = socket.gethostbyname_ex(args.dest)[-1][0]
    dest = (ip, args.port)
    logging.info("sending to %s:%d (%s)", ip, args.port, args.dest)
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    
    if args.file.lower().endswith('.bz2'):
        fd = bz2.BZ2File(args.file, 'r')
    elif args.file.lower().endswith('.gz'):
        fd = gzip.open(args.file)
    else:
        fd = open(args.file, 'rU')
    
    line = fd.readline()
    if args.quick is False:
        parsed = json.loads(line)
        last_time = float(parsed['timestamp'])

    while True:
        if line:
            s.sendto(line, dest)
            logging.debug("%s", line.strip())
        else:
            logging.info("EOF")
            exit(0)

        line = fd.readline()

        if args.quick is False:
            try:
                parsed = json.loads(line)
            except ValueError:
                continue

            next_time = float(parsed['timestamp'])
            sleep_time = (next_time - last_time) / args.rate
            last_time = next_time
            logging.info("sleep %fs", sleep_time)
            try:
                sleep(sleep_time)
            except IOError:
                # sometimes sleep_time is negative depending on what was in
                # in the logfile. Ignore the exception that time.sleep would raise
                pass

if __name__ == '__main__':
    main()
