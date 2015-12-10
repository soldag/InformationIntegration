import argparse

from cinemalytics import CinemalyticsMapper


parser = argparse.ArgumentParser(description="This program imports GND dumps into a local PostgreSQL instance.")
parser.add_argument("--dbhost", "-a", help="Host of the database instance.", default="localhost")
parser.add_argument("--dbport", help="Port of the database instance", default=5432)
parser.add_argument("--dbuser", "-u", help="Username for accessing the database.", default='postgres')
parser.add_argument("--dbpwd", "-p", help="Password for accessing the database.", default='')
parser.add_argument("--cmdb", help="Name of the cinemalytics database.")
parser.add_argument("--dstdb", help="Name of the destination database.", required=True)
args = parser.parse_args()

if args.cmdb:
    print "Integrate cinemalytics..."
    mapper = CinemalyticsMapper(args.dbhost, args.dbport, args.dbuser, args.dbpwd, args.cmdb, args.dstdb)
    mapper.map()
    mapper.close()
    print
