import os.path
import logging
import argparse

from gndextractor.DatabaseWriter import DatabaseWriter
from gndextractor.GndExtractor import GndExtractor
from gndextractor.dbschema import db_schema


if __name__ == "__main__":
    # Delete old and create log
    log_file_name = "gnd.log"
    if os.path.isfile(log_file_name):
        os.remove(log_file_name)
    logging.basicConfig(filename=log_file_name, level=logging.DEBUG)

    # Parse arguments
    parser = argparse.ArgumentParser(description="This program imports GND dumps into a local PostgreSQL instance.")
    parser.add_argument("--tp-dump", "-tp", help="GND dump file containing information about persons.")
    parser.add_argument("--tu-dump", "-tu", help="GND dump file containing information about literary works.")
    parser.add_argument("--tg-dump", "-tg", help="GND dump file containing information about geographical places.")
    parser.add_argument("--dbhost", "-a", help="Host of the database instance.", default="localhost")
    parser.add_argument("--dbport", help="Port of the database instance", default=5432)
    parser.add_argument("--dbname", "-n", help="Name of the database.", required=True)
    parser.add_argument("--dbuser", "-u", help="Username for accessing the database.", required=True)
    parser.add_argument("--dbpwd", "-p", help="Password for accessing the database.", required=True)
    parser.add_argument("--clean", "-c", help="Delete and overwrite existing information.", action="store_true")
    parser.add_argument("--quiet", "-q", help="suppress output", action="store_true")
    args = parser.parse_args()

    # Connect to database
    db_writer = DatabaseWriter(args.dbhost, args.dbport, args.dbname, db_schema, args.quiet)
    db_writer.connect(args.dbuser, args.dbpwd)
    if args.clean:
        db_writer.drop_tables()
    db_writer.create_tables()

    # Run extraction
    extractor = GndExtractor(db_writer, args.quiet)
    if args.tp_dump:
        with open(args.tp_dump, "rb") as dump_file:
            extractor.process_dump("tp", dump_file)

    if args.tu_dump:
        with open(args.tu_dump, "rb") as dump_file:
            extractor.process_dump("tu", dump_file)

    if args.tg_dump:
        with open(args.tg_dump, "rb") as dump_file:
            extractor.process_dump("tg", dump_file)

    # Add foreign keys
    db_writer.add_foreign_keys()

    # Close database connection
    db_writer.disconnect()
