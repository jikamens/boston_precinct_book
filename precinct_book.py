#!/usr/bin/env python3

import argparse
import bz2
import cairo
from collections import defaultdict
import csv
import geojson
import gi
import html
from itertools import chain
import math
import os
import pickle
from pyproj import Transformer
import re
import requests
from shapely.ops import transform
from shapely.geometry import shape
import sys

gi.require_version('Pango', '1.0')
gi.require_version('PangoCairo', '1.0')

from gi.repository import Pango, PangoCairo  # noqa

# Note that in the code and comments below, "poll" is used as a synonym for
# "polling place."

# Because as of 2024-03-02 the majority of the data files we're using here use
# the EPSG:4326 coordinate system, we convert any data files that aren't in
# that coordinate system into it.

# https://data.boston.gov/dataset/polling-locations-20221
#
# A different file path can be specified with --polls-file.
#
# We don't use the X and Y coordinates in this file so the script doesn't
# bother to try to convert them between coordinate systems, but for the record,
# as of 2024-03-02 they appear to be in EPSG:4326.
pollingPlacesFile = 'Polling_Locations_2022.csv'
pollingPlacesSlug = 'polling-locations-20221'

# https://data.boston.gov/dataset/boston-ward-boundaries
# Overridable with --wards-file
#
# As of 2024-03-02, the coordinate system listed in this file is
# "urn:ogc:def:crs:OGC:1.3:CRS84", a.k.a. EPSG:4326.
wardBoundariesFile = 'Boston_Ward_Boundaries.geojson'
wardBoundariesSlug = 'boston-ward-boundaries'

# https://data.boston.gov/dataset/boston-precinct-boundaries
# Overridable with --precincts-file
#
# As of 2024-03-02, the coordinate system listed in this file is EPSG:3857.
# Who knows why they changed it or why it's different from other files on the
# open data site. :shrug:
precinctBoundariesFile = 'Boston_Precinct_Boundaries.geojson'
precinctBoundariesSlug = 'boston-precinct-boundaries'

# https://data.boston.gov/dataset/live-street-address-management-sam-addresses
#
# A different file path can be specified with --addresses-file.
#
# As of 2024-03-02, the coordinate system listed in this file is
# "urn:ogc:def:crs:OGC:1.3:CRS84", a.k.a. EPSG:4326.
addressesFile = 'Live_Street_Address_Management_(SAM)_Addresses.geojson.bz2'
addressesSlug = 'live-street-address-management-sam-addresses'

# A different file path can be specified with --pickle-=file.
pickleFile = 'preprocessed.pickle'

# See --column-rows below.
# Firefox, Linux, save to PDF, 100% scale, no headers and footers
columnRows = 30

# Fixes to data errors I discovered in data downloaded 2023-07-19 which prevent
# the script from functioning properly. There are no guarantees that these are
# the only errors in the data; they're just the ones I noticed.
location2Fixes = {
    (15, 5): ('UP ACADEMY OF DORCHESTER (FORMERLY JOHN MARSHALL ELEMENTARY '
              'SCHOOL)'),
    (18, 4): 'GROVELAND COMMUNITY ROOM',
}
location3Fixes = {
    (11, 9): '20 CHILD STREET',
    (11, 10): '20 CHILD STREET',
    (15, 5): '35 WESTVILLE STREET',
    (22, 12): '95 BEECHCROFT STREET',
}
matchAddrFixes = {
    (7, 10): '530 Columbia Road, Dorchester, Massachusetts, 02125',
    # Not 100% certain about these two
    (12, 2): ('280 Martin Luther King Jr Boulevard, Roxbury, Massachusetts, '
              '02119'),
    (12, 5): ('280 Martin Luther King Jr Boulevard, Roxbury, Massachusetts, '
              '02119'),
    (13, 5): '530 Columbia Road, Dorchester, Massachusetts, 02125',
    (21, 10): '91 Washington St, Brighton, Massachusetts, 02135',
    (21, 11): '91 Washington St, Brighton, Massachusetts, 02135',
}
precinctFixes = {
    # What the heck
    '0502A': '0502',
}
zipCodeFixes = {
    # 644R Adams Street is in 02122, not 02124
    161351: "02122",
}


def main():
    '''Script controller

    Processing steps:

    1. Parse polling places, producing a dict mapping ward/precinct tuples to
       unique poll keys, and a second dict mapping poll keys to names.
    2. Parse addresses, producting a dict mapping unique address keys to
       ward/precinct tuples.
    3. Group ward/precincts by poll, producing a dict mapping poll keys to
       lists of the ward/precincts located at each poll.
    4. Produce a dict mapping address keys to poll keys.
    5. Condense the lists of addresses for each poll (the secret sauce of this
       script!), producing a dict mapping poll keys to the condensed list of
       the addresses and ward/precinct tuples at each poll.
    6. Render the resulting condensed lists as HTML that can be printed from
       the browser to produce the final product.
    '''
    args = parse_args()

    if args.download:
        download(pollingPlacesSlug, 'CSV', args.polls_file)
        download(wardBoundariesSlug, 'GeoJSON', args.wards_file)
        download(precinctBoundariesSlug, 'GeoJSON', args.precincts_file)
        download(addressesSlug, 'GeoJSON', args.addresses_file)
        args.pickle_read = False

    pickleRead = False
    if args.pickle_read:
        try:
            with open(args.pickle_file, 'rb') as f:
                polls, pollNames, addresses, pollGroups, addressPolls = \
                    pickle.load(f)
        except FileNotFoundError:
            pass
        else:
            pickleRead = True
    if pickleRead is False:
        polls, pollNames = readPollingPlaces(args)
        addresses = readAddresses(args)
        pollGroups = groupPollingPlaces(args, polls)
        addressPolls = mapAddresses(args, polls, addresses)
        if args.pickle_write:
            with open(args.pickle_file, 'wb') as f:
                pickle.dump(
                    (polls, pollNames, addresses, pollGroups, addressPolls), f)
    pollAddresses = {
        poll: collapseAddresses(args, poll, addresses, addressPolls)
        for poll in set(addressPolls.values())}
    renderPages(args, pollNames, pollAddresses)


def parse_args():
    parser = argparse.ArgumentParser(
        description='Parse City of Boston polling place and address data and '
        'create sheets for every polling place showing the ward and precinct '
        'for every street address that votes there')
    parser.add_argument('--poll-key', choices=('address', 'location'),
                        default='location', help='What to use as the unique '
                        'key distinguishing polling places in the data '
                        '(default: location)')
    parser.add_argument('--pickle-read', action=argparse.BooleanOptionalAction,
                        default=True, help='Whether to read preprocessed '
                        'data from a pickle file to speed up invocations '
                        '(default: True)')
    parser.add_argument('--pickle-write',
                        action=argparse.BooleanOptionalAction,
                        default=True, help='Whether to write preprocessed '
                        'data into a pickle file to speed up future '
                        'invocations (default: True)')
    parser.add_argument('--pickle-file', action='store', default=pickleFile,
                        help='Pickle file preprocessed data is stored in '
                        f'(default: {pickleFile})')
    parser.add_argument('--polls-file', action='store',
                        default=pollingPlacesFile, help='Path of polling '
                        'places CSV downloaded from data.boston.gov')
    parser.add_argument('--addresses-file', action='store',
                        default=addressesFile, help='Path of SAM addresses '
                        'geojson downloaded from data.boston.gov')
    parser.add_argument('--wards-file', action='store',
                        default=wardBoundariesFile, help='Path of ward '
                        'boundaries geojson downloaded from data.boston.gov')
    parser.add_argument('--precincts-file', action='store',
                        default=precinctBoundariesFile, help='Path of '
                        'precinct boundaries geojson downloaded from '
                        'data.boston.gov')
    parser.add_argument('--column-rows', type=int, action='store',
                        default=columnRows,
                        help='Number of data rows per column for HTML '
                        'rendering, determined empirically by how many rows '
                        'fit when you print from your browser with the '
                        f'desired print settings (default: {columnRows})')
    parser.add_argument('--double-sided', default=True,
                        action=argparse.BooleanOptionalAction,
                        help='Insert extra page breaks to keep each polling '
                        'place on its own sheet of paper when printing '
                        'double-sided (default: True)')
    parser.add_argument('--copies-per-precinct', type=int, help='Repeat each '
                        'sheet the specified number of times for each '
                        'precinct at a polling place. See also '
                        '--copies-per-polling-place.')
    parser.add_argument('--copies-per-polling-place', type=int, help='Repeat '
                        'each sheet the specified number of times for each '
                        'polling place. See also --coipes-per-precinct.')
    parser.add_argument('--print-homogeneous', default=False,
                        action=argparse.BooleanOptionalAction,
                        help='Print sheets for polling places with only '
                        'one precinct (default: False)')
    parser.add_argument('--download', default=False, action='store_true',
                        help='Download new versions of files from '
                        'data.boston.gov. Implies --no-pickle-read because '
                        'if you\'ve just downloaded new versions then you '
                        'should parse them.')
    parser.add_argument('--output-format', choices=('pdf', 'html'),
                        default='pdf', help='Output format')
    parser.add_argument('--output-file', '-o',
                        help='Output file (default: stdout)')
    return parser.parse_args()


def readPollingPlaces(args):
    '''Read polling places from CSV

    There's no unique poll identifier in the CSV available on data.boston.gov,
    so we have to come up with our own. Two options are implemented here,
    either of which can be selected on the command line with `--poll-key`:

    * `location` combines the `USER_Location2` and `USER_Location3` fields in
      the CSV.
    * `address` uses the `Match_addr` field in the CSV.

    If you switch between them you need to either delete the pickle file or
    specify `--no-pickle-read` on the next invocation or the change won't take
    effect.

    Neither of these methods is 100% reliable since there are inconsistencies
    in the data, hence the data fixes at the top of the script. To be cautious
    you may wish to run the script with each option and save and compare the
    output of each invocation. If there are any differences, then there are
    issues with the data that should be tracked down and resolved!

    Returns:

    * Dict whose keys are (ward, precinct) tuples and values are poll keys.
    * Dict whose keys are poll keys and values are poll display names.
    '''
    polls = {}
    poll_names = {}
    with open(args.polls_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in (stripAll(row) for row in reader):
            wardPrecinct = (int(row['USER_Ward']), int(row['USER_Precinct']))
            location2 = location2Fixes.get(wardPrecinct, row['USER_Location2'])
            if args.poll_key == 'address':
                key = matchAddrFixes.get(wardPrecinct, row['Match_addr'])
                name = location2
            elif args.poll_key == 'location':
                location3 = location3Fixes.get(
                    wardPrecinct, row['USER_Location3'])
                key = f'{location2} ({location3})'
                name = location2
            else:
                raise Exception(
                    f'Internal error: unrecognized poll key {args.poll_key}')
            polls[wardPrecinct] = key
            poll_names[key] = name
    return polls, poll_names


def groupPollingPlaces(args, polls):
    '''Group ward/precinct tuples by poll

    Returns: dict mapping poll keys to lists of ward/precinct tuples
    '''

    groups = defaultdict(list)
    for wardPrecinct, key in polls.items():
        groups[key].append(wardPrecinct)
    return groups


def readAddresses(args):
    '''Read addresses and their ward/precinct assignments from GeoJSON

    This function assumes that street numbers start with one or more digits.
    Addresses that don't meet that condition are warned about and ignored
    (there are a few in the data!).

    This function also assumes that street address + ZIP code is necessary to
    uniquely identify addresses across the city. Note, however, that later we
    assume that the street address alone, i.e., not including the ZIP code, is
    sufficient to uniquely identify addresses at a single polling place. This
    assumption is, alas, necessary because as noted above there are some
    addresses which appear twice in the addresses CSV with two different ZIP
    codes.

    Another thing this function will warn about that is worth mentioning is if
    it encounters an address in the list multiple times with different
    ward/precinct values.

    Returns: Dict mapping address keys to ward/precinct tuples. Each address
    key is a tuple of street number, street name, ZIP code.
    '''
    loadWards(args)
    addresses = {}
    ranges = {}
    ids = {}
    if args.addresses_file.endswith('.bz2'):
        ofunc = bz2.open
    else:
        ofunc = open
    geo = geojson.load(ofunc(args.addresses_file, 'rb'))

    features = geo['features']
    for feature in features:
        feature['shape'] = shape(feature['geometry'])
    transformCoordinates(geo['crs']['properties']['name'], 4326, features)

    for feature in features:
        row = stripAll(feature['properties'])
        _id = row['SAM_ADDRESS_ID']
        errorKey = (f'{row["FULL_ADDRESS"], row["MAILING_NEIGHBORHOOD"]} '
                    f'(#{_id})')
        isRange = bool(int(row['IS_RANGE']))
        if isRange:
            rangeStart = numberPrefix(row['RANGE_FROM'])
            try:
                # At least one address has "P" as the end of the range
                # ("1-P South St"), whatever the heck that means.
                rangeEnd = numberPrefix(row['RANGE_TO'])
            except Exception:
                print(f'{errorKey} has bad RANGE_TO {row["RANGE_TO"]}, '
                      f'ignoring it', file=sys.stderr)
                rangeEnd = rangeStart
        else:
            try:
                rangeStart = numberPrefix(row['STREET_NUMBER'])
            except Exception:
                print(f'{errorKey} has bad street number', file=sys.stderr)
                continue
            rangeEnd = rangeStart

        wardPrecinct = findPrecinct(args, feature)
        if not wardPrecinct:
            print(f'Could not geolocate {errorKey} in any precinct',
                  file=sys.stderr)
            continue

        # It appears that ranges are always for just one side of the
        # street, hence the step value of 2 here.
        for number in range(rangeStart, rangeEnd + 1, 2):
            street = ' '.join(
                p for p in
                (row['STREET_PREFIX'], row['STREET_BODY'],
                 row['STREET_SUFFIX_ABBR'], row['STREET_SUFFIX_DIR'])
                if p)
            zip = zipCodeFixes.get(_id, row['ZIP_CODE'])
            key = (number, street, zip)
            if addresses.get(key, wardPrecinct) != wardPrecinct:
                # Non-range entries preferred over range entries, because a
                # range can start and end in different precincts but only
                # one precinct can be specified in its entry.
                if isRange != ranges.get(key, False):
                    if isRange:
                        continue
                    del ranges[key]
                else:
                    id2 = ids[key]
                    print(f'Ward/Precinct mismatch for {key}: '
                          f'{wardPrecinct} at {_id} vs. '
                          f'{addresses[key]} at {id2}',
                          file=sys.stderr)
                    continue
            if isRange:
                ranges[key] = True
            addresses[key] = wardPrecinct
            ids[key] = _id
    return addresses


def mapAddresses(args, polls, addresses):
    '''Map addresses to polling places

    Returns: dict mapping address keys to poll keys.
    '''
    addressMap = {}
    for address, wardPrecinct in addresses.items():
        try:
            addressMap[address] = polls[wardPrecinct]
        except KeyError:
            print(f'Invalid ward/precinct {wardPrecinct} for {address}',
                  file=sys.stderr)
    return addressMap


def collapseAddresses(args, poll, addresses, addressPolls):
    '''Generate compacted address list for a single polling place

    It's worth emphasizing that the compacted address list is accurate _for
    this polling place_ but is not necessarily accurate with regards to other
    polling places. For example, if one poll has voters for 1-100 Main Street
    all in a single precinct, and another poll has voters for 101-200 Main
    Street all in a different precinct, then the compacted list for both polls
    is just going to say "Main Street", with no address numbers. The purpose of
    the output produced by this script is to help people identify precincts
    within a single polling place, given the assumption that the voter belongs
    there. If they don't, then that will become obvious when the poll-worker
    tries to check them in.

    Returns: Sorted, compacted list of address ranges for the specified poll.
    Each item in the list is itself a list with the following fields: start
    address number or None if the range is open-ended on the left, end address
    number or None if the range is open-ended on the right, street name,
    ward/precinct tuple, and either "all", "odd", or "even" depending on which
    addresses are included in the range.
    '''
    # I woud rather the code below looked like this:
    # pollAddresses = [[k, addresses[k]] for k, v in addressPolls.items()
    #                  if v == poll]
    # However, it can't right now because as noted at the top of the script
    # there is bad data in the address list, specifically, addresses listed
    # multiple times in different ZIP codes. The complicated code below works
    # around this by ignoring the ZIP code when processing data for an
    # individual polling place.
    pollAddresses = {}
    for k, v in ((k, v) for k, v in addressPolls.items() if v == poll):
        numStreet = (k[0], k[1])
        wp = addresses[k]
        if pollAddresses.get(numStreet, wp) != wp:
            print(f'{k[0]}, {k[1]} occurs twice at {poll}!',
                  file=sys.stderr)
            continue
        pollAddresses[numStreet] = wp
    pollAddresses = list([k, v] for k, v in pollAddresses.items())

    # pollAddresses is now a list of lists, each of which is:
    # [(street number, street name), (ward, precinct)]

    # Sort by street name and then street number
    pollAddresses.sort(key=lambda p: (p[0][1], p[0][0]))

    # Separate into streets and merge each street separately.
    collapsed = []
    while pollAddresses:
        group = [pollAddresses.pop(0)]
        while pollAddresses and pollAddresses[0][0][1] == group[0][0][1]:
            group.append(pollAddresses.pop(0))
        collapsed.extend(mergeAddressesOnStreet(args, group))
        group = []
    if group:
        collapsed.extend(mergeAddressesOnStreet(args, group))
    return collapsed


def mergeAddressesOnStreet(args, group):
    '''Merge and compact the addresses on a street.

    This is the most important function in this script, since it embodies the
    logic for compressing a big long list of addresses down to a compact
    representation without losing any information.

    Input: list of lists, each of which is:
    [(street number, street name), (ward, precinct)]

    Returns: Sorted, compacted list of address ranges from the input. As noted
    above, each item in the list is itself a list with the following fields:
    start address number or None if the range is open-ended on the left, end
    address number or None if the range is open-ended on the right, street
    name, ward/precinct tuple, and either "all", "odd", or "even" depending on
    which addresses are included in the range.
    '''

    # First merge all contiguous ranges with the same w/p. Then merge all
    # even/odd ranges with the same w/p. The rest are unmergeable.
    merged = []
    merged.extend(mergeContiguous(args, group, 'all', validator=hasEvenAndOdd))
    odd = [g for g in group if g[0][0] % 2 or g[1] == 'used']
    even = [g for g in group if g[0][0] % 2 == 0 or g[1] == 'used']
    merged.extend(mergeContiguous(args, odd, 'odd'))
    merged.extend(mergeContiguous(args, even, 'even'))
    for g in (g for g in chain(odd, even) if g[1] != 'used'):
        merged.append([g[0][0], g[0][0], g[0][1], g[1], 'all'])
    # If a range is labeled even or odd but there aren't any other ranges that
    # overlap with it, then we can "promote" it to all. This makes output
    # cleaner in the final render.
    for m in (m for m in merged if m[4] != 'all'):
        if countOverlappingMerges(merged, m[0], m[1]) == 1:
            m[4] = 'all'
    merged.sort()
    # If there's only one group, it doesn't need numbers or odd/even.
    if len(merged) == 1:
        merged[0][0] = None
        merged[0][1] = None
        merged[0][4] = 'all'
    # If there are only two groups and they're even and odd, they don't need
    # numbers.
    elif (len(merged) == 2 and 'all' not in (merged[0][4], merged[1][4]) and
          merged[0][4] != merged[1][4]):
        merged[0][0] = None
        merged[0][1] = None
        merged[1][0] = None
        merged[1][1] = None
    # The starting number on the first group if it's all, or on the first two
    # groups if they're even and odd, can be removed.
    elif merged[0][4] == 'all':
        merged[0][0] = None
    elif (len(merged) > 2 and 'all' not in (merged[0][4], merged[1][4]) and
          merged[0][4] != merged[1][4]):
        merged[0][0] = None
        merged[1][0] = None
    # The ending number on the last group if it's all, or on the last two
    # groups if they're even and odd, can be removed.
    if len(merged) > 1 and merged[-1][4] == 'all':
        merged[-1][1] = None
    elif (len(merged) > 2 and 'all' not in (merged[-1][4], merged[-2][4]) and
          merged[-1][4] != merged[-2][4]):
        merged[-1][1] = None
        merged[-2][1] = None
    merged.sort(key=lambda g: (g[2], g[0] or 0, g[1] or 0))
    return merged


def countOverlappingMerges(merges, start, end):
    '''Count how many merges that overlap with the specified number range'''
    # https://stackoverflow.com/questions/325933/
    # determine-whether-two-date-ranges-overlap/325964#325964
    return sum(map(lambda m: max(start, m[0]) <= min(end, m[1]), merges))


def hasEvenAndOdd(group):
    '''Determine if a list of addresses has both even and odd numbers'''
    return (any(g[0][0] % 2 for g in group) and
            any(g[0][0] % 2 == 0 for g in group))


def mergeContiguous(args, group, which, validator=None):
    '''Merge contiguous addresses into groups

    "Contiguous" means same ward/precinct tuple.

    Side effects: The all/even/odd value for merged addresses is replaced with
    the value "used".

    Returns: List of lists, each of which contains start street number, end
    street number, street name, ward/precinct tuple, and all/even/odd.
    '''
    merged = []
    if not group:
        return merged
    if validator is None:
        def validator(group):
            return True
    street = group[0][0][1]
    mergeable = findContiguousRanges(group, key=lambda a: a[1])
    for start, end in mergeable:
        if not validator(group[start:end+1]):
            continue
        wardPrecinct = group[start][1]
        if wardPrecinct == 'used':
            continue
        thisMinNumber = group[start][0][0]
        thisMaxNumber = group[end][0][0]
        merged.append(
            [thisMinNumber, thisMaxNumber, street, wardPrecinct, which])
        for i in range(start, end + 1):
            group[i][1] = 'used'
    return merged


class RenderPages:
    def __init__(self, args, polls, names, addresses):
        self.args = args
        self.polls = polls
        self.names = names
        self.addresses = addresses
        self._multipleWards = {}
        self._numPrecincts = {}

    def render(self):
        raise NotImplementedError

    def multipleWards(self, poll):
        try:
            return self._multipleWards[poll]
        except KeyError:
            addresses = self.addresses[poll]
            wards = set(a[3][0] for a in addresses)
            self._multipleWards[poll] = len(wards) > 1
            return self._multipleWards[poll]

    def numPrecincts(self, poll):
        try:
            return self._numPrecincts[poll]
        except KeyError:
            addresses = self.addresses[poll]
            self._numPrecincts[poll] = len(set(a[3] for a in addresses))
            return self._numPrecincts[poll]

    def pageTitle(self, poll):
        multipleWards = self.multipleWards(poll)
        title = self.names[poll]
        if not multipleWards:
            title += f' (Ward\u00A0{self.addresses[poll][0][3][0]})'
        return title

    def numCopies(self, poll):
        if self.args.copies_per_precinct or self.args.copies_per_polling_place:
            return (self.args.copies_per_precinct or 0) * \
                self.numPrecincts(poll) + \
                (self.args.copies_per_polling_place or 0)
        return 1


class PdfRenderPages(RenderPages):
    '''Generate PDF output using Pango and Cairo'''
    page_width = 8.5 * 72
    page_height = 11 * 72
    margin_width = .4 * 72
    title_spacing = 0.25 * 72
    column_spacing = 0.25 * 72
    street_pct = 50
    number_pct = 11
    dash_pct = 3
    side_pct = 12
    precinct_pct = 13
    header_grey = 0.7
    data_grey = 0.9
    header_font = 'Times New Roman'
    header_min_font_size = 18
    header_max_font_size = 24
    body_font = 'Times New Roman'
    body_font_size = 12
    page_number_font = 'Times New Roman'
    page_number_font_size = 8

    def __init__(self, args, polls, names, addresses):
        super().__init__(args, polls, names, addresses)
        self.pageCount = 0
        self.output = open(args.output_file, 'wb') if args.output_file \
            else sys.stdout.buffer
        self.content_width = self.page_width - self.margin_width * 2
        self.content_bottom = self.page_height - self.margin_width
        self.column_width = (self.content_width - self.column_spacing) / 2
        self.column_starts = (
            self.margin_width,
            self.margin_width + self.column_width + self.column_spacing)
        self.street_width = self.column_width * self.street_pct / 100
        self.number_width = self.column_width * self.number_pct / 100
        self.dash_width = self.column_width * self.dash_pct / 100
        self.side_width = self.column_width * self.side_pct / 100
        self.precinct_width = self.column_width * self.precinct_pct / 100

        self.surface = cairo.PDFSurface(
            self.output, self.page_width, self.page_height)
        self.ctx = cairo.Context(self.surface)

    def render(self):
        for poll in self.polls:
            if not self.args.print_homogeneous and \
               self.numPrecincts(poll) == 1:
                continue
            for copy in range(self.numCopies(poll)):
                self.printPoll(poll)

        self.surface.finish()
        self.surface.flush()

    def printPoll(self, poll):
        addresses = self.addresses[poll].copy()
        pageNumber = 0
        while addresses:
            self.ctx.move_to(self.margin_width, self.margin_width)
            layout = self.fitToWidth(
                self.margin_width, self.pageTitle(poll), self.content_width,
                self.header_font, self.header_max_font_size,
                min_font_size=self.header_min_font_size)
            PangoCairo.show_layout(self.ctx, layout)
            _ink, rect = layout.get_extents()
            column_top = self.margin_width + rect.height / Pango.SCALE + \
                self.title_spacing
            for left in self.column_starts:
                self.printColumn(poll, addresses, left, column_top)
                if not addresses:
                    break
            if addresses or pageNumber:
                self.ctx.move_to(
                    self.margin_width, column_top - self.title_spacing)
                layout = self.fitToWidth(
                    column_top - self.title_spacing,
                    f'Page {pageNumber+1}', self.content_width,
                    self.page_number_font, self.page_number_font_size)
                PangoCairo.show_layout(self.ctx, layout)
            pageNumber += 1
            self.surface.show_page()
        if self.args.double_sided and pageNumber % 2:
            self.surface.show_page()

    def printColumn(self, poll, addresses, x, y):
        height = self.printColumnHeader(x, y)
        y += height
        multipleWards = self.multipleWards(poll)
        grey = None
        last_street = ""
        while addresses:
            start, end, street, precinct, which = addresses[0]
            start = str(start) if start else ""
            end = str(end) if end else ""
            hyphen = "–" if (start or end) else ""
            which = which if (which and which != "all") else ""
            if multipleWards:
                precinct = f'{precinct[0]}-{precinct[1]}'
            else:
                precinct = str(precinct[1])
            height = self.printRow(x, y, grey=grey, cells=(
                (street if street != last_street else "", self.street_width),
                (start, self.number_width),
                (hyphen, self.dash_width),
                (end, self.number_width),
                (which, self.side_width),
                (precinct, self.precinct_width)))
            last_street = street
            grey = None if grey else self.data_grey
            if height:
                y += height
                addresses.pop(0)
            else:
                return None
        return height

    def printColumnHeader(self, start_x, start_y):
        val = self.printRow(
            start_x, start_y, html=True, grey=self.header_grey,
            cells=(('<b>Street</b>', self.street_width),
                   ('<b>#</b>', self.number_width * 2 + self.dash_width),
                   ('<b>Side</b>', self.side_width),
                   ('<b>Prec.</b>', self.precinct_width)))
        return val

    def printRow(self, x, y, cells=(), html=False, grey=None):
        layouts = []
        for text, width in cells:
            layout = self.fitToWidth(y, text, width, self.body_font,
                                     self.body_font_size, html=html,
                                     bottom=self.content_bottom)
            if not layout:
                return None
            layouts.append((layout, width))
        height = max(layout.get_extents()[1].height / Pango.SCALE
                     for layout, width in layouts)
        if grey:
            width = sum(width for layout, width in layouts)
            self.ctx.set_source_rgb(grey, grey, grey)
            self.ctx.rectangle(x, y, width, height)
            self.ctx.fill()
            self.ctx.set_source_rgb(0, 0, 0)
        for layout, width in layouts:
            self.ctx.move_to(x, y)
            PangoCairo.show_layout(self.ctx, layout)
            x += width
        return height

    def fitToWidth(self, y, text, want_width, font_name, max_font_size,
                   min_font_size=None, bottom=None, html=False):
        wrapping = max_font_size == min_font_size
        font_size = max_font_size
        layout = PangoCairo.create_layout(self.ctx)
        font_description = Pango.font_description_from_string(
            f'{font_name}, {font_size}')
        layout.set_font_description(font_description)
        if wrapping:
            layout.set_width(want_width * Pango.SCALE)
        (layout.set_markup if html else layout.set_text)(text)
        ink_rect, logical_rect = layout.get_extents()
        logical_right_edge = logical_rect.x + logical_rect.width
        got_width = logical_right_edge / Pango.SCALE

        if not wrapping:
            if got_width > want_width:
                font_size = want_width / got_width * font_size
                layout = PangoCairo.create_layout(self.ctx)
                font_description = Pango.font_description_from_string(
                    f'{font_name}, {font_size}')
                layout.set_font_description(font_description)
                (layout.set_markup if html else layout.set_text)(text)
                ink_rect, logical_rect = layout.get_extents()
                logical_right_edge = logical_rect.x + logical_rect.width
                got_width = logical_right_edge / Pango.SCALE

            # I don't understand why I need to do this twice, but for some
            # reason after the first resize above the font is still sticking
            # out a little bit past the right edge of the area we want it in.
            if got_width > want_width:
                font_size = want_width / got_width * font_size
                layout = PangoCairo.create_layout(self.ctx)
                font_description = Pango.font_description_from_string(
                    f'{font_name}, {font_size}')
                layout.set_font_description(font_description)
                (layout.set_markup if html else layout.set_text)(text)
                ink_rect, logical_rect = layout.get_extents()
                logical_right_edge = logical_rect.x + logical_rect.width
                got_width = logical_right_edge / Pango.SCALE

            if min_font_size and font_size < min_font_size:
                return self.fitToWidth(y, text, want_width, font_name,
                                       min_font_size, min_font_size, bottom)

        if bottom:
            new_y = y + logical_rect.height / Pango.SCALE
            if new_y > bottom:
                return None

        return layout


class HtmlRenderPages(RenderPages):
    '''Generate HTML output with CSS page-break markers'''
    def __init__(self, args, polls, names, addresses):
        super().__init__(args, polls, names, addresses)
        self.pageCount = 0
        self.output = open(args.output_file, 'w') if args.output_file \
            else sys.stdout

    def render(self):
        print('<html>', file=self.output)
        print('<head>', file=self.output)
        print('<meta charset="utf-8">', file=self.output)
        print('''<style>
            .columnTable th{background-color: #c2c2c2;}
            .columnTable tr:nth-child(even){background-color: #e2e2e2;}
            </style>''', file=self.output)
        print('</head>', file=self.output)
        print('<body>', file=self.output)

        for poll in self.polls:
            self.printPoll(poll)

        print('</body></html>', file=self.output)

    def printPoll(self, poll):
        addresses = self.addresses[poll]

        pollColumnRows = self.args.column_rows
        pollColumns = math.ceil(len(addresses) / pollColumnRows)
        if pollColumns > 2:
            # Make room for the page number
            pollColumnRows -= 1
            pollColumns = math.ceil(len(addresses) / pollColumnRows)

        if not self.args.print_homogeneous and self.numPrecincts(poll) == 1:
            return
        copies = self.numCopies(poll)

        precinctPad = max(len(str(a[3][1])) for a in addresses)
        addressPad = max(len(str(v)) for v in chain.from_iterable(
            (a[0], a[1]) for a in addresses))

        for i in range(copies):
            rowCount = 0
            columnCount = 0

            columnHeader = '''
                <td style="vertical-align: top;">
                <table class="columnTable"><tbody>
                <tr><th align="left">Street</th><th>#</th><th>Side</th>
                <th>Prec.</th></tr>'''
            columnFooter = '</tbody></table></td>'
            print(self.pageHeader(poll,
                                  None if pollColumns < 3 else
                                  int(1+columnCount/2)), file=self.output)
            print(columnHeader, file=self.output)
            for start, end, street, wardPrecinct, which in addresses:
                if rowCount and not rowCount % pollColumnRows:
                    print(columnFooter, file=self.output)
                    columnCount += 1
                    if not columnCount % 2:
                        print(self.pageFooter(), file=self.output)
                        print(self.pageHeader(poll,
                                              None if pollColumns < 3 else
                                              int(1+columnCount/2)),
                              file=self.output)
                    print(columnHeader, file=self.output)
                rowCount += 1
                print('<tr>', file=self.output)
                print(f'<td>{html.escape(street)}</td>', file=self.output)
                if start is None and end is None:
                    numbers = ''
                elif start is None:
                    numbers = (f'{nbspPad("", addressPad)}&ndash;'
                               f'{nbspPad(end, addressPad)}')
                elif end is None:
                    numbers = (f'{nbspPad(start, addressPad)}&ndash;'
                               f'{nbspPad("", addressPad)}')
                elif start == end:
                    numbers = nbspPad(start, addressPad)
                else:
                    numbers = (f'{nbspPad(start, addressPad)}&ndash;'
                               f'{nbspPad(end, addressPad)}')
                if which == 'all':
                    which = ''
                else:
                    which = which.title()
                print(f'<td style="font-family: monospace;">{numbers}</td>',
                      file=self.output)
                print(f'<td>{which}</td>', file=self.output)
                if self.multipleWards(poll):
                    wardPrecinct = (f'{wardPrecinct[0]}-'
                                    f'{nbspPad(wardPrecinct[1], precinctPad)}')
                else:
                    wardPrecinct = wardPrecinct[1]
                print(f'<td style="font-family: monospace; text-align: right;"'
                      f'>{wardPrecinct}</td>', file=self.output)
                print('</tr>', file=self.output)
            print(columnFooter, file=self.output)
            print(self.pageFooter(pollEnd=True), file=self.output)

    def pageHeader(self, poll, pageNum):
        title = self.pageTitle(poll)
        header = f'<h2>{html.escape(title)}</h2>'
        if pageNum:
            header += f'<h3>Page {pageNum}'
        header += '<table width="100%" style="page-break-after: always;">'
        header += '<tbody>'
        return header

    def pageFooter(self, pollEnd=False):
        self.pageCount += 1
        footer = '</tbody></table>'
        if self.args.double_sided and pollEnd and self.pageCount % 2:
            self.pageCount += 1
            footer += '<div style="page-break-after: always;"></div>'
        return footer


def renderPages(args, pollNames, pollAddresses):
    # We want pages to come out in a consistent order, so let's produce a sort
    # key based on the wards and precincts at each poll.
    sortKeys = {poll: tuple(sorted(set(a[3] for a in addresses)))
                for poll, addresses in pollAddresses.items()}
    polls = sorted(pollAddresses.keys(), key=lambda p: sortKeys[p])
    if args.output_format == 'pdf':
        PdfRenderPages(args, polls, pollNames, pollAddresses).render()
    else:
        HtmlRenderPages(args, polls, pollNames, pollAddresses).render()


def nbspPad(val, width):
    val = str(val)
    pad = '&nbsp;' * (width - len(val))
    val = pad + val
    return val


def findContiguousRanges(group, key=None):
    if key is None:
        def key(v):
            return v
    if not group:
        return []
    ranges = []
    currentKey = key(group[0])
    currentStart = 0
    for i in range(len(group)):
        newKey = key(group[i])
        if newKey == currentKey:
            continue
        if i - currentStart > 1:
            ranges.append((currentStart, i - 1))
        currentKey = newKey
        currentStart = i
    if len(group) - currentStart > 1:
        ranges.append((currentStart, len(group) - 1))
    return ranges


def stripAll(dct):
    return {k: v.strip() if isinstance(v, str) else v
            for k, v in dct.items()}


def numberPrefix(num):
    match = re.match(r'^\d+', num)
    return int(match[0])


def loadWards(args):
    geo = geojson.load(open(args.wards_file, "rb"))
    wards = geo['features']
    for ward in wards:
        ward['shape'] = shape(ward['geometry'])
    transformCoordinates(geo['crs']['properties']['name'], 4326, wards)

    geo = geojson.load(open(args.precincts_file, "rb"))
    precincts = geo['features']
    for precinct in precincts:
        precinct['shape'] = shape(precinct['geometry'])
        precinct['wp'] = (int(precinct['properties']['Ward1']),
                          int(precinct['properties']['Precinct1']))
    transformCoordinates(geo['crs']['properties']['name'], 4326, precincts)

    for ward in wards:
        ward['precincts'] = [
            p for p in precincts
            if ward['properties']['Ward1'] == p['properties']['Ward1']
        ]
        if not ward['precincts']:
            raise Exception(
                f'No precincts for ward {ward["properties"]["Ward1"]}')
    args.wards = wards


def findPrecinct(args, address):
    location = address['shape']
    try:
        ward = next(w for w in args.wards if location.within(w['shape']))
        precinct = next(p for p in ward['precincts']
                        if location.within(p['shape']))
    except StopIteration:
        return None
    return precinct['wp']


def download(slug, _type, target):
    response = requests.get(f'https://data.boston.gov/api/3/action/'
                            f'package_show?id={slug}')
    response.raise_for_status()
    data = response.json()
    url = next(r for r in data['result']['resources']
               if r['name'] == _type)['url']
    response = requests.get(url, stream=True)
    response.raise_for_status()
    if target.endswith('.bz2'):
        ofunc = bz2.open
    else:
        ofunc = open
    with ofunc(f'{target}.new', 'wb') as f:
        for chunk in response.iter_content(chunk_size=1024*1024):
            f.write(chunk)
    os.rename(f'{target}.new', target)


def transformCoordinates(fromCrs, toCrs, features):
    if fromCrs == toCrs:
        return
    fromCrs = normalizeCrs(fromCrs)
    toCrs = normalizeCrs(toCrs)
    if fromCrs == toCrs:
        return
    transformer = Transformer.from_crs(fromCrs, toCrs, always_xy=True)
    for feature in features:
        feature['shape'] = transform(transformer.transform, feature['shape'])


def normalizeCrs(crs):
    if isinstance(crs, int):
        # Already EPSG number
        return crs
    if crs.startswith('EPSG:'):
        return int(crs[5:])
    if crs == "urn:ogc:def:crs:OGC:1.3:CRS84":
        return 4326


if __name__ == '__main__':
    main()
