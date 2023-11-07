#!/usr/bin/env python3

'''Generate compact precinct books for City of Boston polling places

The City of Boston is divided into wards, and each ward is divided into one or
more precincts. Each precinct is assigned to a polling place, of which there
are over 150. Some polling places have only one precinct; some have multiple
precincts within a single ward; and some have multiple precincts from different
wards.

On election day, each precinct table is issued a book listing all of the street
addresses assigned to vote at that polling place and which precinct each is in.
Poll-workers use this to help voters identify their precinct, since voters need
to check in and check out at their precinct's tables.

In the past this precinct book has listed one by one every single street
address at the polling place. This makes the book many pages long, which is
both inefficient (it takes a long time to look up specific addresses) and
wasteful, because the same information can actually be represented in just one
or two pages. Producting precinct books in that compact 1-2 page format is the
purpose of this script.

The working mechanisms of the script are documented below in documentation on
individual functions.

One important note... After running the script, before printing the HTML from
the browser, you should scroll through it to make sure that none of the pages
went too long. If any did, you'll need to reduce `columnRows` below or specify
a smaller value on the command line with `--column-rows`.

In the code and comments below, "poll" is used as a synonym for "polling
place."
'''

import argparse
import bz2
from collections import defaultdict
import csv
import html
from itertools import chain
import math
import pickle
import re
import sys

# https://data.boston.gov/dataset/polling-locations-2022
#
# A different file path can be specified with --polls-file.
pollingPlacesFile = 'Polling_Locations_2022.csv'

# https://data.boston.gov/dataset/live-street-address-management-sam-addresses
#
# A different file path can be specified with --addresses-file.
#
# The data in this file is bad. Several examples:
#
# * When I search for 9 Appleton St Boston in the state voter database, it
#   claims ward 5 / precinct 14, but that's not what this file says.
# * This file actually lists two different precints for that same 9 Appleton
#   St address: apartment 305 says 4 / 1, but all the other apartments
#   say 5 / 1.
# * Addresses on Adams St in Dorchester are listed redundantly in 02122 and
#   02124. Both Google Maps and the USPS believe this is wrong (the correct
#   ZIP is 02122).
#
# Because the data here is bad I can't use this to generate fully accurate
# one-pagers, but I can at least use it for the time being to demonstrate
# what the one-pagers will look like when I have access to fully accurate data.
addressesFile = 'Live_Street_Address_Management_(SAM)_Addresses.csv.bz2'

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
addressPrecinctFixes = {
    (60, 'N Crescent Cirt', '02135'): (22, 7),
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
                        'CSV downloaded from data.boston.gov')
    parser.add_argument('--column-rows', type=int, action='store',
                        default=columnRows,
                        help='Number of data rows per column, determined '
                        'empirically by how many rows fit when you print from '
                        'your browser with the desired print settings '
                        f'(default: {columnRows})')
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
                        'one precinct')
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
    '''Read addresses and their ward/precinct assignments from CSV

    I've determined empirically that the CSV from data.boston.gov has two
    fields in it that determine the ward and precinct. The `WARD` field has the
    obvious meaning, and the `PRECINCT_WARD` field appears to be the precinct
    plus 100 times the ward, so to extract the precinct from it we multiply the
    ward by 100 and subtract it from this field. This sometimes produces an
    invalid ward/precinct. I don't know what to make of this so until I figure
    it out, addresses with invalid ward/precinct values are simply being
    ignored.

    As of 2023-07-19 there's one `PRECINCT_WARD` field in the data that has the
    value `0502A`. I have no idea what that means, so for the time being I'm
    removing the `A` and treating it as ward 5, precinct 2.

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
    addresses = {}
    ranges = {}
    ids = {}
    if args.addresses_file.endswith('.bz2'):
        ofunc = bz2.open
    else:
        ofunc = open
    with ofunc(args.addresses_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in (stripAll(row) for row in reader):
            errorKey = f'{row["FULL_ADDRESS"], row["MAILING_NEIGHBORHOOD"]}'
            if not row['WARD']:
                print(f'{errorKey} has no ward', file=sys.stderr)
                continue
            if not row['PRECINCT_WARD']:
                print(f'{errorKey} has no precinct', file=sys.stderr)
                continue
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
            ward = int(row['WARD'])
            precinctWard = precinctFixes.get(
                row['PRECINCT_WARD'], row['PRECINCT_WARD'])
            try:
                precinct = (int(precinctWard) - ward * 100)
            except ValueError:
                print(f'{errorKey} has bad precinct value {precinctWard}',
                      file=sys.stderr)
                continue

            wardPrecinct = (ward, precinct)
            # It appears that ranges are always for just one side of the
            # street, hence the step value of 2 here.
            for number in range(rangeStart, rangeEnd + 1, 2):
                street = ' '.join(
                    p for p in
                    (row['STREET_PREFIX'], row['STREET_BODY'],
                     row['STREET_SUFFIX_ABBR'], row['STREET_SUFFIX_DIR'])
                    if p)
                key = (number, street, row['ZIP_CODE'])
                thisWardPrecinct = addressPrecinctFixes.get(key, wardPrecinct)
                if addresses.get(key, thisWardPrecinct) != thisWardPrecinct:
                    # Non-range entries preferred over range entries, because a
                    # range can start and end in different precincts but only
                    # one precinct can be specified in its entry.
                    if isRange != ranges.get(key, False):
                        if isRange:
                            continue
                        del ranges[key]
                    else:
                        id1 = row['SAM_ADDRESS_ID']
                        id2 = ids[key]
                        print(f'Ward/Precinct mismatch for {key}: '
                              f'{thisWardPrecinct} at {id1} vs. '
                              f'{addresses[key]} at {id2}',
                              file=sys.stderr)
                        continue
                if isRange:
                    ranges[key] = True
                addresses[key] = thisWardPrecinct
                ids[key] = row['SAM_ADDRESS_ID']
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
    pollAddresses = list(list(t) for t in set(
        ((k[0], k[1]), addresses[k]) for k, v in addressPolls.items()
        if v == poll))

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


def renderPages(args, pollNames, pollAddresses):
    '''Generate HTML output with CSS page-break markers'''
    print('<html>')
    print('<head>')
    print('<meta charset="utf-8">')
    print('''<style>
        .columnTable th{background-color: #c2c2c2;}
        .columnTable tr:nth-child(even){background-color: #e2e2e2;}
        </style>''')
    print('</head>')
    print('<body>')
    pageCount = [0]
    # We want pages to come out in a consistent order, so let's produce a sort
    # key based on the wards and precincts at each poll.
    sortKeys = {poll: tuple(sorted(set(a[3] for a in addresses)))
                for poll, addresses in pollAddresses.items()}
    for poll in sorted(pollAddresses.keys(), key=lambda p: sortKeys[p]):
        addresses = pollAddresses[poll]

        pollColumnRows = args.column_rows
        pollColumns = math.ceil(len(addresses) / pollColumnRows)
        if pollColumns > 2:
            # Make room for the page number
            pollColumnRows -= 1
            pollColumns = math.ceil(len(addresses) / pollColumnRows)

        wards = set(a[3][0] for a in addresses)
        includeWard = len(wards) > 1
        precincts = set(a[3] for a in addresses)
        if not args.print_homogeneous and len(precincts) == 1:
            continue
        if args.copies_per_precinct or args.copies_per_polling_place:
            copies = (args.copies_per_precinct or 0) * len(precincts) + \
                (args.copies_per_polling_place or 0)
        else:
            copies = 1

        precinctPad = max(len(str(a[3][1])) for a in addresses)
        addressPad = max(len(str(v)) for v in chain.from_iterable(
            (a[0], a[1]) for a in addresses))

        def pageHeader():
            title = pollNames[poll]
            if not includeWard:
                title += f' (Ward {addresses[0][3][0]})'
            header = f'<h2>{html.escape(title)}</h2>'
            if pollColumns > 2:
                header += f'<h3>Page {int(1+columnCount/2)}'
            header += '<table width="100%" style="page-break-after: always;">'
            header += '<tbody>'
            return header

        def pageFooter(pollEnd=False):
            pageCount[0] += 1
            footer = '</tbody></table>'
            if args.double_sided and pollEnd and pageCount[0] % 2:
                pageCount[0] += 1
                footer += '<div style="page-break-after: always;"></div>'
            return footer

        for i in range(copies):
            rowCount = 0
            columnCount = 0

            columnHeader = '''
                <td style="vertical-align: top;">
                <table class="columnTable"><tbody>
                <tr><th align="left">Street</th><th>#</th><th>Side</th>
                <th>Prec.</th></tr>'''
            columnFooter = '</tbody></table></td>'
            print(pageHeader())
            print(columnHeader)
            for start, end, street, wardPrecinct, which in addresses:
                if rowCount and not rowCount % pollColumnRows:
                    print(columnFooter)
                    columnCount += 1
                    if not columnCount % 2:
                        print(pageFooter())
                        print(pageHeader())
                    print(columnHeader)
                rowCount += 1
                print('<tr>')
                print(f'<td>{html.escape(street)}</td>')
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
                print(f'<td style="font-family: monospace;">{numbers}</td>')
                print(f'<td>{which}</td>')
                if includeWard:
                    wardPrecinct = (f'{wardPrecinct[0]}-'
                                    f'{nbspPad(wardPrecinct[1], precinctPad)}')
                else:
                    wardPrecinct = wardPrecinct[1]
                print(f'<td style="font-family: monospace; text-align: right;"'
                      f'>{wardPrecinct}</td>')
                print('</tr>')
            print(columnFooter)
            print(pageFooter(pollEnd=True))
    print('</body></html>')


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
    return {k: v.strip() for k, v in dct.items()}


def numberPrefix(num):
    match = re.match(r'^\d+', num)
    return int(match[0])


if __name__ == '__main__':
    main()
