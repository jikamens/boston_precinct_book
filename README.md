Generate compact precinct books for City of Boston polling places
=================================================================

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
purpose of the script in this repository.

The working mechanisms of the script are documented in the script itself.

One important note... After running the script, before printing the HTML from
the browser, you should scroll through the print preview to make sure that none
of the pages went too long. If any did, you'll need to reduce `columnRows`
below or specify a smaller value on the command line with `--column-rows`.

Author
------

Jonathan Kamens <<jik@kamens.us>>.


