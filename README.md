PolishTrainsGTFS
================

Creates a single, GTFS and GTFS-Realtime feeds for all Polish trains coordinated by [PKP PLK](https://www.plk-sa.pl/), including:

- [PolRegio](https://polregio.pl/)
- [PKP Intercity](https://www.intercity.pl/)
- [Koleje Mazowieckie](https://mazowieckie.com.pl/pl)
- [PKP SKM w Trójmieście](https://www.skm.pkp.pl/)
- [Koleje Śląskie](https://www.kolejeslaskie.pl/)
- [Koleje Dolnośląskie](https://kolejedolnoslaskie.pl/)
- [Koleje Wielkopolskie](https://koleje-wielkopolskie.com.pl/)
- [SKM Warszawa](https://www.skm.warszawa.pl/)
- [Łódzka Kolej Aglomeracyjna](https://lka.lodzkie.pl/)
- [Koleje Małopolskie](https://kolejemalopolskie.com.pl/)
- [Arriva RP](https://arriva.pl/)
- [RegioJet](https://regiojet.pl/)
- [Leo Express](https://www.leoexpress.com/pl)


Data comes from the [Otwarte Dane Kolejowe API from PKP PLK](https://pdp-api.plk-sa.pl/).

Notable exceptions include: [WKD](https://wkd.com.pl/) (use data from <https://mkuran.pl/gtfs/>),
[UBB](https://www.ubb-online.com/) and [ODEG](https://www.odeg.de/) (use German data for those).


Data Caveats
------------

- Stop and shape data is collected manually, see [map data](#map-data-datageoosm).
- Platforms with exactly the same lat and lon are deliberate - even if locations are the same, the differing `platform_code` is still useful. Contribute [map data](#map-data-datageoosm) to resolve this.
- Unused fallback platforms are deliberate - they may be used by the realtime feed for trains on detours.
- Timed connections and carriage transfers are not provided - they're missing from the PKP PLK API.
- Platform and track info is missing at stops marked by PKP PLK as disembarking only.
- International trains are kinda messed up. Bus replacement services are sometimes missing (and remain as trains). Sometimes, only partial routes are available (OEDG, NEB). Rarely, the agency is also incorrect (NEB trains to/from Kostrzyn are reported as operated by PolRegio).


Realtime Caveats
----------------

- The static `trip_id` is not particularly stable.
- Prefer JSON to GTFS-Realtime, as it exposes a little bit more data.
- Use a backup matching strategy on `agency_id`+`number`+`start_date`, in case the standard `trip_id`+`start_date` combination fails to match.
- `platform` and `track` in updates.json is simply copied over from static data, and does not reflect changes in platform assignment. PKP PLK's API doesn't have live platform and track data. Those fields are provided solely for convenience of some end-user applications.
- PKP PLK seemingly updates their live data every couple of minutes, there's a noticeable propagation delay from a train actually being disrupted to this being reflected in the feed.
- Alerts are still work-in-progress.
- Live vehicle positions (fetched directly from agencies) are technically feasible, and may be available sometime in the future.


Running
-------

The script creating GTFS Schedule is written in Python with the [Impuls framework](https://github.com/MKuranowski/Impuls).

To set up the project, run:

```terminal
$ python -m venv .venv
$ . .venv/bin/activate
$ pip install -Ur requirements.txt
```

Then, run:

```terminal
$ export PKP_PLK_APIKEY=paste_your_apikey_here
$ python -m polish_trains_gtfs.static
```

The resulting schedules will be put in a file called `polish_trains.zip`.

See `python -m polish_trains_gtfs.static --help` for a list of all available options.


The script creating GTFS Realtime is written in Go. Simply run:

```terminal
$ export PKP_PLK_APIKEY=paste_your_apikey_here
$ go run polish_trains_gtfs/realtime/cmd/main.go
```

This will compile and run the project, and then create `polish_trains.pb` and `polish_trains.json`
files with trip updates. Run with `-help` to see all available options, which includes alerts and
continuous loop mode.

The realtime script requires the GTFS Schedule file, which is by default read from `polish_trains.zip`.


API Keys
--------

In order to run the scripts, an apikey for [Otwarte Dane Kolejowe](https://pdp-api.plk-sa.pl/)
is required. It must be provided in the `PKP_PLK_APIKEY` environment variable. For development,
use your IDE .env file support to avoid having to `export` it in your shell.

PolishTrainsGTFS also supports Docker-style secret passing. Instead of setting the apikey
directly, a path to a file containing the apikey may be provided in the `PKP_PLK_APIKEY_FILE`
environment variable. Note that `PKP_PLK_APIKEY` takes precedence if both variables are set.

The realtime script has an extra `-clients` option, which can be used to granularly control
how data is requested. If present, it overrides the `PKP_PLK_APIKEY`,
and must be a path to a JSON file following the below schema.

```ts
type TopLevelConfig = Client[];

interface Client {
    key: string,
    rate_limit?: string,   // See https://pkg.go.dev/time#ParseDuration for accepted values; defaults to none
    proxy?: string,        // URL; if not provided the HTTP_PROXY, HTTPS_PROXY and NO_PROXY env variables are respected
    wireguard?: Wireguard, // Wireguard VPN config; if provided any proxy config is ignored
}

interface Wireguard {
    endpoint: string,    // IPv4 or IPv6 host and port (and possibly a zone)
    dns?: string,        // IPv4 or IPv6 address; defaults to 1.1.1.1
    address: string,     // IPv4 or IPv6 address
    public_key: string,  // base64-encoded
    private_key: string, // base64-encoded
    pre_shared_key?: string,  // base64-encoded
}
```


External Data
-------------

By providing the `-e`/`--external` flag to the static script, data for several routes
will be pulled directly from operator APIs. Agency-provided datasets sometimes have
higher-quality data, or PKP PLK API is straight up missing some routes
(like the Modlin Airport shuttle bus). This requires providing extra access credentials:

- `KM_APIKEY` - Koleje Mazowieckie XML schedules apikey.


Map Data (data/geo.osm)
-----------------------

The underlying API does not have any geographical data, especially for shapes. For this purpose, the repository has its own geo-data source, stored in an [OSM XML](https://wiki.openstreetmap.org/wiki/OSM_XML) file. This data was collected manually and migrated from [PLRailMap](https://github.com/MKuranowski/PLRailMap/) - therefore licensed under [CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/).

Before committing any changes to the data/geo.osm file, run the following commands:

1. `python -m polish_trains_gtfs.geo.validate` - and fix any reported issues;
2. `python -m polish_trains_gtfs.geo.stabilize` - to prevent JOSM from renumbering IDs ([which creates unreadable diffs](https://github.com/MKuranowski/PLRailMap/pull/13)).

Geographical data should be tagged with the following schema:

### Stations

Every railway station must have exactly one corresponding node, with the following tags:

- `railway=station`
- `name` - name of the station in local language, preferably as signposted
- `ref` - primary ID used by PKP PLK

The following tags are optional:

- `ref:2` - secondary ID used by PKP PLK
- `int_name` - "international" name of the station, usually transliteration of `name`
- `name:pl` - name of the station in Polish
- `wheelchair=yes` or `wheelchair=no` - wheelchair accessibility of the station
- `country` - [ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2) country code for stations outside of Poland
- `request_stop=yes` - marker for [request stops](https://en.wikipedia.org/wiki/Request_stop)
- `passenger=no` - marker for waypoints - shape only "stations" (usually cargo or _posterunek_)

Unless used with `public_transport=stop_position`, a station must be attached to a `railway=rail` way.

### Platforms

Platforms data is optional. When a platform is missing, the GTFS script will copy over data from the station. The following tags are required:

- `public_transport=platform`
- `ref:station` - primary ID (`ref`) of the station
- `name` - name of the platform, optionally combined with a track number after a `;`

The following tags are optional:

- `wheelchair=yes` or `wheelchair=no` - wheelchair accessibility of the platform

Prefer to map individual platform edges (krawędź peronowa) with two-level names, but it's ok to map entire platforms (peron) if determining individual tracks is not possible.

### Exits

Exit data is optional. The following tags are required:

- `railway=subway_entrance`
- `ref:station` - primary ID (`ref`) of the station
- `name` - name of the exit, as signposted, should be relatively unique

The following tags are optional:

- `platforms` - `;`-separated list of platforms reachable from this exit; if applicable
- `wheelchair=yes` or `wheelchair=no` - wheelchair accessibility of the exit

### Stop Positions

By default, railway=station nodes must be part of a `railway=rail` way.

However, there are a couple of edge-case stations, where such 1-to-1 mapping is not possible (see Opole Główne or Kraków Bieżanów). In these cases extra public_transport=stop_position nodes may be present on the railway=rail ways, and the railway=station node will be left unattached.

Such stop position nodes must be tagged with the following tags:

- `public_transport=stop_position`
- `ref:station` - primary ID (`ref`) of the station
- `platforms` - `;`-separated list of names of platforms for which this stop position applies

The following tags may be optionally provided:

- `towards` - `;`-separated list of IDs of stations which immediately follow or precede this station for this stop_position to apply; or `fallback`. When a station uses stop positions, there must be at least exactly one fallback stop position. This offers an alternative matching method if reliable platform data is not available for trains.

### Bus Stops

By default, bus departures are attached to a fake-ish "unknown" platform. To circumvent all that, bus stop locations can be provided via nodes with the following required tags:

- `highway=bus_stop`
- `ref:station` - primary ID (`ref`) of the station

Matching is done via the following tags:

- `direction` - `;`-separated list of geographical headings of buses departing from this stop; values must be one of: N NE E SE S SW W NW \* (any) or T (terminus - buses starting or terminating here). The "T" direction takes precedence when matching.
- `towards` - `;`-separated list of primary IDs of stations immediately following this bus stop (never ~~preceding~~ - this behavior is explicitly different to stop positions)

Requirements for those tags are a bit complicated:

- A stop must have either a `towards` or a `direction` hint.
- Within a station, any given `towards` and `direction` hint must only be present.
- Within a station, `towards` has highest precedence when matching, then the terminus direction hint, then geographic direction hints.
- Within a station, at least one non-T `direction` hint must be present to act as a fallback when matching on `towards` fails.

Except when a station has exactly one bus stop - then it must not have any `direction` or `towards` hints.

### Shapes

Use `railway=rail` for rail alignments between stations, optionally with `oneway=yes`. Rail alignments are required.

Bus shapes can also be generated based on data in the map.osm file, using standard highway tags (see [BUS routx profile](https://pypi.org/project/routx/#user-content-routxosmprofile)). Bus alignments are optional.


License
-------

_PolishTrainsGTFS_ is provided under the MIT license, included in the `LICENSE` file.
