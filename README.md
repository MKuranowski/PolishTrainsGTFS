PolishTrainsGTFS
================

> ⚠️🏗️ This project is still under construction. While the static generator works,
> and is mostly production-ready; the realtime generator is WIP and probably does not work.

Creates a single, GTFS and GTFS-Realtime feeds for all Polish trains coordinated by [PKP PLK](https://www.plk-sa.pl/)
(this excludes [WKD](https://wkd.com.pl/) or [UBB](https://www.ubb-online.com/)), including:

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


Data Caveats
------------

- Railway stop locations are pulled from [PLRailMap](https://github.com/mkuranowski/plrailmap),
    which sometimes misses position updates. File issues (or better yet, PRs) upstream.
- Bus stop locations are also pulled from [PLRailMap](https://github.com/mkuranowski/plrailmap),
    and are not available for all stations. File PRs upstream.
- Timed connections and carriage transfers are not provided - they're missing from the PKP PLK API.
- Platform and track info is missing at stops marked by PKP PLK as disembarking only.
- International trains are kinda messed up. Bus replacement services are sometimes missing
    (and remain as trains). Sometimes, only partial routes are available (OEDG, NEB). Rarely,
    the agency is also incorrect (NEB trains to/from Kostrzyn are reported as operated by PolRegio).


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


External Data
-------------

By providing the `-e`/`--external` flag to the static script, data for several routes
will be pulled directly from operator APIs. Agency-provided datasets sometimes have
higher-quality data, or PKP PLK API is straight up missing some routes
(like the Modlin Airport shuttle bus). This requires providing extra access credentials:

- `KM_APIKEY` - Koleje Mazowieckie XML schedules apikey.


License
-------

_PolishTrainsGTFS_ is provided under the MIT license, included in the `LICENSE` file.
