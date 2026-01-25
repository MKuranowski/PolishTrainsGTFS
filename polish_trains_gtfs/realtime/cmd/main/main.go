// SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"flag"
	"log"
	"log/slog"

	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/fact"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/match"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/schedules"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/source"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/util/secret"
)

var (
	flagAlerts = flag.Bool("alerts", false, "parse disruptions instead of operations")
)

func main() {
	flag.Parse()

	apikey, err := secret.FromEnvironment("PKP_PLK_APIKEY")
	if err != nil {
		log.Fatal(err)
	}

	slog.Info("Loading static schedules")
	static, err := schedules.LoadGTFSFromPath("polish_trains.zip")
	if err != nil {
		log.Fatal(err)
	}

	var facts *fact.Container
	var stats match.Stats
	if *flagAlerts {
		slog.Info("Fetching disruptions")
		real, err := source.FetchDisruptions(context.Background(), apikey, nil)
		if err != nil {
			log.Fatal(err)
		}
		slog.Info("Fetched disruptions ", "items", len(real.Disruptions))

		slog.Info("Parsing alerts")
		facts = match.Alerts(real, static, &stats)
		slog.Info("Parsed alerts", "facts", len(facts.Alerts), "stats", stats)
	} else {
		slog.Info("Fetching operations")
		real, err := source.FetchOperations(context.Background(), apikey, nil, source.NewPageFetchOptions())
		if err != nil {
			log.Fatal(err)
		}
		slog.Info("Fetched operations", "items", len(real.Trains))

		slog.Info("Parsing trip updates")
		facts = match.TripUpdates(real, static, &stats)
		slog.Info("Parsed trip updates", "facts", len(facts.TripUpdates), "stats", stats)
	}

	slog.Info("Dumping GTFS-Realtime")
	err = facts.DumpGTFSFile("polish_trains.pb", fact.HumanReadable)
	if err != nil {
		log.Fatal(err)
	}

	slog.Info("Dumping JSON")
	err = facts.DumpJSONFile("polish_trains.json", fact.HumanReadable)
	if err != nil {
		log.Fatal(err)
	}
}
