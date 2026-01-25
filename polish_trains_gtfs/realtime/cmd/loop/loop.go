// SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"time"

	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/backoff"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/fact"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/match"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/schedules"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/source"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/util/http2"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/util/secret"
)

var (
	flagAlerts  = flag.Bool("alerts", false, "parse disruptions instead of operations")
	flagGTFS    = flag.String("gtfs", "polish_trains.zip", "path to GTFS Schedules feed")
	flagPeriod  = flag.Duration("period", 1*time.Minute, "how often to fetch latest data")
	flagVerbose = flag.Bool("verbose", false, "show DEBUG logging")
)

func main() {
	flag.Parse()
	if *flagVerbose {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}

	apikey, err := secret.FromEnvironment("PKP_PLK_APIKEY")
	if err != nil {
		log.Fatal(err)
	}

	slog.Info("Loading static schedules")
	static, err := schedules.LoadGTFSFromPath(*flagGTFS)
	if err != nil {
		log.Fatal(err)
	}

	b := backoff.Backoff{Period: *flagPeriod, MaxBackoffExponent: 6}
	for {
		b.Wait()
		b.StartRun()
		facts, stats, err := fetch(static, apikey)

		if httpErr, ok := err.(*http2.Error); ok && isTemporaryAPIFailure(httpErr) {
			b.EndRun(backoff.Failure)
			continue
		} else if err != nil {
			log.Fatal(err)
		}
		b.EndRun(backoff.Success)

		writeOutput(facts)
		slog.Info("Feed updated successfully", "facts", facts.TotalFacts(), "stats", stats)
	}
}

func fetch(static *schedules.Package, apikey string) (*fact.Container, match.Stats, error) {
	if *flagAlerts {
		return fetchAlerts(static, apikey)
	}
	return fetchUpdates(static, apikey)
}

func fetchAlerts(static *schedules.Package, apikey string) (*fact.Container, match.Stats, error) {
	var stats match.Stats

	slog.Debug("Fetching disruptions")
	real, err := source.FetchDisruptions(context.Background(), apikey, nil)
	if err != nil {
		return nil, stats, err
	}
	slog.Debug("Fetched disruptions ", "items", len(real.Disruptions))

	slog.Debug("Parsing alerts")
	facts := match.Alerts(real, static, &stats)
	slog.Debug("Parsed alerts", "facts", len(facts.Alerts), "stats", stats)

	return facts, stats, nil
}

func fetchUpdates(static *schedules.Package, apikey string) (*fact.Container, match.Stats, error) {
	var stats match.Stats

	slog.Debug("Fetching operations")
	real, err := source.FetchOperations(context.Background(), apikey, nil, source.NewPageFetchOptions())
	if err != nil {
		return nil, stats, err
	}
	slog.Debug("Fetched operations", "items", len(real.Trains))

	slog.Debug("Parsing trip updates")
	facts := match.TripUpdates(real, static, &stats)
	slog.Debug("Parsed trip updates", "facts", len(facts.TripUpdates), "stats", stats)

	return facts, stats, nil
}

func writeOutput(facts *fact.Container) {
	slog.Debug("Dumping GTFS-Realtime")
	err := facts.DumpGTFSFile("polish_trains.pb", fact.HumanReadable)
	if err != nil {
		log.Fatal(err)
	}

	slog.Debug("Dumping JSON")
	err = facts.DumpJSONFile("polish_trains.json", fact.HumanReadable)
	if err != nil {
		log.Fatal(err)
	}
}

func isTemporaryAPIFailure(err *http2.Error) bool {
	switch err.StatusCode {
	case 429, 500, 503:
		return true
	default:
		return false
	}
}
