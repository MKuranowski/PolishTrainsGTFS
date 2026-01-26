// SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"

	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/backoff"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/fact"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/match"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/schedules"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/source"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/util/http2"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/util/secret"
)

var (
	flagAlerts   = flag.Bool("alerts", false, "parse disruptions instead of operations")
	flagGTFS     = flag.String("gtfs", "polish_trains.zip", "path to GTFS Schedules feed")
	flagLoop     = flag.Duration("loop", 0, "when non-zero, update the feed continuously with the given period")
	flagReadable = flag.Bool("readable", false, "dump output in human-readable format")
	flagVerbose  = flag.Bool("verbose", false, "show DEBUG logging")
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

	if *flagLoop == 0 {
		totalFacts, stats, err := run(static, apikey)
		if err != nil {
			log.Fatal(err)
		}
		slog.Info("Feed updated successfully", "facts", totalFacts, "stats", stats)
	} else {
		b := backoff.Backoff{Period: *flagLoop, MaxBackoffExponent: 6}
		for {
			b.Wait()
			b.StartRun()
			totalFacts, stats, err := run(static, apikey)
			if err != nil && canBackoff(err) {
				nextTry := b.EndRun(backoff.Failure)
				slog.Error("Feed update failure", "error", err, "next_try", nextTry)
			} else if err != nil {
				log.Fatal(err)
			} else {
				b.EndRun(backoff.Success)
				slog.Info("Feed updated successfully", "facts", totalFacts, "stats", stats)
			}
		}
	}
}

func run(static *schedules.Package, apikey string) (int, match.Stats, error) {
	facts, stats, err := fetch(static, apikey)
	if err != nil {
		return 0, stats, err
	}

	err = writeOutput(facts)
	return facts.TotalFacts(), stats, err
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

func writeOutput(facts *fact.Container) error {
	slog.Debug("Dumping GTFS-Realtime")
	err := facts.DumpGTFSFile("polish_trains.pb", *flagReadable)
	if err != nil {
		return fmt.Errorf("polish_trains.pb: %w", err)
	}

	slog.Debug("Dumping JSON")
	err = facts.DumpJSONFile("polish_trains.json", *flagReadable)
	if err != nil {
		return fmt.Errorf("polish_trains.json: %w", err)
	}

	return nil
}

func canBackoff(err error) bool {
	// Only backoff on 429, 500 i 503 HTTP errors
	if httpErr, ok := err.(*http2.Error); ok {
		switch httpErr.StatusCode {
		case 429, 500, 503:
			return true
		}
	}
	return false
}
