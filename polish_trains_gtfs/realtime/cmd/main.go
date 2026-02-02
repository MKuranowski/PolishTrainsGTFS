// SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/alternative"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/backoff"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/fact"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/match"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/schedules"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/source"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/util/http2"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/util/secret"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/util/vpn"
)

var (
	flagAlerts      = flag.Bool("alerts", false, "parse disruptions instead of operations")
	flagAlternative = flag.Duration("alternative", 20*time.Minute, "when non-zero, fetch fresh schedules from API")
	flagGTFS        = flag.String("gtfs", "polish_trains.zip", "path to GTFS Schedule feed")
	flagLoop        = flag.Duration("loop", 0, "when non-zero, update the feed continuously with the given period")
	flagOutput      = flag.String("output", "polish_trains.pb", "path to output .pb file")
	flagReadable    = flag.Bool("readable", false, "dump output in human-readable format")
	flagVerbose     = flag.Bool("verbose", false, "show DEBUG logging")
	flagVpn         = flag.String("vpn", "", "when non-empty, route all traffic through VPN(s) set-up with a WireGuard config file or directory with such files")
)

var jsonOutput = ""
var altLookupReloader alternative.LookupReloader = alternative.NopLookupReloader{}
var client http2.Doer

func main() {
	flag.Parse()
	if *flagVerbose {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}
	initJsonOutput()

	apikey, err := secret.FromEnvironment("PKP_PLK_APIKEY")
	if err != nil {
		log.Fatal(err)
	}

	slog.Info("Loading static schedules")
	static, err := schedules.LoadGTFSFromPath(*flagGTFS)
	if err != nil {
		log.Fatal(err)
	}

	client = getHttpClient()
	if *flagAlternative != 0 {
		altLookupReloader = &alternative.TimeLimitedLookupReloader{
			Wrapped: alternative.UnconditionalLookupReloader{},
			Period:  *flagAlternative,
		}
	}

	if *flagLoop == 0 {
		totalFacts, stats, err := run(static, apikey)
		if err != nil {
			log.Fatal(err)
		}
		slog.Info("Feed updated successfully", "facts", totalFacts, "stats", stats)
	} else {
		b := backoff.Backoff{
			Period:                 *flagLoop,
			ExponentialBackoffBase: 30 * time.Second,
			MaxBackoffExponent:     6,
		}

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
	err := altLookupReloader.Reload(context.Background(), static, apikey, client)
	if err != nil {
		return 0, match.Stats{}, err
	}

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
	real, err := source.FetchDisruptions(context.Background(), apikey, client)
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
	real, err := source.FetchOperations(context.Background(), apikey, client, source.NewPageFetchOptions())
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
	err := facts.DumpGTFSFile(*flagOutput, *flagReadable)
	if err != nil {
		return fmt.Errorf("%s: %w", *flagOutput, err)
	}

	slog.Debug("Dumping JSON")
	err = facts.DumpJSONFile(jsonOutput, *flagReadable)
	if err != nil {
		return fmt.Errorf("%s: %w", jsonOutput, err)
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

func initJsonOutput() {
	dir, name := filepath.Split(*flagOutput)
	parts := strings.Split(name, ".")
	if len(parts) <= 1 {
		parts = append(parts, "json")
	} else {
		parts[len(parts)-1] = "json"
	}
	name = strings.Join(parts, ".")
	jsonOutput = dir + name
}

func getHttpClient() http2.Doer {
	var base http2.Doer
	if *flagVpn == "" {
		base = http.DefaultClient
	} else if !isDir(*flagVpn) {
		config, err := vpn.LoadWireguardConfigFromFile(*flagVpn)
		if err != nil {
			log.Fatalf("%s: %s", *flagVpn, err)
		}

		base, err = vpn.NewWireguardClient(config)
		if err != nil {
			log.Fatalf("%s: %s", *flagVpn, err)
		}
	} else {
		files, err := os.ReadDir(*flagVpn)
		if err != nil {
			log.Fatal(err)
		}

		vpns := make([]http2.Doer, 0, len(files))
		for _, file := range files {
			name := filepath.Join(*flagVpn, file.Name())
			if file.IsDir() || filepath.Ext(name) != ".conf" {
				continue
			}

			config, err := vpn.LoadWireguardConfigFromFile(name)
			if err != nil {
				log.Fatalf("%s: %s", name, err)
			}

			base, err = vpn.NewWireguardClient(config)
			if err != nil {
				log.Fatalf("%s: %s", name, err)
			}

			vpns = append(vpns, base)
		}

		if len(vpns) == 0 {
			log.Fatalf("%s: no WireGuard .conf files", *flagVpn)
		}
		base = http2.RandomDoer(vpns)
	}

	var rateLimit time.Duration
	if *flagLoop == 0 {
		rateLimit = 100 * time.Millisecond
	} else {
		rateLimit = time.Second
	}
	return http2.NewRateLimitedDoer(base, rateLimit)
}

func isDir(path string) bool {
	stat, err := os.Stat(path)
	if err != nil {
		return false
	}
	return stat.IsDir()
}
