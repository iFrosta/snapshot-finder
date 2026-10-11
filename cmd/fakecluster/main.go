// Command fakecluster runs a fake Solana cluster (JSON-RPC + snapshot nodes)
// for trying snapshot-finder without touching mainnet. Archives contain
// deterministic bytes, so downloads can be verified (-verify).
//
//	fakecluster -nodes 3 -full-mb 64 -inc-mb 8 -rate-mb 0,200,50
//	snapshot-finder -r http://127.0.0.1:18899 --snapshot_path /tmp/remote --min_download_speed 100
package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"

	fc "github.com/iFrosta/snapshot-finder/internal/fakecluster"
)

func main() {
	nodes := flag.Int("nodes", 3, "number of snapshot nodes")
	rpcPort := flag.Int("rpc-port", 18899, "port of the fake JSON-RPC endpoint")
	slot := flag.Uint64("slot", 1_000_000, "current slot")
	fullMB := flag.Int64("full-mb", 64, "full snapshot size, MiB")
	incMB := flag.Int64("inc-mb", 8, "incremental snapshot size, MiB")
	rates := flag.String("rate-mb", "", "comma-separated per-node rate limit in MB/s (0 = unlimited)")
	rangeOK := flag.Bool("range", false, "honour HTTP Range (Agave does not)")
	verify := flag.String("verify", "", "verify a downloaded archive file and exit")
	flag.Parse()

	if *verify != "" {
		st, err := os.Stat(*verify)
		if err == nil {
			name := (*verify)[strings.LastIndexByte(*verify, '/')+1:]
			err = fc.Verify(*verify, name, st.Size())
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "verify:", err)
			os.Exit(1)
		}
		fmt.Println("ok", *verify)
		return
	}

	rateList := strings.Split(*rates, ",")
	var specs []fc.NodeSpec
	for i := 0; i < *nodes; i++ {
		s := fc.NodeSpec{FullSlot: *slot - 5000, IncSlot: *slot - 20 - uint64(i), FullSize: *fullMB << 20, IncSize: *incMB << 20, Range: *rangeOK}
		if i < len(rateList) {
			if r, err := strconv.ParseFloat(strings.TrimSpace(rateList[i]), 64); err == nil {
				s.RateBps = r * 1e6
			}
		}
		specs = append(specs, s)
	}
	c := fc.New(*slot, specs...)
	defer c.Close()

	// Expose the RPC on a fixed port as well.
	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", *rpcPort))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	go func() {
		_ = http.Serve(l, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			c.RPC.Config.Handler.ServeHTTP(w, r)
		}))
	}()
	fmt.Printf("fake cluster: rpc http://127.0.0.1:%d (also %s), slot %d\n", *rpcPort, c.RPC.URL, *slot)
	for i, n := range c.Nodes {
		fmt.Printf("  node %d %s rate=%.0fMB/s %s + %s\n", i, n.Addr, n.Spec.RateBps/1e6, n.FullName(), n.IncName())
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
}
