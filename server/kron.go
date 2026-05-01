package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	pb "github.com/Kobdik/kronos/kron/kronstub"
)

const B15_mask int64 = 1<<15 - 1

type Kronos struct {
	wg sync.WaitGroup
	pb.UnimplementedKronServer
	server  *grpc.Server
	bandCnt atomic.Int32
	cellCh  chan *pb.Cell
	signCh  chan os.Signal
	status  int8
	step    time.Duration
	ns      int32
	nt      int64
}

func (k *Kronos) Ready(ctx context.Context, req *pb.EmptyReq) (*pb.Cell, error) {
	var stat string = "*"
	switch k.status {
	//next series
	case 0:
		stat = "zero"
	case 1:
		stat = "wait"
	case 2:
		stat = "busy"
	}
	cell := pb.Cell{
		Cmd:  "calend",
		Day:  0,
		Keys: []string{stat},
		Val:  k.ns,
		Dms:  &k.nt,
	}
	return &cell, nil
}

func (k *Kronos) Band(req *pb.EmptyReq, stream grpc.ServerStreamingServer[pb.Cell]) error {
	var (
		cell *pb.Cell
		err  error = nil
		cnt  int
	)
	k.bandCnt.Add(1)
	defer k.bandCnt.Add(-1)

	fmt.Println("Band called at", time.Now().UnixMilli())
	cnt = 0
	for cell = range k.cellCh {
		err = stream.Send(cell)
		if err != nil {
			fmt.Printf("Client failed: %s\n", err)
			break
		}
		cnt++
		if cell.Day == 60 {
			break
		}
	}
	fmt.Printf("Total %d cells sent.\n", cnt)
	return err
}

func main() {
	fmt.Println("main started")
	// next start series time
	// tm = time.Now().Add(10 * time.Second)
	k := Kronos{
		cellCh: make(chan *pb.Cell, 128),
		signCh: make(chan os.Signal, 1),
		step:   100 * time.Millisecond,
	}
	signal.Notify(k.signCh, os.Interrupt)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	go k.startServer(7777)

	go k.waitBandReqs(ctx)

	select {
	case <-k.signCh:
		cancel()
		fmt.Println("Terminated!")
	case <-ctx.Done():
		fmt.Println("Context done.")
	}
	// release resources
	close(k.cellCh)
	k.server.GracefulStop()

	k.wg.Wait()
	fmt.Printf("Stream stopped with %d active workers\n", k.bandCnt.Load())
	fmt.Println("Done.")
}

func (k *Kronos) startServer(port int) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		fmt.Printf("failed to listen: %v\n", err)
		os.Exit(1)
	}
	k.server = grpc.NewServer()
	pb.RegisterKronServer(k.server, k)
	fmt.Printf("server listening at %v\n", lis.Addr())
	if err := k.server.Serve(lis); err != nil {
		fmt.Printf("failed to serve: %s\n", err)
	}
	fmt.Println("Server stopped")
}

func (k *Kronos) waitBandReqs(ctx context.Context) {
	var (
		ticker *time.Ticker
		tt     time.Time
		cont   bool = true
	)
	k.wg.Add(1)
	defer k.wg.Done()
	k.status = 1 // wait
	k.ns = 1     // band series
	k.nt = time.Now().Add(100 * k.step).UnixMilli()
	ticker = time.NewTicker(100 * k.step)
	for cont {
		select {
		case <-ctx.Done():
			cont = false
			fmt.Println("Band generator context done.")
			continue
		case tt = <-ticker.C:
			k.nt = tt.Add(100 * k.step).UnixMilli()
			go k.generateBand(ctx)
		}
	}
	ticker.Stop()
}

func (k *Kronos) generateBand(ctx context.Context) {
	var (
		cell   *pb.Cell
		cnt    int32
		day    int32 = 0
		dms    int64
		skp    int32 = 0
		ticker *time.Ticker
		ts     time.Time
		cont   bool = true
	)
	k.wg.Add(1)
	defer k.wg.Done()
	fmt.Printf("%d active stream, ns %d requested\n", cnt, k.ns)
	ticker = time.NewTicker(k.step)
	for cont {
		select {
		case <-ctx.Done():
			cont = false
			fmt.Println("Cell generator context done.")
			continue
		case ts = <-ticker.C:
			if skp < 20 {
				skp++
				// wait clients to connect
				if skp == 10 {
					k.status = 2 // busy
					fmt.Printf("Day %d, %d active streams, skp %d\n", day, cnt, skp)
					dms = ts.UnixMilli() & B15_mask
					cell = &pb.Cell{
						Cmd:  "calend",
						Day:  day,
						Keys: []string{"*", "c2025"},
						Val:  k.ns,
						Dms:  &dms,
					}
					cnt = k.bandCnt.Load()
					for range cnt {
						k.cellCh <- cell
					}
				}
				// 10 steps before generation
				continue
			}
			if day < 60 {
				day++
				// time marker in ms
				dms = ts.UnixMilli() & B15_mask
				cell = &pb.Cell{
					Cmd:  "calend",
					Day:  day,
					Keys: []string{"*", time.Date(2025, 8, int(day), 0, 0, 0, 0, time.UTC).Format("2006-01-02")},
					Val:  k.ns,
					Dms:  &dms,
				}
				if day%10 == 0 {
					fmt.Printf("Day %d, %s for %d active streams\n", day, cell.Keys[1], cnt)
				}
				cnt = k.bandCnt.Load()
				for range cnt {
					k.cellCh <- cell
				}
				continue
			}
			cont = false
		}
	}
	k.status = 1 // wait
	ticker.Stop()
	fmt.Printf("Cell generator %d series done, skp: %d cnt: %d\n", k.ns, skp, cnt)
	k.ns++ // num of cell series
}
