package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/Kobdik/kronos/kronc/kronstub"
)

const B15_mask int64 = 1<<15 - 1

type Kronc struct {
	wg sync.WaitGroup
	pb.KronClient
	bandCnt atomic.Int32
	cellCh  chan *pb.Cell
	signCh  chan os.Signal
	status  int8
	step    time.Duration
	ns      int32
	nt      int64
}

func (k *Kronc) Ready(ctx context.Context, req *pb.EmptyReq) (*pb.Cell, error) {
	// td := req.GetDur()
	cell := pb.Cell{
		Cmd:  "calend",
		Day:  0,
		Keys: []string{"*", "c2025"},
		Val:  k.ns,
	}
	return &cell, nil
}

func main() {
	fmt.Println("client started")
	// next start series time
	// tm = time.Now().Add(10 * time.Second)
	k := Kronc{
		cellCh: make(chan *pb.Cell, 128),
		signCh: make(chan os.Signal, 1),
		step:   100 * time.Millisecond,
	}
	signal.Notify(k.signCh, os.Interrupt)

	conn, err := k.createClient(7777)
	if err != nil {
		fmt.Printf("can't create grpc connection: %s\n", err)
		os.Exit(1)
		return
	}
	defer conn.Close()
	k.KronClient = pb.NewKronClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// go k.sendBandReqs(ctx)
	var (
		ticker *time.Ticker
		dur    time.Duration
		cont   bool = true
	)
	dur, err = k.waitBandReady(ctx)
	if err != nil {
		os.Exit(1)
		return
	}
	time.Sleep(dur - 10*time.Millisecond)
	ticker = time.NewTicker(100 * k.step)
	for cont {
		select {
		case <-k.signCh:
			fmt.Println("Terminated!")
			cont = false
		case <-ctx.Done():
			fmt.Println("Context done.")
			cont = false
		case <-ticker.C:
			dur, err = k.waitBandReady(ctx)
			if err != nil {
				cont = false
				continue
			}
			time.AfterFunc(dur, func() {
				fmt.Printf("request Ready after %d ms\n", dur.Milliseconds())
				k.bandReqs(ctx)
			})
		}
	}

	k.wg.Wait()
	fmt.Println("Client stopped")
}

func (k *Kronc) createClient(port int) (*grpc.ClientConn, error) {
	var (
		opts []grpc.DialOption
	)
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	// Устанавливаем соединение с сервером
	return grpc.NewClient(fmt.Sprintf(":%d", port), opts...)
}

func (k *Kronc) waitBandReady(ctx context.Context) (time.Duration, error) {
	var (
		cell *pb.Cell
		sub  int64
		dur  time.Duration
		err  error
	)
	cell, err = k.KronClient.Ready(ctx, &pb.EmptyReq{})
	if err != nil {
		fmt.Printf("call of Ready failed: %s\n", err)
		return 0, err
	}
	k.ns = cell.Val
	k.nt = *cell.Dms
	fmt.Printf("%d %v\n", time.Now().UnixMilli(), cell)
	sub = *cell.Dms - time.Now().UnixMilli()
	fmt.Printf("request Band after %v ms\n", sub)
	dur = time.Duration(sub) * time.Millisecond
	return dur, nil
}

func (k *Kronc) bandReqs(ctx context.Context) error {
	var (
		stream grpc.ServerStreamingClient[pb.Cell]
		cell   *pb.Cell
		err    error
		cont   bool = true
	)
	stream, err = k.KronClient.Band(ctx, &pb.EmptyReq{})
	if err != nil {
		fmt.Printf("call of Band failed: %s\n", err)
		return err
	}
	k.wg.Add(1)
	defer k.wg.Done()
	fmt.Println("call of Band at", time.Now().UnixMilli())
	for cont {
		select {
		case <-ctx.Done():
			cont = false
		default:
			fmt.Print("read stream..")
			cell, err = stream.Recv()
			if err == io.EOF {
				fmt.Println("done with OK")
				cont = false
				continue
			}
			if err != nil {
				fmt.Printf("problem: %s\n", err)
				continue
			}
			fmt.Printf("ns: %d, day:%d, %s, %d -> %d\n", cell.Val, cell.Day, cell.Keys[1],
				*cell.Dms, time.Now().UnixMilli()&B15_mask)
			time.Sleep(5 * time.Millisecond)
		}
	}

	return nil
}
