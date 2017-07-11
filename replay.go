package cadets

import (
  "io"
  "log"
  "time"
  "errors"
  "sync"
  "sync/atomic"
  /*"fmt"*/
)

/* ---- Replay buffer ---- */

type BufferControl int

const (
  // set all buffer parameters using the ReplayConfig struct
  Manual    BufferControl = iota

  /*
  * Given a SpeedFactor, set buffer sizes and MinDelta so that it matches the
  * ingestion rate from the input trace file
  *
  * TODO(lc525): implement support for Auto
  */
  //Auto
)

type ReplayConfig struct {
  /*
  * How fast should the replay be when compared to realtime?
  * speedFactor == 1   -- realtime
  * speedFactor == 2   -- replay at twice the speed
  * speedFactor == 0.5 -- half the speed
  */
  SpeedFactor   float32

  /*
  * When BufCtlType is set to Auto, ignore the BufSizeOrder and MinDelta
  * settings below
  */
  BufCtlType    BufferControl

  /*
  * 2^BufSizeOrder is the number of events that should be buffered when
  * reading from the trace. This controls how much memory the replay uses, but
  * a too small BufSize could lead to timing inaccuracies in the replay.
  */
  BufSizeOrder  uint

  MaxConsecutiveDelta  uint64

  /*
  * Minimum time difference to consider making the events available at separate
  * times. Considering a starting event timestamp evt_start, all events with
  * timestamps t < evt_start + ChunkDeltaThresh will be replayed as if they would all
  * have timestamp evt_start.
  *
  * As trace times are in ns, a ChunkDeltaThresh of 1e9 represents 1 second
  */
  ChunkDeltaThresh      uint64

}

type ReplayBuffer struct {
  evts             []*TraceEvent
  canRead          *sync.Cond
  canWrite         *sync.Cond

  writerIx         uint64
  availEventIx     uint64
  readerIx         uint64
  szMask           uint64
  done             bool
}

func (buf *ReplayBuffer) write(evt *TraceEvent) (wix uint64) {
  buf.canWrite.L.Lock()
  wix = buf.writerIx
  atomic.AddUint64(&buf.writerIx, 1)
  for wix >= buf.readerIx + buf.szMask + 1 {
    buf.canWrite.Wait()
  }
  buf.canWrite.L.Unlock()
  buf.evts[wix & buf.szMask] = evt
  return
}

func (buf *ReplayBuffer) read() (evt *TraceEvent){
  buf.canRead.L.Lock()
  for buf.readerIx == buf.availEventIx - 1 && !buf.done {
    buf.canRead.Wait()
  }
  rix := buf.readerIx
  atomic.AddUint64(&buf.readerIx, 1)
  buf.canRead.L.Unlock()

  if buf.done {
    atomic.StoreUint64(&buf.readerIx, buf.availEventIx - 1)
    return nil
  }

  evt = buf.evts[rix & buf.szMask]

  /*
  * Don't signal on every single read, as that would wake the writer
  * goroutine too many times. However, signal if 5% of the buffer is
  * available
  */
  if buf.writerIx - rix >= uint64(float32(buf.szMask) * 0.05) {
    buf.canWrite.Signal()
  }

  return
}

func (buf *ReplayBuffer) readChunk() (evts []*TraceEvent){
  buf.canRead.L.Lock()
  for buf.readerIx == buf.availEventIx - 1 {
    buf.canRead.Wait()
  }
  aix := buf.availEventIx
  rix := buf.readerIx
  available := aix - rix
  atomic.AddUint64(&buf.readerIx, available)
  buf.canRead.L.Unlock()

  evts = make([]*TraceEvent, available, available)
  if aix & buf.szMask < buf.readerIx & buf.szMask {
    copy(evts, buf.evts[buf.readerIx & buf.szMask : ])
    evts = append(evts, buf.evts[0 : aix & buf.szMask]...)
  } else {
    copy(evts, buf.evts[buf.readerIx & buf.szMask : aix & buf.szMask])
  }

  buf.canWrite.Signal()

  return
}

func (buf *ReplayBuffer) reset() {
  buf.availEventIx = 0
  buf.readerIx = 0
  buf.writerIx = 0
  buf.done = false
}

// Get the approximate number of elements loaded into the buffer
// The function does not perform locking.
func (buf *ReplayBuffer) nrElements() uint64 {
  return buf.writerIx - buf.readerIx
}

// Get the approximate number of elements available for reading
// The function does not perform locking
func (buf *ReplayBuffer) nrUnreadElements() uint64 {
  return buf.availEventIx - buf.readerIx
}


/* ---- Trace replay ---- */

type ReplayStateCode int

const (
  ReplayNew                ReplayStateCode = iota
  ReplayDone
  ReplayTimerErr
  ReplayBufferErr
  ReplayLastChunkAvailable
  TraceDone
  TraceReaderErr
)

type ReplayState struct {
  //current
  Code   ReplayStateCode
  Err    error
  BufIx  uint64

  //cummulative
  ReplayImprecise_wasSlow  int
}

type TraceTimer struct {
  delay       uint64
  ringReadPos uint64
}

type TraceReplay struct {
  Buf          *ReplayBuffer
  State         ReplayState
  isBuffering  *sync.Cond

  trace     TraceOps
  options   ReplayConfig
  delays    []*TraceTimer
  m_delays  sync.Mutex

  lastChunk_StopTStamp  uint64
  chunkTStamp           uint64
  prevTStamp            uint64
  wix                   uint64
}

func NewTraceReplay(inputTrace TraceOps, opt ReplayConfig) (*TraceReplay) {
  b := ReplayBuffer{}
  var sz uint64 = 1 << opt.BufSizeOrder
  b.evts = make([]*TraceEvent, sz, sz)
  b.szMask = sz - 1
  b.canRead = sync.NewCond(&sync.Mutex{})
  b.canWrite = sync.NewCond(&sync.Mutex{})
  b.done = false
  var delays = make([]*TraceTimer, 0, 20)
  opt.SpeedFactor = 1.0 / opt.SpeedFactor

  trep := TraceReplay{&b,
                      ReplayState{ReplayNew, nil, 0, 0},
                      sync.NewCond(&sync.Mutex{}),
                      inputTrace,
                      opt,
                      delays,
                      sync.Mutex{},
                      0, 0, 0, 0}
  go trep.refillTraceBuffer(0)

  //wait for the buffer to fill a bit
  trep.isBuffering.L.Lock()
  for trep.Buf.writerIx < uint64(float64(trep.Buf.szMask) * 0.75) {
    trep.isBuffering.Wait()
  }
  trep.isBuffering.L.Unlock()

  return &trep
}

func (trr *TraceReplay) Read() (evt *TraceEvent, err error) {
  evt = trr.Buf.read()
  if evt == nil {
    trr.Buf.reset()
    err = io.EOF
  } else {
    err = nil
  }
  return
}

/*
* TODO(lc525):
* There is a potential for deadlock if the ring buffer is not large enough. This
* will happen when a single replay chunk is bigger than the ring buffer, meaning
* trep.Buf.write will block but at the same time the data in the ring will not
* be made accessible to any readers (the data is made "visible" one chunk at a
* time). We should detect this and change the replay parameters.
*
* The tricky bit is doing this while reading the trace as a stream. Perhaps
* measure the number of events in a chunk and don't let that grow beyond
* half of the capacity of the ring buffer?
*/
func (trep *TraceReplay) refillTraceBuffer(maxsize uint64) {
  var i uint64 = 0
  var first = false
  if trep.lastChunk_StopTStamp == 0 {
    first = true
  }

  for {
    if maxsize != 0 && i >= maxsize {
      break;
    }

    // read event from original trace provider
    evt, err := trep.trace.Read()
    if err != nil {
      if err != io.EOF {
        log.Println("trace error:", err)
      } else {
        // end of trace, make the remaining data available to the reader
        // when it comes the time
        trep.m_delays.Lock()
        trep.delays = append(trep.delays,
                             &TraceTimer{trep.prevTStamp - trep.lastChunk_StopTStamp,
                                         trep.wix})
        trep.State.Code = TraceDone
        trep.State.Err = nil
        trep.State.BufIx = trep.wix & trep.Buf.szMask
        trep.m_delays.Unlock()
        trep.isBuffering.Signal()
      }
      break;
    }

    if i == 0 && first {
      trep.lastChunk_StopTStamp = evt.Time
      trep.chunkTStamp = evt.Time
      trep.prevTStamp = evt.Time
      first = false
    } else {
      var deltaC uint64 = evt.Time - trep.chunkTStamp
      var deltaE uint64 = evt.Time - trep.prevTStamp
      if deltaC > trep.options.ChunkDeltaThresh ||
         deltaE > trep.options.MaxConsecutiveDelta {
        // we found the boundaries for a new replay chunk
        trep.m_delays.Lock()
        trep.delays = append(trep.delays,
                             &TraceTimer{trep.prevTStamp - trep.lastChunk_StopTStamp,
                                         trep.wix})
        trep.m_delays.Unlock()
        trep.lastChunk_StopTStamp = trep.prevTStamp
        trep.chunkTStamp = evt.Time

        trep.isBuffering.Signal()
      }
    }

    trep.wix = trep.Buf.write(evt)
    trep.prevTStamp = evt.Time
    i++
  }
}

/* This should be the only function besides TraceReplay.Start
 * (which only gets executed once) that should update availEventIx.
 */
func (trep *TraceReplay) onTimerUpdate() {
  if len(trep.delays) == 0 {
    return
  }

  trep.m_delays.Lock()
  trep.Buf.availEventIx = trep.delays[0].ringReadPos
  trep.delays = trep.delays[1:]

  if len(trep.delays) > 0 {
    trep.Buf.canRead.Signal()
    delayus := time.Duration(uint64(float64(trep.delays[0].delay) * float64(trep.options.SpeedFactor)))
    trep.m_delays.Unlock()
    time.AfterFunc(time.Nanosecond * delayus, trep.onTimerUpdate)
  } else {
    if trep.State.Code != TraceDone {
      trep.m_delays.Unlock()
      // there's still data in the trace to be added to the buffer, but
      // buffering was slow
      trep.State.ReplayImprecise_wasSlow++

      //wait for the buffer to fill a bit
      trep.isBuffering.L.Lock()
      for len(trep.delays) == 0 {
        trep.isBuffering.Wait()
      }
      trep.isBuffering.L.Unlock()
      trep.onTimerUpdate()

    } else {
      trep.m_delays.Unlock()
      // reads no longer block for writers but return nil as the trace replay
      // is done when readers catch up
      trep.Buf.done = true
      trep.Buf.canRead.Signal()

      trep.State.Err = nil
      trep.State.BufIx = trep.Buf.availEventIx & trep.Buf.szMask
      trep.State.Code = ReplayLastChunkAvailable
    }
  }
}

func (trep *TraceReplay) Start() (err error) {
  if len(trep.delays) == 0 {
    err = errors.New("No events available")
    trep.State.Err = err
    trep.State.Code = ReplayTimerErr
    return
  }

  // make the first events available for reading immediately
  trep.Buf.availEventIx = trep.delays[0].ringReadPos
  trep.delays = trep.delays[1:]

  if len(trep.delays) > 0 {
    delayus := time.Duration(uint64(float64(trep.delays[0].delay) * float64(trep.options.SpeedFactor)))
    time.AfterFunc(time.Nanosecond * delayus, trep.onTimerUpdate)
  }
  return nil
}

