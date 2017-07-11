package cadets

import (
  "encoding/json"
  "errors"
  "io"
  "log"
  "os"
  "path"

  "github.com/ulikunitz/xz"
)

type TraceType int

const (
  TraceFile   TraceType = iota
  TraceStream
)

type TraceReader interface {
  Read() (evt *TraceEvent, err error)
  Close()
}

type TraceOps interface {
  TraceReader
  GetTraceSize() (size int64, err error)
  GetCurrentOffset() (offset int64, err error)
  BatchRead(maxSize int) (evt []*TraceEvent, err error)
}

type ReadCloseSeeker interface {
  io.Reader
  io.Closer
  io.Seeker
}

type TraceEvent struct {
  Event          string    "event"
  Time           uint64    "time"
  Pid            int       "pid"
  Ppid           int       "ppid"
  Tid            int       "tid"
  Uid            int       "uid"
  Exec           string    "exec"
  Cmdline        string    "cmdline"
  Upath1         string    "upath1"
  Upath2         string    "upath2"
  Fd             int       "fd"
  Flags          int       "flags"
  Fdpath         string    "fdpath"
  SubjProcUUID   string    "subjprocuuid"
  SubjThrUUID    string    "subjthruuid"
  ArgObjUUID1    string    "arg_objuuid1"
  ArgObjUUID2    string    "arg_objuuid2"
  RetObjUUID1    string    "ret_objuuid1"
  RetObjUUID2    string    "ret_ojjuuid2"
  Retval         int       "retval"
}

type Trace struct {
  name              string
  inputStream       io.ReadCloser
  readerStream      io.Reader
  decoder           *json.Decoder
  compressed        bool
  traceType         TraceType
}

func OpenTraceFile(name string) (tr TraceOps, err error) {
  trace := new(Trace)

  trace.inputStream, err = os.Open(name)
  trace.traceType = TraceFile
  trace.name = name

  if err != nil {
    log.Fatal(err)
    return nil, err
  }

  ext := path.Ext(name)
  if ext == ".xz" {
    trace.compressed = true
    trace.readerStream, err = xz.NewReader(trace.inputStream)
    if err != nil {
      trace.inputStream.Close()
      return nil, err
    }
  } else {
    trace.readerStream = trace.inputStream
  }

  err = trace.setDecoder()

  if err != nil {
    return nil, err
  }

  return trace, err
}

func (trace *Trace) GetTraceSize() (size int64, err error) {
  seekTrace, ok := trace.inputStream.(io.Seeker)
  if ok {
    current, _ := seekTrace.Seek(0, io.SeekCurrent)
    size, err = seekTrace.Seek(0, io.SeekEnd)
    seekTrace.Seek(current, io.SeekStart)
  } else { // Trace not a seek-able
    size = 0
    err = errors.New("Size only available for seek-able traces")
  }
  return
}

func (trace *Trace) GetCurrentOffset() (offset int64, err error) {
  seekTrace, ok := trace.inputStream.(io.Seeker)
  if ok {
    offset, err = seekTrace.Seek(0, io.SeekCurrent)
  } else {
    err = errors.New("Current offset only available for seek-able traces")
    offset = 0
  }
  return
}

func (trace *Trace) setDecoder() (err error) {
  trace.decoder = json.NewDecoder(trace.readerStream)

  // read array start
  t, err := trace.decoder.Token()
  if err != nil {
    return err
  }
  if t != json.Delim('[') {
    return errors.New("malformed json: start delimiter '[' not found for event array")
  }

  return nil
}

func (tr *Trace) Read() (evt *TraceEvent, err error) {
  if tr.decoder.More() {
    var evt TraceEvent

    if err := tr.decoder.Decode(&evt); err != nil {
      return nil, err
    }

    return &evt, nil
  }

  return nil, io.EOF
}

func (tr *Trace) Close() {
  tr.inputStream.Close()
}

// TODO(lc525): implement
func (tr *Trace) BatchRead(maxSize int) (evt []*TraceEvent, err error) {
/*  var evtList []*TraceEvent = make([]*TraceEvent, 0, maxSize)
 *
 *  if tr.decoder.More() {
 *    var evt TraceEvent
 *
 *    if err := tr.decoder.Decode(&evt); err != nil {
 *      return nil, err
 *    }
 *
 *    return &evt, nil
 *  }
 */
 return nil, errors.New("BatchRead: operation not implemented")
}
