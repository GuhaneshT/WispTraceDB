package rollup



type AggregatedMetrics struct{
	BucketKey BucketKey
	Value Value
}

const (
	Magic uint32 = 0x57545331
	CurrentRollupVersion uint16 = 1
	headerSize        = 38
	recordFramingSize = 4+4
	
)

    // Header:
    // Magic           4 bytes
    // Version         2 bytes
    // WindowSize      8 bytes
    // RecordCount     8 bytes
    // MinWindowStart  8 bytes
    // MaxWindowStart  8 bytes


type RollupHeader struct {
	WindowSize         int64
	RecordCount        int64
	MinWindowStart     int64
	MaxWindowStart     int64
	
}

type Writer struct {
	aggregatedMetrics []AggregatedMetrics
}

func NewWriter() *Writer {
	return &Writer{aggregatedMetrics: make([]AggregatedMetrics, 0, 1024)}
}

func (w *Writer) Add(metric AggregatedMetrics) {
	w.aggregatedMetrics = append(w.aggregatedMetrics, metric)
}

func (w *Writer) Len() int {
	return len(w.aggregatedMetrics)
}



func RollupPath(dir string, rollupID uint64) string {
	return filepath.Join(dir, fmt.Sprintf("segment_%06d.seg", rollupID))
}

func CompositeKey(windowStart int64, model string) string {
	return fmt.Sprintf("%d||%s", windowStart, model)
}

func writeHeader(w *bufio.Writer, WindowSize int64, RecordCount int64, MinWindowStart, MaxWindowStart int64) error {
	var buf [headerSize]byte

	binary.LittleEndian.PutUint32(buf[0:4], Magic)
	binary.LittleEndian.PutUint16(buf[4:6], CurrentRollupVersion)
	binary.LittleEndian.PutUint64(buf[6:14], WindowSize)
	binary.LittleEndian.PutUint64(buf[14:22], RecordCount)
	binary.LittleEndian.PutUint64(buf[22:30], uint64(MinWindowStart))
	binary.LittleEndian.PutUint64(buf[30:38], uint64(MaxWindowStart))
	_, err := w.Write(buf[:])
	return err
}

func writeRecord(w *bufio.Writer, agg AggregatedMetrics) (int, error) {
	payload := wal.EncodeAggregatedMetrics(agg)

}

