package redimension

import (
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"

	"github.com/gomodule/redigo/redis"
)

type sItem struct {
	ID  string
	Pos []string
}

type Redimension struct {
	conn redis.Conn
	dim  int
	prec int
}

func NewRedimension(conn redis.Conn, dim, prec int) *Redimension {
	return &Redimension{
		conn: conn,
		dim:  dim,
		prec: prec,
	}
}

func (s *Redimension) checkDim(vars []uint32) error {
	if len(vars) != s.dim {
		return fmt.Errorf("please always use %v vars with this index", s.dim)
	}
	return nil
}

// encode Encode N variables into the bits-interleaved representation.
func (s *Redimension) encode(vars []uint32) string {
	comb := make([][]byte, s.prec)
	for i := range comb {
		comb[i] = make([]byte, s.dim)
	}
	for j, v := range vars {
		vbin := strconv.FormatUint(uint64(v), 2)
		if s.prec-len(vbin) > 0 {
			vbin = fmt.Sprintf("%s%s", strings.Repeat("0", s.prec-len(vbin)), vbin)
		}
		for i, b := range []byte(vbin) {
			comb[i][j] = b
		}
	}
	resBytes := make([]byte, s.prec*s.dim)
	index := 0
	for i := range comb {
		for j := range comb[i] {
			resBytes[index] = comb[i][j]
			index++
		}
	}
	x := big.Int{}
	x.SetString(string(resBytes), 10)
	res := x.Text(16)
	if s.prec*s.dim/4-len(res) > 0 {
		res = fmt.Sprintf("%s%s", strings.Repeat("0", s.prec*s.dim/4-len(res)), res)
	}
	return res
}

// elestring Encode an element coordinates and ID as the whole string to add
// into the sorted set.
func (s *Redimension) elestring(vars []uint32, id string) (string, error) {
	if err := s.checkDim(vars); err != nil {
		return "", err
	}
	ele := s.encode(vars)
	for _, v := range vars {
		ele += fmt.Sprintf(":%v", v)
	}
	ele += ":" + id
	return ele, nil
}

// Index Add a variable with associated data 'ID'
func (s *Redimension) Index(zkey, hkey, id string, vars []uint32) error {
	oldele, err := redis.String(s.conn.Do("HGET", hkey, id))
	if err != nil && err != redis.ErrNil {
		return err
	}
	ele, err := s.elestring(vars, id)
	if err != nil {
		return err
	}
	if len(oldele) != 0 {
		_ = s.conn.Send("ZREM", zkey, oldele)
	}
	_ = s.conn.Send("ZADD", zkey, 0, ele)
	_ = s.conn.Send("HSET", hkey, id, ele)
	return s.conn.Flush()
}

// BatchIndex Batch Add a variable with associated data 'ID'
func (s *Redimension) BatchIndex(zkey, hkey string, ids []string, vars [][]uint32) error {
	args := []interface{}{hkey}
	for _, id := range ids {
		args = append(args, id)
	}
	oldeles, err := redis.Strings(s.conn.Do("HMGET", args...))
	if err != nil && err != redis.ErrNil {
		return err
	}
	for i, id := range ids {
		ele, err := s.elestring(vars[i], id)
		if err != nil {
			return err
		}
		if len(oldeles[i]) != 0 {
			_ = s.conn.Send("ZREM", zkey, oldeles[i])
		}
		_ = s.conn.Send("ZADD", zkey, 0, ele)
		_ = s.conn.Send("HSET", hkey, id, ele)
	}
	return s.conn.Flush()
}

// UnIndex ZREM according to current position in the space and ID.
func (s *Redimension) UnIndex(zkey, hkey, id string, vars []uint32) error {
	ele, err := s.elestring(vars, id)
	if err != nil {
		return err
	}
	_ = s.conn.Send("ZREM", zkey, ele)
	_ = s.conn.Send("HDEL", hkey, id)
	return s.conn.Flush()
}

// UnIndexByID Unidex by just ID in case @hashkey is set to true in order to take
// an associated Redis hash with ID -> current indexed representation,
// so that the user can unindex easily.
func (s *Redimension) UnIndexByID(zkey, hkey, id string) error {
	ele, err := redis.String(s.conn.Do("HGET", hkey, id))
	if err != nil {
		return err
	}
	_ = s.conn.Send("ZREM", zkey, ele)
	_ = s.conn.Send("HDEL", hkey, id)
	return s.conn.Flush()
}

// BatchUnIndexByID batch Unidex by just ID in case @hashkey is set to true in order to take
// an associated Redis hash with ID -> current indexed representation,
// so that the user can unindex easily.
func (s *Redimension) BatchUnIndexByID(zkey, hkey string, ids []string) error {
	args := []interface{}{hkey}
	for _, id := range ids {
		args = append(args, id)
	}
	eles, err := redis.Strings(s.conn.Do("HMGET", args...))
	if err != nil {
		return err
	}
	_ = s.conn.Send("HDEL", args...)
	args = []interface{}{zkey}
	for _, ele := range eles {
		args = append(args, ele)
	}
	_ = s.conn.Send("ZREM", args...)
	return s.conn.Flush()
}

// Update Like #index but makes sure to remove the old index for the specified
// ID. Requires hash mapping enabled.
func (s *Redimension) Update(zkey, hkey, id string, vars []uint32) error {
	ele, err := s.elestring(vars, id)
	if err != nil {
		return err
	}
	oldele, err := redis.String(s.conn.Do("HGET", hkey, id))
	if err != nil {
		return err
	}
	_ = s.conn.Send("ZREM", zkey, oldele)
	_ = s.conn.Send("ZADD", zkey, 0, ele)
	_ = s.conn.Send("HSET", hkey, id, ele)
	return s.conn.Flush()
}

// GetPos 获取坐标
func (s *Redimension) GetPos(hkey, id string) ([]uint32, error) {
	ele, err := redis.String(s.conn.Do("HGET", hkey, id))
	if err != nil {
		return nil, err
	}
	item := strings.Split(ele, ":")
	if len(item) != 4 {
		return nil, fmt.Errorf("item invalid: %v", item)
	}
	x, _ := strconv.ParseUint(item[1], 10, 64)
	y, _ := strconv.ParseUint(item[2], 10, 64)
	return []uint32{uint32(x), uint32(y)}, nil
}

// GetPos 批量获取坐标
func (s *Redimension) BatchGetPos(hkey string, ids []string) ([][]uint32, error) {
	args := []interface{}{hkey}
	for _, id := range ids {
		args = append(args, id)
	}
	eles, err := redis.Strings(s.conn.Do("HMGET", args...))
	if err != nil {
		return nil, err
	}
	res := make([][]uint32, len(ids))
	for i, ele := range eles {
		item := strings.Split(ele, ":")
		if len(item) != 4 {
			return nil, fmt.Errorf("item invalid: %v", item)
		}
		x, _ := strconv.ParseUint(item[1], 10, 64)
		y, _ := strconv.ParseUint(item[2], 10, 64)
		res[i] = []uint32{uint32(x), uint32(y)}
	}
	return res, nil
}

// queryRaw exp is the exponent of two that gives the size of the squares
// we use in the range query. N times the exponent is the number
// of bits we unset and set to get the start and end points of the range.
func (s *Redimension) queryRaw(zkey string, vrange [][]uint32, exp int) ([]*sItem, error) {
	vstart := make([]uint32, len(vrange))
	vend := make([]uint32, len(vrange))
	// We start scaling our indexes in order to iterate all areas, so
	// that to move between N-dimensional areas we can just increment
	// vars.
	for i, vr := range vrange {
		vstart[i] = vr[0] / (1 << exp)
		vend[i] = vr[1] / (1 << exp)
	}
	// Visit all the sub-areas to cover our N-dim search region.
	var ranges [][]string
	vcurrent := make([]uint32, len(vstart))
	copy(vcurrent, vstart)
	notdone := true
	for notdone {
		// For each sub-region, encode all the start-end ranges
		// for each dimension.
		vrangeStart := make([]uint32, s.dim)
		vrangeEnd := make([]uint32, s.dim)
		for i := 0; i < s.dim; i++ {
			vrangeStart[i] = vcurrent[i] * (1 << exp)
			vrangeEnd[i] = vrangeStart[i] | ((1 << exp) - 1)
		}
		// Now we need to combine the ranges for each dimension
		// into a single lexicographcial query, so we turn
		// the ranges it into interleaved form.
		start := s.encode(vrangeStart)
		// Now that we have the start of the range, calculate the end
		// by replacing the specified number of bits from 0 to 1.
		end := s.encode(vrangeEnd)
		ranges = append(ranges, []string{fmt.Sprintf("[%v", start), fmt.Sprintf("[%v", end)})
		// Increment to loop in N dimensions in order to visit
		// all the sub-areas representing the N dimensional area to
		// query.
		for i := 0; i < s.dim; i++ {
			if vcurrent[i] != vend[i] {
				vcurrent[i] += 1
				break
			} else if i == s.dim-1 {
				// Visited everything!
				notdone = false
			} else {
				vcurrent[i] = vstart[i]
			}
		}
	}
	fmt.Printf("Lex query len: %v\n", len(ranges))
	// Perform the ZRANGEBYLEX queries to collect the results from the
	// defined ranges. Use pipelining to speedup.
	for _, v := range ranges {
		fmt.Printf("Lex query: %v\n", v)
		_ = s.conn.Send("zrangebylex", zkey, v[0], v[1])
	}
	if err := s.conn.Flush(); err != nil {
		return nil, err
	}
	var items []*sItem
	for range ranges {
		res, _ := redis.Strings(s.conn.Receive())
		fmt.Printf("res: %v\n", res)
		for _, item := range res {
			fields := strings.Split(item, ":")
			skip := false
			for i := 0; i < s.dim; i++ {
				x, _ := strconv.ParseUint(fields[i+1], 10, 64)
				if uint32(x) < vrange[i][0] || uint32(x) > vrange[i][1] {
					skip = true
					break
				}
			}
			if !skip {
				items = append(items, &sItem{
					ID:  fields[len(fields)-1],
					Pos: fields[1 : len(fields)-1],
				})
			}
		}
	}
	return items, nil
}

// Query Like query_raw, but before performing the query makes sure to order
// parameters so that x0 < x1 and y0 < y1 and so forth.
// Also calculates the exponent for the query_raw masking.
func (s *Redimension) Query(zkey string, vrange [][]uint32) ([]*sItem, error) {
	fmt.Printf("vrange: %+v", vrange)
	if len(vrange) != s.dim {
		return nil, fmt.Errorf("please always use %v vars with this index", s.dim)
	}
	delta := uint32(math.MaxUint32)
	deltas := make([]uint32, len(vrange))
	for i, vr := range vrange {
		if vr[0] > vr[1] {
			vr[1], vr[0] = vr[0], vr[1]
		}
		deltas[i] = (vr[1] - vr[0]) + 1
		if deltas[i] < delta {
			delta = deltas[i]
		}
	}
	exp := 1
	for delta > 2 {
		delta /= 2
		exp += 1
	}
	// If ranges for different dimensions are extremely different in span,
	// we may end with a too small exponent which will result in a very
	// big number of queries in order to be very selective. This is most
	// of the times not a good idea, so at the cost of querying larger
	// areas and filtering more, we scale 'exp' until we can serve this
	// request with less than 20 ZRANGEBYLEX commands.
	//
	// Note: the magic "20" depends on the number of items inside the
	// requested range, since it's a tradeoff with filtering items outside
	// the searched area. It is possible to improve the algorithm by using
	// ZLEXCOUNT to get the number of items.
	for {
		for i, vr := range vrange {
			deltas[i] = (vr[1] / (1 << exp)) - (vr[0] / (1 << exp)) + 1
		}
		x := uint32(1)
		for _, v := range deltas {
			x *= v
		}
		if x < 20 {
			break
		}
		exp += 1
	}
	fmt.Printf("exp: %v\n", exp)
	return s.queryRaw(zkey, vrange, exp)
}
