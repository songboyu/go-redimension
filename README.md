go-redimension
===

Based on https://github.com/antirez/redimension

Redimension is a Redis multi-dimensional indexing and querying library
implemented in order to index items in N-dimensions, and then asking for elements
where each dimension is within the specified ranges.

Usage
===

    import "github.com/songboyu/go-redimension"


Currently the library can index only unsigned integers of the specified
precision. There are no precision limits, you can index integers composed
of as much bits as you like: you specify the number of bits for each dimension
in the constructor when creating a Redimension object.

An example usage in 2D is the following. Imagine you want to index persons
by salary and age:

    myindex = NewRedimension(redisCli, 2, 64)

We created a Redimension object specifying a Redis object that must respond
to the Redis commands. We specified we want 2D indexing, and 64 bits of
precision for each dimension. The first argument is the key name that will
represent the index as a sorted set.

Now we can add elements to our index.

    zkey, hkey := "test_zkey", "test_hkey"
	r.Index(zkey, hkey, "aa", []uint32{45, 120000})
	r.Index(zkey, hkey, "bb", []uint32{50, 110000})
	r.Index(zkey, hkey, "cc", []uint32{30, 125000})

The `index` method takes an array of integers representing the value of each
dimension for the item, and an item name that will be returned when asking
for ranges during the query stage.

Querying is simple. In the following query we ask for all the people with
age between 40 and 50, and salary between 100000 and 115000.

    vrange := make([][]uint32, 0)
	vrange = append(vrange, []uint32{40, 50})
	vrange = append(vrange, []uint32{100000, 115000})
	res, err := r.Query(zkey, vrange)

Ranges are **always** inclusive. Not a big problem since currently we can
only index integers so just increment/decrement to exclude a given value.
