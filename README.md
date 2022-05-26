# lww-element-set

This is a re-implementation in Golang of the [lww-element-set](https://github.com/junjizhi/lww-element-set) written by [junjizhi](https://github.com/junjizhi).

I did this as an exercise to understand CRDT.

## Install dependencies

Run 

```sh
go mod tidy
go mod vendor

# OR
make tidy
```

## Testing

Run 

```sh
go test ./... -count=1 -v

# OR
make test
```

Remove the `-v` flag if you don't want verbose outputs.
