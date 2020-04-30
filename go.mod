module github.com/bdware/go-datastore

go 1.14

replace github.com/ipfs/go-datastore => ./ // v0.4.4-bdw

require (
	github.com/google/uuid v1.1.1
	github.com/ipfs/go-datastore v0.4.4
	github.com/ipfs/go-ipfs-delay v0.0.0-20181109222059-70721b86a9a8
	github.com/jbenet/goprocess v0.0.0-20160826012719-b497e2f366b8
	go.uber.org/multierr v1.5.0
	golang.org/x/xerrors v0.0.0-20190717185122-a985d3407aa7
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15
)
