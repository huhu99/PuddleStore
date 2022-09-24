module puddlestore

go 1.17

require (
	github.com/go-zookeeper/zk v1.0.2
	github.com/google/uuid v1.1.2
	github.com/hashicorp/go-msgpack v1.1.5
	tapestry v0.0.0-00010101000000-000000000000
)

require (
	github.com/golang/protobuf v1.5.0 // indirect
	golang.org/x/net v0.0.0-20201224014010-6772e930b67b // indirect
	golang.org/x/sys v0.0.0-20201119102817-f84b799fce68 // indirect
	golang.org/x/text v0.3.3 // indirect
	google.golang.org/genproto v0.0.0-20200526211855-cb27e3aa2013 // indirect
	google.golang.org/grpc v1.35.0 // indirect
	google.golang.org/protobuf v1.27.1 // indirect
)

// Replace this
replace tapestry => ../tapestry-ta-impl // /path/to/your/tapestry/implementation/root/folder
